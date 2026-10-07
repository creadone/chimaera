//! Bounded container handles and a disposable on-disk free stack.
//! Reopen reconstructs allocation from the verified durable tree, never FREE.
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};

use super::Counters;
use super::page::{PAGE, PageId};
use crate::persistence::files::{FilePermit, FilePermitPool};

pub(super) const PACK_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const PACK_SLOTS: u64 = PACK_BYTES / PAGE as u64;
const MAX_HANDLES: usize = 8;
const FREE_ENTRY: u64 = 28;

struct Writer {
    pack: u64,
    file: File,
    dirty: bool,
    _permit: FilePermit,
}

pub(super) struct Arena {
    path: PathBuf,
    free: File,
    pub(super) free_pages: u64,
    pub(super) slots: u64,
    writers: Vec<Writer>,
    directory_dirty: bool,
    pool: FilePermitPool,
}

impl Arena {
    pub(super) fn close_handles(&mut self) {
        self.writers.clear();
    }
    pub(super) fn open(root: &Path, pool: FilePermitPool) -> Result<Self> {
        let path = root.join("pages");
        let mut slots = 0;
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().context("container name")?;
            let index: u64 = name
                .strip_suffix(".pack")
                .context("container suffix")?
                .parse()?;
            ensure!(
                name == format!("{index:08}.pack"),
                "noncanonical container name"
            );
            let meta = entry.metadata()?;
            ensure!(
                meta.is_file() && meta.len() <= PACK_BYTES,
                "container length/type"
            );
            let end = index
                .checked_mul(PACK_SLOTS)
                .and_then(|base| base.checked_add(meta.len().div_ceil(PAGE as u64)))
                .context("container address overflow")?;
            slots = slots.max(end);
        }
        // Interrupted removal of unreachable trailing containers may leave
        // gaps. Missing *reachable* containers are rejected by tree verification.
        let free = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("FREE"))?;
        Ok(Self {
            path,
            free,
            free_pages: 0,
            slots,
            writers: Vec::with_capacity(MAX_HANDLES),
            directory_dirty: false,
            pool,
        })
    }

    pub(super) fn location(&self, address: u64) -> Result<(PathBuf, u64)> {
        ensure!(
            address > 0 && address <= self.slots,
            "page address outside containers"
        );
        Ok((
            self.pack_path((address - 1) / PACK_SLOTS),
            (address - 1) % PACK_SLOTS * PAGE as u64,
        ))
    }

    fn pack_path(&self, index: u64) -> PathBuf {
        self.path.join(format!("{index:08}.pack"))
    }

    pub(super) fn allocate(&mut self, counters: &mut Counters) -> Result<u64> {
        if self.free_pages == 0 {
            self.slots = self.slots.checked_add(1).context("page address overflow")?;
            return Ok(self.slots);
        }
        let mut entry = [0; FREE_ENTRY as usize];
        self.free
            .read_exact_at(&mut entry, (self.free_pages - 1) * FREE_ENTRY)?;
        ensure!(
            crc32c::crc32c(&entry[..24]) == u32::from_le_bytes(entry[24..].try_into()?),
            "free entry CRC"
        );
        let generation = number(&entry, 0);
        let slot = number(&entry, 8);
        let address = number(&entry, 16);
        let (path, offset) = self.location(address)?;
        if generation != 0 {
            // Reject a duplicate/stale free entry before it could overwrite a
            // page already reused by this or a previous publication.
            let mut header = [0; 24];
            let _permit = self.pool.acquire()?;
            File::open(path)?.read_exact_at(&mut header, offset)?;
            ensure!(
                &header[..8] == b"CHMCAT05"
                    && number(&header, 8) == generation
                    && number(&header, 16) == slot,
                "stale free entry"
            );
        }
        self.free_pages -= 1;

        counters.reused_pages += 1;
        Ok(address)
    }

    pub(super) fn release(&mut self, page: PageId, counters: &mut Counters) -> Result<()> {
        ensure!(
            page.address > 0 && page.address <= self.slots && self.free_pages < self.slots,
            "free address/count"
        );
        let mut entry = [0; FREE_ENTRY as usize];
        entry[..8].copy_from_slice(&page.generation.to_le_bytes());
        entry[8..16].copy_from_slice(&page.slot.to_le_bytes());
        entry[16..24].copy_from_slice(&page.address.to_le_bytes());
        let crc = crc32c::crc32c(&entry[..24]);
        entry[24..].copy_from_slice(&crc.to_le_bytes());
        self.free
            .write_all_at(&entry, self.free_pages * FREE_ENTRY)?;
        counters.written_bytes += FREE_ENTRY;
        self.free_pages += 1;
        Ok(())
    }

    pub(super) fn write(
        &mut self,
        address: u64,
        bytes: &[u8; PAGE],
        counters: &mut Counters,
    ) -> Result<()> {
        ensure!(address > 0 && address <= self.slots, "write address");
        let pack = (address - 1) / PACK_SLOTS;
        let index = if let Some(index) = self.writers.iter().position(|w| w.pack == pack) {
            index
        } else {
            if self.writers.len() == MAX_HANDLES.min((self.pool.capacity() / 4).max(1)) {
                let writer = self.writers.remove(0);
                sync_writer(&writer, counters)?;
            }
            let path = self.pack_path(pack);
            if !path.try_exists()? {
                self.directory_dirty = true;
            }
            let permit = self.pool.acquire()?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            self.writers.push(Writer {
                pack,
                file,
                dirty: false,
                _permit: permit,
            });
            counters.pack_opens += 1;
            self.writers.len() - 1
        };
        let writer = &mut self.writers[index];
        writer.dirty = true;

        writer
            .file
            .write_all_at(bytes, (address - 1) % PACK_SLOTS * PAGE as u64)?;

        counters.writes += 1;
        counters.written_bytes += PAGE as u64;
        Ok(())
    }

    pub(super) fn sync(&mut self, counters: &mut Counters) -> Result<()> {
        for writer in &mut self.writers {
            sync_writer(writer, counters)?;
            writer.dirty = false;
        }
        if self.directory_dirty {
            File::open(&self.path)?.sync_all()?;
            counters.syncs += 1;
            self.directory_dirty = false;
        }
        if self.pool.capacity() < 16 {
            self.writers.clear();
        }

        Ok(())
    }

    pub(super) fn finish_gc(&mut self, counters: &mut Counters) -> Result<()> {
        // FREE is disposable after a crash; truncation bounds its physical
        // size by free capacity, with no durable free-list protocol to replay.
        self.free.set_len(self.free_pages * FREE_ENTRY)?;
        if self.free_pages == self.slots && self.slots != 0 {
            self.writers.clear();
            for entry in fs::read_dir(&self.path)? {
                fs::remove_file(entry?.path())?;
            }
            File::open(&self.path)?.sync_all()?;
            counters.syncs += 1;
            self.free.set_len(0)?;
            self.slots = 0;
            self.free_pages = 0;
        }
        Ok(())
    }

    /// Only after tree verification and the ROOT durability barrier.
    pub(super) fn rebuild(&mut self, marks: &mut Marks, counters: &mut Counters) -> Result<()> {
        ensure!(self.writers.is_empty(), "recovery with open writers");
        let mut slots = self.slots;
        while slots > 0 && !marks.contains(slots)? {
            slots -= 1;
        }
        let keep_packs = slots.div_ceil(PACK_SLOTS);
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let index: u64 = entry
                .file_name()
                .to_string_lossy()
                .trim_end_matches(".pack")
                .parse()?;
            if index >= keep_packs {
                fs::remove_file(entry.path())?;
            }
        }
        if slots != 0 {
            let file = OpenOptions::new()
                .write(true)
                .open(self.pack_path(keep_packs - 1))?;
            file.set_len(((slots - 1) % PACK_SLOTS + 1) * PAGE as u64)?;
            file.sync_all()?;
            counters.syncs += 1;
        }
        File::open(&self.path)?.sync_all()?;
        counters.syncs += 1;
        self.slots = slots;
        self.free.set_len(0)?;
        self.free_pages = 0;
        for address in 1..=slots {
            if !marks.contains(address)? {
                // Zero generation means derived from a fresh verified bitmap,
                // including incomplete/uncommitted pages whose header is bad.
                self.release(
                    PageId {
                        generation: 0,
                        slot: 0,
                        address,
                    },
                    counters,
                )?;
            }
        }
        Ok(())
    }
}

fn sync_writer(writer: &Writer, counters: &mut Counters) -> Result<()> {
    if writer.dirty {
        writer.file.sync_all()?;
        counters.syncs += 1;
    }
    Ok(())
}

fn number(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("fixed arena field"))
}

/// Disk bitmap with exactly one 4 KiB resident block. Dropped on every exit;
/// stale files from abrupt exits are truncated on the next verified attempt.
pub(super) struct Marks {
    path: PathBuf,
    file: File,
    block: [u8; 4096],
    loaded: Option<u64>,
    dirty: bool,
    slots: u64,
}
impl Marks {
    pub(super) fn new(root: &Path, slots: u64) -> Result<Self> {
        let path = root.join("REACHABLE.tmp");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.set_len(slots.div_ceil(8).div_ceil(4096) * 4096)?;
        Ok(Self {
            path,
            file,
            block: [0; 4096],
            loaded: None,
            dirty: false,
            slots,
        })
    }
    fn locate(&mut self, address: u64) -> Result<(usize, u8)> {
        ensure!(address > 0 && address <= self.slots, "bitmap address");
        let byte = (address - 1) / 8;
        let block = byte / 4096;
        if self.loaded != Some(block) {
            if self.dirty {
                self.file
                    .write_all_at(&self.block, self.loaded.context("bitmap block")? * 4096)?;
            }
            self.file.read_exact_at(&mut self.block, block * 4096)?;
            self.loaded = Some(block);
            self.dirty = false;
        }
        Ok(((byte % 4096) as usize, 1 << ((address - 1) % 8)))
    }
    pub(super) fn insert(&mut self, address: u64) -> Result<()> {
        let (at, bit) = self.locate(address)?;
        ensure!(self.block[at] & bit == 0, "aliased physical page in tree");
        self.block[at] |= bit;
        self.dirty = true;
        Ok(())
    }
    pub(super) fn contains(&mut self, address: u64) -> Result<bool> {
        let (at, bit) = self.locate(address)?;
        Ok(self.block[at] & bit != 0)
    }
}
impl Drop for Marks {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
