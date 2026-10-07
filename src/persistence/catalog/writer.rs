//! Single publisher, packed pages and a reconstructible retirement queue.
use super::arena::{Arena, Marks};
use super::page::{Link, Node, PageId};
use super::reader::Reader;
use super::{Counters, Root};
use anyhow::{Context, Result, ensure};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
// An edited path may also replace either adjacent sibling at each depth.
pub(super) const MAX_RETIRED: usize = 3 * super::page::MAX_DEPTH * (super::tree::MAX_EDITS + 1);
pub(super) struct Writer {
    pub reader: Arc<Reader>,
    pub arena: Arena,
    retired: File,
    retired_cursor: u64,
    pub pending_pages: u64,
    pub counters: Counters,
    pub generation: u64,
    pub needs_reopen: bool,
    pub prepared: Option<Root>,
}
impl Writer {
    pub fn open(reader: Arc<Reader>, root: Root) -> Result<Self> {
        let arena = Arena::open(&reader.path, reader.pool.clone())?;
        let retired = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(reader.path.join("RETIRED"))?;
        Ok(Self {
            reader,
            arena,
            retired,
            retired_cursor: 0,
            pending_pages: 0,
            counters: Counters::default(),
            generation: root.generation,
            needs_reopen: false,
            prepared: None,
        })
    }
    /// Append each chunk's obsolete published pages. Private intermediate pages
    /// are reclaimed separately; leases protect only previously published roots.
    pub fn record_retired(&mut self, tx: &Transaction) -> Result<()> {
        let mut bytes = Vec::with_capacity(28 + tx.retired.len() * 24);
        bytes.extend_from_slice(b"CHMRET03");
        bytes.extend_from_slice(&tx.generation.to_le_bytes());
        bytes.extend_from_slice(&(tx.retired.len() as u64).to_le_bytes());
        for id in &tx.retired {
            for value in [id.generation, id.slot, id.address] {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        let bytes = with_crc(bytes);
        self.retired
            .write_all_at(&bytes, self.retired.metadata()?.len())?;

        self.counters.written_bytes += bytes.len() as u64;
        self.pending_pages += tx.retired.len() as u64;
        Ok(())
    }
    /// One durability barrier for all chunks, before returning the proposed root.
    /// Arena writer eviction may still sync a pack early to bound open handles.
    pub fn sync_pages(&mut self) -> Result<()> {
        let _timer = crate::diagnostics::publication::stage(
            crate::diagnostics::publication::Stage::CatalogSync,
        );

        self.arena.sync(&mut self.counters)?;

        self.retired.sync_all()?;
        self.counters.syncs += 1;
        Ok(())
    }
    /// Only pending retirement records are inspected. A failed GC poisons the
    /// Catalog until reopen rebuilds its disposable allocation state.
    pub fn collect(&mut self, committed: u64, oldest: u64) -> Result<()> {
        let end = self.retired.metadata()?.len();
        let start = self.retired_cursor;
        if end == 0 {
            return Ok(());
        }
        while self.retired_cursor < end {
            let mut header = [0; 24];
            self.retired
                .read_exact_at(&mut header, self.retired_cursor)?;
            ensure!(&header[..8] == b"CHMRET03", "retirement format");
            let generation = number(&header, 8);
            let count = usize::try_from(number(&header, 16))?;
            ensure!(count <= MAX_RETIRED, "retirement limit");
            let len = 28 + count * 24;
            ensure!(
                end - self.retired_cursor >= len as u64,
                "truncated retirement record"
            );
            self.counters.retirement_logs += 1;
            let mut bytes = vec![0; len];
            self.retired
                .read_exact_at(&mut bytes, self.retired_cursor)?;
            let payload = checked_payload(&bytes)?;
            ensure!(
                generation > 0 && generation <= committed,
                "uncommitted retirement record"
            );
            if generation > oldest {
                break;
            }
            for i in 0..count {
                let at = 24 + i * 24;
                ensure!(
                    number(payload, at) > 0
                        && number(payload, at) < generation
                        && number(payload, at + 8) > 0
                        && number(payload, at + 16) > 0
                        && number(payload, at + 16) <= self.arena.slots,
                    "invalid retired page"
                );
            }
            for i in 0..count {
                let at = 24 + i * 24;
                let id = PageId {
                    generation: number(payload, at),
                    slot: number(payload, at + 8),
                    address: number(payload, at + 16),
                };
                self.arena.release(id, &mut self.counters)?;

                self.counters.reclaimed_pages += 1;
            }
            self.pending_pages = self
                .pending_pages
                .checked_sub(count as u64)
                .context("retirement count mismatch")?;
            self.retired_cursor += len as u64;
        }
        if self.retired_cursor == start {
            return Ok(());
        }
        if self.retired_cursor == end {
            self.retired.set_len(0)?;

            self.retired_cursor = 0;
        } else if self.retired_cursor >= 16 * 1024
            && self.retired_cursor >= end - self.retired_cursor
        {
            // Moving leases may keep a nonempty tail forever. Bound the file
            // by twice the pending tail plus 16 KiB, not publication history.
            let remaining = end - self.retired_cursor;
            let mut buffer = [0; 16 * 1024];
            let mut at = 0;
            while at < remaining {
                let size = (remaining - at).min(buffer.len() as u64) as usize;
                self.retired
                    .read_exact_at(&mut buffer[..size], self.retired_cursor + at)?;
                self.retired.write_all_at(&buffer[..size], at)?;
                self.counters.written_bytes += size as u64;

                at += size as u64;
            }
            self.retired.set_len(remaining)?;
            self.retired_cursor = 0;
        }
        self.arena.finish_gc(&mut self.counters)
    }

    pub fn recover(&mut self, marks: &mut Marks) -> Result<()> {
        self.arena.rebuild(marks, &mut self.counters)?;

        self.retired.set_len(0)?;
        self.retired_cursor = 0;
        self.pending_pages = 0;
        Ok(())
    }
}
pub(super) struct Transaction<'a> {
    pub generation: u64,
    next_slot: u64,
    retired: Vec<PageId>,
    pub added: u64,
    pub removed: u64,
    base_generation: u64,
    pages: &'a mut super::private::Pages,
}
impl<'a> Transaction<'a> {
    pub fn new(
        generation: u64,
        base_generation: u64,
        pages: &'a mut super::private::Pages,
    ) -> Self {
        Self {
            generation,
            next_slot: 1,
            retired: Vec::new(),
            added: 0,
            removed: 0,
            base_generation,
            pages,
        }
    }
    pub fn write(&mut self, writer: &mut Writer, node: Node) -> Result<Link> {
        let address = writer.arena.allocate(&mut writer.counters)?;
        let id = PageId {
            generation: self.generation,
            slot: self.next_slot,
            address,
        };
        self.next_slot = self
            .next_slot
            .checked_add(1)
            .context("catalog page id overflow")?;
        let link = node.summary(id)?;
        self.pages.write(writer, link, node)?;
        Ok(link)
    }

    pub fn read(&self, writer: &Writer, link: Link) -> Result<Node> {
        match self.pages.read(link)? {
            Some(node) => Ok(node),
            None => Ok(writer.reader.read(link, None)?.into_node()),
        }
    }

    fn retire(&mut self, id: PageId) -> Result<()> {
        ensure!(
            id.generation < self.generation,
            "retire published catalog page"
        );
        ensure!(
            self.retired.len() < MAX_RETIRED,
            "catalog retirement work limit"
        );
        self.retired.push(id);
        Ok(())
    }
    /// No root from this prepare has escaped. Discard its obsolete pages now,
    /// whether made by this chunk or a previous one; only published pages retire.
    pub fn discard(&mut self, writer: &mut Writer, id: PageId) -> Result<()> {
        if id.generation > self.base_generation {
            ensure!(
                id.generation <= self.generation
                    && id.slot > 0
                    && (id.generation != self.generation || id.slot < self.next_slot),
                "discard private catalog page"
            );
            if self.pages.remove(id) {
                // This address was allocated exclusively to an unwritten page.
                // Like recovery-derived FREE entries it has no on-disk identity.
                writer.arena.release(
                    PageId {
                        generation: 0,
                        slot: 0,
                        address: id.address,
                    },
                    &mut writer.counters,
                )?;
            } else {
                writer.arena.release(id, &mut writer.counters)?;
            }
            writer.counters.reclaimed_pages += 1;

            Ok(())
        } else {
            self.retire(id)
        }
    }
}
fn number(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("bounded record field"))
}
fn with_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    bytes
}
fn checked_payload(bytes: &[u8]) -> Result<&[u8]> {
    ensure!(bytes.len() >= 4, "truncated CRC record");
    let at = bytes.len() - 4;
    ensure!(
        crc32c::crc32c(&bytes[..at]) == u32::from_le_bytes(bytes[at..].try_into()?),
        "record CRC"
    );
    Ok(&bytes[..at])
}
