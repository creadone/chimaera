//! Durable identity of the current WAL. Replaced only on creation/rotation,
//! before the new writer can acknowledge anything. One fixed-size record.

use super::{put_u16, put_u32, put_u64, read_u16, read_u32, read_u64, sync_directory};
use crate::error::{Error, Result, corrupt};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"TSWHED01";
const SIZE: usize = 64;

pub(super) struct Head {
    pub id: u64,
    pub first_commit: u64,
}

impl Head {
    pub fn load(directory: &Path, uuid: [u8; 16]) -> Result<Self> {
        let mut file = File::open(directory.join("HEAD")).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                corrupt("required WAL head is missing")
            } else {
                Error::Io(error)
            }
        })?;
        if file.metadata()?.len() != SIZE as u64 {
            return Err(corrupt("invalid WAL head length"));
        }
        let mut bytes = [0_u8; SIZE];
        file.read_exact(&mut bytes)?;
        if &bytes[..8] != MAGIC
            || read_u16(&bytes, 8) != 1
            || read_u16(&bytes, 10) != SIZE as u16
            || bytes[12..28] != uuid
            || bytes[44..60] != [0; 16]
            || read_u32(&bytes, 60) != crc32c::crc32c(&bytes[..60])
            || read_u64(&bytes, 36) == 0
        {
            return Err(corrupt("invalid WAL head"));
        }
        Ok(Self {
            id: read_u64(&bytes, 28),
            first_commit: read_u64(&bytes, 36),
        })
    }

    pub fn persist(&self, directory: &Path, uuid: [u8; 16]) -> Result<()> {
        let mut bytes = [0_u8; SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        put_u16(&mut bytes, 8, 1);
        put_u16(&mut bytes, 10, SIZE as u16);
        bytes[12..28].copy_from_slice(&uuid);
        put_u64(&mut bytes, 28, self.id);
        put_u64(&mut bytes, 36, self.first_commit);
        let checksum = crc32c::crc32c(&bytes[..60]);
        put_u32(&mut bytes, 60, checksum);
        // A single writer owns HEAD.tmp; a crash may leave it, but never makes it
        // authoritative. The next publication overwrites this bounded scratch.
        let temporary = directory.join("HEAD.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;

        file.sync_all()?;

        fs::rename(&temporary, directory.join("HEAD"))?;

        sync_directory(directory)?;

        Ok(())
    }
}

/// Only for a newly created database or a complete, WAL-free snapshot.
pub(crate) fn initialize_empty(directory: &Path, uuid: [u8; 16], commit: u64) -> Result<()> {
    let first_commit = commit
        .checked_add(1)
        .ok_or_else(|| corrupt("WAL commit ID is exhausted"))?;
    fs::create_dir_all(directory)?;
    Head {
        id: 0,
        first_commit,
    }
    .persist(directory, uuid)
}

pub(crate) fn validate_empty(directory: &Path, uuid: [u8; 16], commit: u64) -> Result<()> {
    let head = Head::load(directory, uuid)?;
    if head.id != 0 || commit.checked_add(1) != Some(head.first_commit) {
        return Err(corrupt("snapshot WAL head is not empty"));
    }
    Ok(())
}
