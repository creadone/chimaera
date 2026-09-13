//! Межпроцессная блокировка каталога открытой базы.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::Path;

use crate::error::{Error, Result};

pub(crate) struct StorageLock {
    file: File,
    released: bool,
}

impl StorageLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        let path = root.join("LOCK");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Lock(path.display().to_string()));
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        file.sync_data()?;
        Ok(Self {
            file,
            released: false,
        })
    }

    pub(crate) fn release(mut self) -> Result<()> {
        self.file.unlock()?;
        self.released = true;
        Ok(())
    }
}

impl Drop for StorageLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.file.unlock();
        }
    }
}
