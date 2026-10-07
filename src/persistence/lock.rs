//! Interprocess locking for a database directory.

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

    /// Protect offline readers against catalogue page reuse without creating or
    /// modifying a file in a snapshot or a read-only database directory.
    pub(crate) fn acquire_shared(root: &Path) -> Result<Self> {
        let file = File::open(root)?;
        file.lock_shared()?;
        Ok(Self {
            file,
            released: false,
        })
    }

    /// Held by recovery and the single publisher while roots or files change.
    /// Offline diagnostics share the directory lock; ordinary query snapshots
    /// use in-process generation leases and never acquire it.
    pub(crate) fn catalog_exclusive(root: &Path) -> Result<Self> {
        let file = File::open(root)?;
        file.lock()?;
        Ok(Self {
            file,
            released: false,
        })
    }

    /// Admin readers take the root lock first, then the WAL directory lock.
    /// Rotation only takes the latter: it must not wait for catalog publication.
    /// Each guard owns one descriptor for the duration of the operation.
    pub(crate) fn wal_shared(root: &Path) -> Result<Self> {
        Self::acquire_shared(&root.join("wal"))
    }

    pub(crate) fn wal_exclusive(root: &Path) -> Result<Self> {
        Self::catalog_exclusive(&root.join("wal"))
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
