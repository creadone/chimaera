//! Общие проверки, ожидания каналов и небольшие файловые утилиты.

use crate::error::{Error, Result, corrupt, invalid};
use crate::lifecycle::{OverloadReason, WAIT_SLICE};
use crate::query::QueryBudget;
use crate::segment::OperationCursor;
use crate::types::VersionedPoint;
use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{fs, thread};

#[cfg(unix)]
use std::ffi::CString;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

pub(super) struct ActivityGauge<'a> {
    pub(super) gauge: &'a AtomicU64,
    pub(super) previous: u64,
}

impl<'a> ActivityGauge<'a> {
    pub(super) fn enter(gauge: &'a AtomicU64, value: u64) -> Self {
        Self {
            gauge,
            previous: gauge.swap(value, Ordering::Relaxed),
        }
    }
}

impl Drop for ActivityGauge<'_> {
    fn drop(&mut self) {
        self.gauge.store(self.previous, Ordering::Relaxed);
    }
}

struct PrefetchedCursor {
    source: Box<dyn OperationCursor>,
    first: Option<VersionedPoint>,
}

impl OperationCursor for PrefetchedCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        if self.first.is_some() {
            return Ok(self.first.take());
        }
        self.source.next_operation()
    }
}

pub(super) fn non_empty_cursor(
    mut source: Box<dyn OperationCursor>,
) -> Result<Option<Box<dyn OperationCursor>>> {
    let Some(first) = source.next_operation()? else {
        return Ok(None);
    };
    Ok(Some(Box::new(PrefetchedCursor {
        source,
        first: Some(first),
    })))
}

pub(super) fn wal_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(directory)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    Ok(paths)
}

pub(super) fn file_bytes(paths: &[PathBuf]) -> Result<u64> {
    paths.iter().try_fold(0_u64, |total, path| {
        Ok(total.saturating_add(fs::metadata(path)?.len()))
    })
}

pub(super) fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(Error::Io(error)),
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

#[cfg(unix)]
pub(super) fn available_disk_bytes(path: &Path) -> Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| invalid("storage path contains a NUL byte"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a live NUL-terminated C string and `stats` points to writable,
    // correctly aligned storage. We read it only after statvfs reports success.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: the successful statvfs call initialized the complete output structure.
    let stats = unsafe { stats.assume_init() };
    let available = u128::from(stats.f_bavail) * u128::from(stats.f_frsize);
    Ok(available.min(u128::from(u64::MAX)) as u64)
}

#[cfg(not(unix))]
pub(super) fn available_disk_bytes(_path: &Path) -> Result<u64> {
    Ok(u64::MAX)
}

pub(super) fn open_file_count() -> u64 {
    fs::read_dir("/dev/fd").ok().map_or(0, |entries| {
        entries.filter_map(std::result::Result::ok).count() as u64
    })
}

pub(super) fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

pub(super) fn nanos_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

pub(super) fn micros_seconds(micros: u64) -> f64 {
    micros as f64 / 1_000_000.0
}

pub(super) fn read<T>(lock: &RwLock<T>) -> Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|_| corrupt("state read lock is poisoned"))
}

pub(super) fn write<T>(lock: &RwLock<T>) -> Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write()
        .map_err(|_| corrupt("state write lock is poisoned"))
}

pub(super) fn lock<T>(lock: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    lock.lock().map_err(|_| corrupt("mutex is poisoned"))
}

pub(super) fn validate_batch_len(len: usize) -> Result<()> {
    if len == 0 || len > 100_000 {
        return Err(invalid("batch must contain 1..=100000 operations"));
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct DrainState {
    pub(super) reply: Option<Receiver<Result<u64>>>,
    pub(super) result: Option<std::result::Result<u64, String>>,
}

pub(super) fn receive_until<R>(
    reply: &Receiver<Result<R>>,
    deadline: Instant,
    operation: &'static str,
) -> Result<R> {
    match reply.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => Err(Error::OperationPending(operation)),
        Err(RecvTimeoutError::Disconnected) => Err(Error::WorkerStopped),
    }
}

struct BudgetedOperationCursor {
    inner: Box<dyn OperationCursor>,
    budget: Arc<QueryBudget>,
}

impl OperationCursor for BudgetedOperationCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        self.budget.check()?;
        let operation = self.inner.next_operation()?;
        if operation.is_some() {
            self.budget.record_operations(1)?;
        }
        Ok(operation)
    }
}

pub(super) fn budgeted_sources(
    sources: Vec<Box<dyn OperationCursor>>,
    budget: &Arc<QueryBudget>,
) -> Vec<Box<dyn OperationCursor>> {
    sources
        .into_iter()
        .map(|inner| {
            Box::new(BudgetedOperationCursor {
                inner,
                budget: Arc::clone(budget),
            }) as Box<dyn OperationCursor>
        })
        .collect()
}

pub(super) fn write_for_query<T>(
    state: &RwLock<T>,
    mut check: impl FnMut() -> Result<()>,
) -> Result<std::sync::RwLockWriteGuard<'_, T>> {
    loop {
        check()?;
        match state.try_write() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(corrupt("query state lock is poisoned"));
            }
            Err(std::sync::TryLockError::WouldBlock) => thread::sleep(WAIT_SLICE),
        }
    }
}

pub(super) fn compaction_admission_error<T>(error: SendTimeoutError<T>) -> Error {
    match error {
        SendTimeoutError::Timeout(_) => Error::Overloaded(OverloadReason::CompactionDebt),
        SendTimeoutError::Disconnected(_) => Error::WorkerStopped,
    }
}

pub(super) fn wait_for_compaction(reply: &Receiver<Result<u64>>, deadline: Instant) -> Result<u64> {
    match reply.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => Err(Error::Overloaded(OverloadReason::CompactionDebt)),
        Err(RecvTimeoutError::Disconnected) => Err(Error::WorkerStopped),
    }
}
