use std::collections::HashMap;
use std::fs::File;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, bounded};

use crate::error::{Error, Result, corrupt};
use crate::query::QueryBudget;

#[derive(Debug)]
struct Inner {
    sender: Sender<()>,
    receiver: Receiver<()>,
    capacity: usize,
    in_use: AtomicUsize,
    peak_in_use: AtomicUsize,
    waits: AtomicU64,
    wait_nanos: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct FilePermitPool {
    inner: Arc<Inner>,
}

impl FilePermitPool {
    pub(crate) fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(corrupt("file permit pool capacity must be positive"));
        }
        let (sender, receiver) = bounded(capacity);
        for _ in 0..capacity {
            sender
                .send(())
                .map_err(|_| corrupt("file permit pool initialization failed"))?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
                receiver,
                capacity,
                in_use: AtomicUsize::new(0),
                peak_in_use: AtomicUsize::new(0),
                waits: AtomicU64::new(0),
                wait_nanos: AtomicU64::new(0),
            }),
        })
    }

    pub(crate) fn acquire(&self) -> Result<FilePermit> {
        match self.inner.receiver.try_recv() {
            Ok(()) => {}
            Err(TryRecvError::Empty) => {
                self.inner.waits.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                self.inner
                    .receiver
                    .recv_timeout(crate::lifecycle::DEFAULT_WAIT_TIMEOUT)
                    .map_err(|error| match error {
                        RecvTimeoutError::Timeout => {
                            Error::Overloaded(crate::OverloadReason::FilePermits)
                        }
                        RecvTimeoutError::Disconnected => Error::WorkerStopped,
                    })?;
                self.record_wait(started.elapsed());
            }
            Err(TryRecvError::Disconnected) => return Err(Error::WorkerStopped),
        }
        Ok(self.acquired_permit())
    }

    pub(crate) fn acquire_for_query(&self, budget: &QueryBudget) -> Result<FilePermit> {
        budget.check()?;
        match self.inner.receiver.try_recv() {
            Ok(()) => {}
            Err(TryRecvError::Empty) => {
                self.inner.waits.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                let result = loop {
                    if let Err(error) = budget.check_waiting_for(crate::OverloadReason::FilePermits)
                    {
                        break Err(error);
                    }
                    match self.inner.receiver.recv_timeout(Duration::from_millis(10)) {
                        Ok(()) => break Ok(()),
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => break Err(Error::WorkerStopped),
                    }
                };
                self.record_wait(started.elapsed());
                result?;
            }
            Err(TryRecvError::Disconnected) => return Err(Error::WorkerStopped),
        }
        let permit = self.acquired_permit();
        budget.check()?;
        Ok(permit)
    }

    fn acquired_permit(&self) -> FilePermit {
        let in_use = self.inner.in_use.fetch_add(1, Ordering::AcqRel) + 1;
        self.inner.peak_in_use.fetch_max(in_use, Ordering::Relaxed);
        FilePermit {
            inner: Arc::clone(&self.inner),
        }
    }

    fn record_wait(&self, duration: Duration) {
        self.inner.wait_nanos.fetch_add(
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn stats(&self) -> (usize, usize, usize, u64, u64) {
        (
            self.inner.capacity,
            self.inner.in_use.load(Ordering::Relaxed),
            self.inner.peak_in_use.load(Ordering::Relaxed),
            self.inner.waits.load(Ordering::Relaxed),
            self.inner.wait_nanos.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

#[derive(Debug)]
pub(crate) struct FilePermit {
    inner: Arc<Inner>,
}

impl Drop for FilePermit {
    fn drop(&mut self) {
        let previous = self.inner.in_use.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        let sent = self.inner.sender.try_send(());
        debug_assert!(sent.is_ok());
    }
}

#[derive(Debug)]
pub(crate) struct CachedFile {
    #[cfg(unix)]
    file: File,
    #[cfg(not(unix))]
    file: Mutex<File>,
    length: u64,
    _permit: FilePermit,
}

impl CachedFile {
    pub(crate) const fn length(&self) -> u64 {
        self.length
    }

    pub(crate) fn read_exact_at(
        &self,
        offset: u64,
        bytes: &mut [u8],
        budget: &QueryBudget,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            // Immutable files have no shared cursor to serialize. The Arc
            // retains the file permit until the last reader has finished.
            budget.check()?;
            std::os::unix::fs::FileExt::read_exact_at(&self.file, bytes, offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut file = crate::query::lock_for_query(&self.file, budget)?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(bytes)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CacheEntry {
    file: Arc<CachedFile>,
    last_used: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<PathBuf, CacheEntry>,
    clock: u64,
}

/// Общий ограниченный кэш физических файлов segment/pack.
///
/// Кэш владеет разрешением на каждый открытый файл, поэтому его записи входят в
/// тот же глобальный предел, что flush, compaction и некэшированное чтение.
#[derive(Debug)]
pub(crate) struct OpenFileCache {
    pool: FilePermitPool,
    maximum_entries: usize,
    state: Mutex<CacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
    opens: AtomicU64,
    evictions: AtomicU64,
    uncached_opens: AtomicU64,
}

impl OpenFileCache {
    pub(crate) fn new(pool: FilePermitPool, background_file_reserve: usize) -> Arc<Self> {
        // Все pack-workers могут одновременно держать выходные файлы, а ещё
        // одно разрешение нужно, чтобы хотя бы один из них дочитал входной.
        let reserved = (pool.capacity() / 5)
            .max(background_file_reserve)
            .min(pool.capacity());
        let maximum_entries = pool.capacity().saturating_sub(reserved).min(96);
        Arc::new(Self {
            pool,
            maximum_entries,
            state: Mutex::new(CacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            opens: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            uncached_opens: AtomicU64::new(0),
        })
    }

    pub(crate) fn get(&self, path: &Path, budget: &QueryBudget) -> Result<Arc<CachedFile>> {
        budget.check()?;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| corrupt("open file cache mutex is poisoned"))?;
            state.clock = state.clock.saturating_add(1);
            let clock = state.clock;
            if let Some(entry) = state.entries.get_mut(path) {
                entry.last_used = clock;
                self.hits.fetch_add(1, Ordering::Relaxed);
                budget.record_file_cache_hit();
                return Ok(Arc::clone(&entry.file));
            }
            self.evict_one_if_full(&mut state);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        budget.record_file_cache_miss();
        let permit = self.pool.acquire_for_query(budget)?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) => {
                drop(permit);
                return Err(error.into());
            }
        };
        let length = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                drop(permit);
                return Err(error.into());
            }
        };
        self.opens.fetch_add(1, Ordering::Relaxed);
        budget.record_file_open();
        let opened = Arc::new(CachedFile {
            #[cfg(unix)]
            file,
            #[cfg(not(unix))]
            file: Mutex::new(file),
            length,
            _permit: permit,
        });

        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("open file cache mutex is poisoned"))?;
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(path) {
            entry.last_used = clock;
            return Ok(Arc::clone(&entry.file));
        }
        self.evict_one_if_full(&mut state);
        if state.entries.len() < self.maximum_entries {
            state.entries.insert(
                path.to_path_buf(),
                CacheEntry {
                    file: Arc::clone(&opened),
                    last_used: clock,
                },
            );
        } else {
            self.uncached_opens.fetch_add(1, Ordering::Relaxed);
        }
        Ok(opened)
    }

    fn evict_one_if_full(&self, state: &mut CacheState) {
        if state.entries.len() < self.maximum_entries {
            return;
        }
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.file) == 1)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(path, _)| path.clone());
        if let Some(path) = candidate {
            state.entries.remove(&path);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn invalidate(&self, path: &Path) -> Result<()> {
        let removed = self
            .state
            .lock()
            .map_err(|_| corrupt("open file cache mutex is poisoned"))?
            .entries
            .remove(path);
        drop(removed);
        Ok(())
    }

    pub(crate) fn stats(&self) -> Result<(usize, usize, u64, u64, u64, u64, u64)> {
        let entries = self
            .state
            .lock()
            .map_err(|_| corrupt("open file cache mutex is poisoned"))?
            .entries
            .len();
        Ok((
            self.maximum_entries,
            entries,
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.opens.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
            self.uncached_opens.load(Ordering::Relaxed),
        ))
    }
}

#[cfg(test)]
mod tests;
