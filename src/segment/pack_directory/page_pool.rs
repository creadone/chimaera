//! A bounded second-chance search used only after managed memory admission fails.
use super::{DirectoryPage, OfflinePackDirectory, PAGE_REFERENCED};
use crate::cache::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::error::{Result, corrupt};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError, Weak};

const MAX_DIRECTORY_PROBES: usize = 128;

#[derive(Default)]
struct Registry {
    entries: BTreeMap<usize, Weak<OfflinePackDirectory>>,
    cursor: usize,
}

#[derive(Default)]
pub(crate) struct DirectoryPagePool {
    registry: Mutex<Registry>,
    probes: AtomicU64,
    reclaimed_bytes: AtomicU64,
    admission_failures: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectoryPagePoolStats {
    pub(crate) pressure_probes: u64,
    pub(crate) reclaimed_bytes: u64,
    pub(crate) admission_failures: u64,
}

impl DirectoryPagePool {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn stats(&self) -> DirectoryPagePoolStats {
        DirectoryPagePoolStats {
            pressure_probes: self.probes.load(Ordering::Relaxed),
            reclaimed_bytes: self.reclaimed_bytes.load(Ordering::Relaxed),
            admission_failures: self.admission_failures.load(Ordering::Relaxed),
        }
    }

    pub(super) fn register(&self, directory: &Arc<OfflinePackDirectory>) -> Result<()> {
        self.registry
            .lock()
            .map_err(|_| corrupt("directory page pool mutex is poisoned"))?
            .entries
            .insert(Arc::as_ptr(directory) as usize, Arc::downgrade(directory));
        Ok(())
    }

    pub(super) fn unregister(&self, directory: &OfflinePackDirectory) {
        // Drop must release the weak entry even after an unrelated panic.
        self.registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .remove(&(std::ptr::from_ref(directory) as usize));
    }

    pub(super) fn reserve_page(
        &self,
        governor: &Arc<MemoryGovernor>,
        bytes: u64,
    ) -> Result<Option<MemoryReservation>> {
        let limit = self
            .registry
            .lock()
            .map_err(|_| corrupt("directory page pool mutex is poisoned"))?
            .entries
            .len()
            .saturating_mul(2)
            .min(MAX_DIRECTORY_PROBES);
        for _ in 0..limit {
            let candidate = {
                let mut registry = self
                    .registry
                    .lock()
                    .map_err(|_| corrupt("directory page pool mutex is poisoned"))?;
                let next = registry
                    .entries
                    .range((Excluded(registry.cursor), Unbounded))
                    .next()
                    .or_else(|| registry.entries.first_key_value())
                    .map(|(key, directory)| (*key, Weak::clone(directory)));
                if let Some((key, directory)) = next {
                    registry.cursor = key;
                    Some(directory)
                } else {
                    None
                }
            };
            let Some(candidate) = candidate.and_then(|candidate| candidate.upgrade()) else {
                continue;
            };
            self.probes.fetch_add(1, Ordering::Relaxed);
            // Never keep the registry locked while acquiring a local cache lock,
            // releasing a page, or dropping the final directory reference.
            if let Some(page) = candidate.evict_cold_page()? {
                self.reclaimed_bytes
                    .fetch_add(page.bytes.len() as u64, Ordering::Relaxed);
                drop(page);
                if let Some(memory) = governor.try_reserve_token(MemoryClass::MetadataCache, bytes)
                {
                    return Ok(Some(memory));
                }
            }
        }
        self.admission_failures.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}

impl OfflinePackDirectory {
    fn evict_cold_page(&self) -> Result<Option<Arc<DirectoryPage>>> {
        let mut cache = match self.page_cache.try_lock() {
            Ok(cache) => cache,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                return Err(corrupt("directory page cache mutex is poisoned"));
            }
        };
        let victim = cache
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.last_used & PAGE_REFERENCED == 0 && Arc::strong_count(&entry.page) == 1
            })
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(offset, _)| *offset);
        if let Some(offset) = victim {
            if let Some(entry) = cache.entries.remove(&offset) {
                cache.bytes = cache.bytes.saturating_sub(entry.page.bytes.len() as u64);
                return Ok(Some(entry.page));
            }
        } else {
            // Hot hits set this flag under the existing local lock. No global
            // atomic or lock is added to the cache-hit path.
            for entry in cache.entries.values_mut() {
                entry.last_used &= !PAGE_REFERENCED;
            }
        }
        Ok(None)
    }
}

impl Drop for OfflinePackDirectory {
    fn drop(&mut self) {
        self.page_pool.unregister(self);
    }
}

#[cfg(test)]
mod tests;
