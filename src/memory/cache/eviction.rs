//! Three optional cache owners per Storage, without extending their lifetime.
//! Reclamation never waits for a cache lock and never performs I/O. Busy/pinned
//! entries are skipped; failed admission remains an uncached read.
use super::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::error::{Result, corrupt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone, Copy)]
pub(crate) enum CacheOwner {
    Index,
    Catalog,
    Handles,
}

pub(crate) trait Evictable: Send + Sync {
    /// Release at most a bounded number of idle entries; report their charges.
    fn evict(&self, bytes: u64) -> u64;
}

#[derive(Default)]
pub(super) struct Owners {
    slots: Mutex<[Option<Weak<dyn Evictable>>; 3]>,
    next: AtomicUsize,
}

impl std::fmt::Debug for Owners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Owners(3 weak slots)")
    }
}

impl MemoryGovernor {
    pub(crate) fn register_cache<T: Evictable + 'static>(
        &self,
        owner: CacheOwner,
        cache: &Arc<T>,
    ) -> Result<()> {
        let cache: Arc<dyn Evictable> = cache.clone();
        let mut slots = self
            .evictors
            .slots
            .lock()
            .map_err(|_| corrupt("cache owners poisoned"))?;
        let slot = &mut slots[owner as usize];
        if slot.as_ref().is_some_and(|old| old.strong_count() != 0) {
            return Err(corrupt("duplicate metadata cache owner"));
        }
        *slot = Some(Arc::downgrade(&cache));
        Ok(())
    }

    pub(crate) fn reserve_reclaiming(
        self: &Arc<Self>,
        class: MemoryClass,
        bytes: u64,
    ) -> Option<MemoryReservation> {
        if let Some(memory) = self.try_reserve_token(class, bytes) {
            return Some(memory);
        }
        if bytes > self.maximum_bytes
            || (class.is_optional_metadata() && bytes > self.metadata_maximum_bytes)
        {
            return None;
        }
        let owners = self.evictors.slots.lock().ok()?.clone();
        let start = self.evictors.next.fetch_add(1, Ordering::Relaxed) % owners.len();
        // Drop the registry lock before entering any owner. Each owner uses
        // try_lock, so a caller may already hold its own cache lock safely.
        for offset in 0..owners.len() {
            if let Some(owner) = owners[(start + offset) % owners.len()]
                .as_ref()
                .and_then(Weak::upgrade)
            {
                owner.evict(bytes);
                if let Some(memory) = self.try_reserve_token(class, bytes) {
                    return Some(memory);
                }
            }
        }
        None
    }
}
