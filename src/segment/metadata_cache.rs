//! Ограниченный кэш метаданных сегментов и статистика его использования.

use super::{SegmentMetadata, duration_nanos};
use crate::aggregate;
use crate::cache::{MemoryClass, MemoryGovernor};
use crate::error::{Result, corrupt};
use crate::types::{AggregateResolution, SeriesKey};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum MetadataCacheKey {
    Blocks(u64),
    IndexPage(u64, u64),
    Aggregate(u64),
    AggregateSeries(u64, SeriesKey, AggregateResolution),
}

impl MetadataCacheKey {
    pub(super) const fn segment_id(self) -> u64 {
        match self {
            Self::Blocks(id)
            | Self::IndexPage(id, _)
            | Self::Aggregate(id)
            | Self::AggregateSeries(id, _, _) => id,
        }
    }
}

pub(super) enum MetadataValue {
    Blocks(Arc<SegmentMetadata>),
    Aggregate(Arc<aggregate::Section>),
}

pub(super) struct MetadataEntry {
    pub(super) value: MetadataValue,
    pub(super) bytes: u64,
    pub(super) previous: Option<MetadataCacheKey>,
    pub(super) next: Option<MetadataCacheKey>,
}

pub(super) fn metadata_cache_entry_bytes(payload_bytes: u64) -> u64 {
    payload_bytes.saturating_add(std::mem::size_of::<(MetadataCacheKey, MetadataEntry)>() as u64)
}

#[derive(Default)]
pub(super) struct MetadataCacheState {
    pub(super) entries: HashMap<MetadataCacheKey, MetadataEntry>,
    pub(super) used_bytes: u64,
    pub(super) lru_head: Option<MetadataCacheKey>,
    pub(super) lru_tail: Option<MetadataCacheKey>,
}

impl MetadataCacheState {
    pub(super) fn touch(&mut self, key: MetadataCacheKey) {
        if self.lru_tail == Some(key) || !self.entries.contains_key(&key) {
            return;
        }
        self.detach(key);
        self.attach_tail(key);
    }

    pub(super) fn insert_entry(&mut self, key: MetadataCacheKey, value: MetadataValue, bytes: u64) {
        self.entries.insert(
            key,
            MetadataEntry {
                value,
                bytes,
                previous: None,
                next: None,
            },
        );
        self.attach_tail(key);
        self.used_bytes = self.used_bytes.saturating_add(bytes);
    }

    pub(super) fn remove_entry(&mut self, key: MetadataCacheKey) -> Option<MetadataEntry> {
        self.detach(key);
        let removed = self.entries.remove(&key)?;
        self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
        Some(removed)
    }

    pub(super) fn pop_lru(&mut self) -> Option<MetadataEntry> {
        self.lru_head.and_then(|key| self.remove_entry(key))
    }

    pub(super) fn detach(&mut self, key: MetadataCacheKey) {
        let Some((previous, next)) = self
            .entries
            .get(&key)
            .map(|entry| (entry.previous, entry.next))
        else {
            return;
        };
        if let Some(previous) = previous {
            if let Some(entry) = self.entries.get_mut(&previous) {
                entry.next = next;
            }
        } else {
            self.lru_head = next;
        }
        if let Some(next) = next {
            if let Some(entry) = self.entries.get_mut(&next) {
                entry.previous = previous;
            }
        } else {
            self.lru_tail = previous;
        }
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.previous = None;
            entry.next = None;
        }
    }

    pub(super) fn attach_tail(&mut self, key: MetadataCacheKey) {
        let previous = self.lru_tail;
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.previous = previous;
            entry.next = None;
        }
        if let Some(previous) = previous
            && let Some(entry) = self.entries.get_mut(&previous)
        {
            entry.next = Some(key);
        }
        if self.lru_head.is_none() {
            self.lru_head = Some(key);
        }
        self.lru_tail = Some(key);
    }
}

pub(crate) struct MetadataCache {
    pub(super) maximum_bytes: u64,
    pub(super) governor: Arc<MemoryGovernor>,
    pub(super) state: Mutex<MetadataCacheState>,
    pub(super) hits: AtomicU64,
    pub(super) misses: AtomicU64,
    pub(super) membership_negatives: AtomicU64,
    pub(super) membership_false_positives: AtomicU64,
    pub(super) mutex_wait_nanos: AtomicU64,
    pub(super) victim_scans: AtomicU64,
    pub(super) eviction_nanos: AtomicU64,
}

pub(crate) struct MetadataCacheStats {
    pub bytes: u64,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub membership_negatives: u64,
    pub membership_false_positives: u64,
    pub mutex_wait_nanos: u64,
    pub victims_scanned: u64,
    pub eviction_nanos: u64,
}

impl fmt::Debug for MetadataCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCache")
            .field("maximum_bytes", &self.maximum_bytes)
            .finish_non_exhaustive()
    }
}

impl MetadataCache {
    #[cfg(test)]
    pub(crate) fn new(maximum_bytes: u64) -> Self {
        Self::with_governor(maximum_bytes, Arc::new(MemoryGovernor::new(maximum_bytes)))
    }

    pub(crate) fn with_governor(maximum_bytes: u64, governor: Arc<MemoryGovernor>) -> Self {
        Self {
            maximum_bytes,
            governor,
            state: Mutex::new(MetadataCacheState::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            membership_negatives: AtomicU64::new(0),
            membership_false_positives: AtomicU64::new(0),
            mutex_wait_nanos: AtomicU64::new(0),
            victim_scans: AtomicU64::new(0),
            eviction_nanos: AtomicU64::new(0),
        }
    }

    pub(super) fn lock_state(&self) -> Result<MutexGuard<'_, MetadataCacheState>> {
        let started = Instant::now();
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("metadata cache mutex is poisoned"))?;
        self.mutex_wait_nanos
            .fetch_add(duration_nanos(started.elapsed()), AtomicOrdering::Relaxed);
        Ok(state)
    }

    pub(super) fn get(&self, segment_id: u64) -> Result<Option<Arc<SegmentMetadata>>> {
        self.get_blocks_entry(MetadataCacheKey::Blocks(segment_id))
    }

    pub(super) fn get_blocks_entry(
        &self,
        key: MetadataCacheKey,
    ) -> Result<Option<Arc<SegmentMetadata>>> {
        let mut state = self.lock_state()?;
        if state.entries.contains_key(&key) {
            state.touch(key);
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            let entry = state
                .entries
                .get(&key)
                .ok_or_else(|| corrupt("metadata cache LRU lost an entry"))?;
            let MetadataValue::Blocks(metadata) = &entry.value else {
                return Err(corrupt("metadata cache entry has the wrong kind"));
            };
            return Ok(Some(Arc::clone(metadata)));
        }
        self.misses.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(None)
    }

    pub(super) fn insert(
        &self,
        segment_id: u64,
        metadata: Arc<SegmentMetadata>,
    ) -> Result<Arc<SegmentMetadata>> {
        self.insert_with_status(segment_id, metadata)
            .map(|(metadata, _)| metadata)
    }

    pub(super) fn insert_with_status(
        &self,
        segment_id: u64,
        metadata: Arc<SegmentMetadata>,
    ) -> Result<(Arc<SegmentMetadata>, bool)> {
        self.insert_blocks_entry(MetadataCacheKey::Blocks(segment_id), metadata)
    }

    pub(super) fn insert_blocks_entry(
        &self,
        key: MetadataCacheKey,
        mut metadata: Arc<SegmentMetadata>,
    ) -> Result<(Arc<SegmentMetadata>, bool)> {
        let bytes = metadata_cache_entry_bytes(metadata.bytes());
        if bytes == 0 || bytes > self.maximum_bytes {
            return Ok((metadata, false));
        }
        let mut state = self.lock_state()?;
        let eviction_started = Instant::now();
        let mut evicted = false;
        // Concurrent misses may decode the same immutable page. Reuse the
        // first admitted result instead of replacing a pinned cache entry.
        if matches!(key, MetadataCacheKey::IndexPage(..))
            && let Some(entry) = state.entries.get(&key)
        {
            let MetadataValue::Blocks(existing) = &entry.value else {
                return Err(corrupt("index page cache entry has the wrong kind"));
            };
            let existing = Arc::clone(existing);
            state.touch(key);
            return Ok((existing, true));
        }
        state.remove_entry(key);
        while state.used_bytes.saturating_add(bytes) > self.maximum_bytes {
            self.victim_scans.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                break;
            };
            evicted = true;
        }
        let reservation = loop {
            if let Some(reservation) = self
                .governor
                .try_reserve_owned(MemoryClass::MetadataCache, bytes)
            {
                break reservation;
            }
            self.victim_scans.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                return Ok((metadata, false));
            };
            evicted = true;
        };
        let unique = Arc::get_mut(&mut metadata)
            .ok_or_else(|| corrupt("new metadata cache entry is unexpectedly shared"))?;
        unique._reservation = Some(reservation);
        state.insert_entry(key, MetadataValue::Blocks(Arc::clone(&metadata)), bytes);
        if evicted {
            self.eviction_nanos.fetch_add(
                duration_nanos(eviction_started.elapsed()),
                AtomicOrdering::Relaxed,
            );
        }
        Ok((metadata, true))
    }

    pub(super) fn get_aggregate(&self, segment_id: u64) -> Result<Option<Arc<aggregate::Section>>> {
        self.get_aggregate_entry(MetadataCacheKey::Aggregate(segment_id))
    }

    pub(super) fn get_aggregate_entry(
        &self,
        key: MetadataCacheKey,
    ) -> Result<Option<Arc<aggregate::Section>>> {
        let mut state = self.lock_state()?;
        if state.entries.contains_key(&key) {
            state.touch(key);
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            let entry = state
                .entries
                .get(&key)
                .ok_or_else(|| corrupt("metadata cache LRU lost an aggregate"))?;
            let MetadataValue::Aggregate(section) = &entry.value else {
                return Err(corrupt("metadata cache entry has the wrong kind"));
            };
            return Ok(Some(Arc::clone(section)));
        }
        self.misses.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(None)
    }

    pub(super) fn insert_aggregate(
        &self,
        segment_id: u64,
        section: Arc<aggregate::Section>,
    ) -> Result<Arc<aggregate::Section>> {
        self.insert_aggregate_with_status(segment_id, section)
            .map(|(section, _)| section)
    }

    pub(super) fn insert_aggregate_with_status(
        &self,
        segment_id: u64,
        section: Arc<aggregate::Section>,
    ) -> Result<(Arc<aggregate::Section>, bool)> {
        self.insert_aggregate_entry(MetadataCacheKey::Aggregate(segment_id), section)
    }

    pub(super) fn insert_aggregate_entry(
        &self,
        key: MetadataCacheKey,
        mut section: Arc<aggregate::Section>,
    ) -> Result<(Arc<aggregate::Section>, bool)> {
        let bytes = metadata_cache_entry_bytes(section.cache_bytes());
        if bytes == 0 || bytes > self.maximum_bytes {
            return Ok((section, false));
        }
        let mut state = self.lock_state()?;
        let eviction_started = Instant::now();
        let mut evicted = false;
        state.remove_entry(key);
        while state.used_bytes.saturating_add(bytes) > self.maximum_bytes {
            self.victim_scans.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                break;
            };
            evicted = true;
        }
        let reservation = loop {
            if let Some(reservation) = self
                .governor
                .try_reserve_owned(MemoryClass::MetadataCache, bytes)
            {
                break reservation;
            }
            self.victim_scans.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                return Ok((section, false));
            };
            evicted = true;
        };
        let unique = Arc::get_mut(&mut section)
            .ok_or_else(|| corrupt("new aggregate cache entry is unexpectedly shared"))?;
        unique.set_reservation(reservation);
        state.insert_entry(key, MetadataValue::Aggregate(Arc::clone(&section)), bytes);
        if evicted {
            self.eviction_nanos.fetch_add(
                duration_nanos(eviction_started.elapsed()),
                AtomicOrdering::Relaxed,
            );
        }
        Ok((section, true))
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        state.entries.clear();
        state.lru_head = None;
        state.lru_tail = None;
        state.used_bytes = 0;
        Ok(())
    }

    pub(super) fn record_membership(&self, negative: bool, false_positive: bool) {
        if negative {
            self.membership_negatives
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if false_positive {
            self.membership_false_positives
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    pub(crate) fn stats(&self) -> Result<MetadataCacheStats> {
        let state = self.lock_state()?;
        let entries = state
            .entries
            .keys()
            .map(|key| key.segment_id())
            .collect::<HashSet<_>>()
            .len();
        Ok(MetadataCacheStats {
            bytes: state.used_bytes,
            entries,
            hits: self.hits.load(AtomicOrdering::Relaxed),
            misses: self.misses.load(AtomicOrdering::Relaxed),
            membership_negatives: self.membership_negatives.load(AtomicOrdering::Relaxed),
            membership_false_positives: self
                .membership_false_positives
                .load(AtomicOrdering::Relaxed),
            mutex_wait_nanos: self.mutex_wait_nanos.load(AtomicOrdering::Relaxed),
            victims_scanned: self.victim_scans.load(AtomicOrdering::Relaxed),
            eviction_nanos: self.eviction_nanos.load(AtomicOrdering::Relaxed),
        })
    }
}
