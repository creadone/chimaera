use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Result, corrupt};
use crate::query::QueryBudget;
use crate::types::VersionedPoint;

mod eviction;
pub(crate) use eviction::{CacheOwner, Evictable};
mod metadata;
pub(crate) use metadata::MetadataMemoryUsage;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BlockKey {
    segment_id: u64,
    offset: u64,
}

impl BlockKey {
    pub(crate) const fn new(segment_id: u64, offset: u64) -> Self {
        Self { segment_id, offset }
    }
}

struct Entry {
    block: Arc<CachedBlock>,
    bytes: u64,
    previous: Option<BlockKey>,
    next: Option<BlockKey>,
}

pub(crate) struct CachedBlock {
    points: Arc<[VersionedPoint]>,
    _reservation: Arc<MemoryReservation>,
}

impl CachedBlock {
    pub(crate) fn points(&self) -> &Arc<[VersionedPoint]> {
        &self.points
    }
}

#[derive(Default)]
struct State {
    entries: HashMap<BlockKey, Entry>,
    recent_scans: HashMap<BlockKey, u64>,
    recent_scan_order: VecDeque<(BlockKey, u64)>,
    used_bytes: u64,
    clock: u64,
    lru_head: Option<BlockKey>,
    lru_tail: Option<BlockKey>,
}

impl State {
    fn touch(&mut self, key: BlockKey) {
        if self.lru_tail == Some(key) || !self.entries.contains_key(&key) {
            return;
        }
        self.detach(key);
        self.attach_tail(key);
    }

    fn insert_entry(&mut self, key: BlockKey, block: Arc<CachedBlock>, bytes: u64) {
        self.entries.insert(
            key,
            Entry {
                block,
                bytes,
                previous: None,
                next: None,
            },
        );
        self.attach_tail(key);
        self.used_bytes = self.used_bytes.saturating_add(bytes);
    }

    fn remove_entry(&mut self, key: BlockKey) -> Option<Entry> {
        self.detach(key);
        let removed = self.entries.remove(&key)?;
        self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
        Some(removed)
    }

    fn pop_lru(&mut self) -> Option<Entry> {
        self.lru_head.and_then(|key| self.remove_entry(key))
    }

    fn detach(&mut self, key: BlockKey) {
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

    fn attach_tail(&mut self, key: BlockKey) {
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

pub(crate) struct BlockCache {
    maximum_bytes: u64,
    recent_scan_maximum: usize,
    governor: Arc<MemoryGovernor>,
    state: Mutex<State>,
    load_transition: Mutex<()>,
    loads: Arc<BlockLoadCoordinator>,
    hits: AtomicU64,
    misses: AtomicU64,
    mutex_wait_nanos: AtomicU64,
    victim_scans: AtomicU64,
    eviction_nanos: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BlockLoadLimits {
    pub(crate) maximum_loaders: usize,
    pub(crate) maximum_waiters: usize,
    pub(crate) maximum_bytes: u64,
}

pub(crate) struct BlockCacheStats {
    pub(crate) bytes: u64,
    pub(crate) entries: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) mutex_wait_nanos: u64,
    pub(crate) victims_scanned: u64,
    pub(crate) eviction_nanos: u64,
    pub(crate) coalesced_loads: u64,
    pub(crate) coalesced_waiters: u64,
    pub(crate) load_bypasses: u64,
    pub(crate) load_wait_nanos: u64,
    pub(crate) loads_in_flight: usize,
    pub(crate) load_bytes_in_flight: u64,
    pub(crate) peak_loads_in_flight: u64,
    pub(crate) peak_load_bytes_in_flight: u64,
}

#[derive(Default)]
struct BlockLoadTable {
    entries: HashMap<BlockKey, Arc<BlockLoad>>,
    waiters: usize,
    bytes: u64,
}

struct BlockLoadCoordinator {
    limits: BlockLoadLimits,
    state: Mutex<BlockLoadTable>,
    coalesced_loads: AtomicU64,
    coalesced_waiters: AtomicU64,
    bypasses: AtomicU64,
    wait_nanos: AtomicU64,
    peak_loads: AtomicU64,
    peak_bytes: AtomicU64,
}

struct BlockLoadStats {
    loads: u64,
    waiters: u64,
    bypasses: u64,
    wait_nanos: u64,
    in_flight: usize,
    bytes_in_flight: u64,
    peak_in_flight: u64,
    peak_bytes_in_flight: u64,
}

struct BlockLoad {
    state: Mutex<BlockLoadState>,
    ready: Condvar,
}

enum BlockLoadState {
    Loading,
    Loaded(Arc<[VersionedPoint]>),
    Retry,
}

pub(crate) enum BlockLoadDecision {
    Cached(Arc<CachedBlock>),
    Leader(BlockLoadLeader),
    Waiter(BlockLoadWaiter),
    Independent,
}

pub(crate) enum BlockLoadWaitOutcome {
    Loaded(Arc<[VersionedPoint]>),
    Retry,
}

pub(crate) struct BlockLoadLeader {
    coordinator: Arc<BlockLoadCoordinator>,
    key: BlockKey,
    load: Arc<BlockLoad>,
    bytes: u64,
    completed: bool,
}

pub(crate) struct BlockLoadWaiter {
    coordinator: Arc<BlockLoadCoordinator>,
    load: Arc<BlockLoad>,
    active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlockCacheAdmission {
    Always,
    Repeated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemoryClass {
    WriteQueue,
    Memtable,
    SeriesWatermark,
    BlockCache,
    MetadataCache,
    MetadataCatalogPage,
    MetadataHandle,
    MetadataIndexRoot,
    MetadataCatalog,
    MetadataDirectoryRoot,
    MetadataDirectoryPage,
    MetadataLoading,
}

impl MemoryClass {
    fn is_write(self) -> bool {
        matches!(
            self,
            Self::WriteQueue | Self::Memtable | Self::SeriesWatermark
        )
    }
    fn is_optional_metadata(self) -> bool {
        matches!(
            self,
            Self::MetadataCache
                | Self::MetadataDirectoryPage
                | Self::MetadataCatalogPage
                | Self::MetadataHandle
        ) || (matches!(self, Self::MetadataIndexRoot | Self::MetadataLoading))
    }
}

fn reserve_counter(counter: &AtomicU64, bytes: u64, maximum: u64) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|next| *next <= maximum)
        })
        .is_ok()
}

#[derive(Debug)]
pub(crate) struct MemoryGovernor {
    maximum_bytes: u64,
    metadata_maximum_bytes: u64,
    write_reserve_bytes: u64,
    non_write_bytes: AtomicU64,
    optional_metadata_bytes: AtomicU64,
    used_bytes: AtomicU64,
    write_queue_bytes: AtomicU64,
    memtable_bytes: AtomicU64,
    series_watermark_bytes: AtomicU64,
    block_cache_bytes: AtomicU64,
    metadata_cache_bytes: AtomicU64,
    metadata_catalog_page_bytes: AtomicU64,
    metadata_handle_bytes: AtomicU64,
    evictors: eviction::Owners,
    metadata_index_root_bytes: AtomicU64,
    metadata_catalog_bytes: AtomicU64,
    metadata_directory_root_bytes: AtomicU64,
    metadata_directory_page_bytes: AtomicU64,
    metadata_loading_bytes: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct MemoryReservation {
    governor: Arc<MemoryGovernor>,
    class: MemoryClass,
    bytes: u64,
}

impl MemoryGovernor {
    pub(crate) fn new(maximum_bytes: u64) -> Self {
        Self::with_limits(maximum_bytes, maximum_bytes, 0)
    }

    pub(crate) fn with_limits(
        maximum_bytes: u64,
        metadata_maximum_bytes: u64,
        write_reserve_bytes: u64,
    ) -> Self {
        assert!(write_reserve_bytes <= maximum_bytes);
        Self {
            maximum_bytes,
            metadata_maximum_bytes: metadata_maximum_bytes.min(maximum_bytes),
            write_reserve_bytes,
            non_write_bytes: AtomicU64::new(0),
            optional_metadata_bytes: AtomicU64::new(0),
            used_bytes: AtomicU64::new(0),
            write_queue_bytes: AtomicU64::new(0),
            memtable_bytes: AtomicU64::new(0),
            series_watermark_bytes: AtomicU64::new(0),
            block_cache_bytes: AtomicU64::new(0),
            metadata_cache_bytes: AtomicU64::new(0),
            metadata_catalog_page_bytes: AtomicU64::new(0),
            metadata_handle_bytes: AtomicU64::new(0),
            evictors: eviction::Owners::default(),
            metadata_index_root_bytes: AtomicU64::new(0),
            metadata_catalog_bytes: AtomicU64::new(0),
            metadata_directory_root_bytes: AtomicU64::new(0),
            metadata_directory_page_bytes: AtomicU64::new(0),
            metadata_loading_bytes: AtomicU64::new(0),
        }
    }

    pub(crate) const fn maximum_bytes(&self) -> u64 {
        self.maximum_bytes
    }

    pub(crate) fn admission_limits(&self) -> (u64, u64, u64, u64) {
        (
            self.metadata_maximum_bytes,
            self.optional_metadata_bytes.load(Ordering::Relaxed),
            self.write_reserve_bytes,
            self.non_write_bytes.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn try_reserve(&self, class: MemoryClass, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        // Charge each parent before publishing a reservation. A failed later
        // charge rolls back; concurrent admissions can only see a tighter limit.
        if class.is_optional_metadata()
            && !reserve_counter(
                &self.optional_metadata_bytes,
                bytes,
                self.metadata_maximum_bytes,
            )
        {
            return false;
        }
        if !class.is_write()
            && !reserve_counter(
                &self.non_write_bytes,
                bytes,
                self.maximum_bytes - self.write_reserve_bytes,
            )
        {
            if class.is_optional_metadata() {
                self.optional_metadata_bytes
                    .fetch_sub(bytes, Ordering::AcqRel);
            }
            return false;
        }
        if !reserve_counter(&self.used_bytes, bytes, self.maximum_bytes) {
            if !class.is_write() {
                self.non_write_bytes.fetch_sub(bytes, Ordering::AcqRel);
            }
            if class.is_optional_metadata() {
                self.optional_metadata_bytes
                    .fetch_sub(bytes, Ordering::AcqRel);
            }
            return false;
        }
        self.class_counter(class)
            .fetch_add(bytes, Ordering::Relaxed);
        true
    }

    pub(crate) fn try_reserve_owned(
        self: &Arc<Self>,
        class: MemoryClass,
        bytes: u64,
    ) -> Option<Arc<MemoryReservation>> {
        self.try_reserve(class, bytes)
            .then(|| Arc::new(MemoryReservation::new(Arc::clone(self), class, bytes)))
    }

    pub(crate) fn try_reserve_token(
        self: &Arc<Self>,
        class: MemoryClass,
        bytes: u64,
    ) -> Option<MemoryReservation> {
        self.try_reserve(class, bytes)
            .then(|| MemoryReservation::new(Arc::clone(self), class, bytes))
    }

    pub(crate) fn release(&self, class: MemoryClass, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let class_previous = self.class_counter(class).fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(class_previous >= bytes);
        let previous = self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes);
        if !class.is_write() {
            self.non_write_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
        if class.is_optional_metadata() {
            self.optional_metadata_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    pub(crate) fn stats(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        (
            self.maximum_bytes,
            self.used_bytes.load(Ordering::Relaxed),
            self.write_queue_bytes.load(Ordering::Relaxed),
            self.memtable_bytes.load(Ordering::Relaxed),
            self.series_watermark_bytes.load(Ordering::Relaxed),
            self.block_cache_bytes.load(Ordering::Relaxed),
            self.metadata_usage().total_bytes(),
        )
    }

    fn class_counter(&self, class: MemoryClass) -> &AtomicU64 {
        match class {
            MemoryClass::WriteQueue => &self.write_queue_bytes,
            MemoryClass::Memtable => &self.memtable_bytes,
            MemoryClass::SeriesWatermark => &self.series_watermark_bytes,
            MemoryClass::BlockCache => &self.block_cache_bytes,
            MemoryClass::MetadataCache => &self.metadata_cache_bytes,
            MemoryClass::MetadataCatalogPage => &self.metadata_catalog_page_bytes,
            MemoryClass::MetadataHandle => &self.metadata_handle_bytes,
            MemoryClass::MetadataIndexRoot => &self.metadata_index_root_bytes,
            MemoryClass::MetadataCatalog => &self.metadata_catalog_bytes,
            MemoryClass::MetadataDirectoryRoot => &self.metadata_directory_root_bytes,
            MemoryClass::MetadataDirectoryPage => &self.metadata_directory_page_bytes,
            MemoryClass::MetadataLoading => &self.metadata_loading_bytes,
        }
    }
}

impl MemoryReservation {
    fn new(governor: Arc<MemoryGovernor>, class: MemoryClass, bytes: u64) -> Self {
        Self {
            governor,
            class,
            bytes,
        }
    }

    pub(crate) fn reclassify(mut self, class: MemoryClass) -> Self {
        // Queue -> memtable keeps the same admission parents and bytes.
        assert_eq!(self.class.is_write(), class.is_write());
        assert_eq!(
            self.class.is_optional_metadata(),
            class.is_optional_metadata()
        );
        if self.class != class && self.bytes != 0 {
            let previous = self
                .governor
                .class_counter(self.class)
                .fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes);
            self.governor
                .class_counter(class)
                .fetch_add(self.bytes, Ordering::Relaxed);
            self.class = class;
        }
        self
    }

    pub(crate) fn try_grow(&mut self, bytes: u64) -> bool {
        if self.governor.try_reserve(self.class, bytes) {
            self.bytes = self.bytes.saturating_add(bytes);
            true
        } else {
            false
        }
    }

    pub(crate) fn shrink(&mut self, bytes: u64) {
        let released = bytes.min(self.bytes);
        self.governor.release(self.class, released);
        self.bytes -= released;
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.governor.release(self.class, self.bytes);
    }
}

impl fmt::Debug for BlockCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockCache")
            .field("maximum_bytes", &self.maximum_bytes)
            .finish_non_exhaustive()
    }
}

impl BlockLoadCoordinator {
    fn new(limits: BlockLoadLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(BlockLoadTable::default()),
            coalesced_loads: AtomicU64::new(0),
            coalesced_waiters: AtomicU64::new(0),
            bypasses: AtomicU64::new(0),
            wait_nanos: AtomicU64::new(0),
            peak_loads: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
        })
    }

    fn begin(self: &Arc<Self>, key: BlockKey, bytes: u64) -> Result<BlockLoadDecision> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("block load table mutex is poisoned"))?;
        if let Some(load) = state.entries.get(&key).cloned() {
            if state.waiters >= self.limits.maximum_waiters {
                self.bypasses.fetch_add(1, Ordering::Relaxed);
                return Ok(BlockLoadDecision::Independent);
            }
            state.waiters = state.waiters.saturating_add(1);
            self.coalesced_waiters.fetch_add(1, Ordering::Relaxed);
            return Ok(BlockLoadDecision::Waiter(BlockLoadWaiter {
                coordinator: Arc::clone(self),
                load,
                active: true,
            }));
        }
        if state.entries.len() >= self.limits.maximum_loaders
            || bytes > self.limits.maximum_bytes
            || state.bytes.saturating_add(bytes) > self.limits.maximum_bytes
        {
            self.bypasses.fetch_add(1, Ordering::Relaxed);
            return Ok(BlockLoadDecision::Independent);
        }
        let load = Arc::new(BlockLoad {
            state: Mutex::new(BlockLoadState::Loading),
            ready: Condvar::new(),
        });
        state.entries.insert(key, Arc::clone(&load));
        state.bytes = state.bytes.saturating_add(bytes);
        self.coalesced_loads.fetch_add(1, Ordering::Relaxed);
        self.peak_loads.fetch_max(
            u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.peak_bytes.fetch_max(state.bytes, Ordering::Relaxed);
        Ok(BlockLoadDecision::Leader(BlockLoadLeader {
            coordinator: Arc::clone(self),
            key,
            load,
            bytes,
            completed: false,
        }))
    }

    fn finish(
        &self,
        key: BlockKey,
        load: &Arc<BlockLoad>,
        bytes: u64,
        outcome: BlockLoadState,
    ) -> Result<()> {
        {
            let mut state = load
                .state
                .lock()
                .map_err(|_| corrupt("block load result mutex is poisoned"))?;
            if matches!(*state, BlockLoadState::Loading) {
                *state = outcome;
                load.ready.notify_all();
            }
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("block load table mutex is poisoned"))?;
        if state
            .entries
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, load))
        {
            state.entries.remove(&key);
            state.bytes = state.bytes.saturating_sub(bytes);
        }
        Ok(())
    }

    fn release_waiter(&self) {
        if let Ok(mut state) = self.state.lock() {
            debug_assert!(state.waiters > 0);
            state.waiters = state.waiters.saturating_sub(1);
        }
    }

    fn record_wait(&self, duration: Duration) {
        self.wait_nanos
            .fetch_add(duration_nanos(duration), Ordering::Relaxed);
    }

    fn stats(&self) -> Result<BlockLoadStats> {
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("block load table mutex is poisoned"))?;
        Ok(BlockLoadStats {
            loads: self.coalesced_loads.load(Ordering::Relaxed),
            waiters: self.coalesced_waiters.load(Ordering::Relaxed),
            bypasses: self.bypasses.load(Ordering::Relaxed),
            wait_nanos: self.wait_nanos.load(Ordering::Relaxed),
            in_flight: state.entries.len(),
            bytes_in_flight: state.bytes,
            peak_in_flight: self.peak_loads.load(Ordering::Relaxed),
            peak_bytes_in_flight: self.peak_bytes.load(Ordering::Relaxed),
        })
    }
}

impl BlockLoadLeader {
    fn complete(mut self, points: Arc<[VersionedPoint]>) -> Result<()> {
        self.coordinator.finish(
            self.key,
            &self.load,
            self.bytes,
            BlockLoadState::Loaded(points),
        )?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for BlockLoadLeader {
    fn drop(&mut self) {
        if !self.completed {
            let _ =
                self.coordinator
                    .finish(self.key, &self.load, self.bytes, BlockLoadState::Retry);
        }
    }
}

impl BlockLoadWaiter {
    pub(crate) fn wait(mut self, budget: Option<&QueryBudget>) -> Result<BlockLoadWaitOutcome> {
        let started = Instant::now();
        let result = (|| {
            loop {
                if let Some(budget) = budget {
                    budget.check()?;
                }
                let state = self
                    .load
                    .state
                    .lock()
                    .map_err(|_| corrupt("block load result mutex is poisoned"))?;
                match &*state {
                    BlockLoadState::Loaded(points) => {
                        return Ok(BlockLoadWaitOutcome::Loaded(Arc::clone(points)));
                    }
                    BlockLoadState::Retry => return Ok(BlockLoadWaitOutcome::Retry),
                    BlockLoadState::Loading => {
                        let (_state, _) = self
                            .load
                            .ready
                            .wait_timeout(state, Duration::from_millis(10))
                            .map_err(|_| corrupt("block load result mutex is poisoned"))?;
                    }
                }
            }
        })();
        let elapsed = started.elapsed();
        self.coordinator.record_wait(elapsed);
        if let Some(budget) = budget {
            budget.record_block_load_wait(elapsed);
        }
        self.release();
        result
    }

    fn release(&mut self) {
        if self.active {
            self.coordinator.release_waiter();
            self.active = false;
        }
    }
}

impl Drop for BlockLoadWaiter {
    fn drop(&mut self) {
        self.release();
    }
}

impl BlockCache {
    pub(crate) fn with_governor(maximum_bytes: u64, governor: Arc<MemoryGovernor>) -> Self {
        Self::with_governor_and_load_limits(
            maximum_bytes,
            governor,
            BlockLoadLimits {
                maximum_loaders: 64,
                maximum_waiters: 1_024,
                maximum_bytes: maximum_bytes.max(1),
            },
        )
    }

    pub(crate) fn with_governor_and_load_limits(
        maximum_bytes: u64,
        governor: Arc<MemoryGovernor>,
        load_limits: BlockLoadLimits,
    ) -> Self {
        Self {
            maximum_bytes,
            recent_scan_maximum: usize::try_from(maximum_bytes / (16 * 1024))
                .unwrap_or(4_096)
                .clamp(64, 4_096),
            governor,
            state: Mutex::new(State::default()),
            load_transition: Mutex::new(()),
            loads: BlockLoadCoordinator::new(load_limits),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            mutex_wait_nanos: AtomicU64::new(0),
            victim_scans: AtomicU64::new(0),
            eviction_nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn acquire_for_load(
        &self,
        key: BlockKey,
        estimated_bytes: u64,
    ) -> Result<BlockLoadDecision> {
        let _transition = self
            .load_transition
            .lock()
            .map_err(|_| corrupt("block load transition mutex is poisoned"))?;
        if let Some(block) = self.get(key)? {
            return Ok(BlockLoadDecision::Cached(block));
        }
        self.loads.begin(key, estimated_bytes)
    }

    pub(crate) fn complete_load(
        &self,
        leader: BlockLoadLeader,
        points: Arc<[VersionedPoint]>,
        admission: BlockCacheAdmission,
    ) -> Result<Option<Arc<CachedBlock>>> {
        let _transition = self
            .load_transition
            .lock()
            .map_err(|_| corrupt("block load transition mutex is poisoned"))?;
        let cached = self.insert_with_admission(leader.key, Arc::clone(&points), admission)?;
        leader.complete(points)?;
        Ok(cached)
    }

    pub(crate) fn get(&self, key: BlockKey) -> Result<Option<Arc<CachedBlock>>> {
        let lock_started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("block cache mutex is poisoned"))?;
        self.record_mutex_wait(lock_started);
        if state.entries.contains_key(&key) {
            state.touch(key);
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(state
                .entries
                .get(&key)
                .map(|entry| Arc::clone(&entry.block)));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    pub(crate) fn insert_with_admission(
        &self,
        key: BlockKey,
        points: Arc<[VersionedPoint]>,
        admission: BlockCacheAdmission,
    ) -> Result<Option<Arc<CachedBlock>>> {
        let bytes = u64::try_from(points.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(std::mem::size_of::<VersionedPoint>() as u64);
        if bytes == 0 || bytes > self.maximum_bytes {
            return Ok(None);
        }
        let lock_started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("block cache mutex is poisoned"))?;
        self.record_mutex_wait(lock_started);
        let eviction_started = Instant::now();
        let mut evicted = false;
        if admission == BlockCacheAdmission::Repeated
            && !state.entries.contains_key(&key)
            && !remember_repeated_scan(&mut state, key, self.recent_scan_maximum)
        {
            return Ok(None);
        }
        state.remove_entry(key);
        while state.used_bytes.saturating_add(bytes) > self.maximum_bytes {
            self.victim_scans.fetch_add(1, Ordering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                break;
            };
            evicted = true;
        }
        let reservation = loop {
            if let Some(reservation) = self
                .governor
                .try_reserve_owned(MemoryClass::BlockCache, bytes)
            {
                break reservation;
            }
            self.victim_scans.fetch_add(1, Ordering::Relaxed);
            let Some(_removed) = state.pop_lru() else {
                if evicted {
                    self.eviction_nanos.fetch_add(
                        duration_nanos(eviction_started.elapsed()),
                        Ordering::Relaxed,
                    );
                }
                return Ok(None);
            };
            evicted = true;
        };
        let block = Arc::new(CachedBlock {
            points,
            _reservation: reservation,
        });
        state.insert_entry(key, Arc::clone(&block), bytes);
        if evicted {
            self.eviction_nanos.fetch_add(
                duration_nanos(eviction_started.elapsed()),
                Ordering::Relaxed,
            );
        }
        Ok(Some(block))
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let lock_started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("block cache mutex is poisoned"))?;
        self.record_mutex_wait(lock_started);
        state.entries.clear();
        state.lru_head = None;
        state.lru_tail = None;
        state.recent_scans.clear();
        state.recent_scan_order.clear();
        state.used_bytes = 0;
        Ok(())
    }

    pub(crate) fn stats(&self) -> Result<BlockCacheStats> {
        let lock_started = Instant::now();
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("block cache mutex is poisoned"))?;
        self.record_mutex_wait(lock_started);
        let loads = self.loads.stats()?;
        Ok(BlockCacheStats {
            bytes: state.used_bytes,
            entries: state.entries.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            mutex_wait_nanos: self.mutex_wait_nanos.load(Ordering::Relaxed),
            victims_scanned: self.victim_scans.load(Ordering::Relaxed),
            eviction_nanos: self.eviction_nanos.load(Ordering::Relaxed),
            coalesced_loads: loads.loads,
            coalesced_waiters: loads.waiters,
            load_bypasses: loads.bypasses,
            load_wait_nanos: loads.wait_nanos,
            loads_in_flight: loads.in_flight,
            load_bytes_in_flight: loads.bytes_in_flight,
            peak_loads_in_flight: loads.peak_in_flight,
            peak_load_bytes_in_flight: loads.peak_bytes_in_flight,
        })
    }

    fn record_mutex_wait(&self, started: Instant) {
        self.mutex_wait_nanos
            .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
    }
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn remember_repeated_scan(state: &mut State, key: BlockKey, maximum: usize) -> bool {
    if state.recent_scans.remove(&key).is_some() {
        return true;
    }
    state.clock = state.clock.wrapping_add(1);
    let generation = state.clock;
    state.recent_scans.insert(key, generation);
    state.recent_scan_order.push_back((key, generation));
    while state.recent_scans.len() > maximum
        || state.recent_scan_order.len() > maximum.saturating_mul(2)
    {
        let Some((oldest, oldest_generation)) = state.recent_scan_order.pop_front() else {
            break;
        };
        if state.recent_scans.get(&oldest) == Some(&oldest_generation) {
            state.recent_scans.remove(&oldest);
        }
    }
    false
}
