//! Общий план файлового чтения, prefetch и резервирование памяти запроса.

use super::cursor::OperationCursor;
use super::segment_cursor::{SegmentCursor, TimestampFilter};
use super::{BlockIndex, Segment, read_count_sum_summary};
use crate::cache::{BlockCacheAdmission, BlockKey};
use crate::error::{Result, corrupt, invalid};
use crate::file_pool::OpenFileCache;
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::{AggregateSet, AggregateState};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone)]
pub(crate) struct QuerySegmentRead {
    pub(super) segment: Arc<Segment>,
    pub(super) blocks: Vec<BlockIndex>,
}

impl QuerySegmentRead {
    /// A boundary block can prevent count_sum_for_range from using otherwise
    /// complete blocks in the same day. This is only a planning hint: the
    /// persisted-summary path still proves query coverage and source exclusivity.
    pub(crate) fn has_daily_summary_fallback_candidate(&self) -> bool {
        let mut first_boundary_day = i64::MAX;
        let mut last_boundary_day = i64::MIN;
        for block in &self.blocks {
            let first = block.min_timestamp_us.div_euclid(crate::types::DAY_US);
            let last = block.max_timestamp_us.div_euclid(crate::types::DAY_US);
            if first != last {
                first_boundary_day = first_boundary_day.min(first);
                last_boundary_day = last_boundary_day.max(last);
            }
        }
        first_boundary_day <= last_boundary_day
            && self.blocks.iter().any(|block| {
                let first = block.min_timestamp_us.div_euclid(crate::types::DAY_US);
                let last = block.max_timestamp_us.div_euclid(crate::types::DAY_US);
                block.count_sum.is_some()
                    && first == last
                    && first >= first_boundary_day
                    && first <= last_boundary_day
            })
    }

    pub(crate) fn time_ranges(&self) -> impl ExactSizeIterator<Item = (i64, i64)> + '_ {
        self.blocks
            .iter()
            .map(|block| (block.min_timestamp_us, block.max_timestamp_us))
    }

    pub(crate) fn segment(&self) -> &Arc<Segment> {
        &self.segment
    }

    pub(crate) fn cursor(
        &self,
        from_us: i64,
        to_us: i64,
        cache_admission: BlockCacheAdmission,
        query_budget: Arc<QueryBudget>,
        read_plan: Arc<QueryReadPlan>,
    ) -> Result<Option<Box<dyn OperationCursor>>> {
        // Hybrid grouping asks the remaining plan about individual days. Do
        // not walk its blocks if the segment proves this day cannot intersect.
        let descriptor = self.segment.descriptor();
        if descriptor.max_timestamp_us < from_us || descriptor.min_timestamp_us >= to_us {
            return Ok(None);
        }
        let blocks = self
            .blocks
            .iter()
            .copied()
            .filter(|entry| entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
            .collect::<Vec<_>>();
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(SegmentCursor::new_planned(
            Arc::clone(&self.segment),
            blocks,
            TimestampFilter::HalfOpen { from_us, to_us },
            cache_admission,
            Some(query_budget),
            Some(read_plan),
        )?)))
    }

    pub(crate) fn remove_blocks_covered_by(&mut self, ranges: &[(i64, i64)]) -> u64 {
        self.remove_blocks_where(|minimum, maximum| {
            ranges
                .iter()
                .any(|(from_us, to_us)| minimum >= *from_us && maximum < *to_us)
        })
    }

    pub(crate) fn remove_blocks_where(&mut self, mut covered: impl FnMut(i64, i64) -> bool) -> u64 {
        let before = self.blocks.len();
        self.blocks
            .retain(|entry| !covered(entry.min_timestamp_us, entry.max_timestamp_us));
        if self.blocks.len() != before {
            // The caller releases the corresponding query-plan reservation.
            self.blocks.shrink_to_fit();
        }
        u64::try_from(before.saturating_sub(self.blocks.len())).unwrap_or(u64::MAX)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub(crate) fn intersects_time_range(&self, from_us: i64, to_us: i64) -> bool {
        self.blocks
            .iter()
            .any(|entry| entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
    }

    pub(crate) fn count_sum_for_range(
        &self,
        from_us: i64,
        to_us: i64,
    ) -> Result<Option<(AggregateState, u64, i64, i64)>> {
        let mut state = AggregateState::for_aggregates(AggregateSet::COUNT_SUM);
        let mut summaries = 0_u64;
        let mut minimum_timestamp_us = i64::MAX;
        let mut maximum_timestamp_us = i64::MIN;
        for entry in self
            .blocks
            .iter()
            .filter(|entry| entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
        {
            if entry.min_timestamp_us < from_us || entry.max_timestamp_us >= to_us {
                return Ok(None);
            }
            let Some(summary) = entry.count_sum else {
                return Ok(None);
            };
            let summary = read_count_sum_summary(summary)?;
            if summary.count != 0 {
                state.merge(summary)?;
            }
            summaries = summaries.saturating_add(1);
            minimum_timestamp_us = minimum_timestamp_us.min(entry.min_timestamp_us);
            maximum_timestamp_us = maximum_timestamp_us.max(entry.max_timestamp_us);
        }
        if summaries == 0 || state.count == 0 {
            Ok(None)
        } else {
            Ok(Some((
                state,
                summaries,
                minimum_timestamp_us,
                maximum_timestamp_us
                    .checked_add(1)
                    .ok_or_else(|| corrupt("block summary time range overflows i64"))?,
            )))
        }
    }

    pub(crate) fn time_range_within(&self, from_us: i64, to_us: i64) -> Result<Option<(i64, i64)>> {
        let mut minimum_timestamp_us = i64::MAX;
        let mut maximum_timestamp_us = i64::MIN;
        for entry in self
            .blocks
            .iter()
            .filter(|entry| entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
        {
            minimum_timestamp_us = minimum_timestamp_us.min(entry.min_timestamp_us.max(from_us));
            maximum_timestamp_us = maximum_timestamp_us.max(
                entry.max_timestamp_us.min(
                    to_us
                        .checked_sub(1)
                        .ok_or_else(|| invalid("query time range is empty"))?,
                ),
            );
        }
        if minimum_timestamp_us > maximum_timestamp_us {
            Ok(None)
        } else {
            Ok(Some((
                minimum_timestamp_us,
                maximum_timestamp_us
                    .checked_add(1)
                    .ok_or_else(|| corrupt("planned block time range overflows i64"))?,
            )))
        }
    }
}

#[derive(Clone)]
pub(super) struct PlannedBlock {
    pub(super) key: BlockKey,
    pub(super) path: Arc<PathBuf>,
    pub(super) offset: u64,
    pub(super) size: u32,
}

pub(super) struct PlannedLocation {
    pub(super) block: PlannedBlock,
    pub(super) read_or_prefetched: AtomicBool,
}

#[derive(Default)]
pub(super) struct QueryReadState {
    pub(super) prefetched: HashMap<BlockKey, Vec<u8>>,
    pub(super) used_bytes: u64,
}

/// План физических чтений одного запроса. Он хранит только небольшой набор
/// закодированных блоков и освобождает его вместе с запросом.
pub(crate) struct QueryReadPlan {
    pub(super) locations: HashMap<BlockKey, PlannedLocation>,
    pub(super) by_path: HashMap<Arc<PathBuf>, Vec<PlannedBlock>>,
    pub(super) state: Mutex<QueryReadState>,
    pub(super) query_budget: Arc<QueryBudget>,
    pub(super) file_cache: Arc<OpenFileCache>,
    pub(super) prefetch_limit: u64,
    pub(super) _plan_reservation: QueryPlanReservation,
}

pub(super) const QUERY_PLAN_BYTES_PER_BLOCK: u64 = 208;

pub(crate) struct QueryPlanReservation {
    pub(super) query_budget: Arc<QueryBudget>,
    pub(super) planned_blocks: u64,
    pub(super) bytes: u64,
}

impl QueryPlanReservation {
    pub(crate) fn new(query_budget: Arc<QueryBudget>) -> Self {
        Self {
            query_budget,
            planned_blocks: 0,
            bytes: 0,
        }
    }

    pub(super) fn reserve_blocks(&mut self, blocks: u64) -> Result<()> {
        let bytes = blocks.saturating_mul(QUERY_PLAN_BYTES_PER_BLOCK);
        self.query_budget.reserve_memory(bytes)?;
        self.planned_blocks = self.planned_blocks.saturating_add(blocks);
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub(crate) fn release_blocks(&mut self, blocks: u64) {
        let blocks = blocks.min(self.planned_blocks);
        let bytes = blocks.saturating_mul(QUERY_PLAN_BYTES_PER_BLOCK);
        self.query_budget.release_memory(bytes);
        self.planned_blocks = self.planned_blocks.saturating_sub(blocks);
        self.bytes = self.bytes.saturating_sub(bytes);
    }

    pub(crate) fn absorb(&mut self, mut other: Self) -> Result<()> {
        if !Arc::ptr_eq(&self.query_budget, &other.query_budget) {
            return Err(corrupt(
                "query plan reservations belong to different queries",
            ));
        }
        self.planned_blocks = self.planned_blocks.saturating_add(other.planned_blocks);
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.planned_blocks = 0;
        other.bytes = 0;
        Ok(())
    }
}

impl Drop for QueryPlanReservation {
    fn drop(&mut self) {
        self.query_budget.release_memory(self.bytes);
    }
}

impl QueryReadPlan {
    pub(crate) fn new<'a>(
        reads: impl Iterator<Item = &'a QuerySegmentRead>,
        query_budget: Arc<QueryBudget>,
        file_cache: Arc<OpenFileCache>,
        plan_reservation: QueryPlanReservation,
    ) -> Result<Arc<Self>> {
        let mut locations = HashMap::new();
        let mut by_path = HashMap::<Arc<PathBuf>, Vec<PlannedBlock>>::new();
        let mut paths = HashMap::<PathBuf, Arc<PathBuf>>::new();
        let mut unique_segments = HashSet::new();
        let mut planned_blocks = 0_u64;
        for read in reads {
            unique_segments.insert(read.segment.descriptor.id);
            let path = Arc::clone(
                paths
                    .entry(read.segment.path.clone())
                    .or_insert_with(|| Arc::new(read.segment.path.clone())),
            );
            for entry in &read.blocks {
                let block = PlannedBlock {
                    key: BlockKey::new(read.segment.descriptor.id, entry.offset),
                    path: Arc::clone(&path),
                    offset: read
                        .segment
                        .descriptor
                        .file_offset
                        .saturating_add(entry.offset),
                    size: entry.size,
                };
                planned_blocks = planned_blocks.saturating_add(1);
                locations
                    .entry(block.key)
                    .or_insert_with(|| PlannedLocation {
                        block: block.clone(),
                        read_or_prefetched: AtomicBool::new(false),
                    });
                by_path
                    .entry(Arc::clone(&block.path))
                    .or_default()
                    .push(block);
            }
        }
        for blocks in by_path.values_mut() {
            blocks.sort_unstable_by_key(|block| block.offset);
            blocks.dedup_by_key(|block| block.key);
        }
        let prefetch_limit = query_budget.prefetch_memory_limit();
        if planned_blocks > plan_reservation.planned_blocks {
            return Err(corrupt("query read plan exceeded its reserved block count"));
        }
        query_budget.record_read_plan(
            u64::try_from(unique_segments.len()).unwrap_or(u64::MAX),
            u64::try_from(by_path.len()).unwrap_or(u64::MAX),
            planned_blocks,
            u64::try_from(locations.len()).unwrap_or(u64::MAX),
        );
        Ok(Arc::new(Self {
            locations,
            by_path,
            state: Mutex::new(QueryReadState::default()),
            query_budget,
            file_cache,
            prefetch_limit,
            _plan_reservation: plan_reservation,
        }))
    }

    pub(super) fn read_block(&self, key: BlockKey) -> Result<Vec<u8>> {
        self.query_budget.check()?;
        let location = self
            .locations
            .get(&key)
            .ok_or_else(|| corrupt("query block is missing from its read plan"))?;
        // This is an optional I/O claim, not publication of the block bytes.
        // Demand reads still proceed if another worker is loading this block.
        location
            .read_or_prefetched
            .store(true, AtomicOrdering::Relaxed);
        if let Some(bytes) = self.take_prefetched(key)? {
            self.query_budget.record_prefetch_hit();
            return Ok(bytes);
        }
        let requested = &location.block;
        let Some(path_blocks) = self.by_path.get(&requested.path) else {
            return Err(corrupt("query pack is missing from its read plan"));
        };
        let start = path_blocks
            .binary_search_by_key(&requested.offset, |block| block.offset)
            .map_err(|_| corrupt("query block offset is missing from its pack plan"))?;
        let mut planned = Vec::new();
        let mut planned_bytes = 0_u64;
        let mut window_bytes = 0_u64;
        for block in &path_blocks[start..] {
            let size = u64::from(block.size);
            if !planned.is_empty() && window_bytes.saturating_add(size) > self.prefetch_limit {
                break;
            }
            window_bytes = window_bytes.saturating_add(size);
            let location = self
                .locations
                .get(&block.key)
                .ok_or_else(|| corrupt("query pack block is missing from its read plan"))?;
            if block.key != key && location.read_or_prefetched.load(AtomicOrdering::Relaxed) {
                continue;
            }
            planned.push(location);
            planned_bytes = planned_bytes.saturating_add(size);
            if self.prefetch_limit == 0 {
                break;
            }
        }
        let reserve_prefetch = planned.len() > 1
            && planned_bytes <= self.prefetch_limit
            && planned_bytes <= self.prefetch_available()?
            && self
                .query_budget
                .try_reserve_memory_class(QueryMemoryClass::Compressed, planned_bytes);
        if !reserve_prefetch {
            planned.truncate(1);
            planned_bytes = 0;
        }

        let open_started = Instant::now();
        let file = self
            .file_cache
            .get(requested.path.as_path(), &self.query_budget);
        self.query_budget.record_io(open_started.elapsed());
        let file = file?;
        let mut requested_bytes = None;
        let mut cached_bytes = 0_u64;
        let mut released_bytes = 0_u64;
        for location in planned {
            let block = &location.block;
            if block.key != key
                && location
                    .read_or_prefetched
                    .swap(true, AtomicOrdering::Relaxed)
            {
                // Another worker claimed this optional block after planning.
                self.query_budget
                    .release_memory_class(QueryMemoryClass::Compressed, u64::from(block.size));
                released_bytes = released_bytes.saturating_add(u64::from(block.size));
                continue;
            }
            if let Err(error) = self.query_budget.check() {
                if reserve_prefetch {
                    self.query_budget.release_memory_class(
                        QueryMemoryClass::Compressed,
                        planned_bytes
                            .saturating_sub(cached_bytes)
                            .saturating_sub(released_bytes),
                    );
                }
                return Err(error);
            }
            let mut bytes = vec![0_u8; block.size as usize];
            let read_started = Instant::now();
            let read_result = file.read_exact_at(block.offset, &mut bytes, &self.query_budget);
            self.query_budget.record_io(read_started.elapsed());
            if let Err(error) = read_result {
                if reserve_prefetch {
                    self.query_budget.release_memory_class(
                        QueryMemoryClass::Compressed,
                        planned_bytes
                            .saturating_sub(cached_bytes)
                            .saturating_sub(released_bytes),
                    );
                }
                return Err(error);
            }
            self.query_budget
                .record_payload_bytes(u64::from(block.size));
            self.query_budget.record_physical_block();
            if block.key == key {
                requested_bytes = Some(bytes);
                if reserve_prefetch {
                    self.query_budget
                        .release_memory_class(QueryMemoryClass::Compressed, u64::from(block.size));
                    released_bytes = released_bytes.saturating_add(u64::from(block.size));
                }
            } else if reserve_prefetch {
                self.query_budget.record_prefetched_block();
                cached_bytes = cached_bytes.saturating_add(u64::from(block.size));
                self.insert_prefetched(block.key, bytes)?;
            }
        }
        requested_bytes.ok_or_else(|| corrupt("query read plan did not read its requested block"))
    }

    pub(super) fn take_prefetched(&self, key: BlockKey) -> Result<Option<Vec<u8>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("query read plan mutex is poisoned"))?;
        let bytes = state.prefetched.remove(&key);
        if let Some(bytes) = &bytes {
            let length = bytes.len() as u64;
            state.used_bytes = state.used_bytes.saturating_sub(length);
            self.query_budget
                .release_memory_class(QueryMemoryClass::Compressed, length);
        }
        Ok(bytes)
    }

    pub(super) fn prefetch_available(&self) -> Result<u64> {
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("query read plan mutex is poisoned"))?;
        Ok(self.prefetch_limit.saturating_sub(state.used_bytes))
    }

    pub(super) fn insert_prefetched(&self, key: BlockKey, bytes: Vec<u8>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("query read plan mutex is poisoned"))?;
        let length = bytes.len() as u64;
        if state.prefetched.insert(key, bytes).is_none() {
            state.used_bytes = state.used_bytes.saturating_add(length);
        } else {
            self.query_budget
                .release_memory_class(QueryMemoryClass::Compressed, length);
        }
        Ok(())
    }
}

impl Drop for QueryReadPlan {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut()
            && state.used_bytes != 0
        {
            self.query_budget
                .release_memory_class(QueryMemoryClass::Compressed, state.used_bytes);
            state.used_bytes = 0;
        }
    }
}
