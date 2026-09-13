//! Планирование группировок и доказательство применимости точных сводок.

use super::batch::{QueryBatch, QueryBatchOverlap};
use super::executor::QueryPlanningTask;
use super::group::{GroupStream, ParallelSeriesPipeline};
use super::hybrid::{contiguous_day_ranges, hybrid_day_span};
use super::{Snapshot, TimeRange};
use crate::aggregate_archive::AggregateArchive;
use crate::cache::BlockCacheAdmission;
use crate::error::{Error, Result, corrupt, invalid};
use crate::layout::shard_for;
use crate::query::QueryBudget;
use crate::segment::{QueryPlanReservation, QueryReadPlan, QuerySegmentRead, Segment, SliceCursor};
use crate::types::{
    AggregateResolution, AggregateSet, AggregateState, DAY_US, GroupRequest, GroupScope,
    GroupSource, SeriesKey, utc_day_start,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

pub(super) struct ExactAggregatePlan {
    pub(super) segments: Vec<Arc<Segment>>,
    pub(super) archives: Vec<Arc<AggregateArchive>>,
}

pub(super) struct HybridAggregatePlan {
    pub(super) days: BTreeMap<i64, HybridDayPlan>,
    pub(super) first_day_us: i64,
}

pub(super) struct HybridDayPlan {
    pub(super) summary_segments: Vec<Arc<Segment>>,
    pub(super) base_segments: Vec<Arc<Segment>>,
    pub(super) base_ranges: Vec<TimeRange>,
    pub(super) persisted_segments: Vec<Arc<Segment>>,
    pub(super) block_summary: Option<AggregateDayState>,
    pub(super) has_raw_sources: bool,
}

fn hybrid_summaries_cover_block(
    days: &BTreeMap<i64, HybridDayPlan>,
    segment_id: u64,
    minimum_us: i64,
    maximum_us: i64,
) -> bool {
    let Some(first_day) = minimum_us.div_euclid(DAY_US).checked_mul(DAY_US) else {
        return false;
    };
    let Some(last_day) = maximum_us.div_euclid(DAY_US).checked_mul(DAY_US) else {
        return false;
    };
    if minimum_us > maximum_us {
        return false;
    }
    let expected_days = (i128::from(last_day) - i128::from(first_day)) / i128::from(DAY_US) + 1;
    if expected_days > days.len() as i128 {
        return false;
    }
    let mut covered_days = 0_usize;
    for (_, plan) in days.range(first_day..=last_day) {
        if !plan
            .summary_segments
            .iter()
            .any(|segment| segment.descriptor().id == segment_id)
        {
            return false;
        }
        covered_days += 1;
    }
    covered_days as i128 == expected_days
}

// Prefix maxima for two distinct read owners answer "does another read
// overlap?" without scanning every block of every read for each candidate.
struct ReadRangePrefix {
    from_us: i64,
    maximum_us: i64,
    owner: usize,
    other_maximum_us: Option<i64>,
}

pub(super) struct HybridReadOverlap {
    ranges: Vec<ReadRangePrefix>,
    pub(super) budget: Arc<QueryBudget>,
    pub(super) bytes: u64,
}

impl HybridReadOverlap {
    pub(super) fn build(reads: &[QuerySegmentRead], budget: Arc<QueryBudget>) -> Option<Self> {
        if reads.len() < 8 {
            return None;
        }
        let count = reads.iter().try_fold(0_usize, |count, read| {
            count.checked_add(read.time_ranges().len())
        })?;
        Self::from_ranges(
            count,
            reads.iter().enumerate().flat_map(|(owner, read)| {
                read.time_ranges()
                    .map(move |(from_us, maximum_us)| (owner, from_us, maximum_us))
            }),
            budget,
        )
    }

    pub(super) fn from_ranges(
        count: usize,
        ranges: impl Iterator<Item = (usize, i64, i64)>,
        budget: Arc<QueryBudget>,
    ) -> Option<Self> {
        let bytes =
            u64::try_from(count.checked_mul(std::mem::size_of::<ReadRangePrefix>())?).ok()?;
        if !budget.try_reserve_memory_class(crate::query::QueryMemoryClass::Plan, bytes) {
            return None;
        }
        let mut index = Self {
            ranges: Vec::with_capacity(count),
            budget,
            bytes,
        };
        for (owner, from_us, maximum_us) in ranges {
            if index.ranges.len() == count {
                return None;
            }
            index.ranges.push(ReadRangePrefix {
                owner,
                from_us,
                maximum_us,
                other_maximum_us: None,
            });
        }
        index.ranges.sort_unstable_by_key(|range| range.from_us);
        let mut first = None::<(i64, usize)>;
        let mut second = None::<i64>;
        for range in &mut index.ranges {
            match first {
                None => first = Some((range.maximum_us, range.owner)),
                Some((maximum, owner)) if owner == range.owner => {
                    first = Some((maximum.max(range.maximum_us), owner));
                }
                Some((maximum, _)) if range.maximum_us > maximum => {
                    second = Some(maximum);
                    first = Some((range.maximum_us, range.owner));
                }
                Some(_) => {
                    second = Some(
                        second.map_or(range.maximum_us, |maximum| maximum.max(range.maximum_us)),
                    )
                }
            }
            if let Some((maximum, owner)) = first {
                range.maximum_us = maximum;
                range.owner = owner;
                range.other_maximum_us = second;
            }
        }
        Some(index)
    }

    pub(super) fn overlaps_other(&self, owner: usize, from_us: i64, to_us: i64) -> bool {
        let end = self.ranges.partition_point(|range| range.from_us < to_us);
        let Some(prefix) = end.checked_sub(1).map(|index| &self.ranges[index]) else {
            return false;
        };
        if prefix.owner == owner {
            prefix
                .other_maximum_us
                .is_some_and(|maximum| maximum >= from_us)
        } else {
            prefix.maximum_us >= from_us
        }
    }
}

impl Drop for HybridReadOverlap {
    fn drop(&mut self) {
        self.ranges = Vec::new();
        self.budget
            .release_memory_class(crate::query::QueryMemoryClass::Plan, self.bytes);
    }
}

struct HybridDayCandidate {
    read_index: usize,
    segment: Arc<Segment>,
    block_summary: Option<AggregateDayState>,
    range: TimeRange,
    needs_overlay_base: bool,
}

pub(super) struct AggregateDayState {
    pub(super) state: AggregateState,
    pub(super) records: u64,
    pub(super) decoded_bytes: u64,
}

pub(super) fn group_series_from(
    snapshot: &Snapshot,
    request: &GroupRequest,
    resolution: Option<AggregateResolution>,
    series: SeriesKey,
) -> i64 {
    let raw_from = snapshot
        .retention_cutoffs
        .get(&series)
        .copied()
        .map_or(request.from_us, |cutoff| request.from_us.max(cutoff));
    let Some(resolution) = resolution else {
        return raw_from;
    };
    if request.source != GroupSource::Auto
        || !snapshot
            .retained_aggregate_resolutions
            .get(&series)
            .is_some_and(|resolutions| resolutions.contains(&resolution))
    {
        return raw_from;
    }
    snapshot
        .aggregate_retention_cutoffs
        .get(&series)
        .copied()
        .map_or(raw_from, |cutoff| request.from_us.max(cutoff))
}
impl GroupStream {
    pub(super) fn new(mut snapshot: Snapshot, mut request: GroupRequest) -> Result<Self> {
        request.validate()?;
        let cache_admission = if request.series.len() == 1 {
            BlockCacheAdmission::Always
        } else {
            BlockCacheAdmission::Repeated
        };
        let query_metrics = QueryBudget::new_with_admission(
            request.options.clone(),
            Arc::clone(&snapshot.work_memory_governor),
        )?;
        let planning_started = Instant::now();
        let preprocessing_bytes = u64::try_from(request.series.len())
            .ok()
            .and_then(|series| series.checked_mul(32))
            .ok_or_else(|| invalid("query preprocessing memory overflows u64"))?;
        query_metrics.reserve_memory(preprocessing_bytes)?;
        let mut ordered_series = request.series.clone();
        ordered_series.sort_unstable();
        request.series.clone_from(&ordered_series);
        let aggregate_resolution = (request.source == GroupSource::Auto)
            .then(|| request.grouping.aggregate_resolution())
            .flatten();
        // A wide UTC-day count/sum query already needs one shared raw block plan
        // for its active tail. Its selected BlockIndex entries carry the same
        // exact count/sum state as TSAGG003, and the hybrid planner proves that
        // every accepted range is disjoint before using it. Avoid opening and
        // scanning a large aggregate root for every sparse segment; fall back to
        // persisted daily records when a boundary block prevents using complete
        // blocks beside it. Both paths still reject overlapping sources; other
        // unavailable summaries fall back to raw MVCC. Aggregate archives remain
        // authoritative after raw retention.
        let prefer_wide_block_summaries = request.series.len() >= 32
            && request.grouping.is_utc_day()
            && request.aggregates == AggregateSet::COUNT_SUM
            && snapshot.aggregate_archives.is_empty();
        let memtable_planning_started = Instant::now();
        let mut batches_by_series = HashMap::<SeriesKey, Vec<QueryBatch>>::new();
        for batch in &snapshot.batches {
            if batch.is_empty() {
                continue;
            }
            if ordered_series.len() >= 32 {
                batch.for_each_requested_series(
                    &ordered_series,
                    |series, minimum_timestamp_us, maximum_timestamp_us| {
                        let from_us =
                            group_series_from(&snapshot, &request, aggregate_resolution, series);
                        if maximum_timestamp_us >= from_us && minimum_timestamp_us < request.to_us {
                            batches_by_series
                                .entry(series)
                                .or_default()
                                .push(QueryBatch {
                                    batch: Arc::clone(batch),
                                    minimum_timestamp_us,
                                    maximum_timestamp_us,
                                });
                        }
                    },
                );
                continue;
            }
            let Some(first) = batch.first() else {
                continue;
            };
            let Some(last) = batch.last() else {
                continue;
            };
            let start = ordered_series.partition_point(|series| *series < first.key.series);
            let end = ordered_series.partition_point(|series| *series <= last.key.series);
            for series in &ordered_series[start..end] {
                let from_us = group_series_from(&snapshot, &request, aggregate_resolution, *series);
                if from_us < request.to_us
                    && let Some((minimum_timestamp_us, maximum_timestamp_us)) =
                        SliceCursor::series_time_bounds(batch, *series, from_us, request.to_us)
                {
                    batches_by_series
                        .entry(*series)
                        .or_default()
                        .push(QueryBatch {
                            batch: Arc::clone(batch),
                            minimum_timestamp_us,
                            maximum_timestamp_us,
                        });
                }
            }
        }
        snapshot.batches.clear();
        query_metrics.record_memtable_planning(memtable_planning_started.elapsed());
        let mut series_by_shard = BTreeMap::<u16, Vec<(SeriesKey, i64)>>::new();
        for series in &ordered_series {
            let from_us = group_series_from(&snapshot, &request, aggregate_resolution, *series);
            if from_us < request.to_us {
                series_by_shard
                    .entry(shard_for(*series, snapshot.virtual_shards))
                    .or_default()
                    .push((*series, from_us));
            }
        }
        let possible_exact = !prefer_wide_block_summaries
            && aggregate_resolution.is_some_and(|resolution| {
                request.from_us.rem_euclid(resolution.duration_us()) == 0
                    && request.to_us.rem_euclid(resolution.duration_us()) == 0
            });
        let exact_aggregate_planning_started = Instant::now();
        let mut candidate_exact_segments = HashMap::<SeriesKey, Vec<Arc<Segment>>>::new();
        let mut descriptors_examined = 0_u64;
        let mut series_index_lookups = 0_u64;
        if let Some(resolution) = aggregate_resolution.filter(|_| possible_exact) {
            for (shard_id, series_ranges) in &series_by_shard {
                let shard_from_us = series_ranges
                    .iter()
                    .map(|(_, from_us)| *from_us)
                    .min()
                    .unwrap_or(request.from_us);
                let aggregate_series = series_ranges
                    .iter()
                    .filter_map(|(series, _)| {
                        (!batches_by_series.contains_key(series)).then_some(*series)
                    })
                    .collect::<Vec<_>>();
                for segment in snapshot
                    .segments_by_shard
                    .get(shard_id)
                    .into_iter()
                    .flatten()
                {
                    descriptors_examined = descriptors_examined.saturating_add(1);
                    if segment.descriptor().min_commit > snapshot.commit_id {
                        continue;
                    }
                    if segment.descriptor().max_timestamp_us < shard_from_us
                        || segment.descriptor().min_timestamp_us >= request.to_us
                    {
                        continue;
                    }
                    if segment.descriptor().level > 0 && !aggregate_series.is_empty() {
                        segment.prepare_aggregates_for_query(
                            &aggregate_series,
                            resolution,
                            &query_metrics,
                        )?;
                    }
                    for (series, from_us) in series_ranges {
                        if batches_by_series.contains_key(series) {
                            continue;
                        }
                        series_index_lookups = series_index_lookups.saturating_add(1);
                        if segment.descriptor().level > 0
                            && segment
                                .aggregate_cursor_for_query(
                                    *series,
                                    resolution,
                                    *from_us,
                                    request.to_us,
                                    &query_metrics,
                                )?
                                .is_some()
                        {
                            query_metrics.reserve_memory(32)?;
                            candidate_exact_segments
                                .entry(*series)
                                .or_default()
                                .push(Arc::clone(segment));
                            continue;
                        }
                        let blocks = segment.query_series_block_count(
                            *series,
                            *from_us,
                            request.to_us,
                            &query_metrics,
                        )?;
                        if blocks == 0 {
                            continue;
                        }
                        query_metrics.reserve_memory(32)?;
                        candidate_exact_segments
                            .entry(*series)
                            .or_default()
                            .push(Arc::clone(segment));
                    }
                }
            }
        }

        let mut candidate_archives = HashMap::<SeriesKey, Vec<Arc<AggregateArchive>>>::new();
        if let Some(resolution) = aggregate_resolution.filter(|_| possible_exact) {
            for series in &ordered_series {
                let Some(policy_id) = snapshot.series_policy_ids.get(series).copied() else {
                    continue;
                };
                for archive in &snapshot.aggregate_archives {
                    let descriptor = archive.descriptor();
                    if descriptor.policy_id == policy_id
                        && descriptor.shard_id == shard_for(*series, snapshot.virtual_shards)
                        && descriptor.resolutions.contains(&resolution)
                        && descriptor
                            .minimum_series
                            .is_none_or(|minimum| *series >= minimum)
                        && descriptor
                            .maximum_series
                            .is_none_or(|maximum| *series <= maximum)
                        && descriptor.coverage_from_us < request.to_us
                        && descriptor.coverage_to_us
                            > group_series_from(&snapshot, &request, aggregate_resolution, *series)
                    {
                        candidate_archives
                            .entry(*series)
                            .or_default()
                            .push(Arc::clone(archive));
                    }
                }
            }
        }

        let mut exact_aggregate_plans_by_series = HashMap::new();
        let archive_aggregate_plans_by_series = candidate_archives.clone();
        for series in &ordered_series {
            let mut segments = candidate_exact_segments.remove(series).unwrap_or_default();
            let mut archives = candidate_archives.remove(series).unwrap_or_default();
            let series_from_us =
                group_series_from(&snapshot, &request, aggregate_resolution, *series);
            segments.sort_by_key(|segment| segment.descriptor().window_start_us);
            archives.sort_by_key(|archive| {
                (
                    archive.descriptor().coverage_from_us,
                    archive.descriptor().chunk_index,
                )
            });
            let disjoint = segments.windows(2).all(|pair| {
                pair[0].descriptor().window_end_us <= pair[1].descriptor().window_start_us
            });
            let archives_disjoint = archives.windows(2).all(|pair| {
                let left = pair[0].descriptor();
                let right = pair[1].descriptor();
                left.coverage_to_us <= right.coverage_from_us
                    || (left.coverage_from_us == right.coverage_from_us
                        && left.coverage_to_us == right.coverage_to_us
                        && left.chunk_count == right.chunk_count
                        && left.chunk_index.saturating_add(1) == right.chunk_index)
            });
            let complete = aggregate_resolution
                .is_some_and(|resolution| series_from_us.rem_euclid(resolution.duration_us()) == 0)
                && (!segments.is_empty() || !archives.is_empty())
                && !batches_by_series.contains_key(series)
                && disjoint
                && archives_disjoint
                && segments
                    .iter()
                    .all(|segment| segment.descriptor().level > 0)
                && segments.iter().try_fold(true, |complete, segment| {
                    Ok::<_, Error>(
                        complete
                            && segment
                                .aggregate_cursor_for_query(
                                    *series,
                                    aggregate_resolution.ok_or_else(|| {
                                        corrupt("exact aggregate resolution disappeared")
                                    })?,
                                    series_from_us,
                                    request.to_us,
                                    &query_metrics,
                                )?
                                .is_some(),
                    )
                })?;
            if complete {
                exact_aggregate_plans_by_series
                    .insert(*series, ExactAggregatePlan { segments, archives });
            }
        }
        query_metrics.record_aggregate_planning(exact_aggregate_planning_started.elapsed());

        let segment_planning_started = Instant::now();
        let mut plan_reservation = QueryPlanReservation::new(Arc::clone(&query_metrics));
        let mut planned_segments_by_series = HashMap::<SeriesKey, Vec<QuerySegmentRead>>::new();
        let mut all_planned_ranges = series_by_shard
            .values()
            .flatten()
            .copied()
            .filter(|(series, _)| !exact_aggregate_plans_by_series.contains_key(series))
            .collect::<Vec<_>>();
        all_planned_ranges.sort_unstable_by_key(|(series, _)| *series);
        if prefer_wide_block_summaries && snapshot.query_executor.is_parallel() {
            let mut tasks = Vec::new();
            if let Some(directory) = &snapshot.offline_pack_directory {
                tasks.push(QueryPlanningTask::Directory {
                    directory: Arc::clone(directory),
                    series_ranges: all_planned_ranges.clone(),
                    to_us: request.to_us,
                });
            } else {
                for directory in &snapshot.published_read_directories {
                    let (policy_id, window_start_us, window_end_us) =
                        directory
                            .published_scope()
                            .ok_or_else(|| corrupt("published read generation has no scope"))?;
                    if window_start_us >= request.to_us || window_end_us <= request.from_us {
                        continue;
                    }
                    let planned_ranges = all_planned_ranges
                        .iter()
                        .copied()
                        .filter(|(series, from_us)| {
                            *from_us < window_end_us
                                && snapshot.series_policy_ids.get(series) == Some(&policy_id)
                                && directory.shard_scope().is_none_or(|shard| {
                                    shard_for(*series, snapshot.virtual_shards) == shard
                                })
                        })
                        .collect::<Vec<_>>();
                    if planned_ranges.is_empty() {
                        continue;
                    }
                    tasks.push(QueryPlanningTask::Directory {
                        directory: Arc::clone(directory),
                        series_ranges: planned_ranges,
                        to_us: request.to_us,
                    });
                }
                for (shard_id, series_ranges) in &series_by_shard {
                    let planned_ranges = series_ranges
                        .iter()
                        .copied()
                        .filter(|(series, _)| !exact_aggregate_plans_by_series.contains_key(series))
                        .collect::<Vec<_>>();
                    if planned_ranges.is_empty() {
                        continue;
                    }
                    let shard_from_us = planned_ranges
                        .iter()
                        .map(|(_, from_us)| *from_us)
                        .min()
                        .unwrap_or(request.from_us);
                    let segments = snapshot
                        .segments_by_shard
                        .get(shard_id)
                        .into_iter()
                        .flatten()
                        .filter(|segment| {
                            !snapshot
                                .published_read_directories
                                .iter()
                                .any(|directory| directory.covers_segment(segment.descriptor()))
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if segments.is_empty() {
                        continue;
                    }
                    tasks.push(QueryPlanningTask::Segments {
                        segments,
                        series_ranges: planned_ranges,
                        from_us: shard_from_us,
                        to_us: request.to_us,
                        commit_id: snapshot.commit_id,
                    });
                }
            }
            for result in
                snapshot
                    .query_executor
                    .plan(tasks, snapshot.query_lease(), &query_metrics)?
            {
                descriptors_examined =
                    descriptors_examined.saturating_add(result.descriptors_examined);
                series_index_lookups =
                    series_index_lookups.saturating_add(result.series_index_lookups);
                plan_reservation.absorb(result.reservation)?;
                for (series, planned) in result.planned {
                    planned_segments_by_series
                        .entry(series)
                        .or_default()
                        .push(planned);
                }
            }
        } else {
            if let Some(directory) = &snapshot.offline_pack_directory {
                series_index_lookups = series_index_lookups
                    .saturating_add(u64::try_from(all_planned_ranges.len()).unwrap_or(u64::MAX));
                for (series, planned) in directory.plan_query_series(
                    &all_planned_ranges,
                    request.to_us,
                    &query_metrics,
                    &mut plan_reservation,
                )? {
                    planned_segments_by_series
                        .entry(series)
                        .or_default()
                        .push(planned);
                }
            } else {
                for directory in &snapshot.published_read_directories {
                    let (policy_id, window_start_us, window_end_us) =
                        directory
                            .published_scope()
                            .ok_or_else(|| corrupt("published read generation has no scope"))?;
                    if window_start_us >= request.to_us || window_end_us <= request.from_us {
                        continue;
                    }
                    let planned_ranges = all_planned_ranges
                        .iter()
                        .copied()
                        .filter(|(series, from_us)| {
                            *from_us < window_end_us
                                && snapshot.series_policy_ids.get(series) == Some(&policy_id)
                                && directory.shard_scope().is_none_or(|shard| {
                                    shard_for(*series, snapshot.virtual_shards) == shard
                                })
                        })
                        .collect::<Vec<_>>();
                    if planned_ranges.is_empty() {
                        continue;
                    }
                    series_index_lookups = series_index_lookups
                        .saturating_add(u64::try_from(planned_ranges.len()).unwrap_or(u64::MAX));
                    for (series, planned) in directory.plan_query_series(
                        &planned_ranges,
                        request.to_us,
                        &query_metrics,
                        &mut plan_reservation,
                    )? {
                        planned_segments_by_series
                            .entry(series)
                            .or_default()
                            .push(planned);
                    }
                }
            }
            if snapshot.offline_pack_directory.is_none() {
                for (shard_id, series_ranges) in &series_by_shard {
                    let planned_ranges = series_ranges
                        .iter()
                        .copied()
                        .filter(|(series, _)| !exact_aggregate_plans_by_series.contains_key(series))
                        .collect::<Vec<_>>();
                    if planned_ranges.is_empty() {
                        continue;
                    }
                    let shard_from_us = planned_ranges
                        .iter()
                        .map(|(_, from_us)| *from_us)
                        .min()
                        .unwrap_or(request.from_us);
                    for segment in snapshot
                        .segments_by_shard
                        .get(shard_id)
                        .into_iter()
                        .flatten()
                    {
                        if snapshot
                            .published_read_directories
                            .iter()
                            .any(|directory| directory.covers_segment(segment.descriptor()))
                        {
                            continue;
                        }
                        descriptors_examined = descriptors_examined.saturating_add(1);
                        if segment.descriptor().min_commit > snapshot.commit_id {
                            continue;
                        }
                        if segment.descriptor().max_timestamp_us < shard_from_us
                            || segment.descriptor().min_timestamp_us >= request.to_us
                        {
                            continue;
                        }
                        series_index_lookups = series_index_lookups.saturating_add(
                            u64::try_from(planned_ranges.len()).unwrap_or(u64::MAX),
                        );
                        for (series, planned) in segment.plan_query_series(
                            &planned_ranges,
                            request.to_us,
                            &query_metrics,
                            &mut plan_reservation,
                        )? {
                            planned_segments_by_series
                                .entry(series)
                                .or_default()
                                .push(planned);
                        }
                    }
                }
            }
        }
        query_metrics.record_segment_planning(segment_planning_started.elapsed());
        let hybrid_aggregate_planning_started = Instant::now();
        let mut hybrid_aggregate_plans_by_series = HashMap::new();
        if request.grouping.is_utc_day() && request.source == GroupSource::Auto {
            for series in &ordered_series {
                if exact_aggregate_plans_by_series.contains_key(series) {
                    continue;
                }
                let from_us = group_series_from(&snapshot, &request, aggregate_resolution, *series);
                let Some((first_day_us, day_limit_us)) = hybrid_day_span(from_us, request.to_us)?
                else {
                    continue;
                };
                let Some(planned_reads) = planned_segments_by_series.get_mut(series) else {
                    continue;
                };
                let read_overlap =
                    HybridReadOverlap::build(planned_reads, Arc::clone(&query_metrics));
                let batch_overlap = QueryBatchOverlap::new(
                    batches_by_series.get(series).map_or(&[], Vec::as_slice),
                );
                let overlaps_other_read = |owner: usize, from_us: i64, to_us: i64| {
                    read_overlap.as_ref().map_or_else(
                        || {
                            planned_reads.iter().enumerate().any(|(index, read)| {
                                index != owner && read.intersects_time_range(from_us, to_us)
                            })
                        },
                        |index| index.overlaps_other(owner, from_us, to_us),
                    )
                };
                let mut candidates_by_day = BTreeMap::<i64, Vec<HybridDayCandidate>>::new();
                for (read_index, planned) in planned_reads.iter().enumerate() {
                    query_metrics.check()?;
                    let descriptor = planned.segment().descriptor();
                    // An unaligned single-series query already loaded a block
                    // plan. If that plan fully covers a disjoint, single-day
                    // segment, its count/sum is sufficient: do not load the same
                    // information again from the persisted aggregate section.
                    if ordered_series.len() == 1
                        && !possible_exact
                        && request.aggregates == AggregateSet::COUNT_SUM
                        && snapshot.aggregate_archives.is_empty()
                        && snapshot.strictly_increasing_series.contains(series)
                        && descriptor.level > 0
                        && descriptor.min_timestamp_us >= from_us
                        && descriptor.max_timestamp_us < request.to_us
                    {
                        let day_start_us = utc_day_start(descriptor.min_timestamp_us)?;
                        if let Some(day_end_us) = day_start_us.checked_add(DAY_US)
                            && descriptor.max_timestamp_us < day_end_us
                            && let Some((state, records, range_from_us, range_to_us)) =
                                planned.count_sum_for_range(day_start_us, day_end_us)?
                            && !overlaps_other_read(read_index, range_from_us, range_to_us)
                            && !batch_overlap.intersects(range_from_us, range_to_us)
                        {
                            candidates_by_day.entry(day_start_us).or_default().push(
                                HybridDayCandidate {
                                    read_index,
                                    segment: Arc::clone(planned.segment()),
                                    block_summary: Some(AggregateDayState {
                                        state,
                                        records,
                                        decoded_bytes: records.saturating_mul(24),
                                    }),
                                    range: TimeRange {
                                        from_us: range_from_us,
                                        to_us: range_to_us,
                                    },
                                    needs_overlay_base: false,
                                },
                            );
                            continue;
                        }
                    }
                    let mut persisted_days = HashSet::new();
                    if planned.segment().descriptor().level > 0
                        && (!prefer_wide_block_summaries
                            || planned.has_daily_summary_fallback_candidate())
                        && let Some(mut cursor) = planned.segment().aggregate_cursor_for_query(
                            *series,
                            AggregateResolution::Day,
                            first_day_us,
                            day_limit_us,
                            &query_metrics,
                        )?
                    {
                        while let Some(record) = cursor.next_record()? {
                            let day_start_us = record.bucket_start_us;
                            let day_end_us = record.bucket_end_us;
                            let full_day_is_requested =
                                from_us <= day_start_us && request.to_us >= day_end_us;
                            let segment_is_inside_query =
                                planned.segment().descriptor().min_timestamp_us >= from_us
                                    && planned.segment().descriptor().max_timestamp_us
                                        < request.to_us;
                            if (full_day_is_requested || segment_is_inside_query)
                                && let Some((range_from_us, range_to_us)) =
                                    planned.time_range_within(day_start_us, day_end_us)?
                            {
                                // A persisted aggregate does not encode enough version
                                // information to subtract an overlapping mutable source
                                // safely. Keep the fast path only when the summary owns
                                // this series/time range exclusively; otherwise the raw
                                // MVCC merge remains the source of truth.
                                let overlaps_other_source =
                                    overlaps_other_read(read_index, range_from_us, range_to_us)
                                        || batch_overlap.intersects(range_from_us, range_to_us);
                                if overlaps_other_source {
                                    continue;
                                }
                                record.total()?;
                                persisted_days.insert(day_start_us);
                                candidates_by_day.entry(day_start_us).or_default().push(
                                    HybridDayCandidate {
                                        read_index,
                                        segment: Arc::clone(planned.segment()),
                                        block_summary: None,
                                        range: TimeRange {
                                            from_us: range_from_us,
                                            to_us: range_to_us,
                                        },
                                        needs_overlay_base: true,
                                    },
                                );
                            }
                        }
                    }
                    if request.aggregates == AggregateSet::COUNT_SUM
                        && (planned.segment().descriptor().level == 0 || persisted_days.is_empty())
                    {
                        let descriptor = planned.segment().descriptor();
                        let mut day_start_us =
                            utc_day_start(descriptor.min_timestamp_us.max(first_day_us))?;
                        while day_start_us < day_limit_us
                            && day_start_us <= descriptor.max_timestamp_us
                        {
                            let day_end_us = day_start_us
                                .checked_add(DAY_US)
                                .ok_or_else(|| corrupt("UTC day boundary overflows i64"))?;
                            let full_day_is_requested =
                                from_us <= day_start_us && request.to_us >= day_end_us;
                            let segment_is_inside_query = descriptor.min_timestamp_us >= from_us
                                && descriptor.max_timestamp_us < request.to_us;
                            if !persisted_days.contains(&day_start_us)
                                && (full_day_is_requested || segment_is_inside_query)
                                && let Some((state, records, range_from_us, range_to_us)) =
                                    planned.count_sum_for_range(day_start_us, day_end_us)?
                            {
                                candidates_by_day.entry(day_start_us).or_default().push(
                                    HybridDayCandidate {
                                        read_index,
                                        segment: Arc::clone(planned.segment()),
                                        block_summary: Some(AggregateDayState {
                                            state,
                                            records,
                                            decoded_bytes: records.saturating_mul(24),
                                        }),
                                        range: TimeRange {
                                            from_us: range_from_us,
                                            to_us: range_to_us,
                                        },
                                        needs_overlay_base: false,
                                    },
                                );
                            }
                            day_start_us = day_end_us;
                        }
                    }
                }
                let mut days = BTreeMap::new();
                let mut fully_aggregated_days = Vec::new();
                for (day_start_us, candidates) in candidates_by_day {
                    query_metrics.check()?;
                    let day_end_us = day_start_us
                        .checked_add(DAY_US)
                        .ok_or_else(|| corrupt("UTC day boundary overflows i64"))?;
                    // L1-сводка остаётся базой даже при более новых L0 и
                    // memtable-операциях. Runtime применит их как MVCC-overlay:
                    // для совпавшего timestamp вычтет старое L1-значение и
                    // добавит победившую новую версию.
                    let mut candidates = candidates;
                    candidates.sort_unstable_by_key(|candidate| {
                        let descriptor = candidate.segment.descriptor();
                        (
                            candidate.range.from_us,
                            candidate.range.to_us,
                            descriptor.min_commit,
                            descriptor.max_commit,
                            descriptor.id,
                        )
                    });
                    let mut selected = Vec::<usize>::new();
                    for candidate in 0..candidates.len() {
                        let candidate_range = candidates[candidate].range;
                        if candidates[candidate].block_summary.is_some() {
                            let overlaps_other_read = overlaps_other_read(
                                candidates[candidate].read_index,
                                candidate_range.from_us,
                                candidate_range.to_us,
                            );
                            let overlaps_batch = batch_overlap
                                .intersects(candidate_range.from_us, candidate_range.to_us);
                            if overlaps_other_read || overlaps_batch {
                                continue;
                            }
                        }
                        if selected.last().is_none_or(|selected_index| {
                            candidates[*selected_index].range.to_us <= candidate_range.from_us
                        }) {
                            selected.push(candidate);
                        }
                    }
                    if selected.is_empty() {
                        continue;
                    }
                    let has_raw_batches = batch_overlap.intersects(day_start_us, day_end_us);
                    let selected_set = selected
                        .iter()
                        .map(|index| candidates[*index].read_index)
                        .collect::<HashSet<_>>();
                    let has_raw_reads = planned_reads.iter().enumerate().any(|(index, read)| {
                        !selected_set.contains(&index)
                            && read.intersects_time_range(day_start_us, day_end_us)
                    });
                    let has_raw_sources = has_raw_batches || has_raw_reads;
                    if !has_raw_sources {
                        fully_aggregated_days.push(day_start_us);
                    }
                    let mut summary_segments = Vec::with_capacity(selected.len());
                    let mut base_segments = Vec::new();
                    let mut base_ranges = Vec::new();
                    let mut persisted_segments = Vec::new();
                    let mut block_summary = None::<AggregateDayState>;
                    for index in selected {
                        let candidate = &candidates[index];
                        summary_segments.push(Arc::clone(&candidate.segment));
                        if candidate.needs_overlay_base {
                            let overlaps_raw_read =
                                planned_reads.iter().enumerate().any(|(read_index, read)| {
                                    !selected_set.contains(&read_index)
                                        && read.intersects_time_range(
                                            candidate.range.from_us,
                                            candidate.range.to_us,
                                        )
                                });
                            let overlaps_raw_batch = batch_overlap
                                .intersects(candidate.range.from_us, candidate.range.to_us);
                            if overlaps_raw_read || overlaps_raw_batch {
                                base_segments.push(Arc::clone(&candidate.segment));
                                base_ranges.push(candidate.range);
                            }
                        }
                        if let Some(summary) = &candidate.block_summary {
                            let aggregate =
                                block_summary.get_or_insert_with(|| AggregateDayState {
                                    state: AggregateState::for_aggregates(AggregateSet::COUNT_SUM),
                                    records: 0,
                                    decoded_bytes: 0,
                                });
                            aggregate.state.merge(summary.state)?;
                            aggregate.records = aggregate.records.saturating_add(summary.records);
                            aggregate.decoded_bytes = aggregate
                                .decoded_bytes
                                .saturating_add(summary.decoded_bytes);
                        } else {
                            persisted_segments.push(Arc::clone(&candidate.segment));
                        }
                    }
                    base_ranges.sort_unstable_by_key(|range| range.from_us);
                    let reservation =
                        u64::try_from(summary_segments.len().saturating_add(base_segments.len()))
                            .unwrap_or(u64::MAX)
                            .saturating_mul(std::mem::size_of::<Arc<Segment>>() as u64)
                            .saturating_add(
                                u64::try_from(base_ranges.len())
                                    .unwrap_or(u64::MAX)
                                    .saturating_mul(std::mem::size_of::<TimeRange>() as u64),
                            )
                            .saturating_add(32);
                    query_metrics.reserve_memory(reservation)?;
                    days.insert(
                        day_start_us,
                        HybridDayPlan {
                            summary_segments,
                            base_segments,
                            base_ranges,
                            persisted_segments,
                            block_summary,
                            has_raw_sources,
                        },
                    );
                }
                if days.is_empty() {
                    continue;
                }
                let aggregate_ranges = contiguous_day_ranges(&fully_aggregated_days)?;
                let mut removed_blocks = planned_reads.iter_mut().fold(0_u64, |removed, read| {
                    removed.saturating_add(read.remove_blocks_covered_by(&aggregate_ranges))
                });
                if snapshot.strictly_increasing_series.contains(series) {
                    // Strict overlays never point-read the summarized base.
                    // Keeping these blocks in the shared read plan lets a raw
                    // boundary read prefetch data the executor will only skip.
                    for read in planned_reads.iter_mut() {
                        let segment_id = read.segment().descriptor().id;
                        removed_blocks = removed_blocks.saturating_add(read.remove_blocks_where(
                            |minimum, maximum| {
                                hybrid_summaries_cover_block(&days, segment_id, minimum, maximum)
                            },
                        ));
                    }
                }
                planned_reads.retain(|read| !read.is_empty());
                plan_reservation.release_blocks(removed_blocks);
                hybrid_aggregate_plans_by_series
                    .insert(*series, HybridAggregatePlan { days, first_day_us });
            }
        }
        query_metrics.record_aggregate_planning(hybrid_aggregate_planning_started.elapsed());
        query_metrics.record_planned_segment_reads(
            u64::try_from(
                planned_segments_by_series
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
            )
            .unwrap_or(u64::MAX),
        );
        let read_plan = QueryReadPlan::new(
            planned_segments_by_series.values().flatten(),
            Arc::clone(&query_metrics),
            Arc::clone(&snapshot.open_file_cache),
            plan_reservation,
        )?;
        snapshot.segments_by_shard.clear();
        query_metrics.record_plan(
            descriptors_examined,
            series_index_lookups,
            planning_started.elapsed(),
        );
        let parallel_daily = request.scope == GroupScope::PerKey
            && request.grouping.is_utc_day()
            && request.series.len() >= 32
            && snapshot.query_executor.is_parallel();
        let parallel_pipeline = parallel_daily
            .then(|| ParallelSeriesPipeline::new(snapshot.query_executor.worker_count));
        Ok(Self {
            snapshot,
            request,
            query_metrics,
            planned_segments_by_series,
            exact_aggregate_plans_by_series,
            archive_aggregate_plans_by_series,
            hybrid_aggregate_plans_by_series,
            cache_admission,
            read_plan: Some(read_plan),
            batches_by_series,
            parallel_daily,
            parallel_rows: VecDeque::new(),
            parallel_buffer_bytes: 0,
            parallel_pipeline,
            series_index: 0,
            current: None,
            combined: None,
            combined_initialized: false,
            reported_query_resources: false,
            finished: false,
            failed: false,
        })
    }
}
