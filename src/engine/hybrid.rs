//! Точное наложение свежего MVCC-хвоста на базовые суточные сводки.

use super::batch::{OrderedBatchCursor, QueryBatch, append_batch_sources};
use super::group_cursor::{SeriesGroupCursor, measure_mvcc};
use super::planning::{AggregateDayState, HybridDayPlan};
use super::{MAX_HYBRID_QUERY_DAYS, Metrics};
use crate::cache::BlockCacheAdmission;
use crate::error::{Result, corrupt, invalid};
use crate::query::QueryBudget;
use crate::segment::{
    OperationCursor, QueryReadPlan, QuerySegmentRead, Segment, SliceCursor, VisibleCursor,
};
use crate::types::{
    AggregateResolution, AggregateSet, AggregateState, DAY_US, GroupRow, Grouping, SeriesKey,
    VersionedPoint, utc_day_start,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) struct HybridGroupCursor {
    pub(super) series: SeriesKey,
    pub(super) strictly_increasing: bool,
    pub(super) from_us: i64,
    pub(super) to_us: i64,
    pub(super) next_day_us: i64,
    pub(super) commit_id: u64,
    pub(super) batches: Vec<QueryBatch>,
    pub(super) planned_segments: Vec<QuerySegmentRead>,
    pub(super) read_plan: Arc<QueryReadPlan>,
    pub(super) aggregate_days: BTreeMap<i64, HybridDayPlan>,
    pub(super) cache_admission: BlockCacheAdmission,
    pub(super) metrics: Arc<Metrics>,
    pub(super) query_metrics: Arc<QueryBudget>,
    pub(super) aggregates: AggregateSet,
}

impl HybridGroupCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        while self.next_day_us < self.to_us {
            let day_start_us = self.next_day_us;
            let day_end_us = day_start_us
                .checked_add(DAY_US)
                .ok_or_else(|| corrupt("UTC day boundary overflows i64"))?;
            self.next_day_us = day_end_us;
            let from_us = self.from_us.max(day_start_us);
            let to_us = self.to_us.min(day_end_us);
            if let Some(day_plan) = self.aggregate_days.remove(&day_start_us) {
                let aggregate = self.aggregate_day(day_start_us, day_end_us, &day_plan)?;
                let row = if day_plan.has_raw_sources {
                    self.overlay_day(
                        from_us,
                        to_us,
                        day_start_us,
                        day_end_us,
                        &day_plan,
                        aggregate,
                    )?
                } else {
                    Some(self.finish_aggregate_day(day_start_us, day_end_us, aggregate)?)
                };
                if row.is_some() {
                    return Ok(row);
                }
                continue;
            }
            if let Some(row) = self.exact_day(from_us, to_us, &[])? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub(super) fn aggregate_day(
        &self,
        from_us: i64,
        to_us: i64,
        plan: &HybridDayPlan,
    ) -> Result<AggregateDayState> {
        let mut aggregate = plan.block_summary.as_ref().map_or(
            AggregateDayState {
                state: AggregateState::default(),
                records: 0,
                decoded_bytes: 0,
            },
            |summary| AggregateDayState {
                state: summary.state,
                records: summary.records,
                decoded_bytes: summary.decoded_bytes,
            },
        );
        for segment in &plan.persisted_segments {
            let Some(mut cursor) = segment.aggregate_cursor_for_query(
                self.series,
                AggregateResolution::Day,
                from_us,
                to_us,
                &self.query_metrics,
            )?
            else {
                return Err(corrupt("planned daily aggregate section disappeared"));
            };
            let record = cursor
                .next_record()?
                .ok_or_else(|| corrupt("planned daily aggregate record disappeared"))?;
            if cursor.next_record()?.is_some() {
                return Err(corrupt("multiple aggregate records cover one UTC day"));
            }
            record.total()?;
            aggregate.state.merge(record.state)?;
            aggregate.records = aggregate.records.saturating_add(1);
            aggregate.decoded_bytes = aggregate
                .decoded_bytes
                .saturating_add(crate::aggregate::RECORD_SIZE as u64);
        }
        if aggregate.state.count == 0 || aggregate.records == 0 {
            return Err(corrupt("planned daily aggregate is empty"));
        }
        if self.strictly_increasing && self.aggregates == AggregateSet::COUNT_SUM {
            // Persisted records carry extended statistics. Project them before
            // appending the strict tail so Count+Sum does not run Welford,
            // extrema and first/last updates for every fresh point.
            let (count, sum, compensation) = aggregate.state.count_sum_parts();
            aggregate.state = AggregateState::from_count_sum_parts(count, sum, compensation)?;
        }
        Ok(aggregate)
    }

    fn finish_aggregate_day(
        &self,
        bucket_start_us: i64,
        bucket_end_us: i64,
        aggregate: AggregateDayState,
    ) -> Result<GroupRow> {
        self.metrics
            .query_aggregate_records_read
            .fetch_add(aggregate.records, Ordering::Relaxed);
        self.metrics
            .query_points_returned
            .fetch_add(aggregate.state.count, Ordering::Relaxed);
        if self.query_metrics.mark_exact_daily_aggregates() {
            self.metrics
                .query_exact_daily_aggregate_queries
                .fetch_add(1, Ordering::Relaxed);
        }
        self.query_metrics
            .record_aggregate_records(aggregate.records, aggregate.decoded_bytes)?;
        self.query_metrics.record_points(aggregate.state.count)?;
        GroupRow::from_state(
            Some(self.series),
            bucket_start_us,
            bucket_end_us,
            aggregate.state,
            self.aggregates,
        )
        .map(|row| row.with_source_resolution(AggregateResolution::Day))
    }

    fn overlay_day(
        &self,
        from_us: i64,
        to_us: i64,
        bucket_start_us: i64,
        bucket_end_us: i64,
        plan: &HybridDayPlan,
        aggregate: AggregateDayState,
    ) -> Result<Option<GroupRow>> {
        let query_metrics = Arc::clone(&self.query_metrics);
        measure_mvcc(&query_metrics, || {
            self.overlay_day_inner(
                from_us,
                to_us,
                bucket_start_us,
                bucket_end_us,
                plan,
                aggregate,
            )
        })
    }

    fn overlay_day_inner(
        &self,
        from_us: i64,
        to_us: i64,
        bucket_start_us: i64,
        bucket_end_us: i64,
        plan: &HybridDayPlan,
        mut aggregate: AggregateDayState,
    ) -> Result<Option<GroupRow>> {
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        let ordered_batches =
            if self.strictly_increasing && self.aggregates == AggregateSet::COUNT_SUM {
                let raw_maximum = self
                    .planned_segments
                    .iter()
                    .filter(|planned| {
                        !plan.summary_segments.iter().any(|segment| {
                            segment.descriptor().id == planned.segment().descriptor().id
                        })
                    })
                    .flat_map(QuerySegmentRead::time_ranges)
                    .filter(|(minimum, maximum)| *maximum >= from_us && *minimum < to_us)
                    .map(|(_, maximum)| maximum)
                    .max();
                let cursors = self
                    .batches
                    .iter()
                    .filter(|batch| batch.intersects(from_us, to_us))
                    .filter_map(|batch| {
                        SliceCursor::series(Arc::clone(&batch.batch), self.series, from_us, to_us)
                    })
                    .collect();
                match OrderedBatchCursor::try_new(cursors, Arc::clone(&self.query_metrics)) {
                    Ok(cursor)
                        if cursor
                            .cursors
                            .first()
                            .and_then(SliceCursor::key_bounds)
                            .is_some_and(|(first, _)| {
                                raw_maximum.is_none_or(|maximum| first.timestamp_us > maximum)
                            })
                            && cursor.only_visible_unique_puts(self.commit_id)? =>
                    {
                        Some(cursor)
                    }
                    _ => None,
                }
            } else {
                None
            };
        if ordered_batches.is_none() {
            append_batch_sources(
                &mut sources,
                &self.batches,
                self.series,
                (from_us, to_us),
                self.commit_id,
                &self.metrics,
                &self.query_metrics,
            )?;
        }
        for planned in &self.planned_segments {
            let summarized = plan
                .summary_segments
                .iter()
                .any(|segment| segment.descriptor().id == planned.segment().descriptor().id);
            if !summarized {
                if let Some(cursor) = planned.cursor(
                    from_us,
                    to_us,
                    self.cache_admission,
                    Arc::clone(&self.query_metrics),
                    Arc::clone(&self.read_plan),
                )? {
                    sources.push(cursor);
                }
                continue;
            }
        }
        let mut overlay = VisibleCursor::new(sources, self.commit_id)?;
        let needs_extrema = self.aggregates.contains(AggregateSet::MIN)
            || self.aggregates.contains(AggregateSet::MAX);
        let needs_edges = self.aggregates.contains(AggregateSet::FIRST)
            || self.aggregates.contains(AggregateSet::LAST)
            || self.aggregates.contains(AggregateSet::DELTA)
            || self.aggregates.contains(AggregateSet::RATE);
        let mut requires_exact = false;
        if self.strictly_increasing {
            while let Some(operation) = overlay.next_latest()? {
                self.query_metrics.check()?;
                if operation.is_delete {
                    return Err(corrupt(
                        "strictly increasing series contains a delete in its aggregate overlay",
                    ));
                }
                aggregate
                    .state
                    .add(operation.key.timestamp_us, operation.put_value()?)?;
            }
            if let Some(ordered) = &ordered_batches {
                // The proved memory ranges follow every raw disk range. Keep
                // exactly the same timestamp and floating-point addition order.
                ordered.append_aggregate(&mut aggregate.state, &self.metrics)?;
            }
        } else {
            const POINT_LOOKUP_THRESHOLD: usize = 8;
            let mut buffered = Vec::with_capacity(POINT_LOOKUP_THRESHOLD + 1);
            let mut exhausted = false;
            let may_overlap_base = |timestamp_us| {
                let end = plan
                    .base_ranges
                    .partition_point(|range| range.from_us <= timestamp_us);
                end > 0 && timestamp_us < plan.base_ranges[end - 1].to_us
            };
            while buffered.len() <= POINT_LOOKUP_THRESHOLD {
                let Some(operation) = overlay.next_latest()? else {
                    exhausted = true;
                    break;
                };
                self.query_metrics.check()?;
                if may_overlap_base(operation.key.timestamp_us) {
                    buffered.push(operation);
                } else if !operation.is_delete {
                    aggregate
                        .state
                        .add(operation.key.timestamp_us, operation.put_value()?)?;
                }
            }
            let mut apply = |operation: VersionedPoint,
                             base: Option<VersionedPoint>,
                             looked_up_base: bool|
             -> Result<()> {
                if looked_up_base {
                    self.query_metrics.record_aggregate_base_point_lookup();
                }
                if let Some(base) = base {
                    let safety = aggregate
                        .state
                        .remove(operation.key.timestamp_us, base.put_value()?)?;
                    requires_exact |=
                        (needs_extrema && !safety.extrema) || (needs_edges && !safety.edges);
                }
                if !operation.is_delete {
                    aggregate
                        .state
                        .add(operation.key.timestamp_us, operation.put_value()?)?;
                }
                Ok(())
            };
            if exhausted {
                for operation in buffered {
                    let base = self.base_point(&plan.base_segments, operation.key.timestamp_us)?;
                    apply(operation, base, true)?;
                }
            } else {
                let base_from_us = buffered
                    .first()
                    .map(|operation| operation.key.timestamp_us)
                    .ok_or_else(|| corrupt("aggregate overlay threshold lost its first point"))?;
                let mut base_sources: Vec<Box<dyn OperationCursor>> = Vec::new();
                for planned in &self.planned_segments {
                    let aggregate_base = plan.base_segments.iter().any(|segment| {
                        segment.descriptor().id == planned.segment().descriptor().id
                    });
                    if aggregate_base
                        && let Some(cursor) = planned.cursor(
                            base_from_us,
                            to_us,
                            self.cache_admission,
                            Arc::clone(&self.query_metrics),
                            Arc::clone(&self.read_plan),
                        )?
                    {
                        base_sources.push(cursor);
                    }
                }
                let mut base = VisibleCursor::new(base_sources, self.commit_id)?;
                let mut base_pending = base.next_visible()?;
                for operation in buffered {
                    while base_pending
                        .is_some_and(|base| base.key.timestamp_us < operation.key.timestamp_us)
                    {
                        base_pending = base.next_visible()?;
                    }
                    let matching = base_pending
                        .filter(|base| base.key.timestamp_us == operation.key.timestamp_us);
                    apply(operation, matching, true)?;
                }
                while let Some(operation) = overlay.next_latest()? {
                    self.query_metrics.check()?;
                    if !may_overlap_base(operation.key.timestamp_us) {
                        apply(operation, None, false)?;
                        continue;
                    }
                    while base_pending
                        .is_some_and(|base| base.key.timestamp_us < operation.key.timestamp_us)
                    {
                        base_pending = base.next_visible()?;
                    }
                    let matching = base_pending
                        .filter(|base| base.key.timestamp_us == operation.key.timestamp_us);
                    apply(operation, matching, true)?;
                }
                let base_scanned = base.operations_read();
                self.query_metrics.record_operations(base_scanned)?;
                self.metrics
                    .query_operations_scanned
                    .fetch_add(base_scanned, Ordering::Relaxed);
            }
        }
        let scanned = overlay.operations_read();
        self.query_metrics.record_operations(scanned)?;
        self.metrics
            .query_operations_scanned
            .fetch_add(scanned, Ordering::Relaxed);
        if requires_exact {
            return self.exact_day(from_us, to_us, &[]);
        }
        if aggregate.state.count == 0 {
            return Ok(None);
        }
        self.finish_aggregate_day(bucket_start_us, bucket_end_us, aggregate)
            .map(|mut row| {
                row.source_resolution = None;
                Some(row)
            })
    }

    fn base_point(
        &self,
        aggregate_segments: &[Arc<Segment>],
        timestamp_us: i64,
    ) -> Result<Option<VersionedPoint>> {
        for segment in aggregate_segments {
            let descriptor = segment.descriptor();
            if descriptor.min_timestamp_us > timestamp_us
                || descriptor.max_timestamp_us < timestamp_us
            {
                continue;
            }
            let Some(cursor) = segment.point_cursor_for_query(
                self.series,
                timestamp_us,
                Arc::clone(&self.query_metrics),
            )?
            else {
                continue;
            };
            let mut visible = VisibleCursor::new(vec![cursor], self.commit_id)?;
            let point = visible.next_visible()?;
            let scanned = visible.operations_read();
            self.query_metrics.record_operations(scanned)?;
            self.metrics
                .query_operations_scanned
                .fetch_add(scanned, Ordering::Relaxed);
            if point.is_some() {
                return Ok(point);
            }
        }
        Ok(None)
    }

    fn exact_day(
        &self,
        from_us: i64,
        to_us: i64,
        aggregate_segments: &[Arc<Segment>],
    ) -> Result<Option<GroupRow>> {
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        append_batch_sources(
            &mut sources,
            &self.batches,
            self.series,
            (from_us, to_us),
            self.commit_id,
            &self.metrics,
            &self.query_metrics,
        )?;
        for planned in &self.planned_segments {
            if aggregate_segments
                .iter()
                .any(|segment| segment.descriptor().id == planned.segment().descriptor().id)
            {
                continue;
            }
            if let Some(cursor) = planned.cursor(
                from_us,
                to_us,
                self.cache_admission,
                Arc::clone(&self.query_metrics),
                Arc::clone(&self.read_plan),
            )? {
                sources.push(cursor);
            }
        }
        let visible = VisibleCursor::new(sources, self.commit_id)?;
        let mut cursor = SeriesGroupCursor {
            series: self.series,
            visible,
            pending: None,
            grouping: Grouping::UtcDay,
            aggregates: self.aggregates,
            metrics: Arc::clone(&self.metrics),
            query_metrics: Arc::clone(&self.query_metrics),
            reported_operations: 0,
            returned_points: 0,
            reported_points: 0,
        };
        cursor.next_row()
    }
}

pub(super) fn hybrid_day_span(from_us: i64, to_us: i64) -> Result<Option<(i64, i64)>> {
    let first_day = utc_day_start(from_us)?;
    let last_timestamp = to_us
        .checked_sub(1)
        .ok_or_else(|| invalid("query end cannot be decremented"))?;
    let last_day = utc_day_start(last_timestamp)?;
    let days = (i128::from(last_day) - i128::from(first_day)) / i128::from(DAY_US) + 1;
    if days > MAX_HYBRID_QUERY_DAYS as i128 {
        return Ok(None);
    }
    let day_limit = last_day
        .checked_add(DAY_US)
        .ok_or_else(|| invalid("query day boundary overflows i64"))?;
    Ok(Some((first_day, day_limit)))
}

pub(super) fn contiguous_day_ranges(days: &[i64]) -> Result<Vec<(i64, i64)>> {
    let Some(first) = days.first().copied() else {
        return Ok(Vec::new());
    };
    let mut ranges = Vec::new();
    let mut range_from = first;
    let mut range_to = first
        .checked_add(DAY_US)
        .ok_or_else(|| corrupt("aggregate UTC day range overflows i64"))?;
    for day in &days[1..] {
        if *day == range_to {
            range_to = range_to
                .checked_add(DAY_US)
                .ok_or_else(|| corrupt("aggregate UTC day range overflows i64"))?;
        } else {
            ranges.push((range_from, range_to));
            range_from = *day;
            range_to = day
                .checked_add(DAY_US)
                .ok_or_else(|| corrupt("aggregate UTC day range overflows i64"))?;
        }
    }
    ranges.push((range_from, range_to));
    Ok(ranges)
}
