//! One immutable part: stored summaries, boundary points, and block count/sum.

use crate::aggregate::{AggregateRecord, Cursor};
use crate::engine::Metrics;
use crate::error::{Result, corrupt};
use crate::memory::cache::BlockCacheAdmission;
use crate::query::{QueryBudget, QueryReservation};
use crate::segment::{BlockReadReservation, PartSource, SegmentCursor};
use crate::types::{
    AggregateResolution, AggregateSet, AggregateState, GroupRow, Grouping, SeriesKey,
    VersionedPoint,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;

struct Piece {
    start: i64,
    end: i64,
    state: AggregateState,
    resolution: Option<AggregateResolution>,
}

pub(in crate::engine) struct PartCursor {
    series: SeriesKey,
    raw: SegmentCursor,
    summaries: Option<Cursor>,
    next_summary: Option<AggregateRecord>,
    pending: Option<Piece>,
    bucket: Option<(i64, i64)>,
    unreported_operations: u64,
    unreported_points: u64,
    grouping: Grouping,
    aggregates: AggregateSet,
    read_commit: u64,
    deleted_through: u64,
    to_us: i64,
    block_summaries: bool,
    budget: Arc<QueryBudget>,
    metrics: Arc<Metrics>,
    _blocks: Arc<BlockReadReservation>,
    _memory: QueryReservation,
}

impl PartCursor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        read: PartSource,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        grouping: Grouping,
        aggregates: AggregateSet,
        use_summaries: bool,
        read_commit: u64,
        deleted_through: u64,
        admission: BlockCacheAdmission,
        budget: Arc<QueryBudget>,
        metrics: Arc<Metrics>,
        blocks: Arc<BlockReadReservation>,
    ) -> Result<Self> {
        #[cfg(feature = "read-diagnostics")]
        let _timer = crate::diagnostics::query::timing::stage(
            crate::diagnostics::query::timing::Stage::SummaryOpen,
        );
        let memory = budget.reserve_owned(std::mem::size_of::<Self>() as u64)?;
        let fully_visible = read.segment().descriptor().max_commit <= read_commit
            && read.segment().descriptor().min_commit > deleted_through;
        let mut summaries = if use_summaries && fully_visible {
            grouping
                .aggregate_resolution()
                .map(|resolution| {
                    read.segment()
                        .aggregate_cursor_for_query(series, resolution, from_us, to_us, &budget)
                })
                .transpose()?
                .flatten()
        } else {
            None
        };
        let next_summary = summaries
            .as_mut()
            .map(Cursor::next_record_for_part)
            .transpose()?
            .flatten();
        Ok(Self {
            series,
            raw: read.into_group_cursor(series, from_us, to_us, admission, Arc::clone(&budget))?,
            summaries,
            next_summary,
            pending: None,
            bucket: None,
            unreported_operations: 0,
            unreported_points: 0,
            grouping,
            aggregates,
            read_commit,
            deleted_through,
            to_us,
            block_summaries: use_summaries
                && fully_visible
                && aggregates == AggregateSet::COUNT_SUM,
            budget,
            metrics,
            _blocks: blocks,
            _memory: memory,
        })
    }

    fn next_piece(&mut self) -> Result<Option<Piece>> {
        #[cfg(feature = "read-diagnostics")]
        let _timer = crate::diagnostics::query::timing::stage(
            crate::diagnostics::query::timing::Stage::SummaryNext,
        );
        loop {
            self.budget.check()?;
            let before = self
                .next_summary
                .as_ref()
                .map_or(self.to_us, |record| record.bucket_start_us);
            if self.block_summaries
                && let Some((start, end, state)) = self.raw.take_count_sum(self.grouping, before)?
            {
                self.record_summary(state.count, 0)?;
                return Ok(Some(Piece {
                    start,
                    end,
                    state,
                    resolution: self.grouping.aggregate_resolution(),
                }));
            }
            if let Some(point) = self.raw.next_before(before)? {
                self.budget.record_operations(1)?;
                self.unreported_operations = self.unreported_operations.saturating_add(1);
                if point.commit_id > self.read_commit || point.commit_id <= self.deleted_through {
                    continue;
                }
                let (start, end) = self.bucket_bounds(point.key.timestamp_us)?;
                self.budget.record_points(1)?;
                self.unreported_points = self.unreported_points.saturating_add(1);
                let state = self.read_raw_group(point, end.min(before))?;
                return Ok(Some(Piece {
                    start,
                    end,
                    state,
                    resolution: None,
                }));
            }
            let Some(record) = self.next_summary.take() else {
                return Ok(None);
            };
            self.raw.skip_before(record.bucket_end_us)?;
            self.next_summary = self
                .summaries
                .as_mut()
                .map(Cursor::next_record_for_part)
                .transpose()?
                .flatten();
            let (start, end) = self.bucket_bounds(record.bucket_start_us)?;
            if record.bucket_end_us > end {
                return Err(corrupt(
                    "stored summary crosses the requested group boundary",
                ));
            }
            let state = if self.aggregates == AggregateSet::COUNT_SUM {
                let (count, sum, compensation) = record.state.count_sum_parts();
                AggregateState::from_count_sum_parts(count, sum, compensation)?
            } else {
                record.state
            };
            self.record_summary(state.count, crate::aggregate::RECORD_SIZE as u64)?;
            return Ok(Some(Piece {
                start,
                end,
                state,
                resolution: Some(record.resolution),
            }));
        }
    }

    fn read_raw_group(&mut self, point: VersionedPoint, limit: i64) -> Result<AggregateState> {
        let mut state = AggregateState::for_aggregates(self.aggregates);
        state.add(point.key.timestamp_us, point.put_value()?)?;
        loop {
            if self.block_summaries
                && let Some((_, _, summary)) = self.raw.take_count_sum(self.grouping, limit)?
            {
                self.record_summary(summary.count, 0)?;
                state.merge(summary)?;
                continue;
            }
            let Some(points) = self.raw.next_chunk_before(limit)? else {
                break;
            };
            // Charge the exact slice before processing it. No unused
            // quota is held by a worker, and the global limit is hard.
            self.budget.record_operations(points.len() as u64)?;
            self.unreported_operations = self
                .unreported_operations
                .saturating_add(points.len() as u64);
            let mut visible = 0;
            // These points are already decoded. Bound cooperative checks to
            // 64 cheap additions; keep visibility and the scan quota exact.
            for chunk in points.chunks(64) {
                self.budget.check()?;
                for point in chunk {
                    if point.commit_id <= self.read_commit && point.commit_id > self.deleted_through
                    {
                        state.add(point.key.timestamp_us, point.put_value()?)?;
                        visible += 1;
                    }
                }
            }
            self.budget.record_points(visible)?;
            self.unreported_points = self.unreported_points.saturating_add(visible);
        }
        Ok(state)
    }

    fn bucket_bounds(&mut self, timestamp_us: i64) -> Result<(i64, i64)> {
        if let Some((start, end)) = self.bucket
            && timestamp_us >= start
            && timestamp_us < end
        {
            return Ok((start, end));
        }
        let bounds = self.grouping.bucket_bounds(timestamp_us)?;
        self.bucket = Some(bounds);
        Ok(bounds)
    }

    fn record_summary(&mut self, points: u64, bytes: u64) -> Result<()> {
        self.budget.record_aggregate_records(1, bytes)?;
        self.budget.record_points(points)?;
        self.metrics
            .query_aggregate_records_read
            .fetch_add(1, Ordering::Relaxed);
        self.unreported_points = self.unreported_points.saturating_add(points);
        if self.grouping.is_utc_day() && self.budget.mark_exact_daily_aggregates() {
            self.metrics
                .query_exact_daily_aggregate_queries
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        let result = self.next_row_inner();
        self.publish_metrics();
        result
    }

    fn publish_metrics(&mut self) {
        self.metrics.query_operations_scanned.fetch_add(
            std::mem::take(&mut self.unreported_operations),
            Ordering::Relaxed,
        );
        self.metrics.query_points_returned.fetch_add(
            std::mem::take(&mut self.unreported_points),
            Ordering::Relaxed,
        );
    }

    fn next_row_inner(&mut self) -> Result<Option<GroupRow>> {
        let first = match self.pending.take() {
            Some(piece) => Some(piece),
            None => self.next_piece()?,
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let (start, end) = (first.start, first.end);
        let mut resolution = first.resolution;
        let mut state = first.state;
        while let Some(piece) = self.next_piece()? {
            if piece.start != start {
                if piece.start < end {
                    return Err(corrupt("part group boundaries overlap"));
                }
                self.pending = Some(piece);
                break;
            }
            if piece.end != end {
                return Err(corrupt("part group boundaries disagree"));
            }
            state.merge(piece.state)?;
            if resolution != piece.resolution {
                resolution = None;
            }
        }
        let mut row = GroupRow::from_state(Some(self.series), start, end, state, self.aggregates)?;
        row.source_resolution = resolution;
        Ok(Some(row))
    }
}

impl Drop for PartCursor {
    fn drop(&mut self) {
        self.publish_metrics();
    }
}
