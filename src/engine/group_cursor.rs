//! Курсоры группировок, объединение рядов и чтение архивов сводок.

use super::Metrics;
use super::hybrid::HybridGroupCursor;
use crate::aggregate::Cursor as AggregateCursor;
use crate::aggregate_archive::{AggregateArchive, ArchiveCursor};
use crate::error::{Result, corrupt};
use crate::query::{QueryBudget, thread_external_phase_duration};
use crate::segment::VisibleCursor;
use crate::types::{
    AggregateResolution, AggregateSet, AggregateState, GroupRow, Grouping, SeriesKey,
    VersionedPoint,
};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub(super) struct PendingGroup {
    pub(super) cursor_index: usize,
    pub(super) row: GroupRow,
}

impl PartialEq for PendingGroup {
    fn eq(&self, other: &Self) -> bool {
        self.cursor_index == other.cursor_index
            && self.row.bucket_start_us == other.row.bucket_start_us
            && self.row.bucket_end_us == other.row.bucket_end_us
    }
}

impl Eq for PendingGroup {}

impl PartialOrd for PendingGroup {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingGroup {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .row
            .bucket_start_us
            .cmp(&self.row.bucket_start_us)
            .then_with(|| other.row.bucket_end_us.cmp(&self.row.bucket_end_us))
            .then_with(|| other.cursor_index.cmp(&self.cursor_index))
    }
}

pub(super) struct CombinedGroupCursor {
    pub(super) cursors: Vec<SeriesCursor>,
    pub(super) pending: BinaryHeap<PendingGroup>,
}

impl CombinedGroupCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        let Some(first) = self.pending.pop() else {
            return Ok(None);
        };
        let bucket_start_us = first.row.bucket_start_us;
        let bucket_end_us = first.row.bucket_end_us;
        let aggregates = first.row.aggregates;
        let mut state = AggregateState::for_aggregates(aggregates);
        self.merge_row(first, bucket_start_us, &mut state)?;
        while self.pending.peek().is_some_and(|entry| {
            entry.row.bucket_start_us == bucket_start_us && entry.row.bucket_end_us == bucket_end_us
        }) {
            let entry = self
                .pending
                .pop()
                .ok_or_else(|| corrupt("combined group heap lost its head"))?;
            self.merge_row(entry, bucket_start_us, &mut state)?;
        }
        if self
            .pending
            .peek()
            .is_some_and(|entry| entry.row.bucket_start_us == bucket_start_us)
        {
            return Err(corrupt(
                "combined group sources disagree about bucket boundaries",
            ));
        }
        GroupRow::from_state(None, bucket_start_us, bucket_end_us, state, aggregates).map(Some)
    }

    fn merge_row(
        &mut self,
        entry: PendingGroup,
        bucket_start_us: i64,
        state: &mut AggregateState,
    ) -> Result<()> {
        state.merge(entry.row.state())?;
        if let Some(next) = self.cursors[entry.cursor_index].next_row()? {
            if next.bucket_start_us <= bucket_start_us {
                return Err(corrupt("series group buckets are not strictly increasing"));
            }
            self.pending.push(PendingGroup {
                cursor_index: entry.cursor_index,
                row: next,
            });
        }
        Ok(())
    }
}

pub(super) enum SeriesCursor {
    Points(SeriesGroupCursor),
    Aggregates(AggregateGroupCursor),
    Hybrid(HybridGroupCursor),
    ArchivedAndRaw(Box<ArchivedAndRawGroupCursor>),
}

impl SeriesCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        match self {
            Self::Points(cursor) => cursor.next_row(),
            Self::Aggregates(cursor) => cursor.next_row(),
            Self::Hybrid(cursor) => cursor.next_row(),
            Self::ArchivedAndRaw(cursor) => cursor.next_row(),
        }
    }
}

pub(super) struct ArchivedAndRawGroupCursor {
    pub(super) archived: AggregateGroupCursor,
    pub(super) raw: Box<SeriesCursor>,
    pub(super) archived_pending: Option<GroupRow>,
    pub(super) raw_pending: Option<GroupRow>,
    pub(super) aggregates: AggregateSet,
}

impl ArchivedAndRawGroupCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        if self.archived_pending.is_none() {
            self.archived_pending = self.archived.next_row()?;
        }
        if self.raw_pending.is_none() {
            self.raw_pending = self.raw.next_row()?;
        }
        match (self.archived_pending, self.raw_pending) {
            (None, None) => Ok(None),
            (Some(row), None) => {
                self.archived_pending = None;
                Ok(Some(row))
            }
            (None, Some(row)) => {
                self.raw_pending = None;
                Ok(Some(row))
            }
            (Some(archived), Some(raw)) => {
                if archived.bucket_start_us < raw.bucket_start_us {
                    self.archived_pending = None;
                    return Ok(Some(archived));
                }
                if raw.bucket_start_us < archived.bucket_start_us {
                    self.raw_pending = None;
                    return Ok(Some(raw));
                }
                if archived.bucket_end_us != raw.bucket_end_us || archived.series != raw.series {
                    return Err(corrupt("archived and raw aggregate buckets disagree"));
                }
                self.archived_pending = None;
                self.raw_pending = None;
                let mut state = archived.state();
                state.merge(raw.state())?;
                GroupRow::from_state(
                    archived.series,
                    archived.bucket_start_us,
                    archived.bucket_end_us,
                    state,
                    self.aggregates,
                )
                .map(Some)
            }
        }
    }
}

pub(super) struct AggregateGroupCursor {
    pub(super) series: SeriesKey,
    pub(super) cursors: Vec<StoredAggregateCursor>,
    pub(super) cursor_index: usize,
    pub(super) pending: Option<crate::aggregate::AggregateRecord>,
    pub(super) grouping: Grouping,
    pub(super) metrics: Arc<Metrics>,
    pub(super) query_metrics: Arc<QueryBudget>,
    pub(super) aggregates: AggregateSet,
}

pub(super) enum StoredAggregateCursor {
    Segment(AggregateCursor),
    PendingArchive {
        archive: Arc<AggregateArchive>,
        series: SeriesKey,
        resolution: AggregateResolution,
        from_us: i64,
        to_us: i64,
        budget: Arc<QueryBudget>,
    },
    Archive(ArchiveCursor),
    Empty,
}

impl StoredAggregateCursor {
    pub(super) fn next_record(&mut self) -> Result<Option<crate::aggregate::AggregateRecord>> {
        loop {
            match self {
                Self::Segment(cursor) => return cursor.next_record(),
                Self::Archive(cursor) => return cursor.next_record(),
                Self::Empty => return Ok(None),
                Self::PendingArchive {
                    archive,
                    series,
                    resolution,
                    from_us,
                    to_us,
                    budget,
                } => {
                    let cursor = archive.cursor(
                        *series,
                        *resolution,
                        *from_us,
                        *to_us,
                        Arc::clone(budget),
                    )?;
                    *self = cursor.map_or(Self::Empty, Self::Archive);
                }
            }
        }
    }
}

impl AggregateGroupCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        let first = match self.pending.take() {
            Some(record) => record,
            None => match self.next_record()? {
                Some(record) => record,
                None => return Ok(None),
            },
        };
        let resolution = first.resolution;
        let (bucket_start_us, bucket_end_us) =
            self.grouping.bucket_bounds(first.bucket_start_us)?;
        if first.bucket_start_us < bucket_start_us || first.bucket_end_us > bucket_end_us {
            return Err(corrupt("aggregate record crosses requested bucket"));
        }
        let mut state = first.state;
        let mut records = 1_u64;
        while let Some(record) = self.next_record()? {
            if record.bucket_start_us >= bucket_end_us {
                self.pending = Some(record);
                break;
            }
            if record.resolution != resolution || record.bucket_end_us > bucket_end_us {
                return Err(corrupt(
                    "aggregate records disagree with requested grouping",
                ));
            }
            state.merge(record.state)?;
            records = records.saturating_add(1);
        }
        self.metrics
            .query_aggregate_records_read
            .fetch_add(records, Ordering::Relaxed);
        self.metrics
            .query_points_returned
            .fetch_add(state.count, Ordering::Relaxed);
        if resolution == AggregateResolution::Day
            && self.query_metrics.mark_exact_daily_aggregates()
        {
            self.metrics
                .query_exact_daily_aggregate_queries
                .fetch_add(1, Ordering::Relaxed);
        }
        self.query_metrics.record_aggregate_records(
            records,
            records.saturating_mul(crate::aggregate::RECORD_SIZE as u64),
        )?;
        self.query_metrics.record_points(state.count)?;
        GroupRow::from_state(
            Some(self.series),
            bucket_start_us,
            bucket_end_us,
            state,
            self.aggregates,
        )
        .map(|row| Some(row.with_source_resolution(resolution)))
    }

    pub(super) fn next_record(&mut self) -> Result<Option<crate::aggregate::AggregateRecord>> {
        loop {
            let Some(cursor) = self.cursors.get_mut(self.cursor_index) else {
                return Ok(None);
            };
            let Some(record) = cursor.next_record()? else {
                self.cursors[self.cursor_index] = StoredAggregateCursor::Empty;
                self.cursor_index += 1;
                continue;
            };
            record.total()?;
            return Ok(Some(record));
        }
    }
}

pub(super) struct SeriesGroupCursor {
    pub(super) series: SeriesKey,
    pub(super) visible: VisibleCursor,
    pub(super) pending: Option<VersionedPoint>,
    pub(super) grouping: Grouping,
    pub(super) aggregates: AggregateSet,
    pub(super) metrics: Arc<Metrics>,
    pub(super) query_metrics: Arc<QueryBudget>,
    pub(super) reported_operations: u64,
    pub(super) returned_points: u64,
    pub(super) reported_points: u64,
}

pub(super) fn measure_mvcc<T>(
    query_budget: &QueryBudget,
    work: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let external_before = thread_external_phase_duration();
    let result = work();
    let external_time = thread_external_phase_duration().saturating_sub(external_before);
    query_budget.record_mvcc(started.elapsed().saturating_sub(external_time));
    result
}

impl SeriesGroupCursor {
    pub(super) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        let query_metrics = Arc::clone(&self.query_metrics);
        measure_mvcc(&query_metrics, || self.next_row_inner())
    }

    fn next_row_inner(&mut self) -> Result<Option<GroupRow>> {
        let first = match self.pending.take() {
            Some(point) => point,
            None => match self.next_point()? {
                Some(point) => point,
                None => return Ok(None),
            },
        };
        let (bucket_start_us, bucket_end_us) =
            self.grouping.bucket_bounds(first.key.timestamp_us)?;
        let accumulation_started = Instant::now();
        let mut state = AggregateState::for_aggregates(self.aggregates);
        state.add(first.key.timestamp_us, first.put_value()?)?;
        while let Some(point) = self.next_point()? {
            // `VisibleCursor` выдаёт точки по возрастанию времени, а все виды
            // grouping задают непрерывные непересекающиеся корзины. Границу
            // текущей корзины уже вычислили выше, поэтому здесь не нужен новый
            // календарный расчёт (для UTC суток это деление i128) на каждую точку.
            if point.key.timestamp_us >= bucket_end_us {
                self.pending = Some(point);
                break;
            }
            state.add(point.key.timestamp_us, point.put_value()?)?;
        }
        self.query_metrics
            .record_aggregate_accumulation(accumulation_started.elapsed());
        let finalization_started = Instant::now();
        let row = GroupRow::from_state(
            Some(self.series),
            bucket_start_us,
            bucket_end_us,
            state,
            self.aggregates,
        )?;
        self.query_metrics
            .record_aggregate_finalization(finalization_started.elapsed());
        self.publish_metrics();
        Ok(Some(row))
    }

    fn next_point(&mut self) -> Result<Option<VersionedPoint>> {
        self.query_metrics.check()?;
        let before = self.visible.operations_read();
        let point = self.visible.next_visible()?;
        let scanned = self.visible.operations_read().saturating_sub(before);
        self.query_metrics.record_operations(scanned)?;
        if point.is_some() {
            self.query_metrics.record_points(1)?;
        }
        self.returned_points = self
            .returned_points
            .saturating_add(u64::from(point.is_some()));
        Ok(point)
    }

    pub(super) fn publish_metrics(&mut self) {
        let scanned = self
            .visible
            .operations_read()
            .saturating_sub(self.reported_operations);
        let returned = self.returned_points.saturating_sub(self.reported_points);
        self.metrics
            .query_operations_scanned
            .fetch_add(scanned, Ordering::Relaxed);
        self.metrics
            .query_points_returned
            .fetch_add(returned, Ordering::Relaxed);
        self.reported_operations = self.visible.operations_read();
        self.reported_points = self.returned_points;
    }
}

impl Drop for SeriesGroupCursor {
    fn drop(&mut self) {
        self.publish_metrics();
    }
}
