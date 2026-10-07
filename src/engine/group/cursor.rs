//! Streaming aggregate cursors and time-bucket accumulation.

use super::part::PartCursor;
use crate::aggregate::archive::{AggregateArchive, ArchiveCursor};
use crate::engine::Metrics;
use crate::error::{Error, Result, corrupt};
use crate::query::{QueryBudget, QueryReservation, thread_external_phase_duration};
use crate::segment::VisibleCursor;
use crate::types::{
    AggregateResolution, AggregateSet, AggregateState, GroupRow, Grouping, SeriesKey,
    VersionedPoint,
};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub(in crate::engine) struct PendingGroup {
    pub(in crate::engine) cursor_index: usize,
    pub(in crate::engine) row: GroupRow,
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

pub(in crate::engine) struct CombinedGroupCursor {
    pub(in crate::engine) cursors: Vec<SeriesCursor>,
    series: Option<SeriesKey>,
    _memory: QueryReservation,
    pub(in crate::engine) pending: BinaryHeap<PendingGroup>,
    initialized: bool,
}

impl CombinedGroupCursor {
    pub(in crate::engine) fn reserve(
        count: usize,
        budget: &Arc<QueryBudget>,
    ) -> Result<QueryReservation> {
        budget.reserve_owned((count as u64).saturating_mul(
            (std::mem::size_of::<SeriesCursor>() + std::mem::size_of::<PendingGroup>()) as u64,
        ))
    }

    pub(in crate::engine) fn new(
        cursors: Vec<SeriesCursor>,
        series: Option<SeriesKey>,
        memory: QueryReservation,
    ) -> Self {
        Self {
            pending: BinaryHeap::with_capacity(cursors.len()),
            cursors,
            initialized: false,
            series,
            _memory: memory,
        }
    }

    pub(in crate::engine) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        // Opening a series must not read its first group on the submitting
        // thread: the bounded executor owns that work for parallel queries.
        if !self.initialized {
            for (cursor_index, cursor) in self.cursors.iter_mut().enumerate() {
                if let Some(row) = cursor.next_row()? {
                    self.pending.push(PendingGroup { cursor_index, row });
                }
            }
            self.initialized = true;
        }
        let Some(first) = self.pending.pop() else {
            return Ok(None);
        };
        let bucket_start_us = first.row.bucket_start_us;
        let bucket_end_us = first.row.bucket_end_us;
        let aggregates = first.row.aggregates;
        let mut resolution = first.row.source_resolution;
        let mut state = AggregateState::for_aggregates(aggregates);
        self.merge_row(first, bucket_start_us, &mut state)?;
        while self.pending.peek().is_some_and(|entry| {
            entry.row.bucket_start_us == bucket_start_us && entry.row.bucket_end_us == bucket_end_us
        }) {
            let entry = self
                .pending
                .pop()
                .ok_or_else(|| corrupt("combined group heap lost its head"))?;
            if resolution != entry.row.source_resolution {
                resolution = None;
            }
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
        let mut row = GroupRow::from_state(
            self.series,
            bucket_start_us,
            bucket_end_us,
            state,
            aggregates,
        )?;
        row.source_resolution = resolution;
        Ok(Some(row))
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

pub(in crate::engine) enum SeriesCursor {
    Points(SeriesGroupCursor),
    Aggregates(AggregateGroupCursor),
    Part(Box<PartCursor>),
    Merged(CombinedGroupCursor),
}

impl SeriesCursor {
    pub(in crate::engine) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        match self {
            Self::Points(cursor) => cursor.next_row(),
            Self::Aggregates(cursor) => cursor.next_row(),
            Self::Part(cursor) => cursor.next_row(),
            Self::Merged(cursor) => cursor.next_row(),
        }
    }
}

pub(in crate::engine) struct AggregateGroupCursor {
    pub(in crate::engine) series: SeriesKey,
    pub(in crate::engine) cursors: Vec<StoredAggregateCursor>,
    pub(in crate::engine) cursor_index: usize,
    pub(in crate::engine) pending: Option<crate::aggregate::AggregateRecord>,
    pub(in crate::engine) grouping: Grouping,
    pub(in crate::engine) metrics: Arc<Metrics>,
    pub(in crate::engine) query_metrics: Arc<QueryBudget>,
    pub(in crate::engine) aggregates: AggregateSet,
}

pub(in crate::engine) enum StoredAggregateCursor {
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
    pub(in crate::engine) fn next_record(
        &mut self,
    ) -> Result<Option<crate::aggregate::AggregateRecord>> {
        loop {
            match self {
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
    pub(in crate::engine) fn next_row(&mut self) -> Result<Option<GroupRow>> {
        let first = match self.pending.take() {
            Some(record) => record,
            None => match self.next_record()? {
                Some(record) => record,
                None => return Ok(None),
            },
        };
        let resolution = first.resolution;
        let (bucket_start_us, bucket_end_us) = self.record_bucket(&first)?;
        let mut state = first.state;
        let mut records = 1_u64;
        while let Some(record) = self.next_record()? {
            let (record_start, record_end) = self.record_bucket(&record)?;
            if record_start >= bucket_end_us {
                self.pending = Some(record);
                break;
            }
            if record.resolution != resolution || record_end != bucket_end_us {
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

    fn record_bucket(&self, record: &crate::aggregate::AggregateRecord) -> Result<(i64, i64)> {
        let (first, last) = record
            .state
            .first
            .zip(record.state.last)
            .ok_or_else(|| corrupt("nonempty archive record has no endpoints"))?;
        let bucket = self.grouping.bucket_bounds(first.timestamp_us)?;
        if last.timestamp_us >= bucket.1 {
            return Err(Error::QueryUnavailable(
                "grouping splits a retained aggregate; raw points have expired".into(),
            ));
        }
        Ok(bucket)
    }

    pub(in crate::engine) fn next_record(
        &mut self,
    ) -> Result<Option<crate::aggregate::AggregateRecord>> {
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

pub(in crate::engine) struct SeriesGroupCursor {
    pub(in crate::engine) series: SeriesKey,
    pub(in crate::engine) visible: VisibleCursor,
    pub(in crate::engine) pending: Option<VersionedPoint>,
    pub(in crate::engine) grouping: Grouping,
    pub(in crate::engine) aggregates: AggregateSet,
    pub(in crate::engine) metrics: Arc<Metrics>,
    pub(in crate::engine) query_metrics: Arc<QueryBudget>,
    pub(in crate::engine) _memory: QueryReservation,
    pub(in crate::engine) reported_operations: u64,
    pub(in crate::engine) returned_points: u64,
    pub(in crate::engine) reported_points: u64,
}

pub(in crate::engine) fn measure_mvcc<T>(
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
    pub(in crate::engine) fn next_row(&mut self) -> Result<Option<GroupRow>> {
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

    pub(in crate::engine) fn publish_metrics(&mut self) {
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
