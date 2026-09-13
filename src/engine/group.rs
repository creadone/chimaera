//! Выдача группировок, окно параллельных рядов и освобождение ресурсов.

use super::batch::{QueryBatch, append_batch_sources};
use super::group_cursor::{
    AggregateGroupCursor, ArchivedAndRawGroupCursor, CombinedGroupCursor, PendingGroup,
    SeriesCursor, SeriesGroupCursor, StoredAggregateCursor,
};
use super::hybrid::HybridGroupCursor;
use super::planning::{ExactAggregatePlan, HybridAggregatePlan, group_series_from};
use super::{QUERY_WORK_WAIT, Snapshot};
use crate::aggregate_archive::AggregateArchive;
use crate::cache::BlockCacheAdmission;
use crate::error::{Error, Result, corrupt};
use crate::query::{QueryBudget, QueryMemoryClass, QueryStats};
use crate::segment::{OperationCursor, QueryReadPlan, QuerySegmentRead, VisibleCursor};
use crate::types::{GroupRequest, GroupRow, GroupScope, GroupSource, SeriesKey};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Ленивый поток группировок одного снимка.
///
/// `PerKey` хранит курсоры одного ряда. `Combined` хранит один курсор и одну готовую
/// корзину на ряд; дисковые курсоры этого режима закрывают файл после каждого блока.
pub struct GroupStream {
    pub(super) snapshot: Snapshot,
    pub(super) request: GroupRequest,
    pub(super) query_metrics: Arc<QueryBudget>,
    pub(super) planned_segments_by_series: HashMap<SeriesKey, Vec<QuerySegmentRead>>,
    pub(super) exact_aggregate_plans_by_series: HashMap<SeriesKey, ExactAggregatePlan>,
    pub(super) archive_aggregate_plans_by_series: HashMap<SeriesKey, Vec<Arc<AggregateArchive>>>,
    pub(super) hybrid_aggregate_plans_by_series: HashMap<SeriesKey, HybridAggregatePlan>,
    pub(super) cache_admission: BlockCacheAdmission,
    pub(super) read_plan: Option<Arc<QueryReadPlan>>,
    pub(super) batches_by_series: HashMap<SeriesKey, Vec<QueryBatch>>,
    pub(super) parallel_daily: bool,
    pub(super) parallel_rows: VecDeque<GroupRow>,
    pub(super) parallel_buffer_bytes: u64,
    pub(super) parallel_pipeline: Option<ParallelSeriesPipeline>,
    pub(super) series_index: usize,
    pub(super) current: Option<SeriesCursor>,
    pub(super) combined: Option<CombinedGroupCursor>,
    pub(super) combined_initialized: bool,
    pub(super) reported_query_resources: bool,
    pub(super) finished: bool,
    pub(super) failed: bool,
}

pub(super) struct ParallelSeriesPipeline {
    pub(super) reply: Sender<(usize, Result<Vec<GroupRow>>)>,
    pub(super) replies: Receiver<(usize, Result<Vec<GroupRow>>)>,
    pub(super) completed: BTreeMap<usize, Vec<GroupRow>>,
    pub(super) submitted: usize,
    pub(super) received: usize,
    pub(super) next_result: usize,
    pub(super) window: usize,
}

impl ParallelSeriesPipeline {
    pub(super) fn new(worker_count: usize) -> Self {
        let window = worker_count.saturating_mul(2).max(1);
        let (reply, replies) = bounded(window);
        Self {
            reply,
            replies,
            completed: BTreeMap::new(),
            submitted: 0,
            received: 0,
            next_result: 0,
            window,
        }
    }

    fn release_rows(query_budget: &QueryBudget, rows: Vec<GroupRow>) {
        query_budget.release_memory_class(
            QueryMemoryClass::Results,
            (rows.len() as u64).saturating_mul(std::mem::size_of::<GroupRow>() as u64),
        );
    }

    pub(super) fn drain(&mut self, query_budget: &QueryBudget) {
        let (replacement, _) = bounded(1);
        drop(std::mem::replace(&mut self.reply, replacement));
        for (_, rows) in std::mem::take(&mut self.completed) {
            Self::release_rows(query_budget, rows);
        }
        while self.received < self.submitted {
            let Ok((_, result)) = self.replies.try_recv() else {
                break;
            };
            self.received += 1;
            if let Ok(rows) = result {
                Self::release_rows(query_budget, rows);
            }
        }
    }
}
impl GroupStream {
    #[must_use]
    pub const fn read_commit_id(&self) -> u64 {
        self.snapshot.commit_id
    }

    /// Текущие счётчики только этого потока.
    #[must_use]
    pub fn stats(&self) -> QueryStats {
        self.query_metrics.stats()
    }

    pub(super) fn open_next_series(&mut self, _keep_file_open: bool) -> Result<bool> {
        self.query_metrics.check()?;
        let Some(series) = self.request.series.get(self.series_index).copied() else {
            return Ok(false);
        };
        self.series_index += 1;
        let planning_started = Instant::now();
        let series_batches = self.batches_by_series.remove(&series).unwrap_or_default();
        let aggregate_resolution = (self.request.source == GroupSource::Auto)
            .then(|| self.request.grouping.aggregate_resolution())
            .flatten();
        let from_us =
            group_series_from(&self.snapshot, &self.request, aggregate_resolution, series);
        let to_us = self.request.to_us;
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        if from_us >= to_us {
            self.current = Some(SeriesCursor::Points(SeriesGroupCursor {
                series,
                visible: VisibleCursor::new(sources, self.snapshot.commit_id)?,
                pending: None,
                grouping: self.request.grouping,
                aggregates: self.request.aggregates,
                metrics: Arc::clone(&self.snapshot.metrics),
                query_metrics: Arc::clone(&self.query_metrics),
                reported_operations: 0,
                returned_points: 0,
                reported_points: 0,
            }));
            self.query_metrics
                .record_plan(0, 0, planning_started.elapsed());
            return Ok(true);
        }
        if let Some(exact_plan) = self.exact_aggregate_plans_by_series.remove(&series) {
            self.archive_aggregate_plans_by_series.remove(&series);
            let resolution = self
                .request
                .grouping
                .aggregate_resolution()
                .ok_or_else(|| corrupt("planned aggregate resolution disappeared"))?;
            let mut aggregates = Vec::<StoredAggregateCursor>::with_capacity(
                exact_plan
                    .segments
                    .len()
                    .saturating_add(exact_plan.archives.len()),
            );
            for archive in exact_plan.archives {
                aggregates.push(StoredAggregateCursor::PendingArchive {
                    archive,
                    series,
                    resolution,
                    from_us,
                    to_us,
                    budget: Arc::clone(&self.query_metrics),
                });
            }
            for segment in exact_plan.segments {
                let cursor = segment
                    .aggregate_cursor_for_query(
                        series,
                        resolution,
                        from_us,
                        to_us,
                        &self.query_metrics,
                    )?
                    .ok_or_else(|| corrupt("planned exact aggregate disappeared"))?;
                aggregates.push(StoredAggregateCursor::Segment(cursor));
            }
            self.current = Some(SeriesCursor::Aggregates(AggregateGroupCursor {
                series,
                cursors: aggregates,
                cursor_index: 0,
                pending: None,
                grouping: self.request.grouping,
                metrics: Arc::clone(&self.snapshot.metrics),
                query_metrics: Arc::clone(&self.query_metrics),
                aggregates: self.request.aggregates,
            }));
            self.query_metrics
                .record_plan(0, 0, planning_started.elapsed());
            return Ok(true);
        }

        let mut relevant_reads = self
            .planned_segments_by_series
            .remove(&series)
            .unwrap_or_default();
        let read_plan = Arc::clone(
            self.read_plan
                .as_ref()
                .ok_or_else(|| corrupt("group read plan was released"))?,
        );
        self.query_metrics
            .record_plan(0, 0, planning_started.elapsed());

        if let Some(hybrid) = self.hybrid_aggregate_plans_by_series.remove(&series) {
            let raw = SeriesCursor::Hybrid(HybridGroupCursor {
                series,
                strictly_increasing: self.snapshot.strictly_increasing_series.contains(&series),
                from_us,
                to_us,
                next_day_us: hybrid.first_day_us,
                commit_id: self.snapshot.commit_id,
                batches: series_batches,
                planned_segments: relevant_reads,
                read_plan,
                aggregate_days: hybrid.days,
                cache_admission: self.cache_admission,
                metrics: Arc::clone(&self.snapshot.metrics),
                query_metrics: Arc::clone(&self.query_metrics),
                aggregates: self.request.aggregates,
            });
            self.current = Some(self.with_archive_cursor(series, from_us, to_us, raw)?);
            return Ok(true);
        }

        append_batch_sources(
            &mut sources,
            &series_batches,
            series,
            (from_us, to_us),
            self.snapshot.commit_id,
            &self.snapshot.metrics,
            &self.query_metrics,
        )?;

        for planned in relevant_reads.drain(..) {
            let cursor = planned.cursor(
                from_us,
                to_us,
                self.cache_admission,
                Arc::clone(&self.query_metrics),
                Arc::clone(&read_plan),
            )?;
            if let Some(cursor) = cursor {
                sources.push(cursor);
            }
        }
        let visible = VisibleCursor::new(sources, self.snapshot.commit_id)?;
        let raw = SeriesCursor::Points(SeriesGroupCursor {
            series,
            visible,
            pending: None,
            grouping: self.request.grouping,
            aggregates: self.request.aggregates,
            metrics: Arc::clone(&self.snapshot.metrics),
            query_metrics: Arc::clone(&self.query_metrics),
            reported_operations: 0,
            returned_points: 0,
            reported_points: 0,
        });
        self.current = Some(self.with_archive_cursor(series, from_us, to_us, raw)?);
        Ok(true)
    }

    fn with_archive_cursor(
        &mut self,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        raw: SeriesCursor,
    ) -> Result<SeriesCursor> {
        let Some(archives) = self.archive_aggregate_plans_by_series.remove(&series) else {
            return Ok(raw);
        };
        let Some(resolution) = self.request.grouping.aggregate_resolution() else {
            return Ok(raw);
        };
        let mut cursors = Vec::with_capacity(archives.len());
        for archive in archives {
            cursors.push(StoredAggregateCursor::PendingArchive {
                archive,
                series,
                resolution,
                from_us,
                to_us,
                budget: Arc::clone(&self.query_metrics),
            });
        }
        if cursors.is_empty() {
            return Ok(raw);
        }
        Ok(SeriesCursor::ArchivedAndRaw(Box::new(
            ArchivedAndRawGroupCursor {
                archived: AggregateGroupCursor {
                    series,
                    cursors,
                    cursor_index: 0,
                    pending: None,
                    grouping: self.request.grouping,
                    metrics: Arc::clone(&self.snapshot.metrics),
                    query_metrics: Arc::clone(&self.query_metrics),
                    aggregates: self.request.aggregates,
                },
                raw: Box::new(raw),
                archived_pending: None,
                raw_pending: None,
                aggregates: self.request.aggregates,
            },
        )))
    }

    fn initialize_combined(&mut self) -> Result<()> {
        let mut cursors = Vec::with_capacity(self.request.series.len());
        let mut pending = BinaryHeap::with_capacity(self.request.series.len());
        while self.open_next_series(false)? {
            let mut cursor = self
                .current
                .take()
                .ok_or_else(|| corrupt("group series cursor was not initialized"))?;
            let cursor_index = cursors.len();
            if let Some(row) = cursor.next_row()? {
                pending.push(PendingGroup { cursor_index, row });
            }
            cursors.push(cursor);
        }
        self.combined = Some(CombinedGroupCursor { cursors, pending });
        self.combined_initialized = true;
        Ok(())
    }

    fn fill_parallel_window(&mut self) -> Result<()> {
        loop {
            let has_capacity = self.parallel_pipeline.as_ref().is_some_and(|pipeline| {
                pipeline.submitted.saturating_sub(pipeline.next_result) < pipeline.window
            });
            if !has_capacity || !self.open_next_series(false)? {
                return Ok(());
            }
            let cursor = self
                .current
                .take()
                .ok_or_else(|| corrupt("parallel group cursor was not initialized"))?;
            let index = self.series_index - 1;
            let reply = self
                .parallel_pipeline
                .as_ref()
                .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?
                .reply
                .clone();
            self.snapshot.query_executor.submit_series(
                index,
                self.snapshot.query_lease(),
                cursor,
                &self.query_metrics,
                &reply,
            )?;
            self.parallel_pipeline
                .as_mut()
                .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?
                .submitted += 1;
        }
    }

    fn fill_parallel_rows(&mut self) -> Result<bool> {
        self.fill_parallel_window()?;
        loop {
            let next_result = self
                .parallel_pipeline
                .as_ref()
                .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?
                .next_result;
            let completed = self
                .parallel_pipeline
                .as_mut()
                .and_then(|pipeline| pipeline.completed.remove(&next_result));
            if let Some(result) = completed {
                self.parallel_pipeline
                    .as_mut()
                    .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?
                    .next_result += 1;
                let rows = result;
                let row_bytes = std::mem::size_of::<GroupRow>() as u64;
                debug_assert!(self.parallel_rows.is_empty());
                self.parallel_buffer_bytes = (rows.len() as u64).saturating_mul(row_bytes);
                self.parallel_rows = rows.into();
                self.fill_parallel_window()?;
                return Ok(true);
            }

            let done = self.parallel_pipeline.as_ref().is_some_and(|pipeline| {
                pipeline.next_result == pipeline.submitted
                    && self.series_index >= self.request.series.len()
            });
            if done {
                return Ok(false);
            }

            let received = self
                .parallel_pipeline
                .as_ref()
                .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?
                .replies
                .recv_timeout(QUERY_WORK_WAIT);
            match received {
                Ok((index, result)) => {
                    let pipeline = self
                        .parallel_pipeline
                        .as_mut()
                        .ok_or_else(|| corrupt("parallel group pipeline was not initialized"))?;
                    pipeline.received += 1;
                    let rows = result?;
                    if pipeline.completed.contains_key(&index) {
                        ParallelSeriesPipeline::release_rows(&self.query_metrics, rows);
                        return Err(corrupt("parallel group worker replied twice"));
                    }
                    pipeline.completed.insert(index, rows);
                }
                Err(RecvTimeoutError::Timeout) => self.query_metrics.check()?,
                Err(RecvTimeoutError::Disconnected) => return Err(Error::WorkerStopped),
            }
        }
    }

    pub(super) fn fail(&mut self, error: Error) -> Option<Result<GroupRow>> {
        self.failed = true;
        Some(Err(error))
    }

    pub(super) fn finish(&mut self) -> Option<Result<GroupRow>> {
        self.finished = true;
        None
    }

    fn next_group_row(&mut self) -> Option<Result<GroupRow>> {
        if self.parallel_daily {
            loop {
                if let Some(row) = self.parallel_rows.pop_front() {
                    let row_bytes = std::mem::size_of::<GroupRow>() as u64;
                    self.parallel_buffer_bytes =
                        self.parallel_buffer_bytes.saturating_sub(row_bytes);
                    self.query_metrics
                        .release_memory_class(QueryMemoryClass::Results, row_bytes);
                    return match self.query_metrics.record_result_row() {
                        Ok(()) => Some(Ok(row)),
                        Err(error) => self.fail(error),
                    };
                }
                match self.fill_parallel_rows() {
                    Ok(true) => {}
                    Ok(false) => return self.finish(),
                    Err(error) => return self.fail(error),
                }
            }
        }
        if self.request.scope == GroupScope::Combined {
            if !self.combined_initialized
                && let Err(error) = self.initialize_combined()
            {
                return self.fail(error);
            }
            let next = self.combined.as_mut()?.next_row();
            return match next {
                Ok(Some(row)) => match self.query_metrics.record_result_row() {
                    Ok(()) => Some(Ok(row)),
                    Err(error) => self.fail(error),
                },
                Ok(None) => self.finish(),
                Err(error) => self.fail(error),
            };
        }
        loop {
            if self.current.is_none() {
                match self.open_next_series(true) {
                    Ok(true) => {}
                    Ok(false) => return self.finish(),
                    Err(error) => return self.fail(error),
                }
            }
            let next = self.current.as_mut()?.next_row();
            match next {
                Ok(Some(row)) => match self.query_metrics.record_result_row() {
                    Ok(()) => return Some(Ok(row)),
                    Err(error) => return self.fail(error),
                },
                Ok(None) => self.current = None,
                Err(error) => return self.fail(error),
            }
        }
    }

    fn release_sources(&mut self) {
        self.query_metrics.cancel();
        if let Some(mut pipeline) = self.parallel_pipeline.take() {
            pipeline.drain(&self.query_metrics);
        }
        if !self.reported_query_resources {
            let stats = self.query_metrics.stats();
            self.snapshot
                .metrics
                .query_operation_bytes_decoded
                .fetch_add(stats.operation_bytes_decoded, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_metadata_bytes_read
                .fetch_add(stats.metadata_bytes_read, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_payload_bytes_read
                .fetch_add(stats.payload_bytes_read, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_blocks_read
                .fetch_add(stats.blocks_read, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_physical_blocks_decoded
                .fetch_add(stats.physical_blocks_decoded, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_physical_decoded_bytes
                .fetch_add(stats.physical_decoded_bytes, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_file_opens
                .fetch_add(stats.file_opens, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_prefetched_blocks
                .fetch_add(stats.prefetched_blocks, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_prefetch_hits
                .fetch_add(stats.prefetch_hits, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_planned_segment_reads
                .fetch_add(stats.planned_segment_reads, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_peak_memory_bytes
                .fetch_max(stats.peak_memory_bytes, Ordering::Relaxed);
            if let Some(regulator) = &self.snapshot.write_regulator {
                regulator.record_read(
                    stats.planning_duration.saturating_add(stats.read_duration),
                    self.request.series.len() > 1,
                );
            }
            self.reported_query_resources = true;
        }
        self.current = None;
        self.combined = None;
        self.parallel_rows.clear();
        self.query_metrics
            .release_memory_class(QueryMemoryClass::Results, self.parallel_buffer_bytes);
        self.parallel_buffer_bytes = 0;
        self.batches_by_series.clear();
        self.planned_segments_by_series.clear();
        self.exact_aggregate_plans_by_series.clear();
        self.archive_aggregate_plans_by_series.clear();
        self.hybrid_aggregate_plans_by_series.clear();
        self.read_plan = None;
        self.snapshot.batches.clear();
        self.snapshot.segments_by_shard.clear();
        self.snapshot.guard = None;
        self.snapshot.storage_lock = None;
        if Arc::strong_count(&self.query_metrics) == 1 {
            let _ = self.query_metrics.release_local_metadata();
            self.query_metrics.release_remaining_memory();
        }
        // Otherwise an abandoned worker still owns reservations. QueryBudget::drop
        // releases the remainder only after that worker has released its buffers.
    }
}

impl Drop for GroupStream {
    fn drop(&mut self) {
        self.release_sources();
    }
}

impl Iterator for GroupStream {
    type Item = Result<GroupRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.finished {
            return None;
        }
        let started = Instant::now();
        let result = self.next_group_row();
        self.query_metrics.record_read(started.elapsed());
        if self.failed || self.finished {
            self.release_sources();
        }
        result
    }
}
