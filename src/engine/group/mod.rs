//! Grouping output, bounded parallel series execution, and resource release.

mod archive;
pub(super) mod cursor;
mod part;
mod sources;

use self::cursor::{CombinedGroupCursor, SeriesCursor};
use super::{QUERY_WORK_WAIT, Snapshot};
use crate::error::{Error, Result, corrupt};
use crate::memory::cache::BlockCacheAdmission;
use crate::query::{QueryBudget, QueryMemoryClass, QueryStats};
use crate::types::{GroupRequest, GroupRow, GroupScope};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// A lazy grouping stream from one snapshot.
///
/// `PerKey` traverses series in sorted [`SeriesKey`](crate::SeriesKey) order,
/// with bounded parallel prefetch when enabled.
/// `Combined` merges one cursor and a ready bucket per series.
pub struct GroupStream {
    pub(super) snapshot: Snapshot,
    pub(super) request: GroupRequest,
    pub(super) query_metrics: Arc<QueryBudget>,
    pub(super) cache_admission: BlockCacheAdmission,
    pub(super) request_memory: Option<crate::query::QueryReservation>,
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

    /// Current counters for this stream only.
    #[must_use]
    pub fn stats(&self) -> QueryStats {
        self.query_metrics.stats()
    }

    fn initialize_combined(&mut self) -> Result<()> {
        let memory = CombinedGroupCursor::reserve(self.request.series.len(), &self.query_metrics)?;
        let mut cursors = Vec::with_capacity(self.request.series.len());
        while self.open_next_series(false)? {
            cursors.push(
                self.current
                    .take()
                    .ok_or_else(|| corrupt("group cursor missing"))?,
            );
        }
        self.combined = Some(CombinedGroupCursor::new(cursors, None, memory));
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
                .query_part_reads
                .fetch_add(stats.part_reads, Ordering::Relaxed);
            self.snapshot
                .metrics
                .query_peak_memory_bytes
                .fetch_max(stats.peak_memory_bytes, Ordering::Relaxed);
            if let Some(regulator) = &self.snapshot.write_regulator {
                regulator.record_read(
                    stats
                        .source_open_duration
                        .saturating_add(stats.read_duration),
                    self.request.series.len() > 1,
                );
            }
            self.reported_query_resources = true;
        }
        self.current = None;
        self.combined = None;
        self.parallel_rows = VecDeque::new();
        self.query_metrics
            .release_memory_class(QueryMemoryClass::Results, self.parallel_buffer_bytes);
        self.parallel_buffer_bytes = 0;
        self.request.series = Vec::new();
        self.request_memory = None;
        self.snapshot.release();
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
