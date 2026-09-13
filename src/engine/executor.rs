//! Ограниченный пул планирования и исполнения многорядных запросов.

use super::group_cursor::SeriesCursor;
use super::snapshots::SnapshotGuard;
use super::{MAX_QUERY_WORKERS, QUERY_WORK_WAIT, Snapshot};
use crate::error::{Error, Result};
use crate::lifecycle::OverloadReason;
use crate::query::{ExecutorTaskKind, QueryBudget, QueryMemoryClass};
use crate::segment::{OfflinePackDirectory, QueryPlanReservation, QuerySegmentRead, Segment};
use crate::storage_lock::StorageLock;
use crate::types::{GroupRow, SeriesKey};
use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender, bounded};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub(super) struct QueryPlanningResult {
    pub(super) planned: Vec<(SeriesKey, QuerySegmentRead)>,
    pub(super) reservation: QueryPlanReservation,
    pub(super) descriptors_examined: u64,
    pub(super) series_index_lookups: u64,
}

pub(super) enum QueryPlanningTask {
    #[cfg(test)]
    Probe(Box<dyn FnOnce() -> Result<()> + Send>),
    Directory {
        directory: Arc<OfflinePackDirectory>,
        series_ranges: Vec<(SeriesKey, i64)>,
        to_us: i64,
    },
    Segments {
        segments: Vec<Arc<Segment>>,
        series_ranges: Vec<(SeriesKey, i64)>,
        from_us: i64,
        to_us: i64,
        commit_id: u64,
    },
}

impl QueryPlanningTask {
    pub(super) fn run(self, query_budget: &Arc<QueryBudget>) -> Result<QueryPlanningResult> {
        let mut reservation = QueryPlanReservation::new(Arc::clone(query_budget));
        match self {
            #[cfg(test)]
            Self::Probe(run) => {
                run()?;
                Ok(QueryPlanningResult {
                    planned: Vec::new(),
                    reservation,
                    descriptors_examined: 0,
                    series_index_lookups: 0,
                })
            }
            Self::Directory {
                directory,
                series_ranges,
                to_us,
            } => {
                let series_index_lookups = u64::try_from(series_ranges.len()).unwrap_or(u64::MAX);
                let planned = directory.plan_query_series(
                    &series_ranges,
                    to_us,
                    query_budget,
                    &mut reservation,
                )?;
                Ok(QueryPlanningResult {
                    planned,
                    reservation,
                    descriptors_examined: 0,
                    series_index_lookups,
                })
            }
            Self::Segments {
                segments,
                series_ranges,
                from_us,
                to_us,
                commit_id,
            } => {
                let mut planned = Vec::new();
                let mut descriptors_examined = 0_u64;
                let mut series_index_lookups = 0_u64;
                for segment in segments {
                    descriptors_examined = descriptors_examined.saturating_add(1);
                    if segment.descriptor().min_commit > commit_id
                        || segment.descriptor().max_timestamp_us < from_us
                        || segment.descriptor().min_timestamp_us >= to_us
                    {
                        continue;
                    }
                    series_index_lookups = series_index_lookups
                        .saturating_add(u64::try_from(series_ranges.len()).unwrap_or(u64::MAX));
                    planned.extend(segment.plan_query_series(
                        &series_ranges,
                        to_us,
                        query_budget,
                        &mut reservation,
                    )?);
                }
                Ok(QueryPlanningResult {
                    planned,
                    reservation,
                    descriptors_examined,
                    series_index_lookups,
                })
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct QueryLease {
    pub(super) _guard: Option<Arc<SnapshotGuard>>,
    pub(super) _storage_lock: Option<Arc<StorageLock>>,
}

impl Snapshot {
    pub(super) fn query_lease(&self) -> QueryLease {
        QueryLease {
            _guard: self.guard.clone(),
            _storage_lock: self.storage_lock.clone(),
        }
    }
}

pub(super) enum QueryWork {
    #[cfg(test)]
    Pause {
        entered: Sender<()>,
        release: Receiver<()>,
    },
    Series {
        lease: QueryLease,
        index: usize,
        cursor: Box<SeriesCursor>,
        enqueued_at: Instant,
        query_budget: Arc<QueryBudget>,
        reply: Sender<(usize, Result<Vec<GroupRow>>)>,
    },
    Planning {
        lease: QueryLease,
        index: usize,
        task: QueryPlanningTask,
        enqueued_at: Instant,
        query_budget: Arc<QueryBudget>,
        reply: Sender<(usize, Result<QueryPlanningResult>)>,
    },
}

pub(super) struct QueryExecutor {
    pub(super) sender: Sender<QueryWork>,
    pub(super) worker_count: usize,
    pub(super) _workers: Vec<JoinHandle<()>>,
}

impl QueryExecutor {
    pub(super) fn new() -> Result<Arc<Self>> {
        let worker_count = thread::available_parallelism()
            .map_or(1, std::num::NonZeroUsize::get)
            .min(MAX_QUERY_WORKERS);
        let (sender, receiver) = bounded(worker_count.saturating_mul(2).max(1));
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = receiver.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("chimaera-query-{index}"))
                    .spawn(move || query_worker_loop(&receiver))?,
            );
        }
        Ok(Arc::new(Self {
            sender,
            worker_count,
            _workers: workers,
        }))
    }

    pub(super) fn is_parallel(&self) -> bool {
        self.worker_count > 1
    }

    pub(super) fn plan(
        &self,
        tasks: Vec<QueryPlanningTask>,
        lease: QueryLease,
        query_budget: &Arc<QueryBudget>,
    ) -> Result<Vec<QueryPlanningResult>> {
        let jobs = tasks.len();
        let (reply, replies) = bounded(jobs.max(1));
        let mut submitted = 0_usize;
        let mut first_error = None;
        'submit: for (index, task) in tasks.into_iter().enumerate() {
            let mut work = QueryWork::Planning {
                lease: lease.clone(),
                index,
                task,
                enqueued_at: Instant::now(),
                query_budget: Arc::clone(query_budget),
                reply: reply.clone(),
            };
            loop {
                match self.sender.send_timeout(work, QUERY_WORK_WAIT) {
                    Ok(()) => {
                        submitted += 1;
                        break;
                    }
                    Err(SendTimeoutError::Timeout(returned)) => {
                        if let Err(error) =
                            query_budget.check_waiting_for(OverloadReason::QueryQueue)
                        {
                            first_error = Some(error);
                            break 'submit;
                        }
                        work = returned;
                    }
                    Err(SendTimeoutError::Disconnected(_)) => {
                        return Err(Error::WorkerStopped);
                    }
                }
            }
        }
        drop(reply);
        if let Some(error) = first_error {
            query_budget.cancel();
            return Err(error);
        }
        let mut completed = Vec::with_capacity(submitted);
        let mut received = 0_usize;
        while received < submitted {
            match replies.recv_timeout(QUERY_WORK_WAIT) {
                Ok((index, result)) => {
                    received += 1;
                    match result {
                        Ok(result) if first_error.is_none() => completed.push((index, result)),
                        Ok(_) => {}
                        Err(error) => {
                            query_budget.cancel();
                            return Err(error);
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if first_error.is_none()
                        && let Err(error) = query_budget.check()
                    {
                        query_budget.cancel();
                        return Err(error);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => return Err(Error::WorkerStopped),
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        completed.sort_unstable_by_key(|(index, _)| *index);
        Ok(completed.into_iter().map(|(_, result)| result).collect())
    }

    pub(super) fn submit_series(
        &self,
        index: usize,
        lease: QueryLease,
        cursor: SeriesCursor,
        query_budget: &Arc<QueryBudget>,
        reply: &Sender<(usize, Result<Vec<GroupRow>>)>,
    ) -> Result<()> {
        let mut work = QueryWork::Series {
            lease,
            index,
            cursor: Box::new(cursor),
            enqueued_at: Instant::now(),
            query_budget: Arc::clone(query_budget),
            reply: reply.clone(),
        };
        loop {
            match self.sender.send_timeout(work, QUERY_WORK_WAIT) {
                Ok(()) => return Ok(()),
                Err(SendTimeoutError::Timeout(returned)) => {
                    query_budget.check_waiting_for(OverloadReason::QueryQueue)?;
                    work = returned;
                }
                Err(SendTimeoutError::Disconnected(_)) => {
                    return Err(Error::WorkerStopped);
                }
            }
        }
    }
}

// The last executor owner disconnects the bounded queue. Workers finish their
// current cancelled work and exit; each job owns its snapshot/directory lease.
// Dropping a public query stream must never join a blocked worker.
fn query_worker_loop(receiver: &Receiver<QueryWork>) {
    while let Ok(work) = receiver.recv() {
        match work {
            #[cfg(test)]
            QueryWork::Pause { entered, release } => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            QueryWork::Series {
                lease,
                index,
                mut cursor,
                enqueued_at,
                query_budget,
                reply,
            } => {
                let queue_duration = enqueued_at.elapsed();
                let active_started = Instant::now();
                let mut reserved_bytes = 0_u64;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut rows = Vec::new();
                    while let Some(row) = cursor.next_row()? {
                        let row_bytes = std::mem::size_of::<GroupRow>() as u64;
                        query_budget.reserve_memory_class(QueryMemoryClass::Results, row_bytes)?;
                        reserved_bytes = reserved_bytes.saturating_add(row_bytes);
                        rows.push(row);
                    }
                    Ok::<_, Error>(rows)
                }));
                // Reply publication is the synchronization point used by the caller
                // before it releases the remaining query budget. Drop every cursor
                // reservation before making that reply observable.
                drop(cursor);
                drop(lease);
                query_budget.record_executor(
                    ExecutorTaskKind::Series,
                    queue_duration,
                    active_started.elapsed(),
                );
                match result {
                    Ok(Err(error)) => {
                        query_budget
                            .release_memory_class(QueryMemoryClass::Results, reserved_bytes);
                        let _ = reply.send((index, Err(error)));
                    }
                    Ok(Ok(rows)) => {
                        if reply.send((index, Ok(rows))).is_err() {
                            query_budget
                                .release_memory_class(QueryMemoryClass::Results, reserved_bytes);
                        }
                    }
                    Err(_) => {
                        query_budget
                            .release_memory_class(QueryMemoryClass::Results, reserved_bytes);
                        let _ = reply.send((index, Err(Error::WorkerStopped)));
                    }
                }
            }
            QueryWork::Planning {
                lease,
                index,
                task,
                enqueued_at,
                query_budget,
                reply,
            } => {
                let queue_duration = enqueued_at.elapsed();
                let active_started = Instant::now();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    task.run(&query_budget)
                }))
                .unwrap_or(Err(Error::WorkerStopped));
                drop(lease);
                query_budget.record_executor(
                    ExecutorTaskKind::Planning,
                    queue_duration,
                    active_started.elapsed(),
                );
                let _ = reply.send((index, result));
            }
        }
    }
}

#[cfg(test)]
mod tests;
