//! Bounded executor for multi-series queries.

use super::snapshots::SnapshotGuard;
use super::{QUERY_WORK_WAIT, Snapshot};
use crate::engine::group::cursor::SeriesCursor;
use crate::engine::lifecycle::admission::OverloadReason;
use crate::error::{Error, Result};
use crate::persistence::lock::StorageLock;
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::GroupRow;
use crossbeam_channel::{Receiver, SendTimeoutError, Sender, bounded};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

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
    Series {
        lease: QueryLease,
        index: usize,
        cursor: Box<SeriesCursor>,
        enqueued_at: Instant,
        query_budget: Arc<QueryBudget>,
        reply: Sender<(usize, Result<Vec<GroupRow>>)>,
    },
}

pub(super) struct QueryExecutor {
    pub(super) sender: Sender<QueryWork>,
    pub(super) worker_count: usize,
    pub(super) _workers: Vec<JoinHandle<()>>,
}

impl QueryExecutor {
    pub(super) fn new(worker_count: usize) -> Result<Arc<Self>> {
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
                query_budget.record_executor(queue_duration, active_started.elapsed());
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
        }
    }
}
