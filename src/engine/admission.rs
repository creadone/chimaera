//! Public batch admission, tickets, and bounded write queues.

use super::catalog::validate_write_time;
use super::support::{duration_nanos, lock, validate_batch_len};
use super::{APPROXIMATE_POINT_BYTES, Storage, WriteCommand};
use crate::catalog::{NamedPoint, SeriesRef};
use crate::engine::lifecycle::admission::{
    AdmissionOptions, OverloadReason, WAIT_SLICE, WriteReply,
};
use crate::engine::ticket::WriteTicket;
use crate::error::{Error, Result, invalid};
use crate::memory::cache::{MemoryClass, MemoryReservation};
use crate::types::{Point, WriteReceipt};
use crossbeam_channel::{TryRecvError, TrySendError, bounded};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

impl Storage {
    /// Write an atomic batch of named series through GroupCommit.
    ///
    /// All names resolve against one catalog snapshot before the batch reaches WAL.
    ///
    /// # Errors
    ///
    /// Returns catalog, value, queue, or WAL errors.
    pub fn write_named_batch(&self, operations: Vec<NamedPoint>) -> Result<WriteReceipt> {
        self.enqueue_named_batch(operations)?.wait()
    }

    /// Enqueue a named batch in the bounded queue and return a ticket.
    ///
    /// # Errors
    ///
    /// Returns catalog, value, queue, or closed-storage errors.
    pub fn enqueue_named_batch(&self, operations: Vec<NamedPoint>) -> Result<WriteTicket> {
        self.ensure_available()?;
        if operations.is_empty() || operations.len() > 100_000 {
            return Err(invalid("batch must contain 1..=100000 operations"));
        }
        let catalog = self.catalog()?;
        let mut resolved = Vec::with_capacity(operations.len());
        for point in operations {
            let (series, policy) = catalog.resolve(&point.series)?;
            resolved.push((Point::new(series, point.timestamp_us, point.value)?, policy));
        }
        let observed_now_us = self.inner.options.clock.now_us();
        {
            let mut state = self.inner.state.write()?;
            state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
            let now_us = state.maintenance_now_us;
            for (operation, policy) in &resolved {
                validate_write_time(&mut state, policy, operation.timestamp_us, now_us)?;
            }
        }
        self.enqueue_batch(
            resolved
                .into_iter()
                .map(|(operation, _)| operation)
                .collect(),
        )
    }

    /// Append the next point using a logical series name.
    ///
    /// # Errors
    ///
    /// Returns catalog, value, queue, or WAL errors.
    pub fn append(&self, series: SeriesRef, timestamp_us: i64, value: f64) -> Result<WriteReceipt> {
        self.write_named_batch(vec![NamedPoint::new(series, timestamp_us, value)?])
    }

    /// Write an atomic batch with strictly increasing timestamps per series.
    /// A successful receipt follows WAL synchronization and publication.
    /// Adjacent batches share a WAL sync through GroupCommit.
    /// A retry has no request identity. A timestamp-order error does not prove
    /// that the original batch committed; resolve an uncertain result before
    /// admitting newer writes or deletions to the affected series.
    ///
    /// # Errors
    /// Returns validation, timestamp-order, admission, or WAL errors; a batch is never partially committed.
    pub fn write_batch(&self, points: Vec<Point>) -> Result<WriteReceipt> {
        self.enqueue_batch(points)?.wait()
    }

    /// Enqueue a batch and return a ticket for its commit.
    /// The ticket itself does not establish durability: observe `WriteStatus::Durable`
    /// or a successful `WriteTicket::wait` result.
    ///
    /// # Errors
    /// Returns validation, closed-storage, or stopped-writer errors.
    pub fn enqueue_batch(&self, points: Vec<Point>) -> Result<WriteTicket> {
        self.enqueue_batch_with_options(points, AdmissionOptions::default())
    }

    /// Use one admission deadline for memory reservation and queueing. An accepted batch
    /// gets a ticket; expiration of its completion wait does not cancel the write.
    ///
    /// # Errors
    /// `Overloaded` means this batch was not handed to the writer.
    /// Other errors describe validation, stopped admission, or storage failure.
    pub fn enqueue_batch_with_options(
        &self,
        operations: Vec<Point>,
        options: AdmissionOptions,
    ) -> Result<WriteTicket> {
        #[cfg(feature = "pipeline-trace")]
        let trace = self
            .inner
            .pipeline_trace
            .get()
            .map_or_else(crate::diagnostics::trace::Token::default, |collector| {
                collector.begin(&operations)
            });
        #[cfg(feature = "pipeline-trace")]
        let mut trace_admission = crate::diagnostics::trace::AdmissionTrace::new(trace.clone());
        self.ensure_writable()?;
        validate_batch_len(operations.len())?;
        if operations.iter().any(|point| !point.value.is_finite()) {
            return Err(invalid("point value must be finite"));
        }
        let operation_count = operations.len() as u64;
        let memory_bytes =
            operation_count * APPROXIMATE_POINT_BYTES + crate::segment::PointBatch::METADATA_BYTES;
        let memory_reservation = self.reserve_write_queue_memory(memory_bytes, options.deadline)?;
        let (reply_tx, reply_rx) = bounded(1);
        let (reply, accepted) =
            WriteReply::new(reply_tx, Arc::clone(&self.inner.admission), operation_count);
        #[cfg(feature = "pipeline-trace")]
        let reply = {
            let mut reply = reply;
            trace.mark(crate::diagnostics::trace::Stage::AdmissionReady);
            reply.trace = trace;
            reply
        };
        let command = WriteCommand::Write {
            operations,
            memory_reservation,
            reply,
        };
        self.admit_command(command, &accepted, operation_count, options.deadline)?;

        #[cfg(feature = "pipeline-trace")]
        trace_admission.accepted();
        Ok(WriteTicket::new(reply_rx).with_admission(Arc::clone(&self.inner.admission)))
    }

    pub(super) fn admit_command(
        &self,
        mut command: WriteCommand,
        accepted: &std::sync::atomic::AtomicBool,
        operation_count: u64,
        deadline: Instant,
    ) -> Result<()> {
        let mut waiting_since = None;
        let outcome = loop {
            if let Err(error) = self.ensure_writable() {
                break Err(error);
            }
            if Instant::now() >= deadline {
                break Err(self.inner.admission.overloaded(OverloadReason::WriteQueue));
            }
            // stop_admission and acceptance share this lock. No channel wait or
            // filesystem operation runs while it is held.
            let mut admission = lock(&self.inner.admission.state)?;
            if admission.stopped {
                break Err(Error::AdmissionStopped);
            }
            match self.inner.write_tx.try_send(command) {
                Ok(()) => {
                    accepted.store(true, Ordering::Release);
                    admission.accepted_batches += 1;
                    admission.accepted_operations += operation_count;
                    admission.outstanding_batches += 1;
                    admission.outstanding_operations += operation_count;
                    break Ok(());
                }
                Err(TrySendError::Full(returned)) => {
                    command = returned;
                    drop(admission);
                    if waiting_since.is_none() {
                        waiting_since = Some(Instant::now());
                        self.inner
                            .metrics
                            .write_backpressure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    thread::sleep(
                        WAIT_SLICE.min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                Err(TrySendError::Disconnected(returned)) => {
                    drop(admission);
                    drop(returned);
                    break Err(Error::WorkerStopped);
                }
            }
        };
        if let Some(started) = waiting_since {
            self.inner
                .metrics
                .write_backpressure_nanos
                .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
        }
        outcome
    }

    pub(super) fn reserve_write_queue_memory(
        &self,
        bytes: u64,
        deadline: Instant,
    ) -> Result<MemoryReservation> {
        if bytes > self.inner.memory_governor.maximum_bytes() {
            return Err(Error::ResourceLimit(
                "write batch exceeds the managed memory limit".into(),
            ));
        }
        let mut cleared = false;
        let mut flush_reply = None;
        loop {
            self.ensure_writable()?;
            if Instant::now() >= deadline {
                return Err(self
                    .inner
                    .admission
                    .overloaded(OverloadReason::ManagedMemory));
            }
            if let Some(reservation) = self.inner.directory_page_pool.reserve_memory(
                &self.inner.memory_governor,
                MemoryClass::WriteQueue,
                bytes,
            )? {
                return Ok(reservation);
            }
            if !cleared {
                self.inner
                    .metrics
                    .memory_backpressure
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .write_queue_memory_backpressure
                    .fetch_add(1, Ordering::Relaxed);
                self.inner.block_cache.clear()?;
                self.inner.metadata_cache.clear()?;
                self.inner.file_resources.clear()?;
                self.inner.file_catalog.clear_cache()?;
                cleared = true;
                continue;
            }
            if flush_reply.is_none() {
                let (reply, receiver) = bounded(1);
                let admission = lock(&self.inner.admission.state)?;
                if admission.stopped {
                    return Err(Error::AdmissionStopped);
                }
                match self.inner.write_tx.try_send(WriteCommand::Flush { reply }) {
                    Ok(()) => flush_reply = Some(receiver),
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => return Err(Error::WorkerStopped),
                }
            }
            if let Some(reply) = &flush_reply {
                match reply.try_recv() {
                    Ok(result) => {
                        result?;
                        flush_reply = None;
                    }
                    Err(TryRecvError::Disconnected) => return Err(Error::WorkerStopped),
                    Err(TryRecvError::Empty) => {}
                }
            }
            thread::sleep(WAIT_SLICE.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
}
