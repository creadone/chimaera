use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError};

use crate::{Error, Result, WriteReceipt};

use crate::engine::lifecycle::admission::{DEFAULT_WAIT_TIMEOUT, WriteAdmission, outcome_unknown};

/// Observed outcome of an accepted batch. Admission is distinct from durability;
/// keep observing the same ticket when completion is still pending.
#[derive(Debug)]
pub enum WriteStatus<'a> {
    /// No result yet. Continue waiting on this ticket.
    Pending,
    /// The batch was published after successful WAL synchronization.
    Durable(WriteReceipt),
    /// The writer rejected the batch during validation or resource checks before WAL.
    Failed(&'a Error),
    /// WAL/publication failure or lost writer. Recovery is needed to determine the outcome.
    OutcomeUnknown(&'a Error),
}

/// A handle to an accepted batch. Expiring or dropping the wait does not cancel it.
#[derive(Debug)]
#[must_use = "write ticket must be observed to learn the write outcome"]
pub struct WriteTicket {
    reply: Receiver<Result<WriteReceipt>>,
    outcome: Option<Result<WriteReceipt>>,
    admission: Option<Arc<WriteAdmission>>,
}

impl WriteTicket {
    pub(crate) const fn new(reply: Receiver<Result<WriteReceipt>>) -> Self {
        Self {
            reply,
            outcome: None,
            admission: None,
        }
    }

    pub(crate) fn with_admission(mut self, admission: Arc<WriteAdmission>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Check without blocking. A received result remains cached in the ticket.
    pub fn status(&mut self) -> WriteStatus<'_> {
        self.observe(None);
        self.current_status()
    }

    /// Wait until an absolute deadline. `Pending` leaves the ticket available
    /// for another wait; resubmitting instead of waiting is unsafe.
    pub fn wait_until(&mut self, deadline: Instant) -> WriteStatus<'_> {
        self.observe(Some(deadline));
        self.current_status()
    }

    /// Wait up to 30 seconds and return a durable receipt or an error.
    ///
    /// # Errors
    /// On timeout, returns `Error::WritePending` containing this ticket.
    /// Also returns writer errors; an error is not general permission to retry a batch.
    pub fn wait(mut self) -> Result<WriteReceipt> {
        self.observe(Some(Instant::now() + DEFAULT_WAIT_TIMEOUT));
        match self.outcome.take() {
            Some(outcome) => outcome,
            None => Err(Error::WritePending(Box::new(self))),
        }
    }

    fn observe(&mut self, deadline: Option<Instant>) {
        if self.outcome.is_some() {
            return;
        }
        match self.reply.try_recv() {
            Ok(result) => self.outcome = Some(result),
            Err(TryRecvError::Disconnected) => self.outcome = Some(Err(Error::WorkerStopped)),
            Err(TryRecvError::Empty) => {
                if let Some(deadline) = deadline {
                    match self
                        .reply
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(result) => self.outcome = Some(result),
                        Err(RecvTimeoutError::Disconnected) => {
                            self.outcome = Some(Err(Error::WorkerStopped))
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            if let Some(admission) = &self.admission {
                                admission.timed_out();
                            }
                        }
                    }
                }
            }
        }
    }

    fn current_status(&self) -> WriteStatus<'_> {
        match &self.outcome {
            None => WriteStatus::Pending,
            Some(Ok(receipt)) => WriteStatus::Durable(*receipt),
            Some(Err(error)) if outcome_unknown(error) => WriteStatus::OutcomeUnknown(error),
            Some(Err(error)) => WriteStatus::Failed(error),
        }
    }
}
