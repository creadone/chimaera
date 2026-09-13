use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError};

use crate::{Error, Result, WriteReceipt};

pub(crate) const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const WAIT_SLICE: Duration = Duration::from_millis(5);

/// Ресурс, из-за которого истёк срок ожидания допуска.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub enum OverloadReason {
    WriteQueue,
    ManagedMemory,
    WorkMemory,
    QueryQueue,
    CompactionDebt,
    FilePermits,
    PublicationQueue,
}

/// Один абсолютный срок на резерв памяти и постановку пакета в очередь.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionOptions {
    pub deadline: Instant,
}

impl Default for AdmissionOptions {
    fn default() -> Self {
        Self {
            deadline: Instant::now() + DEFAULT_WAIT_TIMEOUT,
        }
    }
}

/// Счётчики пакетов; завершение writer учитывается независимо от владельца ticket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct WriteAdmissionStats {
    pub stopped: bool,
    pub accepted_batches: u64,
    pub accepted_operations: u64,
    pub rejected_overload: u64,
    pub wait_timeouts: u64,
    pub outstanding_batches: u64,
    pub outstanding_operations: u64,
    pub completed_batches: u64,
    pub failed_batches: u64,
    pub unknown_batches: u64,
    pub last_durable_commit: u64,
}

/// Drain прекращает новый приём и публикует checkpoint всех принятых записей.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainReport {
    pub checkpoint_commit_id: Option<u64>,
    pub writes: WriteAdmissionStats,
}

/// При `closed=false` остановка продолжается, а блокировка каталога сохраняется.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloseReport {
    pub closed: bool,
    pub writes: WriteAdmissionStats,
}

#[derive(Debug, Default)]
pub(crate) struct WriteAdmission {
    pub(crate) state: Mutex<WriteAdmissionStats>,
}

impl WriteAdmission {
    pub(crate) fn stats(&self) -> WriteAdmissionStats {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopped = true;
    }

    pub(crate) fn timed_out(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .wait_timeouts += 1;
    }

    pub(crate) fn overloaded(&self, reason: OverloadReason) -> Error {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rejected_overload += 1;
        Error::Overloaded(reason)
    }
}

/// Sender отмечает завершение даже тогда, когда вызывающий код уже удалил ticket.
pub(crate) struct WriteReply {
    sender: Sender<Result<WriteReceipt>>,
    admission: Arc<WriteAdmission>,
    accepted: Arc<AtomicBool>,
    operations: u64,
    finished: bool,
}

impl WriteReply {
    pub(crate) fn new(
        sender: Sender<Result<WriteReceipt>>,
        admission: Arc<WriteAdmission>,
        operations: u64,
    ) -> (Self, Arc<AtomicBool>) {
        let accepted = Arc::new(AtomicBool::new(false));
        (
            Self {
                sender,
                admission,
                accepted: Arc::clone(&accepted),
                operations,
                finished: false,
            },
            accepted,
        )
    }

    pub(crate) fn send(
        mut self,
        result: Result<WriteReceipt>,
    ) -> std::result::Result<(), TrySendError<Result<WriteReceipt>>> {
        self.finish(Some(&result));
        self.sender.try_send(result)
    }

    fn finish(&mut self, result: Option<&Result<WriteReceipt>>) {
        // Enqueue holds this short lock through try_send and acceptance accounting.
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.finished || !self.accepted.load(Ordering::Acquire) {
            return;
        }
        self.finished = true;
        state.outstanding_batches -= 1;
        state.outstanding_operations -= self.operations;
        match result {
            Some(Ok(receipt)) => {
                state.completed_batches += 1;
                if receipt.durable {
                    state.last_durable_commit = state.last_durable_commit.max(receipt.commit_id);
                }
            }
            Some(Err(error)) if !outcome_unknown(error) => state.failed_batches += 1,
            _ => state.unknown_batches += 1,
        }
    }
}

impl Drop for WriteReply {
    fn drop(&mut self) {
        self.finish(None);
    }
}

pub(crate) fn outcome_unknown(error: &Error) -> bool {
    // Only known pre-WAL validation failures are classified as definitive.
    !matches!(
        error,
        Error::Invalid(_)
            | Error::Catalog(_)
            | Error::NotFound(_)
            | Error::AlreadyExists(_)
            | Error::ExpiredPoint(_)
            | Error::ResourceLimit(_)
            | Error::InsufficientDiskSpace { .. }
    )
}
