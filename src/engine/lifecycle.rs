//! Остановка admission, drain и завершение фоновых ролей.

use super::reclamation::{finish_retired_archives, finish_retired_segments};
use super::support::{lock, receive_until};
use super::{
    CLOSED, CLOSING, CompactionCommand, FlushCommand, FlushPackCommand, Inner, MaintenanceCommand,
    OPEN, PublishCommand, Storage, WriteCommand,
};
use crate::error::{Error, Result, corrupt, invalid};
use crate::lifecycle::{
    CloseReport, DEFAULT_WAIT_TIMEOUT, DrainReport, OverloadReason, WAIT_SLICE, WriteAdmissionStats,
};
use crate::storage_lock::StorageLock;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

impl Storage {
    /// Прекращает приём новых операций. Уже принятые записи и query streams живут
    /// до своего завершения. Повторное включение приёма не поддерживается.
    pub fn stop_admission(&self) {
        self.inner.admission.stop();
    }

    /// Счётчики доступны также после stop_admission и close.
    #[must_use]
    pub fn write_admission_stats(&self) -> WriteAdmissionStats {
        self.inner.admission.stats()
    }

    /// Прекращает приём и ждёт checkpoint ранее принятых пакетов до deadline.
    /// `checkpoint_commit_id=None` означает, что можно продолжить тот же drain.
    /// Этот барьер не требует полной компакции L0 и освобождения чужих снимков.
    ///
    /// # Errors
    /// Отказ worker, checkpoint или уже начавшееся закрытие базы.
    pub fn drain(&self, deadline: Instant) -> Result<DrainReport> {
        self.stop_admission();
        loop {
            {
                let mut drain = lock(&self.inner.drain_state)?;
                if let Some(result) = &drain.result {
                    return match result {
                        Ok(commit) => Ok(DrainReport {
                            checkpoint_commit_id: Some(*commit),
                            writes: self.inner.admission.stats(),
                        }),
                        Err(message) => Err(corrupt(format!("drain failed: {message}"))),
                    };
                }
                if drain.reply.is_none() {
                    let _admission = lock(&self.inner.admission.state)?;
                    if self.inner.lifecycle.load(Ordering::Acquire) != OPEN {
                        return Err(Error::Closed);
                    }
                    if Instant::now() < deadline {
                        let (reply, receiver) = bounded(1);
                        match self
                            .inner
                            .write_tx
                            .try_send(WriteCommand::Checkpoint { reply })
                        {
                            Ok(()) => drain.reply = Some(receiver),
                            Err(TrySendError::Full(_)) => {}
                            Err(TrySendError::Disconnected(_)) => return Err(Error::WorkerStopped),
                        }
                    }
                }
                if let Some(reply) = &drain.reply {
                    match reply.try_recv() {
                        Ok(result) => {
                            drain.result = Some(result.map_err(|error| error.to_string()));
                            drain.reply = None;
                            continue;
                        }
                        Err(TryRecvError::Disconnected) => {
                            drain.result = Some(Err(Error::WorkerStopped.to_string()));
                            continue;
                        }
                        Err(TryRecvError::Empty) => {}
                    }
                }
            }
            if Instant::now() >= deadline {
                return Ok(DrainReport {
                    checkpoint_commit_id: None,
                    writes: self.inner.admission.stats(),
                });
            }
            thread::sleep(WAIT_SLICE.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    /// Начинает закрытие и ждёт не дольше deadline. Если `closed=false`, единственная
    /// задача закрытия продолжает работу и удерживает блокировку каталога.
    ///
    /// # Errors
    /// Ошибка checkpoint, остановки работников или запуска задачи закрытия.
    pub fn close_until(&self, deadline: Instant) -> Result<CloseReport> {
        close_inner_until(&self.inner, deadline)
    }

    /// Завершает checkpoint и остановку работников с ожиданием до 30 секунд.
    ///
    /// # Errors
    /// `OperationPending` оставляет закрытие в работе; состояние можно получить
    /// через close_until. Остальные ошибки описывают checkpoint или остановку.
    pub fn close(&self) -> Result<()> {
        shutdown_inner(&self.inner)
    }

    fn submit_command_until<T>(
        &self,
        sender: &Sender<T>,
        mut command: T,
        deadline: Instant,
        reason: OverloadReason,
    ) -> Result<()> {
        loop {
            self.ensure_writable()?;
            if Instant::now() >= deadline {
                return Err(Error::Overloaded(reason));
            }
            let admission = lock(&self.inner.admission.state)?;
            if admission.stopped {
                return Err(Error::AdmissionStopped);
            }
            let result = sender.try_send(command);
            drop(admission);
            match result {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => command = returned,
                Err(TrySendError::Disconnected(_)) => return Err(Error::WorkerStopped),
            }
            thread::sleep(WAIT_SLICE.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    pub(super) fn request_command<T, R>(
        &self,
        sender: &Sender<T>,
        command: T,
        reply: &Receiver<Result<R>>,
        reason: OverloadReason,
        operation: &'static str,
    ) -> Result<R> {
        let deadline = Instant::now() + DEFAULT_WAIT_TIMEOUT;
        self.submit_command_until(sender, command, deadline, reason)?;
        receive_until(reply, deadline, operation)
    }

    pub(super) fn ensure_available(&self) -> Result<()> {
        if self.inner.admission.stats().stopped
            && self.inner.lifecycle.load(Ordering::Acquire) == OPEN
        {
            return Err(Error::AdmissionStopped);
        }
        if self.inner.lifecycle.load(Ordering::Acquire) != OPEN {
            return Err(Error::Closed);
        }
        if let Some(message) = lock(&self.inner.degraded)?.as_ref() {
            return Err(corrupt(format!("storage is degraded: {message}")));
        }
        Ok(())
    }

    pub(super) fn ensure_writable(&self) -> Result<()> {
        self.ensure_available()?;
        if self.inner.offline_pack_directory.is_some() {
            return Err(invalid(
                "storage opened with an offline pack directory is query-only",
            ));
        }
        Ok(())
    }

    pub(super) fn clone_storage_lock(&self) -> Result<Arc<StorageLock>> {
        lock(&self.inner.storage_lock)?
            .as_ref()
            .map(Arc::clone)
            .ok_or(Error::Closed)
    }
}

fn checkpoint_inner(inner: &Inner) -> Result<u64> {
    let (reply_tx, reply_rx) = bounded(1);
    inner
        .write_tx
        .send(WriteCommand::Checkpoint { reply: reply_tx })
        .map_err(|_| Error::WorkerStopped)?;
    reply_rx.recv().map_err(|_| Error::WorkerStopped)?
}

pub(super) fn shutdown_inner(inner: &Arc<Inner>) -> Result<()> {
    let report = close_inner_until(inner, Instant::now() + DEFAULT_WAIT_TIMEOUT)?;
    if report.closed {
        Ok(())
    } else {
        Err(Error::OperationPending("close"))
    }
}

fn close_inner_until(inner: &Arc<Inner>, deadline: Instant) -> Result<CloseReport> {
    {
        let mut admission = lock(&inner.admission.state)?;
        admission.stopped = true;
        if inner.lifecycle.load(Ordering::Acquire) == OPEN {
            inner.lifecycle.store(CLOSING, Ordering::Release);
            let owned = Arc::clone(inner);
            if let Err(error) =
                thread::Builder::new()
                    .name("chimaera-close".into())
                    .spawn(move || {
                        let outcome = shutdown_owned(&owned);
                        if let Err(error) = outcome
                            && let Ok(mut failure) = owned.shutdown_failure.lock()
                        {
                            *failure = Some(error.to_string());
                        }
                        // This task owns the directory lock until all workers have stopped.
                        owned.metrics.shutdown_phase.store(9, Ordering::Relaxed);
                        let _guard = owned
                            .shutdown_wait
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        owned.lifecycle.store(CLOSED, Ordering::Release);
                        owned.shutdown_complete.notify_all();
                    })
            {
                inner.lifecycle.store(OPEN, Ordering::Release);
                return Err(error.into());
            }
        }
    }
    let mut guard = lock(&inner.shutdown_wait)?;
    while inner.lifecycle.load(Ordering::Acquire) != CLOSED {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(CloseReport {
                closed: false,
                writes: inner.admission.stats(),
            });
        }
        guard = inner
            .shutdown_complete
            .wait_timeout(guard, remaining)
            .map_err(|_| corrupt("shutdown wait mutex is poisoned"))?
            .0;
    }
    shutdown_status(inner)?;
    Ok(CloseReport {
        closed: true,
        writes: inner.admission.stats(),
    })
}

fn shutdown_status(inner: &Inner) -> Result<()> {
    match lock(&inner.shutdown_failure)?.as_ref() {
        Some(error) => Err(corrupt(format!("storage shutdown failed: {error}"))),
        None => Ok(()),
    }
}

fn shutdown_owned(inner: &Arc<Inner>) -> Result<()> {
    inner.metrics.shutdown_phase.store(1, Ordering::Relaxed);
    let checkpoint_result =
        if inner.options.checkpoint_on_close && inner.offline_pack_directory.is_none() {
            checkpoint_inner(inner)
        } else {
            Ok(0)
        };
    let mut worker_result = Ok(());
    let handles = lock(&inner.workers)?.take();
    if let Some(handles) = handles {
        let mut worker_failed = false;
        inner.metrics.shutdown_phase.store(2, Ordering::Relaxed);
        let _ = inner.maintenance_tx.send(MaintenanceCommand::Stop);
        worker_failed |= handles.maintenance.join().is_err();

        let (writer_stop_tx, writer_stop_rx) = bounded(1);
        inner.metrics.shutdown_phase.store(3, Ordering::Relaxed);
        let writer_stop_result = if inner
            .write_tx
            .send(WriteCommand::Stop {
                reply: writer_stop_tx,
            })
            .is_err()
        {
            Err(Error::WorkerStopped)
        } else {
            match writer_stop_rx.recv() {
                Ok(result) => result,
                Err(_) => Err(Error::WorkerStopped),
            }
        };
        worker_failed |= handles.writer.join().is_err();
        if let Err(error) = writer_stop_result {
            worker_result = Err(error);
        }

        inner.metrics.shutdown_phase.store(4, Ordering::Relaxed);
        let _ = inner.flush_tx.send(FlushCommand::Stop);
        worker_failed |= handles.flush.join().is_err();

        // Publisher stays alive until compaction has drained every trigger
        // queued by completed flush publications.
        inner.metrics.shutdown_phase.store(5, Ordering::Relaxed);
        let _ = inner.compaction_tx.send(CompactionCommand::Stop);
        let _ = inner.history_compaction_tx.send(CompactionCommand::Stop);
        worker_failed |= handles.compaction.join().is_err();

        inner.metrics.shutdown_phase.store(6, Ordering::Relaxed);
        for _ in 0..handles.flush_pack.len() {
            let _ = inner.flush_pack_tx.send(FlushPackCommand::Stop);
        }
        for worker in handles.flush_pack {
            worker_failed |= worker.join().is_err();
        }

        let (retired_tx, retired_rx) = bounded(1);
        inner.metrics.shutdown_phase.store(7, Ordering::Relaxed);
        let (retired, retired_archives) = if inner
            .publish_tx
            .send(PublishCommand::Stop { reply: retired_tx })
            .is_err()
        {
            worker_failed = true;
            (Vec::new(), Vec::new())
        } else {
            match retired_rx.recv() {
                Ok(retired) => retired,
                Err(_) => {
                    worker_failed = true;
                    (Vec::new(), Vec::new())
                }
            }
        };
        worker_failed |= handles.publisher.join().is_err();
        finish_retired_segments(inner, retired);
        finish_retired_archives(retired_archives);
        if worker_failed && worker_result.is_ok() {
            worker_result = Err(Error::WorkerStopped);
        }
    }
    inner.metrics.shutdown_phase.store(8, Ordering::Relaxed);
    let storage_lock = lock(&inner.storage_lock)?.take();
    let lock_result = match storage_lock {
        Some(storage_lock) => match Arc::try_unwrap(storage_lock) {
            Ok(storage_lock) => storage_lock.release(),
            Err(storage_lock) => {
                drop(storage_lock);
                Ok(())
            }
        },
        None => Ok(()),
    };
    checkpoint_result
        .map(|_| ())
        .and(worker_result)
        .and(lock_result)
}
