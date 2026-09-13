//! Публичный приём пакетов, tickets и ограничение очереди записи.

use super::catalog::validate_write_time;
use super::support::{duration_nanos, lock, validate_batch_len, write};
use super::{APPROXIMATE_POINT_BYTES, Storage, WriteCommand};
use crate::cache::{MemoryClass, MemoryReservation};
use crate::catalog::{NamedPoint, NamedWriteOperation, SeriesRef};
use crate::error::{Error, Result, invalid};
use crate::lifecycle::{AdmissionOptions, OverloadReason, WAIT_SLICE, WriteReply};
use crate::types::{Durability, Point, SeriesKey, WriteOperation, WriteReceipt};
use crate::write_ticket::WriteTicket;
use crossbeam_channel::{TryRecvError, TrySendError, bounded};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

impl Storage {
    /// Записывает атомарный batch с логическими именами рядов.
    ///
    /// Все имена разрешаются на одном снимке каталога до постановки batch в WAL.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, значения, очереди или WAL.
    pub fn write_named_operations(
        &self,
        operations: Vec<NamedWriteOperation>,
        durability: Durability,
    ) -> Result<WriteReceipt> {
        self.enqueue_named_operations(operations, durability)?
            .wait()
    }

    /// Ставит именованный batch в ограниченную очередь и возвращает ticket.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, значения, очереди или закрытой базы.
    pub fn enqueue_named_operations(
        &self,
        operations: Vec<NamedWriteOperation>,
        durability: Durability,
    ) -> Result<WriteTicket> {
        self.ensure_available()?;
        if operations.is_empty() || operations.len() > 100_000 {
            return Err(invalid("batch must contain 1..=100000 operations"));
        }
        let catalog = self.catalog()?;
        let mut resolved = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                NamedWriteOperation::Put(point) => {
                    let (series, policy) = catalog.resolve(&point.series)?;
                    resolved.push((
                        WriteOperation::Put(Point::new(series, point.timestamp_us, point.value)?),
                        policy,
                    ));
                }
                NamedWriteOperation::Delete {
                    series,
                    timestamp_us,
                } => {
                    let (series, policy) = catalog.resolve(&series)?;
                    resolved.push((WriteOperation::delete(series, timestamp_us), policy));
                }
            }
        }
        let observed_now_us = self.inner.options.clock.now_us();
        {
            let mut state = write(&self.inner.state)?;
            state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
            let now_us = state.maintenance_now_us;
            for (operation, policy) in &resolved {
                validate_write_time(&mut state, policy, operation.timestamp_us(), now_us)?;
            }
        }
        self.enqueue_operations(
            resolved
                .into_iter()
                .map(|(operation, _)| operation)
                .collect(),
            durability,
        )
    }

    /// Записывает или заменяет одну точку по логическому имени ряда.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, значения, очереди или WAL.
    pub fn upsert(
        &self,
        series: SeriesRef,
        timestamp_us: i64,
        value: f64,
        durability: Durability,
    ) -> Result<WriteReceipt> {
        self.write_named_operations(
            vec![NamedPoint::new(series, timestamp_us, value)?.into()],
            durability,
        )
    }

    /// Удаляет одну точку по логическому имени ряда.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, очереди или WAL.
    pub fn delete_named_point(
        &self,
        series: SeriesRef,
        timestamp_us: i64,
        durability: Durability,
    ) -> Result<WriteReceipt> {
        self.write_named_operations(
            vec![NamedWriteOperation::delete(series, timestamp_us)],
            durability,
        )
    }

    /// Записывает пакет одним кадром WAL и публикует его одним commit ID.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для пустого/слишком большого пакета, нечислового значения,
    /// переполненной закрытой очереди либо ошибки хранилища.
    pub fn write_batch(&self, points: Vec<Point>, durability: Durability) -> Result<WriteReceipt> {
        validate_batch_len(points.len())?;
        self.write_operations(
            points.into_iter().map(WriteOperation::from).collect(),
            durability,
        )
    }

    /// Записывает атомарный пакет put/delete одним кадром WAL и commit ID.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для пустого/слишком большого пакета, нечислового значения,
    /// переполненной закрытой очереди либо ошибки хранилища.
    pub fn write_operations(
        &self,
        operations: Vec<WriteOperation>,
        durability: Durability,
    ) -> Result<WriteReceipt> {
        self.enqueue_operations(operations, durability)?.wait()
    }

    /// Ставит пакет put в ограниченную очередь и возвращает отдельное ожидание commit.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку валидации, закрытой базы или остановленного writer-а.
    pub fn enqueue_batch(&self, points: Vec<Point>, durability: Durability) -> Result<WriteTicket> {
        validate_batch_len(points.len())?;
        self.enqueue_operations(
            points.into_iter().map(WriteOperation::from).collect(),
            durability,
        )
    }

    /// Ставит атомарный пакет put/delete в ограниченную очередь.
    ///
    /// При заполнении очереди вызов применяет backpressure до освобождения места, не
    /// удерживая mutex. Файловая работа выполняется единственным writer worker-ом.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку валидации, закрытой базы или остановленного writer-а.
    pub fn enqueue_operations(
        &self,
        operations: Vec<WriteOperation>,
        durability: Durability,
    ) -> Result<WriteTicket> {
        self.enqueue_operations_with_options(operations, durability, AdmissionOptions::default())
    }

    /// Принимает put-пакет до заданного срока. `Overloaded` означает отказ до WAL.
    ///
    /// # Errors
    /// Ошибка валидации, остановка приёма, перегрузка или отказ writer.
    pub fn enqueue_batch_with_options(
        &self,
        points: Vec<Point>,
        durability: Durability,
        options: AdmissionOptions,
    ) -> Result<WriteTicket> {
        validate_batch_len(points.len())?;
        self.enqueue_operations_with_options(
            points.into_iter().map(WriteOperation::from).collect(),
            durability,
            options,
        )
    }

    /// Один deadline охватывает резерв памяти и очередь. Принятый пакет получает
    /// ticket; срок его ожидания не отменяет запись.
    ///
    /// # Errors
    /// `Overloaded` гарантирует, что этот пакет не был передан writer-у.
    /// Остальные ошибки описывают валидацию, остановку приёма или отказ базы.
    pub fn enqueue_operations_with_options(
        &self,
        operations: Vec<WriteOperation>,
        durability: Durability,
        options: AdmissionOptions,
    ) -> Result<WriteTicket> {
        self.ensure_writable()?;
        validate_batch_len(operations.len())?;
        if operations.iter().any(
            |operation| matches!(operation, WriteOperation::Put(point) if !point.value.is_finite()),
        ) {
            return Err(invalid("point value must be finite"));
        }
        let operation_count = operations.len() as u64;
        let memory_bytes = operation_count * APPROXIMATE_POINT_BYTES;
        let memory_reservation = self.reserve_write_queue_memory(memory_bytes, options.deadline)?;
        let (reply_tx, reply_rx) = bounded(1);
        let (reply, accepted) =
            WriteReply::new(reply_tx, Arc::clone(&self.inner.admission), operation_count);
        let mut command = WriteCommand::Write {
            operations,
            durability,
            memory_reservation,
            reply,
        };
        let mut waiting_since = None;
        let outcome = loop {
            if let Err(error) = self.ensure_writable() {
                break Err(error);
            }
            if Instant::now() >= options.deadline {
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
                        WAIT_SLICE.min(options.deadline.saturating_duration_since(Instant::now())),
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
        outcome?;
        Ok(WriteTicket::new(reply_rx).with_admission(Arc::clone(&self.inner.admission)))
    }

    fn reserve_write_queue_memory(
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
            if let Some(reservation) = self
                .inner
                .memory_governor
                .try_reserve_token(MemoryClass::WriteQueue, bytes)
            {
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

    /// Идемпотентно удаляет точку и возвращает подтверждение commit.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку очереди, WAL или фонового состояния базы.
    pub fn delete_point(
        &self,
        series: SeriesKey,
        timestamp_us: i64,
        durability: Durability,
    ) -> Result<WriteReceipt> {
        self.write_operations(
            vec![WriteOperation::delete(series, timestamp_us)],
            durability,
        )
    }
}
