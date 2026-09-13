use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError};

use crate::{Error, Result, WriteReceipt};

use crate::lifecycle::{DEFAULT_WAIT_TIMEOUT, WriteAdmission, outcome_unknown};

/// Наблюдаемый результат принятого пакета. Только отказ admission до получения
/// ticket означает, что пакет гарантированно не был записан в WAL.
#[derive(Debug)]
pub enum WriteStatus<'a> {
    /// Ответ ещё не получен. Можно продолжать ожидание того же ticket.
    Pending,
    /// Пакет опубликован после успешной синхронизации WAL.
    Durable(WriteReceipt),
    /// Async-пакет опубликован без подтверждения синхронизации WAL.
    Visible(WriteReceipt),
    /// Writer вернул ошибку проверки пакета или ограничения ресурса.
    Failed(&'a Error),
    /// Ошибка WAL/публикации или потеря writer-а. Для выяснения исхода нужен recovery.
    OutcomeUnknown(&'a Error),
}

/// Подтверждение принятого пакета. Истечение срока не отменяет запись.
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

    /// Проверяет результат без ожидания. Полученный ответ сохраняется в ticket.
    pub fn status(&mut self) -> WriteStatus<'_> {
        self.observe(None);
        self.current_status()
    }

    /// Ждёт до абсолютного срока. `Pending` оставляет ticket пригодным для
    /// дальнейшего ожидания; повторная запись вместо ожидания небезопасна.
    pub fn wait_until(&mut self, deadline: Instant) -> WriteStatus<'_> {
        self.observe(Some(deadline));
        self.current_status()
    }

    /// Ожидает ответ не больше 30 секунд и возвращает прежний тип результата.
    ///
    /// # Errors
    /// При истечении срока возвращает `Error::WritePending` с самим ticket.
    /// Также возвращает ошибку writer-а; она не означает безопасный повтор пакета.
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
            Some(Ok(receipt)) if receipt.durable => WriteStatus::Durable(*receipt),
            Some(Ok(receipt)) => WriteStatus::Visible(*receipt),
            Some(Err(error)) if outcome_unknown(error) => WriteStatus::OutcomeUnknown(error),
            Some(Err(error)) => WriteStatus::Failed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use std::time::Duration;

    #[test]
    fn timeout_preserves_ticket_and_the_later_durable_result() {
        let (sender, receiver) = bounded(1);
        let mut ticket = WriteTicket::new(receiver);
        let started = Instant::now();
        assert!(matches!(
            ticket.wait_until(started + Duration::from_millis(10)),
            WriteStatus::Pending
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        let receipt = WriteReceipt {
            commit_id: 9,
            operation_count: 1,
            point_count: 1,
            durable: true,
        };
        sender.send(Ok(receipt)).expect("reply");
        assert!(matches!(ticket.status(), WriteStatus::Durable(value) if value == receipt));
        assert!(
            matches!(ticket.wait_until(started), WriteStatus::Durable(value) if value == receipt)
        );
        assert_eq!(ticket.wait().expect("cached result"), receipt);
    }

    #[test]
    fn async_visibility_and_unknown_wal_outcomes_are_distinct() {
        let (sender, receiver) = bounded(1);
        let mut ticket = WriteTicket::new(receiver);
        sender
            .send(Ok(WriteReceipt {
                commit_id: 1,
                operation_count: 1,
                point_count: 1,
                durable: false,
            }))
            .expect("reply");
        assert!(matches!(ticket.status(), WriteStatus::Visible(_)));
        let (sender, receiver) = bounded(1);
        let mut ticket = WriteTicket::new(receiver);
        sender
            .send(Err(Error::Io(std::io::Error::other("sync failed"))))
            .expect("reply");
        assert!(matches!(
            ticket.status(),
            WriteStatus::OutcomeUnknown(Error::Io(_))
        ));
        let (sender, receiver) = bounded(1);
        drop(sender);
        assert!(matches!(
            WriteTicket::new(receiver).status(),
            WriteStatus::OutcomeUnknown(Error::WorkerStopped)
        ));
    }
}
