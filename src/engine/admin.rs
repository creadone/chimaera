//! Проверка, checkpoint, flush и явная компакция Storage.

use super::options::CompactionOptions;
use super::{CompactionCommand, Storage, WriteCommand};
use crate::error::Result;
use crate::lifecycle::OverloadReason;
use crossbeam_channel::bounded;
use std::path::Path;

impl Storage {
    /// Читает опубликованные метаданные базы без `LOCK`, WAL writer или изменения файлов.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку отсутствующего либо повреждённого IDENTITY/manifest или I/O.
    pub fn info(path: impl AsRef<Path>) -> Result<crate::diagnostics::StorageInfo> {
        crate::diagnostics::storage_info(path.as_ref())
    }

    /// Полностью проверяет segment payload и WAL без открытия базы на запись.
    #[must_use]
    pub fn verify(path: impl AsRef<Path>) -> crate::diagnostics::VerificationReport {
        crate::diagnostics::verify_storage(path.as_ref())
    }
    #[must_use]
    pub fn verify_with_options(
        path: impl AsRef<Path>,
        options: crate::diagnostics::VerificationOptions,
    ) -> crate::diagnostics::VerificationReport {
        crate::diagnostics::verify_storage_with_options(path.as_ref(), options)
    }

    /// Восстанавливает проверяемую базу и допустимый хвост WAL в новый каталог.
    ///
    /// Исходный каталог открывается только на чтение; результат публикуется после
    /// штатного recovery/checkpoint и повторной полной verify.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку повреждения вне допустимого хвоста, существующего назначения
    /// либо файлового I/O.
    pub fn salvage(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<crate::diagnostics::SalvageResult> {
        crate::diagnostics::salvage_storage(source.as_ref(), destination.as_ref())
    }

    /// Сбрасывает все записи до текущего commit ID и публикует checkpoint.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку фонового работника или файловой системы.
    pub fn checkpoint(&self) -> Result<u64> {
        self.ensure_writable()?;
        let (reply, receiver) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Checkpoint { reply },
            &receiver,
            OverloadReason::WriteQueue,
            "checkpoint",
        )
    }

    /// Сбрасывает текущую memtable в L0, сохраняя покрытый WAL до checkpoint.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку writer, flush worker или атомарной публикации сегментов.
    pub fn flush(&self) -> Result<u64> {
        self.ensure_writable()?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Flush { reply: reply_tx },
            &reply_rx,
            OverloadReason::WriteQueue,
            "flush",
        )
    }

    /// Полностью обслуживает активные L0/L1, затем исторические L0/L2.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку чтения, записи или атомарной публикации.
    pub fn compact(&self) -> Result<u64> {
        self.compact_with_options(CompactionOptions::default())
    }

    /// Запускает полный либо пороговый проход слияния опубликованных L0.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку чтения, записи или атомарной публикации.
    pub fn compact_with_options(&self, options: CompactionOptions) -> Result<u64> {
        self.ensure_writable()?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.compaction_tx,
            CompactionCommand::Trigger {
                force: options.full,
                reply: Some(reply_tx),
            },
            &reply_rx,
            OverloadReason::CompactionDebt,
            "compact",
        )
    }
}
