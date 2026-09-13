//! Физические снимки Storage и потоковый JSONL import/export.

use super::read::PointStream;
use super::{Snapshot, Storage, WriteCommand};
use crate::error::{Error, Result};
use crate::exchange::{ExportOptions, ExportResult, ImportOptions, ImportResult};
use crate::lifecycle::OverloadReason;
use crate::segment::{OfflinePackDirectory, OfflinePackDirectoryInfo, Segment};
use crate::snapshot::{SnapshotImage, SnapshotInfo, restore_snapshot, write_snapshot};
use crossbeam_channel::bounded;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

impl Storage {
    /// Создаёт атомарно опубликованный физический snapshot без WAL.
    ///
    /// Writer останавливается только на время checkpoint и фиксации списка файлов.
    /// Копирование выполняется после снятия барьера, а удерживаемые ссылки не дают
    /// compaction reaper удалить входные pack-файлы.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку checkpoint, повреждения исходного образа, существующего
    /// каталога назначения или файлового I/O.
    pub fn snapshot(&self, destination: impl AsRef<Path>) -> Result<SnapshotInfo> {
        self.ensure_writable()?;
        let _storage_lock = self.clone_storage_lock()?;
        let image = self.capture_checkpoint_image()?;
        write_snapshot(
            &self.inner.root,
            destination.as_ref(),
            image,
            &self.inner.file_pool,
        )
    }

    /// Восстанавливает WAL-free snapshot в новый атомарно публикуемый каталог.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку проверки snapshot, существующего назначения или I/O.
    pub fn restore_snapshot(
        snapshot: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<SnapshotInfo> {
        restore_snapshot(snapshot.as_ref(), destination.as_ref())
    }

    /// Потоково записывает логическую JSONL-выгрузку одного commit ID.
    ///
    /// В начале выгрузки публикуется checkpoint, после чего запись может
    /// продолжаться одновременно с обходом удерживаемых immutable segment-файлов.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку лимита, checkpoint, декодирования сегмента, JSON или I/O.
    pub fn export_jsonl<W: std::io::Write>(
        &self,
        writer: W,
        options: ExportOptions,
    ) -> Result<ExportResult> {
        self.ensure_writable()?;
        let storage_lock = self.clone_storage_lock()?;
        options.validate()?;
        let image = self.capture_checkpoint_image()?;
        let mut series = BTreeSet::new();
        for segment in &image.segments {
            for series_key in segment.series_keys()? {
                series.insert(series_key);
                if series.len() > options.max_series {
                    return Err(Error::ResourceLimit("export series limit exceeded".into()));
                }
            }
        }
        let series = series.into_iter().collect::<Vec<_>>();
        let catalog = image.manifest.catalog.snapshot();
        let mut retention_cutoffs = HashMap::new();
        for series_key in &series {
            let policy = catalog.policy_for(*series_key)?;
            if let Some(cutoff) = image.manifest.retention_cutoffs.get(&policy.id) {
                retention_cutoffs.insert(*series_key, *cutoff);
            }
        }
        let mut segments_by_shard = BTreeMap::<u16, Vec<Arc<Segment>>>::new();
        for segment in image.segments {
            segments_by_shard
                .entry(segment.descriptor().shard_id)
                .or_default()
                .push(segment);
        }
        let snapshot = Snapshot {
            commit_id: image.manifest.checkpoint_commit,
            virtual_shards: image.manifest.virtual_shards,
            batches: Vec::new(),
            segments_by_shard,
            retention_cutoffs,
            aggregate_retention_cutoffs: HashMap::new(),
            retained_aggregate_resolutions: HashMap::new(),
            series_policy_ids: HashMap::new(),
            strictly_increasing_series: HashSet::new(),
            aggregate_archives: Vec::new(),
            published_read_directories: Vec::new(),
            metrics: Arc::clone(&self.inner.metrics),
            open_file_cache: Arc::clone(&self.inner.open_file_cache),
            offline_pack_directory: None,
            guard: None,
            write_regulator: None,
            query_executor: Arc::clone(&self.inner.query_executor),
            work_memory_governor: Arc::clone(&self.inner.work_memory_governor),
            storage_lock: Some(storage_lock),
        };
        crate::exchange::export_jsonl(
            writer,
            options,
            self.inner.options.clock.now_us(),
            snapshot.commit_id,
            &catalog,
            &series,
            |series_key| PointStream::from_snapshot(&snapshot, series_key, i64::MIN, i64::MAX),
        )
    }

    /// Потоково загружает JSONL по ограниченным атомарным batch.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку формата, лимита, каталога, WAL, checkpoint или I/O.
    pub fn import_jsonl<R: std::io::Read>(
        &self,
        reader: R,
        options: ImportOptions,
    ) -> Result<ImportResult> {
        self.ensure_writable()?;
        crate::exchange::import_jsonl(self, reader, options)
    }

    fn capture_checkpoint_image(&self) -> Result<SnapshotImage> {
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Snapshot { reply: reply_tx },
            &reply_rx,
            OverloadReason::WriteQueue,
            "snapshot",
        )
    }

    /// Строит checksummed directory прямых block locator для текущего checkpoint.
    ///
    /// Артефакт предназначен только для архитектурного query-only A/B и должен
    /// передаваться следующему [`Self::open`] через [`crate::OpenOptions::offline_pack_directory`].
    ///
    /// # Errors
    ///
    /// Возвращает ошибку checkpoint, чтения metadata, лимита или существующего output.
    pub fn build_offline_pack_directory(
        &self,
        output: impl AsRef<Path>,
    ) -> Result<OfflinePackDirectoryInfo> {
        self.ensure_writable()?;
        let image = self.capture_checkpoint_image()?;
        OfflinePackDirectory::build(output.as_ref(), &image.manifest, &image.segments)
    }
}
