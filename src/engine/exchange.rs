//! Physical storage snapshots and streaming JSONL import/export.

use super::read::PointStream;
use super::{Snapshot, Storage, WriteCommand};
use crate::admin::exchange::{ExportOptions, ExportResult, ImportOptions, ImportResult};
use crate::admin::snapshot::{SnapshotImage, SnapshotInfo, restore_snapshot, write_snapshot};
use crate::engine::lifecycle::admission::OverloadReason;
use crate::error::{Error, Result};
use crate::segment::{OfflinePackDirectory, OfflinePackDirectoryInfo, Segment};
use crossbeam_channel::bounded;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

impl Storage {
    /// Create an atomically published physical snapshot without WAL.
    ///
    /// The writer pauses only for checkpoint and catalogue-root capture.
    /// Copying runs after this barrier; the retained generation prevents
    /// the compaction reaper from deleting source packs.
    ///
    /// # Errors
    ///
    /// Returns checkpoint, corrupt-source, existing-destination,
    /// or filesystem I/O errors.
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

    /// Restore a WAL-free snapshot into a new, atomically published directory.
    ///
    /// # Errors
    ///
    /// Returns snapshot-verification, existing-destination, or I/O errors.
    pub fn restore_snapshot(
        snapshot: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<SnapshotInfo> {
        restore_snapshot(snapshot.as_ref(), destination.as_ref())
    }

    /// Stream a logical JSONL export from one commit ID.
    ///
    /// A checkpoint is published first; writes can then continue
    /// while export traverses the retained immutable segment files.
    /// Every discovered series must belong to a registered bucket and policy.
    /// Use a physical snapshot for databases with unregistered numeric namespaces.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` for an unregistered bucket/policy, or limit,
    /// checkpoint, segment-decoding, JSON, or I/O errors.
    pub fn export_jsonl<W: std::io::Write>(
        &self,
        writer: W,
        options: ExportOptions,
    ) -> Result<ExportResult> {
        self.ensure_writable()?;
        let storage_lock = self.clone_storage_lock()?;
        options.validate()?;
        let mut image = self.capture_checkpoint_image()?;
        // Logical export retains its existing, explicitly budgeted image.
        // Physical snapshot/restore do not need these per-segment handles.
        let commit = image.manifest.checkpoint_commit;
        let (segments, _archives, _image_memory) =
            image.file_view.materialize(&mut image.manifest, commit)?;
        let mut memory = self
            .inner
            .work_memory_governor
            .reserve_background(crate::memory::work::WorkMemoryClass::QuerySources, 0)?;
        let mut series = BTreeSet::new();
        for segment in &segments {
            segment.visit_series_max_timestamps(
                &self.inner.work_memory_governor,
                |series_key, _| {
                    if !series.contains(&series_key) {
                        if series.len() >= options.max_series {
                            return Err(Error::ResourceLimit(
                                "export series limit exceeded".into(),
                            ));
                        }
                        // B-tree entry, collected key vector and retention hash table.
                        if !memory.try_grow(256)? {
                            return Err(Error::ResourceLimit(
                                "export series exceed work memory".into(),
                            ));
                        }
                        series.insert(series_key);
                    }
                    Ok(())
                },
            )?;
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
        for segment in segments {
            segments_by_shard
                .entry(segment.descriptor().shard_id)
                .or_default()
                .push(segment);
        }
        let snapshot = Snapshot {
            deletions: Arc::clone(&image.manifest.deletions),
            file_view: Some(image.file_view),
            commit_id: image.manifest.checkpoint_commit,
            virtual_shards: image.manifest.virtual_shards,
            batches: Vec::new(),
            segments_by_shard,
            retention_cutoffs,
            aggregate_retention_cutoffs: HashMap::new(),
            retained_aggregate_resolutions: HashMap::new(),
            series_policy_ids: HashMap::new(),
            aggregate_archives: Vec::new(),
            metrics: Arc::clone(&self.inner.metrics),
            offline_pack_directory: None,
            guard: None,
            write_regulator: None,
            query_executor: Arc::clone(&self.inner.query_executor),
            work_memory_governor: Arc::clone(&self.inner.work_memory_governor),
            storage_lock: Some(storage_lock),
            memory: None,
        };
        crate::admin::exchange::export_jsonl(
            writer,
            options,
            self.inner.options.clock.now_us(),
            snapshot.commit_id,
            &catalog,
            &series,
            |series_key| PointStream::from_snapshot(&snapshot, series_key, i64::MIN, i64::MAX),
        )
    }

    /// Import JSONL using bounded atomic batches; the whole import is not atomic.
    ///
    /// # Errors
    ///
    /// Returns format, limit, catalog, WAL, checkpoint, or I/O errors.
    pub fn import_jsonl<R: std::io::Read>(
        &self,
        reader: R,
        options: ImportOptions,
    ) -> Result<ImportResult> {
        self.ensure_writable()?;
        crate::admin::exchange::import_jsonl(self, reader, options)
    }

    pub(super) fn capture_checkpoint_image(&self) -> Result<SnapshotImage> {
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Snapshot { reply: reply_tx },
            &reply_rx,
            OverloadReason::WriteQueue,
            "snapshot",
        )
    }

    /// Build a checksummed directory of direct block locators for the current checkpoint.
    ///
    /// This artifact is for experimental query-only comparisons. Pass it to
    /// the next [`Self::open`] through [`crate::OpenOptions::offline_pack_directory`].
    ///
    /// # Errors
    ///
    /// Returns checkpoint, metadata-read, resource-limit, or existing-output errors.
    pub fn build_offline_pack_directory(
        &self,
        output: impl AsRef<Path>,
    ) -> Result<OfflinePackDirectoryInfo> {
        self.ensure_writable()?;
        let mut image = self.capture_checkpoint_image()?;
        // This experimental export still explicitly needs a full segment image.
        let commit = image.manifest.checkpoint_commit;
        let (segments, _archives, _memory) =
            image.file_view.materialize(&mut image.manifest, commit)?;
        OfflinePackDirectory::build(output.as_ref(), &image.manifest, &segments)
    }
}
