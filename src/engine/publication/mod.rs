//! Durable file publication and shared-state updates.

#[cfg(feature = "publication-diagnostics")]
mod diagnostics;

#[cfg(feature = "pipeline-trace")]
use crate::diagnostics::trace::Stage;

use super::catalog::ensure_series_namespace_unused;
use super::reclamation::collect_files;
use super::support::{ActivityGauge, duration_nanos};
use super::{CompactionCommand, Generation, Inner, PublishCommand, degrade};
use crate::admin::snapshot::SnapshotImage;
use crate::aggregate::archive::AggregateArchive;
use crate::diagnostics::publication::{self as cost, Stage as CostStage};
use crate::engine::compaction::selection::validate_l1_publication;
use crate::error::{Error, Result, corrupt};
use crate::persistence::manifest::Manifest;
use crate::persistence::wal::sync_directory;
use crate::segment::Segment;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub(super) fn publisher_loop(
    inner: Arc<Inner>,
    receiver: Receiver<PublishCommand>,
    mut manifest: Manifest,
) {
    let mut pending_wals = Vec::<(u64, Vec<PathBuf>)>::new();
    loop {
        let command = match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(command) => command,
            Err(RecvTimeoutError::Timeout) => {
                #[cfg(feature = "publication-diagnostics")]
                let _diagnostic_scope = cost::enter(inner.publication_diagnostics.get());
                let _phase = ActivityGauge::enter(&inner.metrics.publisher_phase, 5);
                let result = {
                    let _timer = cost::stage(CostStage::PublisherCatalogWait);
                    crate::persistence::lock::StorageLock::catalog_exclusive(&inner.root)
                }
                .and_then(|_guard| collect_files(&inner, &mut manifest));
                if let Err(error) = result
                    && !matches!(error, Error::ResourceLimit(_))
                {
                    degrade(&inner, &error);
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                break;
            }
        };
        #[cfg(feature = "publication-diagnostics")]
        let _diagnostic_scope = cost::enter(inner.publication_diagnostics.get());
        let _catalog_lock = {
            let _timer = cost::stage(CostStage::PublisherCatalogWait);
            match crate::persistence::lock::StorageLock::catalog_exclusive(&inner.root) {
                Ok(guard) => guard,
                Err(error) => {
                    degrade(&inner, &error);
                    break;
                }
            }
        };
        let _command_timer = cost::stage(match &command {
            PublishCommand::Flush { .. } => CostStage::Flush,
            PublishCommand::Compaction { .. } => CostStage::Compaction,
            _ => CostStage::Other,
        });
        let phase = match &command {
            PublishCommand::Flush { .. } => 1,
            PublishCommand::Compaction { .. } => 2,
            PublishCommand::CollectFiles { .. } => 3,
            PublishCommand::Checkpoint { .. } => 4,
            PublishCommand::CaptureSnapshot { .. } => 6,
            PublishCommand::Stop { .. } => 7,
            _ => 5,
        };
        let _phase = ActivityGauge::enter(&inner.metrics.publisher_phase, phase);
        match command {
            PublishCommand::CollectDeletions { swept, reply } => {
                let _ = reply.send(super::deletion::collect(&inner, &mut manifest, &swept));
            }

            PublishCommand::CreateSpace {
                name,
                created_at_us,
                reply,
            } => {
                let result = publish_catalog(&inner, &mut manifest, |catalog| {
                    catalog.create_space(name, created_at_us)
                });
                let _ = reply.send(result);
            }
            PublishCommand::CreatePolicy { options, reply } => {
                let result = publish_catalog(&inner, &mut manifest, |catalog| {
                    catalog.create_policy(options)
                });
                let _ = reply.send(result);
            }
            PublishCommand::CreateBucket {
                space,
                name,
                policy,
                created_at_us,
                reply,
            } => {
                let result = publish_catalog(&inner, &mut manifest, |catalog| {
                    let bucket = catalog.create_bucket(&space, name, &policy, created_at_us)?;
                    ensure_series_namespace_unused(&inner, bucket.space_id, bucket.id)?;
                    Ok(bucket)
                });
                let _ = reply.send(result);
            }
            PublishCommand::Flush {
                #[cfg(feature = "publication-diagnostics")]
                queued,
                generation,
                segments,
                mut unpublished,
                reply,
            } => {
                #[cfg(feature = "pipeline-trace")]
                inner.trace_generation(generation.id, Stage::FlushPublishStarted);
                #[cfg(feature = "publication-diagnostics")]
                cost::record_elapsed(CostStage::QueueFlush, queued);
                let result = publish_flush(
                    &inner,
                    &mut manifest,
                    &generation,
                    &segments,
                    &mut unpublished,
                );
                if result.is_ok() {
                    #[cfg(feature = "pipeline-trace")]
                    inner.trace_outputs(&segments, None);
                    pending_wals.push((generation.maximum_commit, generation.wal_paths.clone()));
                    wake_compaction(&inner.compaction_tx);
                    wake_compaction(&inner.history_compaction_tx);
                }
                let _ = reply.send(result);
            }
            PublishCommand::Compaction {
                #[cfg(feature = "publication-diagnostics")]
                queued,
                inputs,
                outputs,
                new_archives,
                expired_archives,
                mut unpublished,
                reply,
            } => {
                #[cfg(feature = "pipeline-trace")]
                inner.trace_outputs(&outputs, Some(Stage::CompactionPublishStarted));
                #[cfg(feature = "publication-diagnostics")]
                cost::record_elapsed(CostStage::QueueCompaction, queued);
                let result = publish_compaction(
                    &inner,
                    &mut manifest,
                    &inputs,
                    &outputs,
                    &new_archives,
                    &expired_archives,
                    &mut unpublished,
                );
                if result.is_ok() {
                    #[cfg(feature = "pipeline-trace")]
                    inner.trace_outputs(&outputs, None);
                }
                let _ = reply.send(result);
            }
            PublishCommand::Checkpoint {
                commit_id,
                wal_bytes_through_commit,
                reply,
            } => {
                let result =
                    publish_checkpoint(&inner, &mut manifest, commit_id, wal_bytes_through_commit);
                if let Ok(checkpoint_commit) = result.as_ref() {
                    remove_checkpointed_wals(&inner, &mut pending_wals, *checkpoint_commit);
                }
                let _ = reply.send(result);
            }
            PublishCommand::CaptureSnapshot {
                checkpoint_commit,
                reply,
            } => {
                let result = capture_snapshot_image(&inner, &manifest, checkpoint_commit);
                let _ = reply.send(result);
            }
            PublishCommand::CollectFiles { reply } => {
                let result = match collect_files(&inner, &mut manifest) {
                    Err(Error::ResourceLimit(_)) => Ok(()),
                    Err(error) => {
                        degrade(&inner, &error);
                        Err(error)
                    }
                    Ok(()) => Ok(()),
                };
                let _ = reply.send(result);
            }
            PublishCommand::Stop { reply } => {
                if let Err(error) = collect_files(&inner, &mut manifest)
                    && !matches!(error, Error::ResourceLimit(_))
                {
                    degrade(&inner, &error);
                }
                let _ = reply.send(());
                break;
            }
        }
    }
}

// Flush publication and idle retention share this bounded wake path. A pending
// command inspects the latest root; commands carrying a reply stay intact.
pub(super) fn wake_compaction(sender: &crossbeam_channel::Sender<CompactionCommand>) {
    if sender.is_empty() {
        let _ = sender.try_send(CompactionCommand::Trigger {
            force: false,
            reply: None,
        });
    }
}

fn capture_snapshot_image(
    inner: &Inner,
    manifest: &Manifest,
    checkpoint_commit: u64,
) -> Result<SnapshotImage> {
    if manifest.checkpoint_commit != checkpoint_commit {
        return Err(corrupt("snapshot checkpoint changed before capture"));
    }
    let state = inner.state.read()?;
    if state.visible_commit != checkpoint_commit
        || !state.active_batches.is_empty()
        || !state.frozen.is_empty()
    {
        return Err(corrupt("snapshot writer barrier is incomplete"));
    }
    let files = state.files.clone();
    drop(state);
    // The checkpoint manifest has just been published under this publisher.
    // Charge the small cloned header conservatively, including its semantic
    // catalogue and B-tree maps. File descriptors remain on disk under the lease.
    let header_bytes = std::fs::metadata(inner.root.join("manifest/MANIFEST.json"))?.len();
    let memory = inner
        .memory_governor
        .try_reserve_token(
            crate::memory::cache::MemoryClass::MetadataCatalog,
            header_bytes
                .saturating_mul(8)
                .saturating_add(std::mem::size_of::<SnapshotImage>() as u64),
        )
        .ok_or_else(|| {
            crate::Error::ResourceLimit("snapshot header exceeds managed memory".into())
        })?;
    Ok(SnapshotImage {
        manifest: manifest.clone(),
        file_view: files,
        _memory: memory,
    })
}

fn publish_catalog<T>(
    inner: &Inner,
    manifest: &mut Manifest,
    update: impl FnOnce(&mut crate::catalog::CatalogState) -> Result<T>,
) -> Result<T> {
    let mut next = manifest.clone();
    let value = update(&mut next.catalog)?;
    capture_manifest_metadata(inner, &mut next)?;
    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        *manifest = next.clone();
        committed.set(true);
    });
    let snapshot = next.catalog.snapshot();
    complete_manifest_publication(inner, committed.get(), persistence, || {
        let _timer = cost::stage(CostStage::ApplyState);
        let lock_started = Instant::now();
        let mut state = {
            let _timer = cost::stage(CostStage::PublisherStateWait);
            inner.state.write()?
        };
        record_manifest_lock_wait(inner, lock_started.elapsed());
        state.catalog = snapshot;
        Ok(value)
    })
}

fn publish_flush(
    inner: &Inner,
    manifest: &mut Manifest,
    generation: &Generation,
    segments: &[Arc<Segment>],
    unpublished: &mut UnpublishedArtifacts,
) -> Result<()> {
    validate_flush_generation_commits(manifest, generation)?;
    let state = inner.state.read()?;
    if !state.frozen.iter().any(|g| g.id == generation.id) {
        return Err(corrupt("flush lost frozen generation"));
    }
    let base = state.files.clone();
    drop(state);
    let mut builder = super::files::publication::Builder::new(&base, &inner.work_memory_governor)?;
    super::reclamation::collect_into(inner, &mut builder)?;
    builder.replace_segments(&[], segments)?;
    let proposal = builder.finish(
        inner.options.l0_compaction_trigger,
        inner.options.history_rewrite_trigger,
        inner.options.history_segment_target_bytes,
        inner.options.history_window.as_micros() as i64,
    )?;
    install_files(
        inner,
        manifest,
        &base,
        proposal,
        unpublished,
        Some(generation),
    )
}

fn validate_flush_generation_commits(manifest: &Manifest, generation: &Generation) -> Result<()> {
    let expected_first = manifest
        .materialized_commit()
        .checked_add(1)
        .ok_or_else(|| corrupt("materialized commit ID is exhausted"))?;
    let mut expected = expected_first;
    for batch in &generation.batches {
        let commit_id = batch.commit_id();
        if commit_id != expected
            || batch
                .iter()
                .any(|operation| operation.commit_id != commit_id)
        {
            return Err(corrupt(
                "flush generation does not continuously follow the materialized commit",
            ));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| corrupt("flush generation commit ID is exhausted"))?;
    }
    if expected == expected_first || expected - 1 != generation.maximum_commit {
        return Err(corrupt(
            "flush generation maximum commit disagrees with its batches",
        ));
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct UnpublishedArtifacts {
    pub(super) paths: HashSet<PathBuf>,
}

impl UnpublishedArtifacts {
    pub(super) fn push(&mut self, path: PathBuf) {
        self.paths.insert(path);
    }

    pub(super) fn absorb(&mut self, other: &mut Self) {
        self.paths.extend(other.paths.drain());
    }

    pub(super) fn commit(&mut self) {
        self.paths.clear();
    }
}

impl Drop for UnpublishedArtifacts {
    fn drop(&mut self) {
        let mut changed = HashSet::new();
        for path in self.paths.drain() {
            if fs::remove_file(&path).is_ok()
                && let Some(parent) = path.parent()
            {
                changed.insert(parent.to_path_buf());
            }
        }
        for directory in changed {
            let _ = sync_directory(&directory);
        }
    }
}

fn publish_compaction(
    inner: &Inner,
    manifest: &mut Manifest,
    inputs: &[Arc<Segment>],
    outputs: &[Arc<Segment>],
    new_archives: &[Arc<AggregateArchive>],
    expired_archives: &[Arc<AggregateArchive>],
    unpublished: &mut UnpublishedArtifacts,
) -> Result<()> {
    let base = inner.state.read()?.files.clone();
    let input_ids = inputs
        .iter()
        .map(|s| s.descriptor().id)
        .collect::<HashSet<_>>();
    let domains = inputs
        .iter()
        .chain(outputs)
        .map(|s| s.descriptor().domain())
        .collect::<std::collections::BTreeSet<_>>();
    // Validate overlaps against each changed domain, never all history.
    let mut memory = inner.work_memory_governor.reserve_background(
        crate::memory::work::WorkMemoryClass::Compaction,
        1024 * 1024,
    )?;
    let validation_timer = cost::stage(CostStage::PublicationValidation);
    for domain in domains {
        let mut remaining = Vec::new();
        if let Some(summary) = base.domain_metadata(domain)? {
            base.domain_levels(domain, &summary, [false, true], |entry| {
                cost::add(cost::Counter::PublicationValidationDescriptors, 1);
                if !input_ids.contains(&entry.descriptor.id) {
                    if !memory.try_grow(1024)? {
                        return Err(Error::ResourceLimit(
                            "L1 publication validation memory".into(),
                        ));
                    }
                    remaining.push(entry.descriptor);
                }
                Ok(())
            })?;
        }
        validate_l1_publication(
            inputs.iter().map(|s| s.descriptor()),
            outputs.iter().map(|s| s.descriptor()),
            remaining.iter(),
        )?;
    }
    drop(validation_timer);
    let mut builder = super::files::publication::Builder::new(&base, &inner.work_memory_governor)?;
    super::reclamation::collect_into(inner, &mut builder)?;
    builder.reclaim_removed_bytes = !inner.state.read()?.deletions.entries.is_empty();
    builder.replace_segments(inputs, outputs)?;
    builder.replace_archives(expired_archives, new_archives)?;
    let proposal = builder.finish(
        inner.options.l0_compaction_trigger,
        inner.options.history_rewrite_trigger,
        inner.options.history_segment_target_bytes,
        inner.options.history_window.as_micros() as i64,
    )?;
    install_files(inner, manifest, &base, proposal, unpublished, None)?;
    base.resources.invalidate(input_ids.into_iter())?;
    Ok(())
}

pub(in crate::engine) fn install_files(
    inner: &Inner,
    manifest: &mut Manifest,
    base: &super::files::View,
    proposal: super::files::publication::Proposal,
    unpublished: &mut UnpublishedArtifacts,
    generation: Option<&Generation>,
) -> Result<()> {
    let prepared = {
        let _timer = cost::stage(CostStage::CatalogPrepare);
        proposal
            .prepare(&inner.file_catalog, &base.snapshot)
            .inspect_err(|error| {
                if inner.file_catalog.requires_reopen() {
                    degrade(inner, error);
                }
            })?
    };
    let mut next = manifest.clone();
    next.file_catalog = prepared.root;
    next.file_totals = proposal.totals.clone();
    if let Some(generation) = generation {
        next.materialized_commit = Some(generation.maximum_commit);
    }
    capture_manifest_metadata(inner, &mut next).inspect_err(|error| degrade(inner, error))?;
    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        unpublished.commit();
        *manifest = next.clone();
        committed.set(true);
    });
    let durable = persistence.is_ok();
    if let Err(error) = &persistence {
        degrade(inner, error);
    }
    complete_manifest_publication(inner, committed.get(), persistence, || {
        let _timer = cost::stage(CostStage::ApplyState);
        inner.file_catalog.committed(prepared.root, durable)?;
        let mut state = {
            let _timer = cost::stage(CostStage::PublisherStateWait);
            inner.state.write()?
        };
        if state.files.root() != base.root() {
            return Err(corrupt("file publication base changed"));
        }
        if let Some(generation) = generation {
            if !state.frozen.iter().any(|g| g.id == generation.id) {
                return Err(corrupt("flush lost frozen generation at commit"));
            }
            state.frozen.retain(|g| g.id != generation.id);
            state.materialized_commit = generation.maximum_commit;
        }
        state.files = super::files::View {
            snapshot: prepared,
            resources: Arc::clone(&base.resources),
            totals: proposal.totals,
        };
        Ok(())
    })
}

fn publish_checkpoint(
    inner: &Inner,
    manifest: &mut Manifest,
    commit_id: u64,
    wal_bytes_through_commit: u64,
) -> Result<u64> {
    let visible_commit = inner.state.read()?.visible_commit;
    if commit_id > visible_commit {
        return Err(corrupt("checkpoint commit exceeds the visible commit"));
    }
    if commit_id > manifest.materialized_commit() {
        return Err(corrupt("checkpoint commit exceeds the materialized commit"));
    }
    let mut next = manifest.clone();
    next.checkpoint_commit = next.checkpoint_commit.max(commit_id);
    capture_manifest_metadata(inner, &mut next)?;
    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        *manifest = next.clone();
        committed.set(true);
    });
    let checkpoint_commit = next.checkpoint_commit;
    complete_manifest_publication(inner, committed.get(), persistence, || {
        let _timer = cost::stage(CostStage::ApplyState);
        let lock_started = Instant::now();
        let mut state = {
            let _timer = cost::stage(CostStage::PublisherStateWait);
            inner.state.write()?
        };
        record_manifest_lock_wait(inner, lock_started.elapsed());
        state.checkpoint_commit = checkpoint_commit;
        state.checkpoint_wal_bytes = state
            .checkpoint_wal_bytes
            .max(wal_bytes_through_commit.min(state.wal_bytes_total));
        if state
            .last_cold_write
            .is_some_and(|(cold_commit, _)| cold_commit <= checkpoint_commit)
        {
            state.last_cold_write = None;
        }
        Ok(checkpoint_commit)
    })
}

pub(super) fn complete_manifest_publication<T>(
    inner: &Inner,
    committed: bool,
    persistence: Result<()>,
    apply: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if !committed {
        return match persistence {
            Err(error) => Err(error),
            Ok(()) => Err(corrupt("manifest persist completed without a commit point")),
        };
    }
    if let Err(error) = &persistence {
        degrade(inner, error);
    }
    let applied = apply();
    if let Err(error) = &applied {
        degrade(inner, error);
    }
    match applied {
        Err(error) => Err(error),
        Ok(value) => {
            persistence?;
            Ok(value)
        }
    }
}

pub(super) fn persist_manifest_with_commit(
    inner: &Inner,
    manifest: &Manifest,
    committed: impl FnOnce(),
) -> Result<()> {
    let _timer = cost::stage(CostStage::PersistManifest);
    let metrics = manifest.persist_with_commit(&inner.root, committed)?;
    // Only a fully synced manifest permits forgetting strict-write history.
    let mut state = {
        let _timer = cost::stage(CostStage::PublisherStateWait);
        inner.state.write()?
    };
    if state.durable_retention_cutoffs != manifest.retention_cutoffs {
        state
            .durable_retention_cutoffs
            .clone_from(&manifest.retention_cutoffs);
        state.watermark_prune_pending = true;
    }
    drop(state);
    inner
        .metrics
        .manifest_publications
        .fetch_add(1, Ordering::Relaxed);
    inner
        .metrics
        .manifest_encode_nanos
        .fetch_add(metrics.encode_nanos, Ordering::Relaxed);
    inner
        .metrics
        .manifest_write_nanos
        .fetch_add(metrics.write_nanos, Ordering::Relaxed);
    inner
        .metrics
        .manifest_fsync_nanos
        .fetch_add(metrics.fsync_nanos, Ordering::Relaxed);
    debug_assert!(metrics.bytes <= crate::persistence::manifest::MANIFEST_MAX_BYTES);
    Ok(())
}

pub(super) fn record_manifest_lock_wait(inner: &Inner, duration: Duration) {
    inner
        .metrics
        .manifest_lock_wait_nanos
        .fetch_add(duration_nanos(duration), Ordering::Relaxed);
}

fn remove_checkpointed_wals(
    inner: &Inner,
    pending_wals: &mut Vec<(u64, Vec<PathBuf>)>,
    checkpoint_commit: u64,
) {
    let mut retained = Vec::with_capacity(pending_wals.len());
    for (maximum_commit, paths) in pending_wals.drain(..) {
        if maximum_commit > checkpoint_commit {
            retained.push((maximum_commit, paths));
            continue;
        }
        for path in paths {
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                degrade(inner, &Error::Io(error));
            }
        }
    }
    *pending_wals = retained;
    if let Err(error) = sync_directory(&inner.root.join("wal")) {
        degrade(inner, &error);
    }
}

pub(super) fn capture_manifest_metadata(inner: &Inner, manifest: &mut Manifest) -> Result<()> {
    let state = inner.state.read()?;
    manifest.deletions = Arc::clone(&state.deletions);
    manifest
        .retention_cutoffs
        .clone_from(&state.retention_cutoffs);
    manifest
        .aggregate_retention_cutoffs
        .clone_from(&state.aggregate_retention_cutoffs);
    manifest
        .event_watermarks
        .clone_from(&state.event_watermarks);
    drop(state);
    manifest.next_segment_id = inner.next_segment_id.load(Ordering::Relaxed);
    Ok(())
}
