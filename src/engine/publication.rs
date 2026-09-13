//! Атомарная публикация manifest, сегментов и checkpoint.

use super::catalog::ensure_series_namespace_unused;
use super::compaction_selection::{refresh_l1_debt, validate_l1_publication};
use super::index::PublishedReadDirectoryIndex;
use super::read_directory::{
    prepare_published_read_directories, publish_read_directory, retire_read_directories,
};
use super::reclamation::{
    reap_ready_archives, reap_ready_read_directories, reap_ready_segments, record_retired_debt,
};
use super::support::{ActivityGauge, duration_nanos, read, write};
use super::{CompactionCommand, Generation, Inner, PublishCommand, degrade};
use crate::aggregate_archive::AggregateArchive;
use crate::error::{Error, Result, corrupt};
use crate::manifest::Manifest;
use crate::segment::Segment;
use crate::snapshot::SnapshotImage;
use crate::wal::sync_directory;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub(super) fn publisher_loop(
    inner: Arc<Inner>,
    receiver: Receiver<PublishCommand>,
    mut manifest: Manifest,
) {
    let mut retired =
        Vec::<Vec<Arc<Segment>>>::with_capacity(inner.options.publication_queue_capacity);
    let mut retired_archives =
        Vec::<Vec<Arc<AggregateArchive>>>::with_capacity(inner.options.publication_queue_capacity);
    let mut retired_backlog_started = None;
    let mut pending_wals = Vec::<(u64, Vec<PathBuf>)>::new();
    loop {
        let command = match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => command,
            Err(RecvTimeoutError::Timeout) => {
                reap_ready_segments(&inner, &mut retired);
                reap_ready_archives(&inner, &mut retired_archives);
                reap_ready_read_directories(&inner);
                record_retired_debt(&inner, &retired, &mut retired_backlog_started);
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                record_retired_debt(&inner, &retired, &mut retired_backlog_started);
                break;
            }
        };
        let phase = match &command {
            PublishCommand::Flush { .. } => 1,
            PublishCommand::Compaction { .. } => 2,
            PublishCommand::RefreshReadDirectory { .. } => 3,
            PublishCommand::Checkpoint { .. } => 4,
            PublishCommand::CaptureSnapshot { .. } => 6,
            PublishCommand::Stop { .. } => 7,
            _ => 5,
        };
        let _phase = ActivityGauge::enter(&inner.metrics.publisher_phase, phase);
        match command {
            #[cfg(test)]
            PublishCommand::Pause { entered, release } => {
                let _ = entered.send(());
                let _ = release.recv();
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
                generation,
                segments,
                mut unpublished,
                reply,
            } => {
                let result = publish_flush(
                    &inner,
                    &mut manifest,
                    &generation,
                    &segments,
                    &mut unpublished,
                );
                if result.is_ok() {
                    pending_wals.push((generation.maximum_commit, generation.wal_paths.clone()));
                    let command = CompactionCommand::Trigger {
                        force: false,
                        reply: None,
                    };
                    let _ = inner.compaction_tx.try_send(command);
                    let history_command = CompactionCommand::Trigger {
                        force: false,
                        reply: None,
                    };
                    let _ = inner.history_compaction_tx.try_send(history_command);
                }
                let _ = reply.send(result);
            }
            PublishCommand::Compaction {
                inputs,
                outputs,
                new_archives,
                expired_archives,
                mut unpublished,
                reply,
            } => {
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
                    retired.push(inputs);
                    retired_archives.push(expired_archives);
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
            PublishCommand::RefreshReadDirectory { reply } => {
                let result = publish_read_directory(&inner, &mut manifest);
                let _ = reply.send(result);
            }
            PublishCommand::Stop { reply } => {
                reap_ready_segments(&inner, &mut retired);
                reap_ready_archives(&inner, &mut retired_archives);
                reap_ready_read_directories(&inner);
                record_retired_debt(&inner, &retired, &mut retired_backlog_started);
                let _ = reply.send((retired, retired_archives));
                break;
            }
        }
        reap_ready_segments(&inner, &mut retired);
        reap_ready_archives(&inner, &mut retired_archives);
        reap_ready_read_directories(&inner);
        record_retired_debt(&inner, &retired, &mut retired_backlog_started);
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
    if manifest
        .segments
        .iter()
        .any(|segment| segment.max_commit > checkpoint_commit)
    {
        return Err(corrupt(
            "snapshot manifest contains data newer than its checkpoint",
        ));
    }
    let state = read(&inner.state)?;
    if state.visible_commit != checkpoint_commit
        || !state.active_batches.is_empty()
        || !state.frozen.is_empty()
    {
        return Err(corrupt("snapshot writer barrier is incomplete"));
    }
    let expected_segments = manifest.segments.len().saturating_add(
        manifest
            .published_read_directories
            .iter()
            .map(|directory| usize::try_from(directory.segment_count).unwrap_or(usize::MAX))
            .sum(),
    );
    if expected_segments != state.segments.len()
        || state
            .segments
            .iter()
            .any(|segment| segment.descriptor().max_commit > checkpoint_commit)
    {
        return Err(corrupt("snapshot state contains unpublished segments"));
    }
    let segments = state.segments.clone();
    let archives_by_id = state
        .aggregate_archives
        .iter()
        .map(|archive| (archive.descriptor().id, Arc::clone(archive)))
        .collect::<HashMap<_, _>>();
    let aggregate_archives = manifest
        .aggregate_archives
        .iter()
        .map(|descriptor| {
            archives_by_id
                .get(&descriptor.id)
                .cloned()
                .ok_or_else(|| corrupt("snapshot manifest archive is not active"))
        })
        .collect::<Result<Vec<_>>>()?;
    if aggregate_archives.len() != state.aggregate_archives.len() {
        return Err(corrupt(
            "snapshot state contains unpublished aggregate archives",
        ));
    }
    Ok(SnapshotImage {
        manifest: manifest.clone(),
        segments,
        aggregate_archives,
        published_read_directory_paths: state
            .published_read_directories
            .iter()
            .map(|directory| directory.path().to_path_buf())
            .collect(),
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
        let lock_started = Instant::now();
        let mut state = write(&inner.state)?;
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
    let next_index = {
        let state = read(&inner.state)?;
        if !state
            .frozen
            .iter()
            .any(|candidate| candidate.id == generation.id)
        {
            return Err(corrupt("flush publication lost its frozen generation"));
        }
        state.segment_index.with_added(segments)
    };
    let mut next = manifest.clone();
    next.segments
        .extend(segments.iter().map(|segment| segment.descriptor().clone()));
    next.materialized_commit = Some(generation.maximum_commit);
    capture_manifest_metadata(inner, &mut next)?;
    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        unpublished.commit();
        *manifest = next.clone();
        committed.set(true);
    });
    complete_manifest_publication(inner, committed.get(), persistence, || {
        let lock_started = Instant::now();
        let mut state = write(&inner.state)?;
        record_manifest_lock_wait(inner, lock_started.elapsed());
        if !state
            .frozen
            .iter()
            .any(|candidate| candidate.id == generation.id)
        {
            return Err(corrupt("flush publication lost its frozen generation"));
        }
        state
            .frozen
            .retain(|candidate| candidate.id != generation.id);
        let published_at = Instant::now();
        state.segment_published_at.extend(
            segments
                .iter()
                .map(|segment| (segment.descriptor().id, published_at)),
        );
        state.segments.extend(segments.iter().cloned());
        state.segment_index = next_index;
        let shards = segments
            .iter()
            .map(|segment| segment.descriptor().shard_id)
            .collect();
        refresh_l1_debt(&inner.options, &mut state, &shards);
        Ok(())
    })
}

fn validate_flush_generation_commits(manifest: &Manifest, generation: &Generation) -> Result<()> {
    let expected_first = manifest
        .materialized_commit()
        .checked_add(1)
        .ok_or_else(|| corrupt("materialized commit ID is exhausted"))?;
    let mut expected = expected_first;
    for batch in &generation.batches {
        let commit_id = batch
            .first()
            .ok_or_else(|| corrupt("flush generation contains an empty batch"))?
            .commit_id;
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

    pub(super) fn discard(&mut self, path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
        self.paths.remove(path);
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
        Ok(())
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
    let input_ids = inputs
        .iter()
        .map(|segment| segment.descriptor().id)
        .collect::<HashSet<_>>();
    validate_l1_publication(
        inputs.iter().map(|segment| segment.descriptor()),
        outputs.iter().map(|segment| segment.descriptor()),
        manifest
            .segments
            .iter()
            .filter(|part| !input_ids.contains(&part.id)),
    )?;
    let active_ids = read(&inner.state)?
        .segments
        .iter()
        .map(|segment| segment.descriptor().id)
        .collect::<HashSet<_>>();
    if !input_ids.is_subset(&active_ids) {
        return Err(corrupt("compaction input is no longer active"));
    }
    let (next_index, next_segments) = {
        let state = read(&inner.state)?;
        let state_ids = state
            .segments
            .iter()
            .map(|segment| segment.descriptor().id)
            .collect::<HashSet<_>>();
        if !input_ids.is_subset(&state_ids) {
            return Err(corrupt("compaction input is no longer published"));
        }
        let next_segments = state
            .segments
            .iter()
            .filter(|segment| !input_ids.contains(&segment.descriptor().id))
            .cloned()
            .chain(outputs.iter().cloned())
            .collect::<Vec<_>>();
        (
            state.segment_index.with_replaced(&input_ids, outputs),
            next_segments,
        )
    };
    let mut next = manifest.clone();
    next.segments
        .retain(|segment| !input_ids.contains(&segment.id));
    next.segments
        .extend(outputs.iter().map(|segment| segment.descriptor().clone()));
    let expired_archive_ids = expired_archives
        .iter()
        .map(|archive| archive.descriptor().id)
        .collect::<HashSet<_>>();
    next.aggregate_archives
        .retain(|archive| !expired_archive_ids.contains(&archive.id));
    next.aggregate_archives.extend(
        new_archives
            .iter()
            .map(|archive| archive.descriptor().clone()),
    );
    capture_manifest_metadata(inner, &mut next)?;
    let next_published_read_directories =
        prepare_published_read_directories(inner, &mut next, &next_segments, unpublished)?;
    let next_published_read_directory_index =
        PublishedReadDirectoryIndex::build(&next_published_read_directories)?;
    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        unpublished.commit();
        *manifest = next.clone();
        committed.set(true);
    });
    let durable = persistence.is_ok();
    complete_manifest_publication(inner, committed.get(), persistence, || {
        let lock_started = Instant::now();
        let mut state = write(&inner.state)?;
        record_manifest_lock_wait(inner, lock_started.elapsed());
        state
            .segments
            .retain(|segment| !input_ids.contains(&segment.descriptor().id));
        state
            .segment_published_at
            .retain(|segment_id, _| !input_ids.contains(segment_id));
        let published_at = Instant::now();
        state.segment_published_at.extend(
            outputs
                .iter()
                .map(|segment| (segment.descriptor().id, published_at)),
        );
        state.segments.extend(outputs.iter().cloned());
        state.segment_index = next_index;
        let shards = inputs
            .iter()
            .chain(outputs)
            .map(|segment| segment.descriptor().shard_id)
            .collect();
        refresh_l1_debt(&inner.options, &mut state, &shards);
        let previous_directories = std::mem::replace(
            &mut state.published_read_directories,
            next_published_read_directories,
        );
        state.published_read_directory_index = next_published_read_directory_index;
        state
            .aggregate_archives
            .retain(|archive| !expired_archive_ids.contains(&archive.descriptor().id));
        state
            .aggregate_archives
            .extend(new_archives.iter().cloned());
        drop(state);
        if durable {
            retire_read_directories(inner, previous_directories, &next)?;
        }
        Ok(())
    })
}

fn publish_checkpoint(
    inner: &Inner,
    manifest: &mut Manifest,
    commit_id: u64,
    wal_bytes_through_commit: u64,
) -> Result<u64> {
    let visible_commit = read(&inner.state)?.visible_commit;
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
        let lock_started = Instant::now();
        let mut state = write(&inner.state)?;
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
    let metrics = manifest.persist_with_commit(&inner.root, committed)?;
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
    debug_assert!(metrics.bytes <= crate::manifest::MANIFEST_MAX_BYTES);
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
    let state = read(&inner.state)?;
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
