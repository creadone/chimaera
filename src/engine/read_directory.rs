//! Подготовка и публикация поколений индекса закрытой истории.

use super::compaction_selection::sealed_before_for_policy;
use super::index::PublishedReadDirectoryIndex;
use super::publication::{
    UnpublishedArtifacts, capture_manifest_metadata, complete_manifest_publication,
    persist_manifest_with_commit, record_manifest_lock_wait,
};
use super::support::{ActivityGauge, lock, read, write};
use super::{Inner, READ_DIRECTORY_EPOCH_US};
use crate::catalog::WriteMode;
use crate::error::{Error, Result};
use crate::manifest::Manifest;
use crate::segment::{OfflinePackDirectory, Segment, segments_crc32c};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub(super) fn prepare_published_read_directories(
    inner: &Inner,
    manifest: &mut Manifest,
    segments: &[Arc<Segment>],
    unpublished: &mut UnpublishedArtifacts,
) -> Result<Vec<Arc<OfflinePackDirectory>>> {
    let catalog = manifest.catalog.snapshot();
    let eligible_policies = catalog
        .policies()
        .iter()
        .filter(|policy| {
            policy.write_mode == WriteMode::StrictlyIncreasing
                && policy.max_future_skew_us.is_some()
        })
        .filter_map(|policy| {
            sealed_before_for_policy(
                &inner.options,
                &catalog,
                &manifest.event_watermarks,
                policy.id,
            )
            .map(|_| policy.id)
        })
        .collect::<HashSet<_>>();
    let mut groups = BTreeMap::<(u32, u16, i64, i64), Vec<Arc<Segment>>>::new();
    for segment in segments {
        let descriptor = segment.descriptor();
        if !eligible_policies.contains(&descriptor.policy_id) {
            continue;
        }
        let epoch_start_us = descriptor
            .window_start_us
            .div_euclid(READ_DIRECTORY_EPOCH_US)
            * READ_DIRECTORY_EPOCH_US;
        let Some(epoch_end_us) = epoch_start_us.checked_add(READ_DIRECTORY_EPOCH_US) else {
            continue;
        };
        if descriptor.level == 2
            && descriptor.window_start_us >= epoch_start_us
            && descriptor.window_end_us <= epoch_end_us
        {
            groups
                .entry((
                    descriptor.policy_id,
                    descriptor.shard_id,
                    epoch_start_us,
                    epoch_end_us,
                ))
                .or_default()
                .push(Arc::clone(segment));
        }
    }
    for covered in groups.values_mut() {
        covered.sort_unstable_by_key(|segment| segment.descriptor().id);
    }
    let state_directories = read(&inner.state)?
        .published_read_directories
        .iter()
        .map(|directory| (directory.generation_id(), Arc::clone(directory)))
        .collect::<HashMap<_, _>>();
    let existing = manifest
        .published_read_directories
        .iter()
        .map(|descriptor| {
            (
                (
                    descriptor.policy_id,
                    descriptor.shard_id,
                    descriptor.window_start_us,
                    descriptor.window_end_us,
                ),
                descriptor,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut directories = Vec::with_capacity(groups.len());
    let mut descriptors = Vec::with_capacity(groups.len());
    let mut covered_ids = HashSet::new();
    for ((policy_id, shard_id, window_start_us, window_end_us), covered) in groups {
        let segment_count = u64::try_from(covered.len()).unwrap_or(u64::MAX);
        let maximum_segment_id = covered
            .iter()
            .map(|segment| segment.descriptor().id)
            .max()
            .unwrap_or_default();
        let fingerprint = segments_crc32c(&covered)?;
        if let Some(descriptor) = existing
            .get(&(policy_id, Some(shard_id), window_start_us, window_end_us))
            .copied()
            .filter(|descriptor| {
                descriptor.segment_count == segment_count
                    && descriptor.maximum_segment_id == maximum_segment_id
                    && descriptor.segments_crc32c == fingerprint
            })
            && let Some(directory) = state_directories.get(&descriptor.generation_id)
        {
            descriptors.push(descriptor.clone());
            directories.push(Arc::clone(directory));
        } else {
            let generation_id = inner.next_segment_id.fetch_add(1, Ordering::Relaxed);
            let build_started = Instant::now();
            let build_segments = covered.len() as u64;
            inner
                .metrics
                .read_directory_build_attempts
                .fetch_add(1, Ordering::Relaxed);
            inner
                .metrics
                .read_directory_build_max_segments
                .fetch_max(build_segments, Ordering::Relaxed);
            let build = {
                let _phase = ActivityGauge::enter(&inner.metrics.publisher_phase, 3);
                let _segments = ActivityGauge::enter(
                    &inner.metrics.read_directory_build_current_segments,
                    build_segments,
                );
                OfflinePackDirectory::build_published(
                    &inner.root,
                    inner.storage_uuid,
                    generation_id,
                    policy_id,
                    window_start_us,
                    window_end_us,
                    &covered,
                    &inner.work_memory_governor,
                )
            };
            let elapsed = u64::try_from(build_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            inner
                .metrics
                .read_directory_build_nanos
                .fetch_add(elapsed, Ordering::Relaxed);
            inner
                .metrics
                .read_directory_build_max_nanos
                .fetch_max(elapsed, Ordering::Relaxed);
            let descriptor = match build {
                Ok(descriptor) => descriptor,
                Err(Error::ResourceLimit(_)) => {
                    inner
                        .metrics
                        .read_directory_build_resource_skips
                        .fetch_add(1, Ordering::Relaxed);
                    inner
                        .metrics
                        .memory_backpressure
                        .fetch_add(1, Ordering::Relaxed);
                    inner
                        .metrics
                        .read_directory_build_memory_backpressure
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let path = inner.root.join(&descriptor.relative_path);
            unpublished.push(path.clone());
            let directory = match open_published_read_directory(inner, &descriptor, &covered) {
                Ok(directory) => directory,
                Err(Error::ResourceLimit(_)) => {
                    unpublished.discard(&path)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            descriptors.push(descriptor);
            directories.push(directory);
        }
        covered_ids.extend(covered.iter().map(|segment| segment.descriptor().id));
    }
    manifest.segments = segments
        .iter()
        .filter(|segment| !covered_ids.contains(&segment.descriptor().id))
        .map(|segment| segment.descriptor().clone())
        .collect();
    let mut payload_files = segments
        .iter()
        .filter(|segment| covered_ids.contains(&segment.descriptor().id))
        .map(|segment| {
            Ok(crate::manifest::PublishedReadPayloadFile {
                relative_path: segment.descriptor().relative_path.clone(),
                file_size: fs::metadata(segment.path())?.len(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    payload_files.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
    payload_files.dedup_by(|left, right| {
        if left.relative_path == right.relative_path {
            debug_assert_eq!(left.file_size, right.file_size);
            true
        } else {
            false
        }
    });
    manifest.published_read_directories = descriptors;
    manifest.published_read_payload_files = payload_files;
    manifest.next_segment_id = inner.next_segment_id.load(Ordering::Relaxed);
    Ok(directories)
}

fn open_published_read_directory(
    inner: &Inner,
    descriptor: &crate::manifest::PublishedReadDirectoryDescriptor,
    covered: &[Arc<Segment>],
) -> Result<Arc<OfflinePackDirectory>> {
    let open = || {
        OfflinePackDirectory::open_published(
            &inner.root,
            descriptor,
            inner.storage_uuid,
            covered,
            &inner.memory_governor,
            &inner.directory_page_pool,
            &inner.file_pool,
            Arc::clone(&inner.open_file_cache),
            inner.options.metadata_cache_max_bytes,
        )
    };
    match open() {
        Err(Error::ResourceLimit(message))
            if message == "offline pack directory root exceeds managed memory" =>
        {
            inner
                .metrics
                .memory_backpressure
                .fetch_add(1, Ordering::Relaxed);
            inner
                .metrics
                .read_directory_root_memory_backpressure
                .fetch_add(1, Ordering::Relaxed);
            let active = read(&inner.state)?.published_read_directories.clone();
            let retired = lock(&inner.retired_read_directories)?.clone();
            let mut cleared = HashSet::new();
            for directory in active.into_iter().chain(retired) {
                if cleared.insert(directory.path().to_path_buf()) {
                    directory.clear_page_cache()?;
                }
            }
            inner.metadata_cache.clear()?;
            inner.block_cache.clear()?;
            open()
        }
        result => result,
    }
}

pub(super) fn publish_read_directory(
    inner: &Inner,
    manifest: &mut Manifest,
) -> Result<Option<u64>> {
    let segments = read(&inner.state)?.segments.clone();
    let mut next = manifest.clone();
    let mut unpublished = UnpublishedArtifacts::default();
    capture_manifest_metadata(inner, &mut next)?;
    let directories =
        prepare_published_read_directories(inner, &mut next, &segments, &mut unpublished)?;
    if next.published_read_directories == manifest.published_read_directories {
        return Ok(next
            .published_read_directories
            .iter()
            .map(|descriptor| descriptor.generation_id)
            .max());
    }
    let directory_index = PublishedReadDirectoryIndex::build(&directories)?;
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
        let previous = std::mem::replace(&mut state.published_read_directories, directories);
        state.published_read_directory_index = directory_index;
        drop(state);
        if durable {
            retire_read_directories(inner, previous, &next)?;
        }
        Ok(next
            .published_read_directories
            .iter()
            .map(|descriptor| descriptor.generation_id)
            .max())
    })
}

pub(super) fn retire_read_directories(
    inner: &Inner,
    previous: Vec<Arc<OfflinePackDirectory>>,
    manifest: &Manifest,
) -> Result<()> {
    let retained_paths = manifest
        .published_read_directories
        .iter()
        .map(|descriptor| inner.root.join(&descriptor.relative_path))
        .collect::<HashSet<_>>();
    let mut retired = lock(&inner.retired_read_directories)?;
    for directory in previous {
        if !retained_paths.contains(directory.path()) {
            retired.push(directory);
        }
    }
    Ok(())
}
