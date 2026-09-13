//! Открытие Storage, восстановление WAL и очистка неопубликованных файлов.

use super::compaction::compaction_loop;
use super::compaction_selection::refresh_l1_debt;
use super::executor::QueryExecutor;
use super::flush::{flush_loop, flush_pack_loop};
use super::index::{PublishedReadDirectoryIndex, PublishedSegmentIndex};
use super::maintenance::maintenance_loop;
use super::options::OpenOptions;
use super::publication::publisher_loop;
use super::regulator::WriteRegulator;
use super::snapshots::SnapshotTracker;
use super::support::{DrainState, duration_nanos, file_bytes, lock, wal_paths, write};
use super::writer::{StrictWriteState, WriterCommitState, writer_loop};
use super::{
    APPROXIMATE_POINT_BYTES, APPROXIMATE_SERIES_WATERMARK_BYTES, ClientOwner, Inner, Metrics, OPEN,
    State, Storage, UNMANAGED_FILE_RESERVE, WorkerHandles,
};
use crate::admission::{WorkMemoryClass, WorkMemoryGovernor};
use crate::aggregate_archive::AggregateArchive;
use crate::cache::{BlockCache, BlockLoadLimits, MemoryClass, MemoryGovernor};
use crate::catalog::{DataTemperature, WriteMode};
use crate::error::{Error, Result, corrupt, invalid};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::lifecycle::WriteAdmission;
use crate::manifest::{Manifest, ManifestArtifactKind};
use crate::segment::{MetadataCache, OfflinePackDirectory, PointBatch, Segment};
use crate::storage_lock::StorageLock;
use crate::types::{SeriesKey, VersionedPoint};
use crate::wal::{WalWriter, inspect_directory_with, repair_truncated_tail, sync_directory};
use crossbeam_channel::bounded;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Instant;
use std::{fs, thread};

impl Storage {
    /// Открывает или создаёт базу, проверяя identity, manifest, физические границы
    /// сегментов и WAL. Полная проверка metadata и payload доступна через [`Self::verify`].
    ///
    /// # Errors
    ///
    /// Возвращает ошибку при повреждении метаданных, сегмента, WAL или при ошибке I/O.
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        options.validate()?;
        let started = Instant::now();
        let root = path.as_ref().to_path_buf();
        if !root.exists() {
            if !options.create_if_missing {
                return Err(invalid("storage does not exist"));
            }
            fs::create_dir_all(&root)?;
        }
        let storage_lock = StorageLock::acquire(&root)?;
        let manifest_started = Instant::now();
        let manifest = Manifest::load_or_create(
            &root,
            options.create_if_missing,
            options.virtual_shards,
            options.partition_span_us,
        )?;
        let recovery_manifest_nanos = duration_nanos(manifest_started.elapsed());
        fs::create_dir_all(root.join("aggregates"))?;
        let catalog = manifest.catalog.snapshot();
        let retention_cutoffs = manifest.retention_cutoffs.clone();
        let aggregate_retention_cutoffs = manifest.aggregate_retention_cutoffs.clone();
        let mut event_watermarks = manifest.event_watermarks.clone();
        let mut segment_descriptors = manifest.segments.clone();
        let mut published_segment_ids = HashMap::<u64, HashSet<u64>>::new();
        for directory in &manifest.published_read_directories {
            let mut embedded = OfflinePackDirectory::published_segment_descriptors(
                &root,
                directory,
                &manifest.published_read_payload_files,
                manifest.storage_uuid,
            )?
            .ok_or_else(|| corrupt("read generation does not embed its descriptors"))?;
            published_segment_ids.insert(
                directory.generation_id,
                embedded.iter().map(|descriptor| descriptor.id).collect(),
            );
            segment_descriptors.append(&mut embedded);
        }
        segment_descriptors.sort_unstable_by_key(|descriptor| descriptor.id);
        if segment_descriptors
            .windows(2)
            .any(|pair| pair[0].id == pair[1].id)
        {
            return Err(corrupt(
                "manifest and read directory contain duplicate segment IDs",
            ));
        }
        let generation_ids = manifest
            .published_read_directories
            .iter()
            .map(|directory| directory.generation_id)
            .collect::<HashSet<_>>();
        let embedded_segment_ids = published_segment_ids
            .values()
            .flatten()
            .copied()
            .collect::<HashSet<_>>();
        let embedded_payload_paths = segment_descriptors
            .iter()
            .filter(|descriptor| embedded_segment_ids.contains(&descriptor.id))
            .map(|descriptor| descriptor.relative_path.as_str())
            .collect::<HashSet<_>>();
        let published_payload_paths = manifest
            .published_read_payload_files
            .iter()
            .map(|payload| payload.relative_path.as_str())
            .collect::<HashSet<_>>();
        if embedded_payload_paths != published_payload_paths {
            return Err(corrupt(
                "read generation payload inventory does not match its descriptors",
            ));
        }
        if segment_descriptors.iter().any(|descriptor| {
            descriptor.id == 0
                || descriptor.id >= manifest.next_segment_id
                || generation_ids.contains(&descriptor.id)
                || descriptor.shard_id >= manifest.virtual_shards
                || (embedded_segment_ids.contains(&descriptor.id)
                    && catalog.policy_by_id(descriptor.policy_id).is_none())
                || descriptor.window_start_us >= descriptor.window_end_us
                || descriptor.min_timestamp_us > descriptor.max_timestamp_us
                || descriptor.min_timestamp_us < descriptor.window_start_us
                || descriptor.max_timestamp_us >= descriptor.window_end_us
                || descriptor.max_commit > manifest.materialized_commit()
        }) {
            return Err(corrupt("read generation contains invalid segment metadata"));
        }
        for descriptor in &segment_descriptors {
            event_watermarks
                .entry(descriptor.policy_id)
                .and_modify(|watermark| *watermark = (*watermark).max(descriptor.max_timestamp_us))
                .or_insert(descriptor.max_timestamp_us);
        }
        let maintenance_now_us = options.clock.now_us();
        let memory_governor = Arc::new(MemoryGovernor::new(options.memory_max_bytes));
        let directory_page_pool = crate::segment::DirectoryPagePool::new();
        let work_memory_governor = WorkMemoryGovernor::new(options.memory_max_bytes);
        let file_pool = FilePermitPool::new(
            options
                .file_descriptor_max
                .saturating_sub(UNMANAGED_FILE_RESERVE),
        )?;
        let open_file_cache = OpenFileCache::new(
            file_pool.clone(),
            options.flush_worker_count.saturating_add(1),
        );
        let snapshot_tracker = Arc::new(SnapshotTracker::default());
        let write_regulator = WriteRegulator::new(
            options.read_latency_slowdown_threshold,
            options.wide_read_latency_slowdown_threshold,
            options.write_pacing_max_delay,
        );
        let query_executor = QueryExecutor::new()?;
        let block_cache = Arc::new(BlockCache::with_governor_and_load_limits(
            options.block_cache_max_bytes,
            Arc::clone(&memory_governor),
            BlockLoadLimits {
                maximum_loaders: options.block_load_max_in_flight,
                maximum_waiters: options.block_load_max_waiters,
                maximum_bytes: options.block_load_max_bytes.min(options.memory_max_bytes),
            },
        ));
        let metadata_cache = Arc::new(MetadataCache::with_governor(
            options.metadata_cache_max_bytes,
            Arc::clone(&memory_governor),
        ));
        let descriptors_started = Instant::now();
        let mut segments = Vec::with_capacity(segment_descriptors.len());
        for descriptor in segment_descriptors {
            segments.push(Segment::open_with_file_cache_without_resident_index(
                &root,
                manifest.storage_uuid,
                descriptor,
                Arc::clone(&block_cache),
                Arc::clone(&metadata_cache),
                file_pool.clone(),
                Arc::clone(&open_file_cache),
            )?);
        }
        let published_read_directories = manifest
            .published_read_directories
            .iter()
            .map(|descriptor| {
                let ids = published_segment_ids
                    .get(&descriptor.generation_id)
                    .ok_or_else(|| corrupt("read generation segment set disappeared"))?;
                let covered = segments
                    .iter()
                    .filter(|segment| ids.contains(&segment.descriptor().id))
                    .cloned()
                    .collect::<Vec<_>>();
                OfflinePackDirectory::open_published(
                    &root,
                    descriptor,
                    manifest.storage_uuid,
                    &covered,
                    &memory_governor,
                    &directory_page_pool,
                    &file_pool,
                    Arc::clone(&open_file_cache),
                    options.metadata_cache_max_bytes,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let published_read_directory_index =
            PublishedReadDirectoryIndex::build(&published_read_directories)?;
        let mut strict_series_watermarks = HashMap::<SeriesKey, i64>::new();
        let mut strict_series_watermark_memory = memory_governor
            .try_reserve_token(MemoryClass::SeriesWatermark, 0)
            .ok_or_else(|| corrupt("zero-byte series watermark reservation failed"))?;
        let mut recovery_read_directory_pages = 0_u64;
        let mut recovery_read_directory_bytes = 0_u64;
        {
            let mut record_strict_watermark = |series: SeriesKey, timestamp_us: i64| {
                if !strict_series_watermarks.contains_key(&series)
                    && !strict_series_watermark_memory.try_grow(APPROXIMATE_SERIES_WATERMARK_BYTES)
                {
                    return Err(Error::ResourceLimit(
                        "strict series watermarks exceed the managed memory limit".into(),
                    ));
                }
                strict_series_watermarks
                    .entry(series)
                    .and_modify(|last| *last = (*last).max(timestamp_us))
                    .or_insert(timestamp_us);
                Ok(())
            };
            for directory in &published_read_directories {
                let (pages, bytes) =
                    directory.visit_series_max_timestamps(&mut record_strict_watermark)?;
                recovery_read_directory_pages = recovery_read_directory_pages.saturating_add(pages);
                recovery_read_directory_bytes = recovery_read_directory_bytes.saturating_add(bytes);
            }
            for segment in &segments {
                if published_read_directories
                    .iter()
                    .any(|directory| directory.covers_segment(segment.descriptor()))
                {
                    continue;
                }
                let strictly_increasing = catalog
                    .policy_by_id(segment.descriptor().policy_id)
                    .is_some_and(|policy| policy.write_mode == WriteMode::StrictlyIncreasing);
                if !strictly_increasing {
                    continue;
                }
                segment.visit_series_max_timestamps(&mut record_strict_watermark)?;
            }
        }
        let offline_pack_directory = options
            .offline_pack_directory
            .as_deref()
            .map(|path| {
                OfflinePackDirectory::open(
                    path,
                    &manifest,
                    &segments,
                    &memory_governor,
                    &directory_page_pool,
                    &file_pool,
                    Arc::clone(&open_file_cache),
                    options.metadata_cache_max_bytes,
                )
            })
            .transpose()?;
        let mut aggregate_archives = Vec::with_capacity(manifest.aggregate_archives.len());
        for descriptor in manifest.aggregate_archives.iter().cloned() {
            aggregate_archives.push(Arc::new(AggregateArchive::open(
                &root,
                manifest.storage_uuid,
                descriptor,
                file_pool.clone(),
            )?));
        }
        let mut recovery_descriptors_nanos = duration_nanos(descriptors_started.elapsed());
        let materialized_commit = manifest.materialized_commit();
        let wal_directory = root.join("wal");
        let wal_scan_started = Instant::now();
        let mut active_bytes = 0_u64;
        let mut visible_commit = materialized_commit;
        let inspection = inspect_directory_with(
            &wal_directory,
            manifest.storage_uuid,
            materialized_commit,
            |bytes| work_memory_governor.reserve_background(WorkMemoryClass::Recovery, bytes),
            |frame| {
                for point in &frame.points {
                    let policy = catalog.registered_policy(point.key.series);
                    let policy_id = policy.map_or(1, |policy| policy.id);
                    if policy
                        .is_some_and(|policy| policy.write_mode == WriteMode::StrictlyIncreasing)
                    {
                        if point.is_delete
                            || strict_series_watermarks
                                .get(&point.key.series)
                                .is_some_and(|last| point.key.timestamp_us <= *last)
                        {
                            return Err(corrupt(
                                "strictly increasing series contains a repeated, decreasing, or deleted WAL point",
                            ));
                        }
                        if !strict_series_watermarks.contains_key(&point.key.series)
                            && !strict_series_watermark_memory
                                .try_grow(APPROXIMATE_SERIES_WATERMARK_BYTES)
                        {
                            return Err(Error::ResourceLimit(
                                "strict series watermarks exceed the managed memory limit".into(),
                            ));
                        }
                        strict_series_watermarks.insert(point.key.series, point.key.timestamp_us);
                    }
                    event_watermarks
                        .entry(policy_id)
                        .and_modify(|watermark| {
                            *watermark = (*watermark).max(point.key.timestamp_us);
                        })
                        .or_insert(point.key.timestamp_us);
                }
                active_bytes = active_bytes.saturating_add(
                    u64::try_from(frame.points.len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(APPROXIMATE_POINT_BYTES)
                        .saturating_add(std::mem::size_of::<Vec<VersionedPoint>>() as u64),
                );
                visible_commit = frame.commit_id;
                Ok(())
            },
        )?;
        let recovery_wal_scan_nanos = duration_nanos(wal_scan_started.elapsed());
        let strict_write_state = StrictWriteState {
            watermarks: strict_series_watermarks,
            memory: strict_series_watermark_memory,
        };
        let mut recovery_reservation =
            memory_governor.try_reserve_owned(MemoryClass::Memtable, active_bytes);
        if recovery_reservation.is_none() {
            block_cache.clear()?;
            metadata_cache.clear()?;
            recovery_reservation =
                memory_governor.try_reserve_owned(MemoryClass::Memtable, active_bytes);
        }
        let recovery_reservation = recovery_reservation.ok_or_else(|| {
            Error::ResourceLimit(format!(
                "WAL tail needs {active_bytes} bytes of memtable memory above the managed memory limit"
            ))
        })?;
        let wal_replay_started = Instant::now();
        let mut recovered_batches = Vec::new();
        let replay_inspection = inspect_directory_with(
            &wal_directory,
            manifest.storage_uuid,
            materialized_commit,
            |bytes| work_memory_governor.reserve_background(WorkMemoryClass::Recovery, bytes),
            |mut frame| {
                frame.points.sort_unstable();
                recovered_batches.push(frame.points);
                Ok(())
            },
        )?;
        if replay_inspection.maximum_wal_id != inspection.maximum_wal_id
            || replay_inspection.replay_frames != inspection.replay_frames
            || replay_inspection.replay_operations != inspection.replay_operations
            || replay_inspection.truncated_tail_bytes != inspection.truncated_tail_bytes
        {
            return Err(corrupt("WAL changed between recovery passes"));
        }
        let active_batches = recovered_batches
            .into_iter()
            .map(|points| {
                Arc::new(PointBatch::managed(
                    points,
                    Arc::clone(&recovery_reservation),
                ))
            })
            .collect::<Vec<_>>();
        let optional_roots_started = Instant::now();
        metadata_cache.clear()?;
        for segment in &mut segments {
            if embedded_segment_ids.contains(&segment.descriptor().id) {
                continue;
            }
            *segment = Segment::open_with_file_cache(
                &root,
                manifest.storage_uuid,
                segment.descriptor().clone(),
                Arc::clone(&block_cache),
                Arc::clone(&metadata_cache),
                file_pool.clone(),
                Arc::clone(&open_file_cache),
            )?;
        }
        recovery_descriptors_nanos = recovery_descriptors_nanos
            .saturating_add(duration_nanos(optional_roots_started.elapsed()));
        let segment_index = PublishedSegmentIndex::build(&segments);
        let recovered_cold_commit = active_batches
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|point| {
                catalog
                    .registered_policy(point.key.series)
                    .is_some_and(|policy| {
                        policy.temperature(
                            point.key.timestamp_us,
                            maintenance_now_us,
                            retention_cutoffs.get(&policy.id).copied(),
                        ) == DataTemperature::Cold
                    })
            })
            .map(|point| point.commit_id)
            .max();
        let recovery_wal_replay_nanos = duration_nanos(wal_replay_started.elapsed());
        if segments
            .iter()
            .any(|segment| segment.descriptor().max_commit > visible_commit)
        {
            return Err(corrupt(
                "uncheckpointed segment is not covered by the recovered WAL",
            ));
        }
        if offline_pack_directory.is_some() && visible_commit != manifest.checkpoint_commit {
            return Err(invalid(
                "offline pack directory requires a checkpoint without a WAL tail",
            ));
        }
        repair_truncated_tail(&wal_directory, &inspection)?;
        // Only remove unpublished files after every referenced segment and WAL has
        // been validated. A failed open must preserve recovery evidence.
        cleanup_unpublished_storage(&root, &manifest)?;
        let existing_wals = wal_paths(&root.join("wal"))?;
        let recovered_wal_bytes = file_bytes(&existing_wals)?;
        let initial_wal_id = inspection.maximum_wal_id.saturating_add(1).max(1);
        let wal = WalWriter::create(
            &root.join("wal"),
            manifest.storage_uuid,
            initial_wal_id,
            visible_commit.saturating_add(1),
        )?;

        let (write_tx, write_rx) = bounded(options.write_queue_capacity);
        let (flush_tx, flush_rx) = bounded(options.flush_queue_capacity);
        let (flush_pack_tx, flush_pack_rx) = bounded(options.flush_pack_queue_capacity);
        let (publish_tx, publish_rx) = bounded(options.publication_queue_capacity);
        let (compaction_tx, compaction_rx) = bounded(options.compaction_queue_capacity);
        let (history_compaction_tx, history_compaction_rx) =
            bounded(options.history_compaction_queue_capacity);
        let (maintenance_tx, maintenance_rx) = bounded(1);
        let inner = Arc::new(Inner {
            root: root.clone(),
            storage_uuid: manifest.storage_uuid,
            options,
            state: RwLock::new(State {
                visible_commit,
                checkpoint_commit: manifest.checkpoint_commit,
                active_batches,
                active_bytes,
                frozen: Vec::new(),
                segment_published_at: segments
                    .iter()
                    .map(|segment| (segment.descriptor().id, Instant::now()))
                    .collect(),
                segments,
                l1_debt: BTreeMap::new(),
                published_read_directories,
                published_read_directory_index,
                aggregate_archives,
                segment_index,
                catalog,
                maintenance_now_us,
                retention_cutoffs,
                aggregate_retention_cutoffs,
                event_watermarks,
                wal_bytes_total: recovered_wal_bytes,
                checkpoint_wal_bytes: 0,
                last_cold_write: recovered_cold_commit.map(|commit_id| (commit_id, Instant::now())),
            }),
            lifecycle: AtomicU8::new(OPEN),
            admission: Arc::new(WriteAdmission::default()),
            drain_state: Mutex::new(DrainState::default()),
            shutdown_wait: Mutex::new(()),
            shutdown_complete: Condvar::new(),
            shutdown_failure: Mutex::new(None),
            degraded: Mutex::new(None),
            metrics: Arc::new(Metrics::default()),
            block_cache,
            metadata_cache,
            memory_governor,
            directory_page_pool,
            work_memory_governor,
            file_pool,
            open_file_cache,
            offline_pack_directory,
            retired_read_directories: Mutex::new(Vec::new()),
            snapshot_tracker,
            write_regulator,
            query_executor,
            next_segment_id: AtomicU64::new(manifest.next_segment_id),
            write_tx,
            flush_tx,
            flush_pack_tx,
            publish_tx,
            compaction_tx,
            history_compaction_tx,
            maintenance_tx,
            workers: Mutex::new(None),
            storage_lock: Mutex::new(Some(Arc::new(storage_lock))),
        });
        inner
            .metrics
            .recovery_nanos
            .store(duration_nanos(started.elapsed()), Ordering::Relaxed);
        inner
            .metrics
            .recovery_manifest_nanos
            .store(recovery_manifest_nanos, Ordering::Relaxed);
        inner
            .metrics
            .recovery_descriptors_nanos
            .store(recovery_descriptors_nanos, Ordering::Relaxed);
        inner
            .metrics
            .recovery_read_directory_pages
            .store(recovery_read_directory_pages, Ordering::Relaxed);
        inner
            .metrics
            .recovery_read_directory_bytes
            .store(recovery_read_directory_bytes, Ordering::Relaxed);
        inner
            .metrics
            .recovery_wal_scan_nanos
            .store(recovery_wal_scan_nanos, Ordering::Relaxed);
        inner
            .metrics
            .recovery_wal_replay_nanos
            .store(recovery_wal_replay_nanos, Ordering::Relaxed);
        inner
            .metrics
            .recovery_read_ready_nanos
            .store(duration_nanos(started.elapsed()), Ordering::Relaxed);
        inner.metrics.strict_series_watermarks.store(
            u64::try_from(strict_write_state.watermarks.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let writer_commit_state = WriterCommitState {
            next_commit: visible_commit,
            strict: strict_write_state,
        };

        {
            let mut state = write(&inner.state)?;
            let shards = state.segment_index.by_shard.keys().copied().collect();
            refresh_l1_debt(&inner.options, &mut state, &shards);
        }
        let writer_inner = Arc::clone(&inner);
        let writer = thread::Builder::new()
            .name("chimaera-writer".into())
            .spawn(move || {
                writer_loop(
                    writer_inner,
                    write_rx,
                    wal,
                    initial_wal_id.saturating_add(1),
                    existing_wals,
                    writer_commit_state,
                );
            })?;
        let flush_inner = Arc::clone(&inner);
        let flush = thread::Builder::new()
            .name("chimaera-flush-coordinator".into())
            .spawn(move || flush_loop(flush_inner, flush_rx))?;
        let mut flush_pack = Vec::with_capacity(inner.options.flush_worker_count);
        for worker in 0..inner.options.flush_worker_count {
            let pack_inner = Arc::clone(&inner);
            let pack_rx = flush_pack_rx.clone();
            flush_pack.push(
                thread::Builder::new()
                    .name(format!("chimaera-flush-pack-{worker}"))
                    .spawn(move || flush_pack_loop(pack_inner, pack_rx))?,
            );
        }
        let publisher_inner = Arc::clone(&inner);
        let publisher = thread::Builder::new()
            .name("chimaera-publisher".into())
            .spawn(move || publisher_loop(publisher_inner, publish_rx, manifest))?;
        let compaction_inner = Arc::clone(&inner);
        let compaction = thread::Builder::new()
            .name("chimaera-compaction-0".into())
            .spawn(move || {
                compaction_loop(compaction_inner, compaction_rx, history_compaction_rx);
            })?;
        let maintenance_inner = Arc::clone(&inner);
        let maintenance = thread::Builder::new()
            .name("chimaera-maintenance".into())
            .spawn(move || maintenance_loop(maintenance_inner, maintenance_rx))?;
        *lock(&inner.workers)? = Some(WorkerHandles {
            maintenance,
            writer,
            flush,
            flush_pack,
            publisher,
            compaction,
        });
        inner
            .metrics
            .recovery_write_ready_nanos
            .store(duration_nanos(started.elapsed()), Ordering::Relaxed);
        inner
            .metrics
            .recovery_nanos
            .store(duration_nanos(started.elapsed()), Ordering::Relaxed);
        let clients = Arc::new(ClientOwner {
            inner: Arc::downgrade(&inner),
        });
        Ok(Self {
            _clients: clients,
            inner,
        })
    }
}

fn cleanup_unpublished_storage(root: &Path, manifest: &Manifest) -> Result<()> {
    let temporary_directory = root.join("tmp");
    let mut temporary_changed = false;
    for entry in fs::read_dir(&temporary_directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if metadata.file_type().is_symlink() {
            return Err(corrupt("temporary directory contains a symbolic link"));
        }
        if metadata.is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "tmp")
            && ["MANIFEST.json", "PACK-", "SEG-", "AGG-"]
                .iter()
                .any(|prefix| file_name.starts_with(prefix))
        {
            fs::remove_file(entry.path())?;
            temporary_changed = true;
        }
    }
    if temporary_changed {
        sync_directory(&temporary_directory)?;
    }

    let artifacts = manifest.data_artifacts()?;
    let referenced = artifacts
        .iter()
        .filter(|artifact| {
            matches!(
                artifact.kind,
                ManifestArtifactKind::Segment | ManifestArtifactKind::PublishedReadDirectory
            )
        })
        .map(|artifact| root.join(&artifact.relative_path))
        .collect::<HashSet<_>>();
    let mut pending = vec![root.join("segments")];
    let mut changed_directories = HashSet::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(corrupt("segments directory contains a symbolic link"));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file()
                && ((path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(".READ-DIRECTORY-") && name.ends_with(".tmp")
                    }))
                    || (matches!(
                        path.extension().and_then(|extension| extension.to_str()),
                        Some("tss" | "tsp" | "tspd")
                    ) && !referenced.contains(&path)))
            {
                fs::remove_file(&path)?;
                changed_directories.insert(directory.clone());
            }
        }
    }
    for directory in changed_directories {
        sync_directory(&directory)?;
    }
    let referenced_archives = artifacts
        .iter()
        .filter(|artifact| artifact.kind == ManifestArtifactKind::AggregateArchive)
        .map(|artifact| root.join(&artifact.relative_path))
        .collect::<HashSet<_>>();
    let aggregate_directory = root.join("aggregates");
    fs::create_dir_all(&aggregate_directory)?;
    let mut aggregates_changed = false;
    for entry in fs::read_dir(&aggregate_directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(corrupt("aggregates directory contains a symbolic link"));
        }
        if metadata.is_file()
            && path.extension().is_some_and(|extension| extension == "tsa")
            && !referenced_archives.contains(&path)
        {
            fs::remove_file(path)?;
            aggregates_changed = true;
        }
    }
    if aggregates_changed {
        sync_directory(&aggregate_directory)?;
    }
    Ok(())
}
