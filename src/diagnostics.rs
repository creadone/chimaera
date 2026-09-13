use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::{File, OpenOptions as FileOpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use uuid::Uuid;

use crate::aggregate_archive::AggregateArchive;
use crate::cache::{BlockCache, MemoryGovernor};
use crate::engine::{OpenOptions, Storage};
use crate::error::{Error, Result, corrupt};
use crate::file_pool::FilePermitPool;
use crate::layout::Domain;
use crate::manifest::{Manifest, ManifestArtifact, ManifestArtifactKind, STORAGE_FORMAT};
use crate::segment::{MetadataCache, OfflinePackDirectory, Segment};
use crate::wal::inspect_directory_with;

const DIAGNOSTIC_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StorageInfo {
    pub storage_uuid: String,
    pub storage_format: u32,
    pub virtual_shards: u16,
    pub partition_span_us: i64,
    pub checkpoint_commit_id: u64,
    pub next_segment_id: u64,
    pub spaces: usize,
    pub policies: usize,
    pub buckets: usize,
    pub logical_segments: usize,
    pub physical_segment_files: usize,
    pub l0_segments: usize,
    pub l1_segments: usize,
    pub l2_segments: usize,
    pub logical_segment_bytes: u64,
    pub physical_segment_bytes: u64,
    pub aggregate_archives: usize,
    pub aggregate_archive_bytes: u64,
    pub wal_files: usize,
    pub wal_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    pub valid: bool,
    pub repair_needed: bool,
    pub info: Option<StorageInfo>,
    pub verified_segment_operations: u64,
    pub wal_frames: u64,
    pub wal_operations: u64,
    pub truncated_wal_tail_bytes: u64,
    pub orphan_files: usize,
    pub workers: usize,
    pub verified_physical_files: usize,
    pub issues: Vec<VerificationIssue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationOptions {
    pub workers: usize,
}

impl Default for VerificationOptions {
    fn default() -> Self {
        Self { workers: 4 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SalvageResult {
    pub checkpoint_commit_id: u64,
    pub files_copied: usize,
    pub bytes_copied: u64,
    pub repaired_wal_tail_bytes: u64,
}

pub(crate) fn storage_info(path: &Path) -> Result<StorageInfo> {
    let root = fs::canonicalize(path)?;
    let manifest = Manifest::load(&root)?;
    let segments = manifest_segment_descriptors(&root, &manifest)?;
    info_from_manifest(&root, &manifest, &segments)
}

pub(crate) fn verify_storage(path: &Path) -> VerificationReport {
    verify_storage_with_options(path, VerificationOptions::default())
}

pub(crate) fn verify_storage_with_options(
    path: &Path,
    options: VerificationOptions,
) -> VerificationReport {
    let mut report = VerificationReport {
        valid: false,
        repair_needed: false,
        info: None,
        verified_segment_operations: 0,
        wal_frames: 0,
        wal_operations: 0,
        truncated_wal_tail_bytes: 0,
        orphan_files: 0,
        workers: options.workers,
        verified_physical_files: 0,
        issues: Vec::new(),
    };
    if let Err(error) = verify_storage_inner(path, options, &mut report) {
        report.issues.push(issue_for(&error));
        return report;
    }
    report.valid = true;
    report
}

pub(crate) fn salvage_storage(source: &Path, destination: &Path) -> Result<SalvageResult> {
    let source = fs::canonicalize(source)?;
    let report = verify_storage(&source);
    if !report.valid {
        let message = report
            .issues
            .first()
            .map_or("source verification failed", |issue| issue.message.as_str());
        return Err(corrupt(message));
    }
    let info = report
        .info
        .as_ref()
        .ok_or_else(|| corrupt("verified source has no storage information"))?;
    let manifest = Manifest::load(&source)?;
    let (parent, temporary) = prepare_salvage_destination(&source, destination)?;
    let result = (|| {
        fs::create_dir(&temporary)?;
        for directory in ["wal", "segments", "aggregates", "manifest", "tmp"] {
            fs::create_dir_all(temporary.join(directory))?;
        }
        let mut relative_files = manifest
            .durable_artifacts()?
            .into_iter()
            .map(|artifact| artifact.relative_path)
            .collect::<BTreeSet<_>>();
        for wal_path in wal_paths(&source)? {
            relative_files.insert(
                wal_path
                    .strip_prefix(&source)
                    .map_err(|_| corrupt("WAL path escaped the storage root"))?
                    .to_path_buf(),
            );
        }
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        let mut bytes_copied = 0_u64;
        for relative in &relative_files {
            validate_copy_path(relative)?;
            reject_symlink_components(&source, relative)?;
            let target = temporary.join(relative);
            fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| corrupt("salvage file has no parent"))?,
            )?;
            bytes_copied = bytes_copied.saturating_add(copy_synced_file(
                &source.join(relative),
                &target,
                &mut buffer,
            )?);
        }
        sync_all_directories(&temporary)?;

        let storage = Storage::open(
            &temporary,
            OpenOptions {
                create_if_missing: false,
                virtual_shards: info.virtual_shards,
                partition_span_us: info.partition_span_us,
                wal_checkpoint_bytes: u64::MAX,
                l0_compaction_trigger: usize::MAX - 2,
                l0_slowdown_trigger: usize::MAX - 1,
                l0_stop_trigger: usize::MAX,
                ..OpenOptions::default()
            },
        )?;
        storage.close()?;
        let verified = verify_storage(&temporary);
        if !verified.valid || verified.repair_needed {
            return Err(corrupt("salvaged storage failed final verification"));
        }
        let checkpoint_commit_id = verified
            .info
            .as_ref()
            .ok_or_else(|| corrupt("salvaged storage has no information"))?
            .checkpoint_commit_id;
        sync_all_directories(&temporary)?;
        if destination.exists() {
            return Err(crate::error::invalid("salvage destination already exists"));
        }
        fs::rename(&temporary, destination)?;
        crate::wal::sync_directory(&parent)?;
        Ok(SalvageResult {
            checkpoint_commit_id,
            files_copied: relative_files.len(),
            bytes_copied,
            repaired_wal_tail_bytes: report.truncated_wal_tail_bytes,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

#[derive(Clone)]
enum VerificationTask {
    SegmentPack {
        descriptors: Vec<crate::segment::SegmentDescriptor>,
    },
    AggregateArchive {
        descriptor: crate::aggregate_archive::AggregateArchiveDescriptor,
    },
}

fn verify_storage_inner(
    path: &Path,
    options: VerificationOptions,
    report: &mut VerificationReport,
) -> Result<()> {
    if !matches!(options.workers, 1 | 2 | 4) {
        return Err(crate::error::invalid(
            "verification workers must be one of 1, 2, or 4",
        ));
    }
    let root = fs::canonicalize(path)?;
    let manifest = Manifest::load(&root)?;
    let segments = manifest_segment_descriptors(&root, &manifest)?;
    validate_physical_layout(&root, &manifest, &segments)?;
    report.info = Some(info_from_manifest(&root, &manifest, &segments)?);
    let catalog = manifest.catalog.snapshot();
    let governor = Arc::new(MemoryGovernor::new(DIAGNOSTIC_CACHE_BYTES * 2));
    let block_cache = Arc::new(BlockCache::with_governor(1, Arc::clone(&governor)));
    let metadata_cache = Arc::new(MetadataCache::with_governor(
        DIAGNOSTIC_CACHE_BYTES,
        Arc::clone(&governor),
    ));
    let file_pool = FilePermitPool::new(options.workers)?;
    for directory in &manifest.published_read_directories {
        let embedded = OfflinePackDirectory::published_segment_descriptors(
            &root,
            directory,
            &manifest.published_read_payload_files,
            manifest.storage_uuid,
        )?;
        let mut covered =
            embedded.ok_or_else(|| corrupt("read generation does not embed its descriptors"))?;
        covered.sort_unstable_by_key(|segment| segment.id);
        OfflinePackDirectory::verify_published(&root, directory, manifest.storage_uuid, &covered)?;
    }
    let mut tasks_by_path = BTreeMap::<PathBuf, VerificationTask>::new();
    for descriptor in segments.iter().cloned() {
        let path = PathBuf::from(&descriptor.relative_path);
        match tasks_by_path.entry(path) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(VerificationTask::SegmentPack {
                    descriptors: vec![descriptor],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let VerificationTask::SegmentPack { descriptors } = entry.get_mut() else {
                    return Err(corrupt("manifest reuses a data path across artifact kinds"));
                };
                descriptors.push(descriptor);
            }
        }
    }
    for descriptor in manifest.aggregate_archives.iter().cloned() {
        let path = PathBuf::from(&descriptor.relative_path);
        if tasks_by_path
            .insert(path, VerificationTask::AggregateArchive { descriptor })
            .is_some()
        {
            return Err(corrupt(
                "manifest contains duplicate physical artifact paths",
            ));
        }
    }
    for task in tasks_by_path.values_mut() {
        if let VerificationTask::SegmentPack { descriptors } = task {
            descriptors.sort_by_key(|descriptor| (descriptor.file_offset, descriptor.id));
        }
    }
    let tasks = tasks_by_path.into_values().collect::<Vec<_>>();
    let next_task = AtomicUsize::new(0);
    let results = Mutex::new(Vec::<(usize, Result<u64>)>::with_capacity(tasks.len()));
    std::thread::scope(|scope| {
        for _ in 0..options.workers.min(tasks.len().max(1)) {
            scope.spawn(|| {
                loop {
                    let index = next_task.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(index) else {
                        break;
                    };
                    let result = verify_physical_task(
                        task,
                        &root,
                        manifest.storage_uuid,
                        manifest.virtual_shards,
                        manifest.partition_span_us,
                        &catalog,
                        Arc::clone(&block_cache),
                        Arc::clone(&metadata_cache),
                        file_pool.clone(),
                    );
                    if let Ok(mut results) = results.lock() {
                        results.push((index, result));
                    }
                }
            });
        }
    });
    let mut results = results
        .into_inner()
        .map_err(|_| corrupt("verification result mutex is poisoned"))?;
    if results.len() != tasks.len() {
        return Err(corrupt("verification worker did not return every result"));
    }
    results.sort_by_key(|(index, _)| *index);
    let mut verified_operations = 0_u64;
    for (_, result) in results {
        verified_operations = verified_operations
            .checked_add(result?)
            .ok_or_else(|| corrupt("verified operation count overflows"))?;
    }
    report.verified_segment_operations = verified_operations;
    report.verified_physical_files = tasks
        .len()
        .saturating_add(manifest.published_read_directories.len());

    let mut visible_commit = manifest.materialized_commit();
    let inspection = inspect_directory_with(
        &root.join("wal"),
        manifest.storage_uuid,
        manifest.materialized_commit(),
        |_| Ok(()),
        |frame| {
            visible_commit = visible_commit.max(frame.commit_id);
            report.wal_frames = report.wal_frames.saturating_add(1);
            report.wal_operations = report
                .wal_operations
                .saturating_add(frame.points.len() as u64);
            for operation in &frame.points {
                let (policy_id, partition_span_us) = catalog
                    .storage_layout(operation.key.series)
                    .unwrap_or((1, manifest.partition_span_us));
                Domain::for_point(
                    operation.key.series,
                    operation.key.timestamp_us,
                    manifest.virtual_shards,
                    policy_id,
                    partition_span_us,
                )
                .map_err(|_| corrupt("WAL operation has an invalid storage domain"))?;
            }
            Ok(())
        },
    )?;
    report.repair_needed = inspection.truncated_tail_bytes != 0;
    report.truncated_wal_tail_bytes = inspection.truncated_tail_bytes;
    if manifest
        .segments
        .iter()
        .any(|segment| segment.max_commit > visible_commit)
    {
        return Err(corrupt(
            "uncheckpointed segment is not covered by the recovered WAL",
        ));
    }
    report.orphan_files = count_orphan_data_files(&root, &manifest.data_artifacts()?)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_physical_task(
    task: &VerificationTask,
    root: &Path,
    storage_uuid: [u8; 16],
    virtual_shards: u16,
    partition_span_us: i64,
    catalog: &crate::catalog::CatalogSnapshot,
    block_cache: Arc<BlockCache>,
    metadata_cache: Arc<MetadataCache>,
    file_pool: FilePermitPool,
) -> Result<u64> {
    match task {
        VerificationTask::SegmentPack { descriptors } => {
            let mut verified_operations = 0_u64;
            for descriptor in descriptors {
                let segment = Segment::open(
                    root,
                    storage_uuid,
                    descriptor.clone(),
                    Arc::clone(&block_cache),
                    Arc::clone(&metadata_cache),
                    file_pool.clone(),
                )?;
                let count = segment.verify_operations(|operation| {
                    let (policy_id, series_partition_span_us) = catalog
                        .storage_layout(operation.key.series)
                        .unwrap_or((1, partition_span_us));
                    let domain = Domain::for_point(
                        operation.key.series,
                        operation.key.timestamp_us,
                        virtual_shards,
                        policy_id,
                        series_partition_span_us,
                    )
                    .map_err(|_| corrupt("segment operation has an invalid storage domain"))?;
                    if domain != descriptor.domain()
                        || operation.commit_id < descriptor.min_commit
                        || operation.commit_id > descriptor.max_commit
                    {
                        return Err(corrupt("segment operation does not match its descriptor"));
                    }
                    Ok(())
                })?;
                verified_operations = verified_operations
                    .checked_add(count)
                    .ok_or_else(|| corrupt("verified operation count overflows"))?;
            }
            Ok(verified_operations)
        }
        VerificationTask::AggregateArchive { descriptor } => {
            AggregateArchive::open(root, storage_uuid, descriptor.clone(), file_pool)?.verify()?;
            Ok(0)
        }
    }
}

fn info_from_manifest(
    root: &Path,
    manifest: &Manifest,
    segments: &[crate::segment::SegmentDescriptor],
) -> Result<StorageInfo> {
    let catalog = manifest.catalog.snapshot();
    let artifacts = manifest.data_artifacts()?;
    let physical_segment_bytes = artifacts
        .iter()
        .filter(|artifact| artifact.kind == ManifestArtifactKind::Segment)
        .try_fold(0_u64, |total, artifact| {
            Ok::<_, Error>(
                total.saturating_add(fs::metadata(root.join(&artifact.relative_path))?.len()),
            )
        })?;
    let aggregate_archive_bytes = artifacts
        .iter()
        .filter(|artifact| artifact.kind == ManifestArtifactKind::AggregateArchive)
        .try_fold(0_u64, |total, artifact| {
            Ok::<_, Error>(
                total.saturating_add(fs::metadata(root.join(&artifact.relative_path))?.len()),
            )
        })?;
    let wal_paths = wal_paths(root)?;
    let wal_bytes = wal_paths.iter().try_fold(0_u64, |total, path| {
        Ok::<_, Error>(total.saturating_add(fs::metadata(path)?.len()))
    })?;
    Ok(StorageInfo {
        storage_uuid: hex_uuid(manifest.storage_uuid),
        storage_format: STORAGE_FORMAT,
        virtual_shards: manifest.virtual_shards,
        partition_span_us: manifest.partition_span_us,
        checkpoint_commit_id: manifest.checkpoint_commit,
        next_segment_id: manifest.next_segment_id,
        spaces: catalog.spaces().len(),
        policies: catalog.policies().len(),
        buckets: catalog.buckets().len(),
        logical_segments: segments.len(),
        physical_segment_files: artifacts
            .iter()
            .filter(|artifact| artifact.kind == ManifestArtifactKind::Segment)
            .count(),
        l0_segments: segments.iter().filter(|segment| segment.level == 0).count(),
        l1_segments: segments.iter().filter(|segment| segment.level == 1).count(),
        l2_segments: segments.iter().filter(|segment| segment.level == 2).count(),
        logical_segment_bytes: segments.iter().fold(0_u64, |total, segment| {
            total.saturating_add(segment.file_size)
        }),
        physical_segment_bytes,
        aggregate_archives: manifest.aggregate_archives.len(),
        aggregate_archive_bytes,
        wal_files: wal_paths.len(),
        wal_bytes,
    })
}

fn validate_physical_layout(
    root: &Path,
    manifest: &Manifest,
    segments: &[crate::segment::SegmentDescriptor],
) -> Result<()> {
    let artifacts = manifest.data_artifacts()?;
    for artifact in &artifacts {
        artifact.validate_at(root)?;
    }
    let mut ranges = BTreeMap::<PathBuf, Vec<(u64, u64, bool)>>::new();
    for descriptor in segments {
        let relative = PathBuf::from(&descriptor.relative_path);
        let end = descriptor
            .file_offset
            .checked_add(descriptor.file_size)
            .ok_or_else(|| corrupt("segment physical range overflows"))?;
        ranges.entry(relative).or_default().push((
            descriptor.file_offset,
            end,
            descriptor.pack_id.is_some(),
        ));
    }
    for (relative, mut file_ranges) in ranges {
        let physical_size = fs::metadata(root.join(&relative))?.len();
        file_ranges.sort_unstable();
        let mut previous_end = 0_u64;
        for (start, end, packed) in &file_ranges {
            if *start < previous_end || *end > physical_size {
                return Err(corrupt(
                    "segment physical ranges overlap or exceed their file",
                ));
            }
            if !packed && (*start != 0 || *end != physical_size || file_ranges.len() != 1) {
                return Err(corrupt("standalone segment does not own its physical file"));
            }
            previous_end = *end;
        }
    }
    Ok(())
}

fn manifest_segment_descriptors(
    root: &Path,
    manifest: &Manifest,
) -> Result<Vec<crate::segment::SegmentDescriptor>> {
    let mut segments = manifest.segments.clone();
    let mut embedded_segment_ids = BTreeSet::new();
    for directory in &manifest.published_read_directories {
        let mut embedded = OfflinePackDirectory::published_segment_descriptors(
            root,
            directory,
            &manifest.published_read_payload_files,
            manifest.storage_uuid,
        )?
        .ok_or_else(|| corrupt("read generation does not embed its descriptors"))?;
        embedded_segment_ids.extend(embedded.iter().map(|descriptor| descriptor.id));
        segments.append(&mut embedded);
    }
    segments.sort_unstable_by_key(|descriptor| descriptor.id);
    let catalog = manifest.catalog.snapshot();
    let generation_ids = manifest
        .published_read_directories
        .iter()
        .map(|directory| directory.generation_id)
        .collect::<BTreeSet<_>>();
    let embedded_payload_paths = segments
        .iter()
        .filter(|descriptor| embedded_segment_ids.contains(&descriptor.id))
        .map(|descriptor| descriptor.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    let published_payload_paths = manifest
        .published_read_payload_files
        .iter()
        .map(|payload| payload.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    if segments.windows(2).any(|pair| pair[0].id == pair[1].id)
        || embedded_payload_paths != published_payload_paths
        || segments.iter().any(|descriptor| {
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
        })
    {
        return Err(corrupt(
            "manifest or read generation contains invalid segment metadata",
        ));
    }
    Ok(segments)
}

fn count_orphan_data_files(root: &Path, artifacts: &[ManifestArtifact]) -> Result<usize> {
    let referenced = artifacts
        .iter()
        .map(|artifact| root.join(&artifact.relative_path))
        .collect::<BTreeSet<_>>();
    let mut pending = vec![root.join("segments"), root.join("aggregates")];
    let mut orphans = 0_usize;
    while let Some(directory) = pending.pop() {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                return Err(corrupt("segments directory contains a symbolic link"));
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if (matches!(
                entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str()),
                Some("tss" | "tsp" | "tspd" | "tsa")
            ) || entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(".READ-DIRECTORY-") && name.ends_with(".tmp")))
                && !referenced.contains(&entry.path())
            {
                orphans = orphans.saturating_add(1);
            }
        }
    }
    Ok(orphans)
}

fn validate_copy_path(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::CurDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(corrupt("salvage contains an unsafe relative path"));
    }
    Ok(())
}

fn reject_symlink_components(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        if fs::symlink_metadata(&current)?.file_type().is_symlink() {
            return Err(corrupt("referenced artifact path contains a symbolic link"));
        }
    }
    Ok(())
}

fn wal_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(root.join("wal"))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    Ok(paths)
}

fn prepare_salvage_destination(source: &Path, destination: &Path) -> Result<(PathBuf, PathBuf)> {
    if destination.exists() {
        return Err(crate::error::invalid("salvage destination already exists"));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent)?;
    if canonical_parent.starts_with(source) {
        return Err(crate::error::invalid(
            "salvage destination cannot be inside its source",
        ));
    }
    let name = destination
        .file_name()
        .ok_or_else(|| crate::error::invalid("salvage destination must have a file name"))?
        .to_string_lossy();
    let temporary = canonical_parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    Ok((canonical_parent, temporary))
}

fn copy_synced_file(source: &Path, target: &Path, buffer: &mut [u8]) -> Result<u64> {
    let mut input = File::open(source)?;
    let expected = input.metadata()?.len();
    let mut output = FileOpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)?;
    let mut copied = 0_u64;
    loop {
        let read = input.read(buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        copied = copied.saturating_add(read as u64);
    }
    if copied != expected {
        return Err(corrupt("salvage source file changed while it was copied"));
    }
    output.sync_all()?;
    Ok(copied)
}

fn sync_all_directories(root: &Path) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    let mut directories = Vec::new();
    while let Some(directory) = pending.pop() {
        directories.push(directory.clone());
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    directories.sort_by_key(|directory| std::cmp::Reverse(directory.components().count()));
    for directory in directories {
        crate::wal::sync_directory(&directory)?;
    }
    Ok(())
}

fn hex_uuid(uuid: [u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in uuid {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn issue_for(error: &Error) -> VerificationIssue {
    let code = match error {
        Error::Io(_) => "io_error",
        Error::Corruption(_) => "corruption",
        Error::Invalid(_) => "invalid",
        Error::WorkerStopped => "worker_stopped",
        Error::WritePending(_) => "write_pending",
        Error::Overloaded(_) => "overloaded",
        Error::AdmissionStopped => "admission_stopped",
        Error::OperationPending(_) => "operation_pending",
        Error::Closed => "closed",
        Error::Lock(_) => "lock",
        Error::Catalog(_) => "catalog",
        Error::AlreadyExists(_) => "already_exists",
        Error::NotFound(_) => "not_found",
        Error::ExpiredPoint(_) => "expired_point",
        Error::Backpressure => "backpressure",
        Error::ResourceLimit(_) => "resource_limit",
        Error::InsufficientDiskSpace { .. } => "insufficient_disk_space",
        Error::QueryCancelled => "query_cancelled",
        Error::QueryDeadlineExceeded => "query_deadline_exceeded",
        Error::Serialization(_) => "serialization",
    };
    VerificationIssue {
        code: code.into(),
        message: error.to_string(),
    }
}
