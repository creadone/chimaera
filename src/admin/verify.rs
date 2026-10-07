use std::fs;
use std::fs::{File, OpenOptions as FileOpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::Serialize;
use uuid::Uuid;

use super::inventory::Inventory;
use crate::engine::{OpenOptions, Storage};
use crate::error::{Error, Result, corrupt};
use crate::persistence::layout::Domain;
use crate::persistence::wal::inspect_directory_with;

mod stream;

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

/// Full verification outcome; inspect recovery needs and orphan counts separately.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    /// No fatal verification error. Can be `true` while `repair_needed` is also `true`.
    pub valid: bool,
    /// A permitted partial WAL tail or unpublished WAL artifacts need recovery cleanup.
    /// This is not a promise to repair arbitrary corruption.
    pub repair_needed: bool,
    pub info: Option<StorageInfo>,
    pub verified_segment_operations: u64,
    pub wal_frames: u64,
    pub wal_operations: u64,
    pub truncated_wal_tail_bytes: u64,
    /// Unreferenced data files found; counted independently of `repair_needed`.
    pub orphan_files: usize,
    pub workers: usize,
    pub verified_physical_files: usize,
    pub issues: Vec<VerificationIssue>,
}

/// Parallelism for full verification, independent of query-worker settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationOptions {
    /// Accepted values: 1, 2, or 4. Default: 4.
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
    let _lock = crate::persistence::lock::StorageLock::acquire_shared(&root)?;
    let _wal_lock = crate::persistence::lock::StorageLock::wal_shared(&root)?;
    Inventory::open(&root)?.info()
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
    let _lock = crate::persistence::lock::StorageLock::acquire_shared(&source)?;
    let _wal_lock = crate::persistence::lock::StorageLock::wal_shared(&source)?;
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
    let inventory = Inventory::open(&source)?;
    let (parent, temporary) = prepare_salvage_destination(&source, destination)?;
    let result = (|| {
        fs::create_dir(&temporary)?;
        for directory in ["wal", "segments", "aggregates", "manifest", "tmp"] {
            fs::create_dir_all(temporary.join(directory))?;
        }
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        let mut bytes_copied = 0_u64;
        let mut files_copied = 0_usize;
        let mut copy = |relative: &Path| -> Result<()> {
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
            files_copied = files_copied
                .checked_add(1)
                .ok_or_else(|| corrupt("salvage file count overflows"))?;
            Ok(())
        };
        super::inventory::visit_artifacts(&inventory.files, |artifact| {
            copy(&artifact.relative_path)
        })?;
        for relative in ["IDENTITY", "manifest/MANIFEST.json", "wal/HEAD"] {
            copy(Path::new(relative))?;
        }
        // Preserve the validated WAL sequence as well as the published catalogue.
        for wal_path in wal_paths(&source)? {
            copy(
                wal_path
                    .strip_prefix(&source)
                    .map_err(|_| corrupt("WAL path escaped the storage root"))?,
            )?;
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
        crate::persistence::wal::sync_directory(&parent)?;
        Ok(SalvageResult {
            checkpoint_commit_id,
            files_copied,
            bytes_copied,
            repaired_wal_tail_bytes: report.truncated_wal_tail_bytes,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
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
    let _lock = crate::persistence::lock::StorageLock::acquire_shared(&root)?;
    let _wal_lock = crate::persistence::lock::StorageLock::wal_shared(&root)?;
    let inventory = Inventory::open(&root)?;
    let manifest = &inventory.manifest;
    let catalog = manifest.catalog.snapshot();
    report.info = Some(inventory.info()?);
    stream::verify(&inventory, options.workers, report)?;

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
                .saturating_add((frame.points.len() + frame.deletions.len()) as u64);
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
    report.repair_needed =
        inspection.truncated_tail_bytes != 0 || !inspection.unpublished_paths.is_empty();
    report.truncated_wal_tail_bytes = inspection.truncated_tail_bytes;
    if manifest.deletions.epoch > visible_commit {
        return Err(corrupt("deletion barrier exceeds recovered commit"));
    }
    // Inventory validation already bounds every segment by materialized_commit,
    // which is included in visible_commit even when its WAL has been reclaimed.
    report.orphan_files = count_orphan_data_files(&root, &inventory)?;
    Ok(())
}

fn count_orphan_data_files(root: &Path, inventory: &Inventory) -> Result<usize> {
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
                && !inventory.files.references_path(&entry.path())?
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

pub(super) fn wal_paths(root: &Path) -> Result<Vec<PathBuf>> {
    crate::persistence::wal::paths(&root.join("wal"))
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

pub(super) fn sync_all_directories(root: &Path) -> Result<()> {
    fn visit(directory: &Path, depth: usize) -> Result<()> {
        // The engine layout is shallow. Bound the stack even for an invalid tree.
        if depth > 32 {
            return Err(corrupt("copy directory nesting exceeds limit"));
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(&entry.path(), depth + 1)?;
            }
        }
        crate::persistence::wal::sync_directory(directory)
    }
    visit(root, 0)
}

pub(super) fn hex_uuid(uuid: [u8; 16]) -> String {
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
        Error::QueryUnavailable(_) => "query_unavailable",
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
