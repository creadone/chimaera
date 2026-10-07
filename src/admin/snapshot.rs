use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use uuid::Uuid;

use super::inventory::{Inventory, visit_artifacts};
use crate::error::{Result, corrupt, invalid};
use crate::persistence::files::FilePermitPool;
use crate::persistence::manifest::Manifest;
use crate::persistence::wal::sync_directory;

const COPY_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) struct SnapshotImage {
    pub(crate) file_view: crate::engine::files::View,
    pub(crate) manifest: Manifest,
    pub(crate) _memory: crate::memory::cache::MemoryReservation,
}

/// A published physical snapshot or its restored copy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotInfo {
    pub commit_id: u64,
    pub logical_segments: usize,
    pub physical_files: usize,
    pub bytes: u64,
}

pub(crate) fn write_snapshot(
    source_root: &Path,
    destination: &Path,
    image: SnapshotImage,
    file_pool: &FilePermitPool,
) -> Result<SnapshotInfo> {
    validate_checkpoint_image(&image.manifest, &image.file_view)?;
    publish_copy(
        source_root,
        destination,
        &image.manifest,
        &image.file_view,
        Some(file_pool),
    )
}

pub(crate) fn restore_snapshot(source: &Path, destination: &Path) -> Result<SnapshotInfo> {
    let source = fs::canonicalize(source)?;
    let _lock = crate::persistence::lock::StorageLock::acquire_shared(&source)?;
    let _wal_lock = crate::persistence::lock::StorageLock::wal_shared(&source)?;
    let inventory = Inventory::open(&source)?;
    validate_checkpoint_image(&inventory.manifest, &inventory.files)?;
    ensure_wal_free(&source, &inventory.manifest)?;
    publish_copy(
        &source,
        destination,
        &inventory.manifest,
        &inventory.files,
        None,
    )
}

fn validate_checkpoint_image(
    manifest: &Manifest,
    files: &crate::engine::files::View,
) -> Result<()> {
    if manifest.materialized_commit() != manifest.checkpoint_commit
        || manifest.file_catalog != files.root()
    {
        return Err(corrupt(
            "WAL-free snapshot checkpoint is not fully materialized",
        ));
    }
    files.all(|entry| {
        if entry.descriptor.max_commit > manifest.checkpoint_commit {
            return Err(corrupt(
                "WAL-free snapshot contains data newer than its checkpoint",
            ));
        }
        Ok(())
    })
}

fn ensure_wal_free(source: &Path, manifest: &Manifest) -> Result<()> {
    let wal = source.join("wal");
    crate::persistence::wal::validate_empty(
        &wal,
        manifest.storage_uuid,
        manifest.checkpoint_commit,
    )?;
    for entry in fs::read_dir(&wal)? {
        if entry?.file_name() != "HEAD" {
            return Err(invalid("snapshot WAL directory is not empty"));
        }
    }
    Ok(())
}

fn publish_copy(
    source_root: &Path,
    destination: &Path,
    manifest: &Manifest,
    files: &crate::engine::files::View,
    file_pool: Option<&FilePermitPool>,
) -> Result<SnapshotInfo> {
    let (parent, temporary) = prepare_destination(source_root, destination)?;
    let result = (|| {
        fs::create_dir(&temporary)?;
        for directory in ["wal", "segments", "aggregates", "manifest", "tmp"] {
            fs::create_dir_all(temporary.join(directory))?;
        }

        let mut copied_bytes = 0_u64;
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        let mut physical_files = 0_usize;
        visit_artifacts(files, |artifact| {
            let target = temporary.join(&artifact.relative_path);
            let target_parent = target
                .parent()
                .ok_or_else(|| invalid("snapshot segment has no parent directory"))?;
            fs::create_dir_all(target_parent)?;

            let _permit = file_pool.map(FilePermitPool::acquire).transpose()?;
            copied_bytes = copied_bytes.saturating_add(copy_prefix_synced_file(
                &source_root.join(&artifact.relative_path),
                &target,
                &mut buffer,
                artifact.minimum_size,
            )?);
            physical_files = physical_files
                .checked_add(1)
                .ok_or_else(|| corrupt("snapshot file count overflows"))?;
            Ok(())
        })?;

        let identity_bytes = copy_synced_file(
            &source_root.join("IDENTITY"),
            &temporary.join("IDENTITY"),
            &mut buffer,
        )?;
        copied_bytes = copied_bytes.saturating_add(identity_bytes);
        super::verify::sync_all_directories(&temporary)?;
        manifest.persist(&temporary)?;
        crate::persistence::wal::initialize_empty(
            &temporary.join("wal"),
            manifest.storage_uuid,
            manifest.checkpoint_commit,
        )?;
        copied_bytes = copied_bytes.saturating_add(fs::metadata(temporary.join("wal/HEAD"))?.len());
        copied_bytes = copied_bytes
            .saturating_add(fs::metadata(temporary.join("manifest/MANIFEST.json"))?.len());
        sync_directory(&temporary.join("wal"))?;
        sync_directory(&temporary.join("tmp"))?;
        sync_directory(&temporary)?;
        let verification = crate::admin::verify::verify_storage(&temporary);
        if !verification.valid {
            let detail = verification
                .issues
                .first()
                .map_or("unknown verification error", |issue| issue.message.as_str());
            return Err(corrupt(format!(
                "snapshot copy failed full verification: {detail}"
            )));
        }

        if destination.exists() {
            return Err(invalid("snapshot destination already exists"));
        }
        fs::rename(&temporary, destination)?;
        sync_directory(&parent)?;
        Ok(SnapshotInfo {
            commit_id: manifest.checkpoint_commit,
            logical_segments: usize::try_from(files.totals.segments.iter().sum::<u64>())
                .map_err(|_| corrupt("snapshot segment count overflows"))?,
            physical_files,
            bytes: copied_bytes,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

fn prepare_destination(source_root: &Path, destination: &Path) -> Result<(PathBuf, PathBuf)> {
    if destination.exists() {
        return Err(invalid("snapshot destination already exists"));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent)?;
    let canonical_source = fs::canonicalize(source_root)?;
    if canonical_parent.starts_with(&canonical_source) {
        return Err(invalid("snapshot destination cannot be inside its source"));
    }
    let name = destination
        .file_name()
        .ok_or_else(|| invalid("snapshot destination must have a file name"))?
        .to_string_lossy();
    let temporary = canonical_parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    Ok((canonical_parent, temporary))
}

fn copy_synced_file(source: &Path, target: &Path, buffer: &mut [u8]) -> Result<u64> {
    copy_prefix_synced_file(source, target, buffer, fs::metadata(source)?.len())
}

fn copy_prefix_synced_file(
    source: &Path,
    target: &Path,
    buffer: &mut [u8],
    expected: u64,
) -> Result<u64> {
    let input = File::open(source)?;
    if input.metadata()?.len() < expected {
        return Err(corrupt("snapshot source truncated"));
    }
    let mut input = input.take(expected);
    let mut output = OpenOptions::new()
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
        return Err(corrupt("snapshot source file changed while it was copied"));
    }
    output.sync_all()?;
    Ok(copied)
}
