use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use uuid::Uuid;

use crate::aggregate_archive::AggregateArchive;
use crate::error::{Result, corrupt, invalid};
use crate::file_pool::FilePermitPool;
use crate::manifest::Manifest;
use crate::segment::Segment;
use crate::wal::sync_directory;

const COPY_BUFFER_BYTES: usize = 1024 * 1024;

pub(crate) struct SnapshotImage {
    pub(crate) manifest: Manifest,
    pub(crate) segments: Vec<Arc<Segment>>,
    pub(crate) aggregate_archives: Vec<Arc<AggregateArchive>>,
    pub(crate) published_read_directory_paths: Vec<PathBuf>,
}

/// Описание опубликованного физического snapshot или его восстановленной копии.
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
    validate_checkpoint_image(&image.manifest)?;
    let mut files = BTreeMap::<PathBuf, PathBuf>::new();
    for segment in &image.segments {
        let relative = PathBuf::from(&segment.descriptor().relative_path);
        match files.entry(relative) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(segment.path().to_path_buf());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get() != segment.path() =>
            {
                return Err(corrupt(
                    "snapshot segments disagree about a physical pack path",
                ));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    for archive in &image.aggregate_archives {
        files.insert(
            PathBuf::from(&archive.descriptor().relative_path),
            archive.path(),
        );
    }
    if image.published_read_directory_paths.len() != image.manifest.published_read_directories.len()
    {
        return Err(corrupt("snapshot read generation set is incomplete"));
    }
    for (descriptor, path) in image
        .manifest
        .published_read_directories
        .iter()
        .zip(image.published_read_directory_paths)
    {
        if path != source_root.join(&descriptor.relative_path) {
            return Err(corrupt("snapshot pins the wrong read generation"));
        }
        files.insert(PathBuf::from(&descriptor.relative_path), path);
    }
    let manifest_paths = image
        .manifest
        .data_artifacts()?
        .into_iter()
        .map(|artifact| artifact.relative_path)
        .collect::<Vec<_>>();
    if files.keys().ne(manifest_paths.iter()) {
        return Err(corrupt("snapshot manifest and pinned segments disagree"));
    }
    validate_physical_files(&image.manifest, &files)?;
    publish_copy(
        source_root,
        destination,
        &image.manifest,
        &files,
        Some(file_pool),
    )
}

pub(crate) fn restore_snapshot(source: &Path, destination: &Path) -> Result<SnapshotInfo> {
    let manifest = Manifest::load(source)?;
    validate_checkpoint_image(&manifest)?;
    ensure_wal_free(source)?;
    let files = manifest
        .data_artifacts()?
        .into_iter()
        .map(|artifact| {
            let relative = artifact.relative_path;
            (relative.clone(), source.join(relative))
        })
        .collect::<BTreeMap<_, _>>();
    validate_physical_files(&manifest, &files)?;
    publish_copy(source, destination, &manifest, &files, None)
}

fn validate_checkpoint_image(manifest: &Manifest) -> Result<()> {
    if manifest.materialized_commit() != manifest.checkpoint_commit {
        return Err(corrupt(
            "WAL-free snapshot checkpoint is not fully materialized",
        ));
    }
    if manifest
        .segments
        .iter()
        .any(|segment| segment.max_commit > manifest.checkpoint_commit)
    {
        return Err(corrupt(
            "WAL-free snapshot contains data newer than its checkpoint",
        ));
    }
    Ok(())
}

fn ensure_wal_free(source: &Path) -> Result<()> {
    let wal = source.join("wal");
    let mut entries = fs::read_dir(&wal)?;
    if entries.next().transpose()?.is_some() {
        return Err(invalid("snapshot WAL directory is not empty"));
    }
    Ok(())
}

fn validate_physical_files(manifest: &Manifest, files: &BTreeMap<PathBuf, PathBuf>) -> Result<()> {
    let sizes = files
        .iter()
        .map(|(relative, path)| Ok((relative, fs::metadata(path)?.len())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for artifact in manifest.data_artifacts()? {
        let physical_size = sizes
            .get(&artifact.relative_path)
            .ok_or_else(|| corrupt("snapshot manifest artifact is missing"))?;
        if *physical_size < artifact.minimum_size
            || artifact
                .exact_size
                .is_some_and(|expected| *physical_size != expected)
        {
            return Err(corrupt("snapshot manifest artifact length is invalid"));
        }
    }
    Ok(())
}

fn publish_copy(
    source_root: &Path,
    destination: &Path,
    manifest: &Manifest,
    files: &BTreeMap<PathBuf, PathBuf>,
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
        for (relative, source) in files {
            let target = temporary.join(relative);
            let target_parent = target
                .parent()
                .ok_or_else(|| invalid("snapshot segment has no parent directory"))?;
            fs::create_dir_all(target_parent)?;
            let _permit = file_pool.map(FilePermitPool::acquire).transpose()?;
            copied_bytes =
                copied_bytes.saturating_add(copy_synced_file(source, &target, &mut buffer)?);
        }

        let identity_bytes = copy_synced_file(
            &source_root.join("IDENTITY"),
            &temporary.join("IDENTITY"),
            &mut buffer,
        )?;
        copied_bytes = copied_bytes.saturating_add(identity_bytes);
        sync_tree_directories(&temporary, files.keys())?;
        manifest.persist(&temporary)?;
        copied_bytes = copied_bytes
            .saturating_add(fs::metadata(temporary.join("manifest/MANIFEST.json"))?.len());
        sync_directory(&temporary.join("wal"))?;
        sync_directory(&temporary.join("tmp"))?;
        sync_directory(&temporary)?;
        let verification = crate::diagnostics::verify_storage(&temporary);
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
            logical_segments: manifest.segments.len().saturating_add(
                manifest
                    .published_read_directories
                    .iter()
                    .map(|directory| usize::try_from(directory.segment_count).unwrap_or(usize::MAX))
                    .sum(),
            ),
            physical_files: files.len(),
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
    let mut input = File::open(source)?;
    let expected = input.metadata()?.len();
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

fn sync_tree_directories<'a>(
    root: &Path,
    relative_files: impl Iterator<Item = &'a PathBuf>,
) -> Result<()> {
    let mut directories = relative_files
        .filter_map(|path| path.parent())
        .map(|path| root.join(path))
        .collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    directories.dedup();
    for directory in directories {
        sync_directory(&directory)?;
    }
    sync_directory(&root.join("segments"))?;
    Ok(())
}
