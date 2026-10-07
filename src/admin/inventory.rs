//! Read-only administration over the published disk catalogue, without a full image.
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::engine::files::{Resources, View};
use crate::error::{Result, corrupt};
use crate::memory::cache::{BlockCache, MemoryGovernor};
use crate::memory::work::WorkMemoryGovernor;
use crate::persistence::catalog::{Catalog, records};
use crate::persistence::files::{FilePermitPool, OpenFileCache};
use crate::persistence::manifest::{
    Manifest, ManifestArtifact, ManifestArtifactKind, STORAGE_FORMAT,
};
use crate::segment::MetadataCache;

use super::verify::{StorageInfo, hex_uuid, wal_paths};

// One owner per admin call. Cache/handle reservations live until the call ends.
// Catalogue walks and physical-pack range checks share a separate bounded work
// allowance and return ResourceLimit instead of retaining the database inventory.
const CACHE_BYTES: u64 = 64 << 20;
pub(super) struct Inventory {
    pub manifest: Manifest,
    pub files: View,
}

impl Inventory {
    pub fn open(root: &Path) -> Result<Self> {
        let manifest = Manifest::load(root)?;
        if manifest.file_catalog.link.is_some() {
            let mut directory = root.to_path_buf();
            for component in ["manifest", "catalog", "pages"] {
                directory.push(component);
                let metadata = fs::symlink_metadata(&directory)?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(corrupt("catalog path is not a regular directory"));
                }
            }
            for entry in fs::read_dir(root.join("manifest/catalog/pages"))? {
                let metadata = fs::symlink_metadata(entry?.path())?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(corrupt("catalog pack is not a regular file"));
                }
            }
        }
        let governor = Arc::new(MemoryGovernor::with_limits(128 << 20, CACHE_BYTES, 0));
        let work = WorkMemoryGovernor::new(128 << 20);
        let pool = FilePermitPool::new(32)?;
        let snapshot = Catalog::inspect(
            root,
            manifest.storage_uuid,
            manifest.file_catalog,
            pool.clone(),
            Arc::clone(&governor),
            work,
            8 << 20,
        )?;
        let resources = Resources::new(
            root.to_path_buf(),
            manifest.storage_uuid,
            Arc::new(BlockCache::with_governor(1, Arc::clone(&governor))),
            Arc::new(MetadataCache::with_governor(
                CACHE_BYTES,
                Arc::clone(&governor),
            )),
            governor,
            pool.clone(),
            OpenFileCache::new(pool, 4),
            CACHE_BYTES,
        )?;
        let files = View {
            snapshot,
            resources,
            totals: manifest.file_totals.clone(),
        };
        files.validate_inventory(&manifest)?;
        Ok(Self { manifest, files })
    }

    pub fn info(&self) -> Result<StorageInfo> {
        let manifest = &self.manifest;
        let totals = &self.files.totals;
        let catalog = manifest.catalog.snapshot();
        let wal = wal_paths(&self.files.resources.root)?;
        let wal_bytes = wal.iter().try_fold(0_u64, |sum, path| {
            Ok::<_, crate::Error>(sum.saturating_add(fs::metadata(path)?.len()))
        })?;
        let count = |n| usize::try_from(n).map_err(|_| corrupt("inventory count overflows"));
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
            logical_segments: count(totals.segments.iter().sum())?,
            physical_segment_files: count(totals.physical_files)?,
            l0_segments: count(totals.segments[0])?,
            l1_segments: count(totals.segments[1])?,
            l2_segments: count(totals.segments[2])?,
            logical_segment_bytes: totals.payload_bytes.iter().sum(),
            physical_segment_bytes: totals.physical_bytes,
            aggregate_archives: count(totals.archives)?,
            aggregate_archive_bytes: totals.archive_bytes,
            wal_files: wal.len(),
            wal_bytes,
        })
    }
}

/// Visit each physical artifact once, retaining only the current catalogue leaf.
/// The caller must hold either a live View lease or the offline directory lock.
pub(super) fn visit_artifacts(
    files: &View,
    mut visit: impl FnMut(ManifestArtifact) -> Result<()>,
) -> Result<()> {
    let mut checked = |artifact: ManifestArtifact| -> Result<()> {
        let prefix = match artifact.kind {
            ManifestArtifactKind::Segment => "segments",
            ManifestArtifactKind::AggregateArchive => "aggregates",
            ManifestArtifactKind::Catalog => "manifest/catalog/pages",
            _ => return Err(corrupt("unexpected data artifact kind")),
        };
        if !artifact.relative_path.starts_with(prefix)
            || artifact
                .relative_path
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(corrupt("unsafe data artifact path"));
        }
        artifact.validate_at(&files.resources.root)?;
        visit(artifact)
    };
    for kind in [records::PACK, records::SINGLE] {
        files.visit_kind(kind, |key, entry: records::PackEntry| {
            checked(ManifestArtifact {
                relative_path: entry.relative_path.into(),
                kind: ManifestArtifactKind::Segment,
                minimum_size: entry.file_size,
                exact_size: Some(entry.file_size),
                pack_id: (kind == records::PACK).then_some(key.id),
            })
        })?;
    }
    files.visit_kind(
        records::ARCHIVE,
        |_, entry: crate::aggregate::archive::AggregateArchiveDescriptor| {
            checked(ManifestArtifact {
                relative_path: entry.relative_path.into(),
                kind: ManifestArtifactKind::AggregateArchive,
                minimum_size: entry.file_size,
                exact_size: Some(entry.file_size),
                pack_id: None,
            })
        },
    )?;
    if files.root().link.is_some() {
        // A generation lease protects referenced slots from reuse. Copying a
        // bounded prefix also permits concurrent appends to catalogue packs.
        for entry in fs::read_dir(files.resources.root.join("manifest/catalog/pages"))? {
            let path = entry?.path();
            checked(ManifestArtifact {
                relative_path: path
                    .strip_prefix(&files.resources.root)
                    .map_err(|_| corrupt("catalog pack escaped database"))?
                    .to_path_buf(),
                kind: ManifestArtifactKind::Catalog,
                minimum_size: fs::symlink_metadata(&path)?.len(),
                exact_size: None,
                pack_id: None,
            })?;
        }
    }
    Ok(())
}
