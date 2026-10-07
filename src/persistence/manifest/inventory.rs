//! Administrative materialization is separate from the constant-size manifest.
use super::*;
use crate::memory::cache::{MemoryClass, MemoryGovernor, MemoryReservation};

use crate::persistence::catalog::{Snapshot, records};

use std::sync::Arc;

impl Manifest {
    pub(crate) fn materialize_catalog(
        &mut self,
        root: &Path,
        snapshot: &Snapshot,
        governor: &Arc<MemoryGovernor>,
    ) -> Result<MemoryReservation> {
        let mut memory = governor
            .try_reserve_token(MemoryClass::MetadataCatalog, 0)
            .ok_or_else(|| corrupt("administrative inventory reservation"))?;
        let mut charge = || -> Result<()> {
            if !memory.try_grow(4096) {
                return Err(crate::Error::ResourceLimit(
                    "administrative inventory exceeds managed memory".into(),
                ));
            }
            Ok(())
        };
        self.segments.clear();
        self.published_read_directories.clear();
        self.published_read_payload_files.clear();
        self.aggregate_archives.clear();
        self.catalog_artifacts.clear();
        snapshot.visit(0, u64::MAX, i64::MIN, i64::MAX, None, |row| {
            match (row.key.scope >> 56) as u8 {
                records::SEGMENT => {
                    charge()?;
                    let entry: records::SegmentEntry = records::decode(row)?;
                    self.segments.push(entry.descriptor);
                }
                records::DIRECTORY => {
                    charge()?;
                    self.published_read_directories.push(records::decode(row)?);
                }
                records::ARCHIVE => {
                    charge()?;
                    self.aggregate_archives.push(records::decode(row)?);
                }
                records::PACK | records::SINGLE => {
                    charge()?;
                    let pack: records::PackEntry = records::decode(row)?;
                    self.published_read_payload_files
                        .push(PublishedReadPayloadFile {
                            relative_path: pack.relative_path,
                            file_size: pack.file_size,
                        });
                }
                _ => {}
            }
            Ok(())
        })?;
        self.segments.sort_unstable_by_key(|d| d.id);
        if snapshot.root.link.is_some() {
            for entry in fs::read_dir(root.join("manifest/catalog/pages"))? {
                charge()?;
                let entry = entry?;
                let metadata = fs::symlink_metadata(entry.path())?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(corrupt("catalog pack is not a regular file"));
                }
                self.catalog_artifacts.push(ManifestArtifact {
                    relative_path: entry
                        .path()
                        .strip_prefix(root)
                        .map_err(|_| corrupt("catalog pack outside storage"))?
                        .to_path_buf(),
                    kind: ManifestArtifactKind::Catalog,
                    minimum_size: metadata.len(),
                    exact_size: None,
                    pack_id: None,
                });
            }
        }
        Ok(memory)
    }
}
