mod inventory;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::aggregate::archive::AggregateArchiveDescriptor;
use crate::catalog::CatalogState;
use crate::diagnostics::publication::{self as publication_cost, Stage as CostStage};
use crate::error::{Result, corrupt, invalid};
use crate::persistence::layout::{DEFAULT_PARTITION_SPAN_US, validate_layout};
use crate::persistence::wal::{put_i64, put_u16, put_u32, read_u16, read_u32, sync_directory};
use crate::segment::SegmentDescriptor;

const IDENTITY_MAGIC: &[u8; 8] = b"TSIDT001";
const IDENTITY_SIZE: usize = 64;
pub(crate) const STORAGE_FORMAT: u32 = 3;
const MANIFEST_FORMAT: u32 = 3;
const MANIFEST_INTEGRITY_FORMAT: u16 = 1;
pub(crate) const MANIFEST_MAX_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ManifestPersistMetrics {
    pub bytes: u64,
    pub encode_nanos: u64,
    pub write_nanos: u64,
    pub fsync_nanos: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestIntegrity {
    format: u16,
    crc32c: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub format: u32,
    // Имя JSON-поля является частью уже опубликованного формата manifest v1/v2.
    #[serde(rename = "database_uuid")]
    pub storage_uuid: [u8; 16],
    #[serde(default = "legacy_virtual_shards")]
    pub virtual_shards: u16,
    #[serde(default = "default_partition_span_us")]
    pub partition_span_us: i64,
    pub checkpoint_commit: u64,
    #[serde(
        default,
        skip_serializing_if = "crate::deletion::Deletions::is_initial"
    )]
    pub deletions: std::sync::Arc<crate::deletion::Deletions>,
    /// Largest contiguous commit fully published in immutable files.
    ///
    /// An absent field uses the legacy checkpoint as the materialization
    /// boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_commit: Option<u64>,
    pub next_segment_id: u64,
    pub file_catalog: crate::persistence::catalog::Root,
    pub catalog_history_target_bytes: u64,
    pub catalog_history_trigger: usize,
    pub catalog_history_window_us: i64,
    pub file_totals: crate::persistence::catalog::records::Totals,
    /// Materialized only for explicit administrative operations, never persisted.
    #[serde(skip)]
    pub segments: Vec<SegmentDescriptor>,
    /// Immutable read generations for sealed strictly increasing L2 history.
    ///
    /// Each generation owns descriptors for one bounded time epoch.
    /// L0/L1 and the unsealed L2 tail remain ordinary manifest descriptors.
    #[serde(skip)]
    pub published_read_directories: Vec<PublishedReadDirectoryDescriptor>,
    #[serde(skip)]
    pub published_read_payload_files: Vec<PublishedReadPayloadFile>,
    #[serde(skip)]
    pub aggregate_archives: Vec<AggregateArchiveDescriptor>,
    #[serde(default)]
    pub catalog: CatalogState,
    #[serde(skip)]
    pub catalog_artifacts: Vec<ManifestArtifact>,
    #[serde(default)]
    pub retention_cutoffs: BTreeMap<u32, i64>,
    #[serde(default)]
    pub aggregate_retention_cutoffs: BTreeMap<u32, i64>,
    /// Largest published timestamp for each policy.
    ///
    /// If absent, recover the watermark from segments and WAL.
    #[serde(default)]
    pub event_watermarks: BTreeMap<u32, i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    integrity: Option<ManifestIntegrity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublishedReadDirectoryDescriptor {
    /// Storage of descriptors alone never authorizes the strict query shortcut.
    pub generation_id: u64,
    /// None identifies the older directory spanning all shards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<u16>,
    pub policy_id: u32,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub maximum_segment_id: u64,
    pub segment_count: u64,
    pub segments_crc32c: u32,
    pub relative_path: String,
    pub file_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublishedReadPayloadFile {
    pub relative_path: String,
    pub file_size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestArtifactKind {
    Identity,
    Manifest,
    Segment,
    PublishedReadDirectory,
    AggregateArchive,
    Catalog,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestArtifact {
    pub relative_path: PathBuf,
    pub kind: ManifestArtifactKind,
    /// Minimum file length required by all logical references.
    pub minimum_size: u64,
    /// Exact length for files wholly owned by this descriptor.
    pub exact_size: Option<u64>,
    pub pack_id: Option<u64>,
}

impl ManifestArtifact {
    pub(crate) fn validate_at(&self, root: &Path) -> Result<()> {
        let mut current = root.to_path_buf();
        for component in self.relative_path.components() {
            current.push(component.as_os_str());
            let metadata = fs::symlink_metadata(&current)?;
            if metadata.file_type().is_symlink() {
                return Err(corrupt("manifest artifact path contains a symbolic link"));
            }
        }
        let metadata = fs::metadata(&current)?;
        if !metadata.is_file()
            || metadata.len() < self.minimum_size
            || self
                .exact_size
                .is_some_and(|expected| metadata.len() != expected)
        {
            return Err(corrupt("manifest artifact length is invalid"));
        }
        Ok(())
    }
}

impl Manifest {
    pub fn create(root: &Path, virtual_shards: u16, partition_span_us: i64) -> Result<Self> {
        validate_layout(virtual_shards, partition_span_us)?;
        fs::create_dir_all(root)?;
        for directory in ["wal", "segments/packs", "aggregates", "manifest", "tmp"] {
            fs::create_dir_all(root.join(directory))?;
        }
        for directory in [
            "wal",
            "segments/packs",
            "segments",
            "aggregates",
            "manifest",
            "tmp",
        ] {
            sync_directory(&root.join(directory))?;
        }
        sync_directory(root)?;
        let storage_uuid = *Uuid::new_v4().as_bytes();
        write_identity(root, storage_uuid, virtual_shards)?;
        crate::persistence::wal::initialize_empty(&root.join("wal"), storage_uuid, 0)?;
        let manifest = Self {
            format: MANIFEST_FORMAT,
            storage_uuid,
            virtual_shards,
            partition_span_us,
            checkpoint_commit: 0,
            deletions: Default::default(),
            materialized_commit: None,
            next_segment_id: 1,
            file_catalog: Default::default(),
            catalog_history_target_bytes: 64 * 1024 * 1024,
            catalog_history_trigger: 4,
            catalog_history_window_us: 0,
            file_totals: Default::default(),
            catalog_artifacts: Vec::new(),
            segments: Vec::new(),
            published_read_directories: Vec::new(),
            published_read_payload_files: Vec::new(),
            aggregate_archives: Vec::new(),
            catalog: CatalogState::default(),
            retention_cutoffs: BTreeMap::new(),
            aggregate_retention_cutoffs: BTreeMap::new(),
            event_watermarks: BTreeMap::new(),
            integrity: None,
        };
        manifest.persist(root)?;
        Ok(manifest)
    }

    pub fn load(root: &Path) -> Result<Self> {
        let (storage_uuid, identity_shards) = read_identity(root)?;
        let path = root.join("manifest/MANIFEST.json");
        if fs::metadata(&path)?.len() > MANIFEST_MAX_BYTES {
            return Err(corrupt("manifest exceeds 16 MiB"));
        }
        let bytes = fs::read(&path)?;
        if bytes.len() as u64 > MANIFEST_MAX_BYTES {
            return Err(corrupt("manifest exceeds 16 MiB"));
        }
        let manifest: Self = serde_json::from_slice(&bytes)?;
        manifest.validate_integrity()?;
        if manifest.storage_uuid != storage_uuid
            || manifest.virtual_shards != identity_shards
            || validate_layout(manifest.virtual_shards, manifest.partition_span_us).is_err()
        {
            return Err(corrupt("manifest identity or version mismatch"));
        }
        if manifest.next_segment_id == 0 {
            return Err(corrupt("manifest has an invalid next segment ID"));
        }
        if manifest.checkpoint_commit > manifest.materialized_commit() {
            return Err(corrupt(
                "manifest checkpoint exceeds its materialized commit",
            ));
        }
        manifest.catalog.validate_loaded()?;
        if manifest
            .retention_cutoffs
            .keys()
            .any(|policy_id| !manifest.catalog.contains_policy_id(*policy_id))
        {
            return Err(corrupt("retention cutoff references an unknown policy"));
        }
        if manifest
            .aggregate_retention_cutoffs
            .keys()
            .any(|policy_id| !manifest.catalog.contains_policy_id(*policy_id))
        {
            return Err(corrupt(
                "aggregate retention cutoff references an unknown policy",
            ));
        }
        if manifest
            .aggregate_retention_cutoffs
            .iter()
            .any(|(policy, aggregate)| {
                manifest
                    .retention_cutoffs
                    .get(policy)
                    .is_some_and(|raw| aggregate > raw)
            })
        {
            return Err(corrupt(
                "aggregate retention cutoff exceeds raw retention cutoff",
            ));
        }
        if manifest
            .event_watermarks
            .keys()
            .any(|policy_id| *policy_id == 0)
        {
            return Err(corrupt("event watermark references an invalid policy"));
        }
        Ok(manifest)
    }

    pub fn load_or_create(
        root: &Path,
        create_if_missing: bool,
        virtual_shards: u16,
        partition_span_us: i64,
    ) -> Result<Self> {
        if root.join("IDENTITY").exists() {
            let manifest = Self::load(root)?;
            if manifest.virtual_shards != virtual_shards
                || manifest.partition_span_us != partition_span_us
            {
                return Err(invalid("storage layout does not match open options"));
            }
            return Ok(manifest);
        }
        if storage_directory_has_artifacts(root)? {
            return Err(corrupt(
                "storage IDENTITY is missing from a non-empty directory",
            ));
        }
        if !create_if_missing {
            return Err(invalid("storage does not exist"));
        }
        Self::create(root, virtual_shards, partition_span_us)
    }

    pub fn persist(&self, root: &Path) -> Result<ManifestPersistMetrics> {
        self.persist_with_commit(root, || {})
    }

    pub(crate) fn persist_with_commit(
        &self,
        root: &Path,
        committed: impl FnOnce(),
    ) -> Result<ManifestPersistMetrics> {
        self.persist_inner(root, committed, |manifest_directory, storage_root| {
            sync_directory(manifest_directory)?;
            sync_directory(storage_root)
        })
    }

    fn persist_inner(
        &self,
        root: &Path,
        committed: impl FnOnce(),
        sync_after_commit: impl FnOnce(&Path, &Path) -> Result<()>,
    ) -> Result<ManifestPersistMetrics> {
        if self.checkpoint_commit > self.materialized_commit()
            || self
                .segments
                .iter()
                .any(|segment| segment.max_commit > self.materialized_commit())
        {
            return Err(corrupt(
                "manifest publication exceeds its materialized commit",
            ));
        }
        self.durable_artifacts()?;
        let directory = root.join("manifest");
        fs::create_dir_all(&directory)?;
        let temporary = root.join("tmp/MANIFEST.json.tmp");
        let final_path = directory.join("MANIFEST.json");
        let encode_started = Instant::now();
        let mut persisted = self.clone();
        persisted.format = MANIFEST_FORMAT;
        persisted.integrity = None;
        let payload = serde_json::to_vec(&persisted)?;
        persisted.integrity = Some(ManifestIntegrity {
            format: MANIFEST_INTEGRITY_FORMAT,
            crc32c: crc32c::crc32c(&payload),
        });
        let bytes = serde_json::to_vec(&persisted)?;
        let encode_nanos = duration_nanos(encode_started.elapsed());
        if bytes.len() as u64 > MANIFEST_MAX_BYTES {
            return Err(invalid("manifest exceeds 16 MiB"));
        }
        let write_started = Instant::now();

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        let write_nanos = duration_nanos(write_started.elapsed());
        let fsync_started = Instant::now();
        // Includes file sync, rename, commit callback and directory syncs.
        let _sync_timer = publication_cost::stage(CostStage::ManifestSyncAndCommit);
        file.sync_all()?;

        drop(file);
        fs::rename(&temporary, &final_path)?;
        committed();

        sync_after_commit(&directory, root)?;
        Ok(ManifestPersistMetrics {
            bytes: bytes.len() as u64,
            encode_nanos,
            write_nanos,
            fsync_nanos: duration_nanos(fsync_started.elapsed()),
        })
    }

    #[must_use]
    pub(crate) fn materialized_commit(&self) -> u64 {
        self.materialized_commit.unwrap_or(self.checkpoint_commit)
    }

    pub(crate) fn durable_artifacts(&self) -> Result<Vec<ManifestArtifact>> {
        let mut artifacts = BTreeMap::<PathBuf, ManifestArtifact>::new();
        artifacts.insert(
            PathBuf::from("IDENTITY"),
            ManifestArtifact {
                relative_path: PathBuf::from("IDENTITY"),
                kind: ManifestArtifactKind::Identity,
                minimum_size: IDENTITY_SIZE as u64,
                exact_size: Some(IDENTITY_SIZE as u64),
                pack_id: None,
            },
        );
        artifacts.insert(
            PathBuf::from("manifest/MANIFEST.json"),
            ManifestArtifact {
                relative_path: PathBuf::from("manifest/MANIFEST.json"),
                kind: ManifestArtifactKind::Manifest,
                minimum_size: 1,
                exact_size: None,
                pack_id: None,
            },
        );
        for descriptor in &self.segments {
            let relative = PathBuf::from(&descriptor.relative_path);
            validate_artifact_path(&relative, "segments")?;
            let required_size = descriptor
                .file_offset
                .checked_add(descriptor.file_size)
                .ok_or_else(|| corrupt("segment physical range overflows"))?;
            match artifacts.entry(relative.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(ManifestArtifact {
                        relative_path: relative,
                        kind: ManifestArtifactKind::Segment,
                        minimum_size: required_size,
                        exact_size: descriptor.pack_id.is_none().then_some(descriptor.file_size),
                        pack_id: descriptor.pack_id,
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let artifact = entry.get_mut();
                    if artifact.kind != ManifestArtifactKind::Segment
                        || descriptor.pack_id.is_none()
                        || artifact.pack_id != descriptor.pack_id
                    {
                        return Err(corrupt(
                            "manifest descriptors disagree about physical file ownership",
                        ));
                    }
                    artifact.minimum_size = artifact.minimum_size.max(required_size);
                }
            }
        }
        for descriptor in &self.published_read_directories {
            let relative = PathBuf::from(&descriptor.relative_path);
            validate_artifact_path(&relative, "segments")?;
            if artifacts
                .insert(
                    relative.clone(),
                    ManifestArtifact {
                        relative_path: relative,
                        kind: ManifestArtifactKind::PublishedReadDirectory,
                        minimum_size: descriptor.file_size,
                        exact_size: Some(descriptor.file_size),
                        pack_id: None,
                    },
                )
                .is_some()
            {
                return Err(corrupt(
                    "manifest contains duplicate physical artifact paths",
                ));
            }
        }
        for payload in &self.published_read_payload_files {
            let relative = PathBuf::from(&payload.relative_path);
            validate_artifact_path(&relative, "segments")?;
            match artifacts.entry(relative.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(ManifestArtifact {
                        relative_path: relative,
                        kind: ManifestArtifactKind::Segment,
                        minimum_size: payload.file_size,
                        exact_size: Some(payload.file_size),
                        pack_id: None,
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let artifact = entry.get_mut();
                    if artifact.kind != ManifestArtifactKind::Segment
                        || artifact.minimum_size > payload.file_size
                        || artifact
                            .exact_size
                            .is_some_and(|size| size != payload.file_size)
                    {
                        return Err(corrupt(
                            "published read payload disagrees with a live segment file",
                        ));
                    }
                    artifact.minimum_size = payload.file_size;
                    artifact.exact_size = Some(payload.file_size);
                }
            }
        }
        for descriptor in &self.aggregate_archives {
            let relative = PathBuf::from(&descriptor.relative_path);
            validate_artifact_path(&relative, "aggregates")?;
            if artifacts
                .insert(
                    relative.clone(),
                    ManifestArtifact {
                        relative_path: relative,
                        kind: ManifestArtifactKind::AggregateArchive,
                        minimum_size: descriptor.file_size,
                        exact_size: Some(descriptor.file_size),
                        pack_id: None,
                    },
                )
                .is_some()
            {
                return Err(corrupt(
                    "manifest contains duplicate physical artifact paths",
                ));
            }
        }
        for artifact in &self.catalog_artifacts {
            if artifacts
                .insert(artifact.relative_path.clone(), artifact.clone())
                .is_some()
            {
                return Err(corrupt("duplicate catalogue artifact"));
            }
        }
        Ok(artifacts.into_values().collect())
    }

    fn validate_integrity(&self) -> Result<()> {
        self.deletions.validate()?;
        if self.format != MANIFEST_FORMAT {
            return Err(corrupt("manifest version is unsupported"));
        }
        let integrity = self
            .integrity
            .as_ref()
            .ok_or_else(|| corrupt("manifest integrity checksum is missing"))?;
        if integrity.format != MANIFEST_INTEGRITY_FORMAT {
            return Err(corrupt("manifest integrity format is unsupported"));
        }
        let mut payload = self.clone();
        payload.integrity = None;
        let bytes = serde_json::to_vec(&payload)?;
        if integrity.crc32c != crc32c::crc32c(&bytes) {
            return Err(corrupt("manifest integrity checksum mismatch"));
        }
        Ok(())
    }
}

fn validate_artifact_path(path: &Path, directory: &str) -> Result<()> {
    if path.is_absolute()
        || !path.starts_with(directory)
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
        return Err(corrupt("manifest contains an unsafe artifact path"));
    }
    Ok(())
}

fn storage_directory_has_artifacts(root: &Path) -> Result<bool> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name() != "LOCK" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn write_identity(root: &Path, storage_uuid: [u8; 16], virtual_shards: u16) -> Result<()> {
    let temporary = root.join("IDENTITY.tmp");
    let final_path = root.join("IDENTITY");
    let mut bytes = [0_u8; IDENTITY_SIZE];
    bytes[..8].copy_from_slice(IDENTITY_MAGIC);
    put_u16(&mut bytes, 8, STORAGE_FORMAT as u16);
    put_u16(&mut bytes, 10, IDENTITY_SIZE as u16);
    bytes[12..28].copy_from_slice(&storage_uuid);
    put_u16(&mut bytes, 28, STORAGE_FORMAT as u16);
    put_u16(&mut bytes, 30, STORAGE_FORMAT as u16);
    put_u16(&mut bytes, 32, virtual_shards);
    put_i64(&mut bytes, 36, now_us());
    let checksum = crc32c::crc32c(&bytes[..60]);
    put_u32(&mut bytes, 60, checksum);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, final_path)?;
    sync_directory(root)?;
    Ok(())
}

fn read_identity(root: &Path) -> Result<([u8; 16], u16)> {
    let path = root.join("IDENTITY");
    let mut file = File::open(&path)?;
    let mut bytes = [0_u8; IDENTITY_SIZE];
    file.read_exact(&mut bytes)?;
    if file.metadata()?.len() != IDENTITY_SIZE as u64
        || &bytes[..8] != IDENTITY_MAGIC
        || read_u16(&bytes, 10) as usize != IDENTITY_SIZE
        || read_u16(&bytes, 34) != 0
        || bytes[44..60] != [0_u8; 16]
        || read_u32(&bytes, 60) != crc32c::crc32c(&bytes[..60])
    {
        return Err(corrupt(format!("invalid IDENTITY: {}", path.display())));
    }
    if [8, 28, 30]
        .into_iter()
        .any(|offset| read_u16(&bytes, offset) != STORAGE_FORMAT as u16)
    {
        return Err(invalid(format!(
            "unsupported storage format {}; expected {}",
            read_u16(&bytes, 8),
            STORAGE_FORMAT
        )));
    }
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&bytes[12..28]);
    let virtual_shards = read_u16(&bytes, 32);
    validate_layout(virtual_shards, DEFAULT_PARTITION_SPAN_US)
        .map_err(|_| corrupt("invalid IDENTITY virtual shard count"))?;
    Ok((uuid, virtual_shards))
}

const fn legacy_virtual_shards() -> u16 {
    1
}

const fn default_partition_span_us() -> i64 {
    DEFAULT_PARTITION_SPAN_US
}

fn now_us() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
