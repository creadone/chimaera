use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::aggregate_archive::AggregateArchiveDescriptor;
use crate::catalog::CatalogState;
use crate::error::{Result, corrupt, invalid};
use crate::layout::{DEFAULT_PARTITION_SPAN_US, validate_layout};
use crate::segment::SegmentDescriptor;
use crate::wal::{put_i64, put_u16, put_u32, read_u16, read_u32, sync_directory};

const IDENTITY_MAGIC: &[u8; 8] = b"TSIDT001";
const IDENTITY_SIZE: usize = 64;
pub(crate) const STORAGE_FORMAT: u32 = 1;
const LEGACY_MANIFEST_FORMAT: u32 = 1;
const MANIFEST_FORMAT: u32 = 2;
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
    /// Наибольший непрерывный commit, полностью опубликованный в immutable-файлах.
    ///
    /// Отсутствие поля означает старый manifest, где checkpoint одновременно был
    /// границей материализации.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_commit: Option<u64>,
    pub next_segment_id: u64,
    pub segments: Vec<SegmentDescriptor>,
    /// Неизменяемые read generations для окончательно закрытой strict L2.
    ///
    /// Каждое поколение владеет descriptor-ами одного bounded временного epoch.
    /// L0/L1 и ещё не закрытый хвост L2 остаются обычными manifest descriptors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published_read_directories: Vec<PublishedReadDirectoryDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published_read_payload_files: Vec<PublishedReadPayloadFile>,
    #[serde(default)]
    pub aggregate_archives: Vec<AggregateArchiveDescriptor>,
    #[serde(default)]
    pub catalog: CatalogState,
    #[serde(default)]
    pub retention_cutoffs: BTreeMap<u32, i64>,
    #[serde(default)]
    pub aggregate_retention_cutoffs: BTreeMap<u32, i64>,
    /// Наибольшая опубликованная отметка времени для каждой политики.
    ///
    /// При отсутствии значения watermark восстанавливается из сегментов и WAL.
    #[serde(default)]
    pub event_watermarks: BTreeMap<u32, i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    integrity: Option<ManifestIntegrity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublishedReadDirectoryDescriptor {
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestArtifact {
    pub relative_path: PathBuf,
    pub kind: ManifestArtifactKind,
    /// Минимальная длина, необходимая всем логическим ссылкам на файл.
    pub minimum_size: u64,
    /// Точная длина для файлов, которыми descriptor владеет целиком.
    pub exact_size: Option<u64>,
    pub logical_descriptor_ids: Vec<u64>,
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
        let manifest = Self {
            format: MANIFEST_FORMAT,
            storage_uuid,
            virtual_shards,
            partition_span_us,
            checkpoint_commit: 0,
            materialized_commit: None,
            next_segment_id: 1,
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
        let mut ids = manifest
            .segments
            .iter()
            .map(|segment| segment.id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1])
            || ids
                .last()
                .is_some_and(|last| *last >= manifest.next_segment_id)
            || manifest.segments.iter().any(|segment| {
                segment.level > 2
                    || segment.shard_id >= manifest.virtual_shards
                    || segment.policy_id == 0
                    || segment.window_start_us >= segment.window_end_us
                    || segment.min_timestamp_us > segment.max_timestamp_us
                    || segment.min_timestamp_us < segment.window_start_us
                    || segment.max_timestamp_us >= segment.window_end_us
                    || !segment.relative_path.starts_with("segments/")
                    || segment.relative_path.contains("..")
                    || segment.pack_id == Some(0)
                    || (segment.pack_id.is_none() && segment.file_offset != 0)
                    || segment.file_offset.checked_add(segment.file_size).is_none()
            })
        {
            return Err(corrupt("manifest contains invalid segment metadata"));
        }
        let mut archive_ids = manifest
            .aggregate_archives
            .iter()
            .map(|archive| archive.id)
            .collect::<Vec<_>>();
        archive_ids.sort_unstable();
        if archive_ids.windows(2).any(|pair| pair[0] == pair[1])
            || archive_ids.iter().any(|id| ids.binary_search(id).is_ok())
        {
            return Err(corrupt("manifest contains duplicate aggregate archive IDs"));
        }
        for archive in &manifest.aggregate_archives {
            archive.validate(manifest.virtual_shards, manifest.next_segment_id)?;
            let Some(policy) = manifest
                .catalog
                .snapshot()
                .policy_by_id(archive.policy_id)
                .cloned()
            else {
                return Err(corrupt("aggregate archive references an unknown policy"));
            };
            if policy.aggregate_retention_us.is_none()
                || archive
                    .resolutions
                    .iter()
                    .any(|resolution| !policy.retained_aggregate_resolutions.contains(resolution))
            {
                return Err(corrupt(
                    "aggregate archive disagrees with its storage policy",
                ));
            }
        }
        let mut archives = manifest.aggregate_archives.iter().collect::<Vec<_>>();
        archives.sort_unstable_by_key(|archive| {
            (
                archive.shard_id,
                archive.policy_id,
                archive.window_start_us,
                archive.coverage_from_us,
                archive.coverage_to_us,
                archive.chunk_index,
            )
        });
        let mut coverage_groups = Vec::new();
        let mut position = 0_usize;
        while position < archives.len() {
            let first = archives[position];
            let mut end = position + 1;
            while end < archives.len()
                && archives[end].shard_id == first.shard_id
                && archives[end].policy_id == first.policy_id
                && archives[end].window_start_us == first.window_start_us
                && archives[end].coverage_from_us == first.coverage_from_us
                && archives[end].coverage_to_us == first.coverage_to_us
            {
                end += 1;
            }
            let chunks = &archives[position..end];
            if chunks.iter().enumerate().any(|(chunk_index, archive)| {
                archive.chunk_count as usize != chunks.len()
                    || archive.chunk_index as usize != chunk_index
            }) {
                return Err(corrupt("aggregate archive chunk set is incomplete"));
            }
            coverage_groups.push(first);
            position = end;
        }
        if coverage_groups.windows(2).any(|pair| {
            pair[0].shard_id == pair[1].shard_id
                && pair[0].policy_id == pair[1].policy_id
                && pair[0].window_start_us == pair[1].window_start_us
                && pair[0].coverage_to_us > pair[1].coverage_from_us
        }) {
            return Err(corrupt("aggregate archive coverage groups overlap"));
        }
        if manifest
            .segments
            .iter()
            .any(|segment| segment.max_commit > manifest.materialized_commit())
        {
            return Err(corrupt("manifest segment exceeds its materialized commit"));
        }
        let catalog = manifest.catalog.snapshot();
        let mut generation_ids = BTreeMap::new();
        let mut generation_windows = manifest
            .published_read_directories
            .iter()
            .collect::<Vec<_>>();
        generation_windows.sort_unstable_by_key(|directory| {
            (
                directory.policy_id,
                directory.window_start_us,
                directory.window_end_us,
            )
        });
        let mut payload_paths = manifest
            .published_read_payload_files
            .iter()
            .map(|payload| payload.relative_path.as_str())
            .collect::<Vec<_>>();
        payload_paths.sort_unstable();
        if manifest.published_read_payload_files.iter().any(|payload| {
            payload.file_size == 0
                || !payload.relative_path.starts_with("segments/")
                || payload.relative_path.starts_with("segments/directories/")
                || payload.relative_path.contains("..")
        }) || payload_paths.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(corrupt(
                "manifest contains invalid read generation payloads",
            ));
        }
        for directory in &manifest.published_read_directories {
            if directory.generation_id == 0
                || directory.generation_id >= manifest.next_segment_id
                || directory
                    .shard_id
                    .is_some_and(|shard| shard >= manifest.virtual_shards)
                || directory.policy_id == 0
                || directory.window_start_us >= directory.window_end_us
                || directory.maximum_segment_id >= manifest.next_segment_id
                || ids.binary_search(&directory.generation_id).is_ok()
                || archive_ids.binary_search(&directory.generation_id).is_ok()
                || generation_ids
                    .insert(directory.generation_id, &directory.relative_path)
                    .is_some()
                || directory.segment_count == 0
                || directory.file_size == 0
                || !directory.relative_path.starts_with("segments/directories/")
                || directory.relative_path.contains("..")
                || catalog
                    .policy_by_id(directory.policy_id)
                    .is_none_or(|policy| {
                        policy.write_mode != crate::catalog::WriteMode::StrictlyIncreasing
                            || policy.max_future_skew_us.is_none()
                    })
            {
                return Err(corrupt(
                    "manifest contains an invalid published read directory",
                ));
            }
        }
        let mut last_scope_end = BTreeMap::new();
        let mut last_legacy_end = BTreeMap::new();
        let mut last_policy_end = BTreeMap::new();
        for directory in generation_windows {
            let policy = directory.policy_id;
            let start = directory.window_start_us;
            let end = directory.window_end_us;
            let overlaps_scope = last_scope_end
                .insert((policy, directory.shard_id), end)
                .is_some_and(|previous| previous > start);
            let overlaps_legacy = last_legacy_end
                .get(&policy)
                .is_some_and(|previous| *previous > start);
            let legacy_overlaps_any = directory.shard_id.is_none()
                && last_policy_end
                    .get(&policy)
                    .is_some_and(|previous| *previous > start);
            if overlaps_scope || overlaps_legacy || legacy_overlaps_any {
                return Err(corrupt("published read directory windows overlap"));
            }
            last_policy_end
                .entry(policy)
                .and_modify(|previous: &mut i64| *previous = (*previous).max(end))
                .or_insert(end);
            if directory.shard_id.is_none() {
                last_legacy_end.insert(policy, end);
            }
        }
        if manifest.published_read_directories.is_empty()
            != manifest.published_read_payload_files.is_empty()
        {
            return Err(corrupt(
                "read generation directories and payloads must be published together",
            ));
        }
        for artifact in manifest.durable_artifacts()? {
            artifact.validate_at(root)?;
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
            crate::failpoint::io_error("manifest_after_commit")?;
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
        file.sync_all()?;
        crate::failpoint::hit("manifest_after_sync");
        drop(file);
        fs::rename(&temporary, &final_path)?;
        committed();
        crate::failpoint::hit("manifest_after_rename");
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
                logical_descriptor_ids: Vec::new(),
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
                logical_descriptor_ids: Vec::new(),
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
                        logical_descriptor_ids: vec![descriptor.id],
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
                    artifact.logical_descriptor_ids.push(descriptor.id);
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
                        logical_descriptor_ids: vec![descriptor.generation_id],
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
        let generation_ids = self
            .published_read_directories
            .iter()
            .map(|descriptor| descriptor.generation_id)
            .collect::<Vec<_>>();
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
                        logical_descriptor_ids: generation_ids.clone(),
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
                    artifact
                        .logical_descriptor_ids
                        .extend(generation_ids.iter().copied());
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
                        logical_descriptor_ids: vec![descriptor.id],
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
        let mut artifacts = artifacts.into_values().collect::<Vec<_>>();
        for artifact in &mut artifacts {
            artifact.logical_descriptor_ids.sort_unstable();
        }
        Ok(artifacts)
    }

    pub(crate) fn data_artifacts(&self) -> Result<Vec<ManifestArtifact>> {
        Ok(self
            .durable_artifacts()?
            .into_iter()
            .filter(|artifact| {
                matches!(
                    artifact.kind,
                    ManifestArtifactKind::Segment
                        | ManifestArtifactKind::PublishedReadDirectory
                        | ManifestArtifactKind::AggregateArchive
                )
            })
            .collect())
    }

    fn validate_integrity(&self) -> Result<()> {
        if self.format == LEGACY_MANIFEST_FORMAT && self.integrity.is_none() {
            return Ok(());
        }
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
    put_u16(&mut bytes, 8, 1);
    put_u16(&mut bytes, 10, IDENTITY_SIZE as u16);
    bytes[12..28].copy_from_slice(&storage_uuid);
    put_u16(&mut bytes, 28, 1);
    put_u16(&mut bytes, 30, 1);
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
        || read_u16(&bytes, 8) != 1
        || read_u16(&bytes, 10) as usize != IDENTITY_SIZE
        || read_u16(&bytes, 28) != 1
        || read_u16(&bytes, 30) != 1
        || read_u16(&bytes, 34) != 0
        || bytes[44..60] != [0_u8; 16]
        || read_u32(&bytes, 60) != crc32c::crc32c(&bytes[..60])
    {
        return Err(corrupt(format!("invalid IDENTITY: {}", path.display())));
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn identity_and_manifest_round_trip() -> Result<()> {
        let temporary = TempDir::new()?;
        let created = Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
        assert!(persisted.get("database_uuid").is_some());
        assert!(persisted.get("storage_uuid").is_none());
        let loaded = Manifest::load(temporary.path())?;
        assert_eq!(loaded.storage_uuid, created.storage_uuid);
        assert_eq!(loaded.checkpoint_commit, 0);
        assert!(loaded.integrity.is_some());
        assert!(temporary.path().join("segments/packs").is_dir());
        Ok(())
    }

    #[test]
    fn legacy_manifest_without_integrity_remains_readable() -> Result<()> {
        let temporary = TempDir::new()?;
        Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        let path = temporary.path().join("manifest/MANIFEST.json");
        let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("integrity");
        value["format"] = serde_json::Value::from(LEGACY_MANIFEST_FORMAT);
        fs::write(&path, serde_json::to_vec(&value)?)?;
        Manifest::load(temporary.path())?;
        Ok(())
    }

    #[test]
    fn current_manifest_cannot_drop_its_integrity_envelope() -> Result<()> {
        let temporary = TempDir::new()?;
        Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        let path = temporary.path().join("manifest/MANIFEST.json");
        let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("integrity");
        fs::write(&path, serde_json::to_vec(&value)?)?;
        assert!(matches!(
            Manifest::load(temporary.path()),
            Err(crate::error::Error::Corruption(_))
        ));
        Ok(())
    }

    #[test]
    fn artifact_inventory_deduplicates_shared_packs_and_keeps_required_size() -> Result<()> {
        let temporary = TempDir::new()?;
        let mut manifest = Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        let descriptor = |id, file_offset, file_size| SegmentDescriptor {
            id,
            level: 0,
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
            min_timestamp_us: id as i64,
            max_timestamp_us: id as i64,
            relative_path: "segments/packs/PACK-00000000000000000001-S0000.tsp".into(),
            pack_id: Some(1),
            file_offset,
            min_commit: id,
            max_commit: id,
            record_count: 1,
            file_size,
            rewrite_count: 0,
        };
        manifest.materialized_commit = Some(2);
        manifest.next_segment_id = 3;
        manifest.segments = vec![descriptor(1, 0, 100), descriptor(2, 100, 200)];

        let artifacts = manifest.data_artifacts()?;
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].kind, ManifestArtifactKind::Segment);
        assert_eq!(artifacts[0].minimum_size, 300);
        assert_eq!(artifacts[0].exact_size, None);
        assert_eq!(artifacts[0].logical_descriptor_ids, [1, 2]);
        assert_eq!(artifacts[0].pack_id, Some(1));
        Ok(())
    }

    #[test]
    fn manifest_rejects_a_checkpoint_beyond_materialized_data() -> Result<()> {
        let temporary = TempDir::new()?;
        let mut manifest = Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        manifest.checkpoint_commit = 2;
        manifest.materialized_commit = Some(1);
        assert!(matches!(
            manifest.persist(temporary.path()),
            Err(crate::error::Error::Corruption(message))
                if message.contains("materialized commit")
        ));
        Ok(())
    }

    #[test]
    fn manifest_commit_hook_runs_before_post_rename_sync_error() -> Result<()> {
        let temporary = TempDir::new()?;
        let mut manifest = Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        manifest.event_watermarks.insert(7, 42);
        let committed = std::cell::Cell::new(false);

        let result = manifest.persist_inner(
            temporary.path(),
            || committed.set(true),
            |_, _| {
                Err(crate::error::Error::Io(std::io::Error::other(
                    "injected post-rename sync failure",
                )))
            },
        );

        assert!(matches!(result, Err(crate::error::Error::Io(_))));
        assert!(committed.get());
        assert_eq!(
            Manifest::load(temporary.path())?.event_watermarks.get(&7),
            Some(&42)
        );
        Ok(())
    }

    #[test]
    fn fourfold_lifecycle_descriptor_scale_keeps_manifest_headroom() -> Result<()> {
        const ACCEPTED_LIFECYCLE_DESCRIPTORS: u64 = 4_096;
        const SCALE_FACTOR: u64 = 4;

        let temporary = TempDir::new()?;
        let mut manifest = Manifest::create(temporary.path(), 64, DEFAULT_PARTITION_SPAN_US)?;
        manifest.segments = (1..=ACCEPTED_LIFECYCLE_DESCRIPTORS * SCALE_FACTOR)
            .map(|id| SegmentDescriptor {
                id,
                level: 2,
                shard_id: (id % 64) as u16,
                policy_id: 1,
                window_start_us: (id as i64).saturating_mul(DEFAULT_PARTITION_SPAN_US),
                window_end_us: (id as i64)
                    .saturating_add(1)
                    .saturating_mul(DEFAULT_PARTITION_SPAN_US),
                min_timestamp_us: (id as i64).saturating_mul(DEFAULT_PARTITION_SPAN_US),
                max_timestamp_us: (id as i64)
                    .saturating_add(1)
                    .saturating_mul(DEFAULT_PARTITION_SPAN_US)
                    .saturating_sub(1),
                relative_path: format!("segments/SEG-{id:020}.tss"),
                pack_id: None,
                file_offset: 0,
                min_commit: id,
                max_commit: id,
                record_count: 1,
                file_size: 4_096,
                rewrite_count: 1,
            })
            .collect();
        manifest.next_segment_id = ACCEPTED_LIFECYCLE_DESCRIPTORS * SCALE_FACTOR + 1;
        manifest.materialized_commit = Some(ACCEPTED_LIFECYCLE_DESCRIPTORS * SCALE_FACTOR);
        let mut persisted = manifest.clone();
        persisted.integrity = None;
        let payload = serde_json::to_vec(&persisted)?;
        persisted.integrity = Some(ManifestIntegrity {
            format: MANIFEST_INTEGRITY_FORMAT,
            crc32c: crc32c::crc32c(&payload),
        });
        let bytes = serde_json::to_vec(&persisted)?;
        assert!(bytes.len() as u64 <= MANIFEST_MAX_BYTES);
        assert!(MANIFEST_MAX_BYTES - bytes.len() as u64 >= MANIFEST_MAX_BYTES / 2);
        Ok(())
    }
}
