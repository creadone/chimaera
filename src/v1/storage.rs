use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::catalog::{
    BlockCodec, BucketDefinition, CatalogSnapshot, CompressionProfile, SpaceDefinition,
    StoragePolicy, TimestampCodec, ValueCodec,
};
use crate::error::{Result, corrupt, invalid};
use crate::types::{Point, SeriesKey};
use crate::v1::{Operation, OperationKind, SegmentSummary, visit_segment};
use crate::wal::{read_i64, read_u16, read_u32, read_u64};

const IDENTITY_MAGIC: &[u8; 8] = b"TSIDT001";
const MANIFEST_MAGIC: &[u8; 8] = b"TSMAN001";
const FORMAT_VERSION: u16 = 1;
const IDENTITY_SIZE: usize = 64;
const MANIFEST_HEADER_SIZE: usize = 64;
const RECORD_HEADER_SIZE: usize = 20;
const MAX_CURRENT_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const NIL_SPAN: i64 = i64::MIN;
const NIL_LEVEL: i32 = i32::MIN;

/// Проверенное содержимое `IDENTITY` формата legacy v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Identity {
    pub storage_uuid: [u8; 16],
    pub virtual_shard_count: u16,
    pub created_at_us: i64,
}

/// Долговечная ссылка Chimaera manifest на поколение WAL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalReference {
    pub wal_id: u64,
    pub first_commit_id: u64,
    pub last_commit_id: Option<u64>,
}

impl WalReference {
    #[must_use]
    pub const fn is_active(self) -> bool {
        self.last_commit_id.is_none()
    }
}

/// Дескриптор логической области `.tss` или `.tsp` из Chimaera manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    pub segment_id: u64,
    pub shard_id: u16,
    pub policy_id: u32,
    pub level: u8,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
    pub min_commit_id: u64,
    pub max_commit_id: u64,
    pub file_size: u64,
    pub pack_id: Option<u64>,
    pub file_offset: u64,
}

impl SegmentDescriptor {
    /// Возвращает путь в каталоге базы по правилам legacy v1.
    #[must_use]
    pub fn relative_path(&self) -> PathBuf {
        match self.pack_id {
            Some(pack_id) => PathBuf::from("segments")
                .join("packs")
                .join(format!("PACK-S{:04}-{pack_id:020}.tsp", self.shard_id)),
            None => PathBuf::from("segments")
                .join(format!("S{:04}", self.shard_id))
                .join(format!("SEG-{:020}-L{}.tss", self.segment_id, self.level)),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.segment_id == 0
            || self.policy_id == 0
            || self.level > 2
            || self.window_start_us >= self.window_end_us
            || self.min_timestamp_us > self.max_timestamp_us
            || self.min_timestamp_us < self.window_start_us
            || self.max_timestamp_us >= self.window_end_us
            || self.min_commit_id > self.max_commit_id
            || self.file_size == 0
        {
            return Err(corrupt("invalid legacy v1 segment descriptor"));
        }
        match self.pack_id {
            Some(0) => return Err(corrupt("legacy v1 pack ID is zero")),
            Some(_) => {
                self.file_offset
                    .checked_add(self.file_size)
                    .ok_or_else(|| corrupt("legacy v1 packed segment bounds overflow"))?;
            }
            None if self.file_offset != 0 => {
                return Err(corrupt("standalone legacy v1 segment has an offset"));
            }
            None => {}
        }
        Ok(())
    }

    fn matches_summary(&self, summary: SegmentSummary, storage_uuid: [u8; 16]) -> bool {
        summary.storage_uuid == storage_uuid
            && summary.segment_id == self.segment_id
            && summary.shard_id == self.shard_id
            && summary.policy_id == self.policy_id
            && summary.level == self.level
            && summary.window_start_us == self.window_start_us
            && summary.window_end_us == self.window_end_us
            && summary.min_timestamp_us == self.min_timestamp_us
            && summary.max_timestamp_us == self.max_timestamp_us
            && summary.min_commit_id == self.min_commit_id
            && summary.max_commit_id == self.max_commit_id
    }
}

/// Неизменяемый read-only образ checkpoint, опубликованный Chimaera.
#[derive(Clone, Debug)]
pub struct StorageImage {
    root: PathBuf,
    pub identity: Identity,
    pub manifest_id: u64,
    pub manifest_name: String,
    pub checkpoint_commit_id: u64,
    pub next_segment_id: u64,
    pub next_wal_id: u64,
    pub catalog: CatalogSnapshot,
    pub segments: Vec<SegmentDescriptor>,
    pub wal_references: Vec<WalReference>,
    pub retention_cutoffs: BTreeMap<u32, i64>,
}

impl StorageImage {
    /// Открывает и полностью проверяет опубликованный checkpoint legacy v1.
    ///
    /// Метод ничего не создаёт и не исправляет оборванный хвост manifest. Для
    /// согласованного образа базу Chimaera следует закрыть или предварительно
    /// скопировать её через штатный snapshot.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку при несовместимом формате, повреждении checksum,
    /// нарушении каталога или несовпадении manifest и файлов сегментов.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let identity = read_identity(&root.join("IDENTITY"))?;
        let manifest_name = read_current(&root.join("CURRENT"))?;
        let manifest_id = parse_manifest_name(&manifest_name)?;
        let state = replay_manifest(
            &root.join("manifest").join(&manifest_name),
            identity,
            manifest_id,
        )?;
        state.finish(root, identity, manifest_id, manifest_name)
    }

    /// Материализует один ряд на checkpoint commit с MVCC-дедупликацией.
    ///
    /// Основной engine выдаёт запрос потоково. Этот метод предоставляет компактный
    /// read-only API и ограничивает память числом версий выбранного ряда.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для пустого диапазона или повреждённого сегмента.
    pub fn read_points(&self, series: SeriesKey, from_us: i64, to_us: i64) -> Result<Vec<Point>> {
        if from_us >= to_us {
            return Err(invalid("v1 read range must be non-empty"));
        }
        let policy = self.catalog.policy_for(series)?;
        let effective_from = self
            .retention_cutoffs
            .get(&policy.id)
            .copied()
            .map_or(from_us, |cutoff| from_us.max(cutoff));
        if effective_from >= to_us {
            return Ok(Vec::new());
        }
        let mut visible = BTreeMap::<i64, Operation>::new();
        for descriptor in &self.segments {
            if descriptor.policy_id != policy.id
                || descriptor.max_timestamp_us < effective_from
                || descriptor.min_timestamp_us >= to_us
                || descriptor.min_commit_id > self.checkpoint_commit_id
            {
                continue;
            }
            let path = self.root.join(descriptor.relative_path());
            let summary = visit_segment(
                path,
                descriptor.file_offset,
                descriptor.file_size,
                |operation| {
                    if operation.series == series
                        && operation.timestamp_us >= effective_from
                        && operation.timestamp_us < to_us
                        && operation.commit_id <= self.checkpoint_commit_id
                    {
                        let replace = visible.get(&operation.timestamp_us).is_none_or(|current| {
                            (operation.commit_id, operation.ordinal)
                                > (current.commit_id, current.ordinal)
                        });
                        if replace {
                            visible.insert(operation.timestamp_us, operation);
                        }
                    }
                    Ok(())
                },
            )?;
            if !descriptor.matches_summary(summary, self.identity.storage_uuid) {
                return Err(corrupt(
                    "legacy v1 segment changed after the storage image was opened",
                ));
            }
        }
        visible
            .into_values()
            .filter(|operation| operation.kind == OperationKind::Put)
            .map(|operation| {
                Point::new(
                    operation.series,
                    operation.timestamp_us,
                    operation
                        .value
                        .ok_or_else(|| corrupt("legacy v1 put has no value"))?,
                )
            })
            .collect()
    }
}

#[derive(Default)]
struct ReplayState {
    next_space_id: u32,
    next_policy_id: u32,
    next_bucket_id: u32,
    next_segment_id: u64,
    next_wal_id: u64,
    checkpoint_commit_id: u64,
    spaces: Vec<SpaceDefinition>,
    policies: Vec<StoragePolicy>,
    buckets: Vec<BucketDefinition>,
    segments: BTreeMap<u64, SegmentDescriptor>,
    wals: BTreeMap<u64, WalReference>,
    retention_cutoffs: BTreeMap<u32, i64>,
}

impl ReplayState {
    fn new() -> Self {
        Self {
            next_space_id: 1,
            next_policy_id: 1,
            next_bucket_id: 1,
            next_segment_id: 1,
            next_wal_id: 1,
            ..Self::default()
        }
    }

    fn finish(
        self,
        root: &Path,
        identity: Identity,
        manifest_id: u64,
        manifest_name: String,
    ) -> Result<StorageImage> {
        let catalog = CatalogSnapshot::from_storage_parts(
            self.spaces,
            self.policies,
            self.buckets,
            self.next_space_id,
            self.next_policy_id,
            self.next_bucket_id,
        )?;
        for descriptor in self.segments.values() {
            if descriptor.shard_id >= identity.virtual_shard_count
                || catalog.policy_by_id(descriptor.policy_id).is_none()
            {
                return Err(corrupt(
                    "legacy v1 segment references an unknown shard or policy",
                ));
            }
        }
        validate_physical_regions(root, self.segments.values())?;
        for descriptor in self.segments.values() {
            let summary = visit_segment(
                root.join(descriptor.relative_path()),
                descriptor.file_offset,
                descriptor.file_size,
                |_| Ok(()),
            )?;
            if !descriptor.matches_summary(summary, identity.storage_uuid) {
                return Err(corrupt(
                    "legacy v1 segment header does not match its manifest descriptor",
                ));
            }
        }
        let segments = self.segments.into_values().collect();
        let wal_references = self.wals.into_values().collect();
        Ok(StorageImage {
            root: root.to_owned(),
            identity,
            manifest_id,
            manifest_name,
            checkpoint_commit_id: self.checkpoint_commit_id,
            next_segment_id: self.next_segment_id,
            next_wal_id: self.next_wal_id,
            catalog,
            segments,
            wal_references,
            retention_cutoffs: self.retention_cutoffs,
        })
    }

    fn apply(&mut self, edit_type: u8, payload: &[u8]) -> Result<()> {
        match edit_type {
            1 => self.create_space(payload),
            2 => self.create_policy(payload),
            3 => self.create_bucket(payload),
            4 => self.add_wal(payload),
            5 => self.close_wal(payload),
            6 => self.add_segment(payload),
            7 => Err(corrupt(
                "obsolete RemoveSegment edit is unsupported by legacy v1",
            )),
            8 => self.set_checkpoint(payload),
            9 => self.set_next_ids(payload),
            10 => self.compaction_commit(payload),
            11 => self.set_retention_cutoff(payload),
            _ => Err(corrupt(format!(
                "unknown legacy v1 manifest edit type: {edit_type}"
            ))),
        }
    }

    fn create_space(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let definition = SpaceDefinition {
            id: reader.u32()?,
            created_at_us: reader.i64()?,
            name: reader.name()?,
        };
        reader.finish()?;
        if definition.id == 0
            || self
                .spaces
                .iter()
                .any(|item| item.id == definition.id || item.name == definition.name)
        {
            return Err(corrupt("duplicate or zero space in legacy v1 manifest"));
        }
        self.next_space_id = next_u32_after(definition.id, self.next_space_id);
        self.spaces.push(definition);
        Ok(())
    }

    fn create_policy(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let id = reader.u32()?;
        let name = reader.name()?;
        let hot_span_us = required_span(reader.i64()?)?;
        let retention_us = optional_span(reader.i64()?);
        let max_lateness_us = optional_span(reader.i64()?);
        let partition_span_us = required_span(reader.i64()?)?;
        let hot_compression = reader.profile()?;
        let cold_compression = reader.profile()?;
        reader.finish()?;
        if id == 0
            || self
                .policies
                .iter()
                .any(|item| item.id == id || item.name == name)
        {
            return Err(corrupt("duplicate or zero policy in legacy v1 manifest"));
        }
        let policy = StoragePolicy {
            id,
            name,
            hot_span_us,
            retention_us,
            aggregate_retention_us: None,
            retained_aggregate_resolutions: Vec::new(),
            write_mode: crate::catalog::WriteMode::Mutable,
            max_lateness_us,
            max_future_skew_us: None,
            partition_span_us,
            hot_compression,
            cold_compression,
        };
        self.next_policy_id = next_u32_after(id, self.next_policy_id);
        self.policies.push(policy);
        Ok(())
    }

    fn create_bucket(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let definition = BucketDefinition {
            id: reader.u32()?,
            space_id: reader.u32()?,
            policy_id: reader.u32()?,
            created_at_us: reader.i64()?,
            name: reader.name()?,
        };
        reader.finish()?;
        if definition.id == 0
            || !self
                .spaces
                .iter()
                .any(|item| item.id == definition.space_id)
            || !self
                .policies
                .iter()
                .any(|item| item.id == definition.policy_id)
            || self.buckets.iter().any(|item| {
                item.id == definition.id
                    || (item.space_id == definition.space_id && item.name == definition.name)
            })
        {
            return Err(corrupt("invalid or duplicate bucket in legacy v1 manifest"));
        }
        self.next_bucket_id = next_u32_after(definition.id, self.next_bucket_id);
        self.buckets.push(definition);
        Ok(())
    }

    fn add_wal(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let wal_id = reader.u64()?;
        let first_commit_id = reader.u64()?;
        reader.finish()?;
        if wal_id == 0 || self.wals.contains_key(&wal_id) {
            return Err(corrupt("duplicate or zero WAL in legacy v1 manifest"));
        }
        self.wals.insert(
            wal_id,
            WalReference {
                wal_id,
                first_commit_id,
                last_commit_id: None,
            },
        );
        self.next_wal_id = wal_id
            .checked_add(1)
            .ok_or_else(|| corrupt("legacy v1 WAL ID space exhausted"))?;
        Ok(())
    }

    fn close_wal(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let wal_id = reader.u64()?;
        let last_commit_id = if reader.remaining() == 0 {
            0
        } else {
            reader.u64()?
        };
        reader.finish()?;
        let reference = self
            .wals
            .get(&wal_id)
            .copied()
            .ok_or_else(|| corrupt("legacy v1 manifest closes an unknown WAL"))?;
        if !reference.is_active() {
            return Err(corrupt("legacy v1 manifest closes an inactive WAL"));
        }
        if last_commit_id < reference.first_commit_id || last_commit_id <= self.checkpoint_commit_id
        {
            self.wals.remove(&wal_id);
        } else if let Some(reference) = self.wals.get_mut(&wal_id) {
            reference.last_commit_id = Some(last_commit_id);
        }
        Ok(())
    }

    fn add_segment(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let descriptor = reader.segment_descriptor()?;
        reader.finish()?;
        if self.segments.contains_key(&descriptor.segment_id) {
            return Err(corrupt("duplicate segment ID in legacy v1 manifest"));
        }
        self.next_segment_id = next_u64_after(descriptor.segment_id, self.next_segment_id)?;
        self.segments.insert(descriptor.segment_id, descriptor);
        Ok(())
    }

    fn set_checkpoint(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let checkpoint = reader.u64()?;
        reader.finish()?;
        if checkpoint < self.checkpoint_commit_id {
            return Err(corrupt("legacy v1 checkpoint moved backwards"));
        }
        self.checkpoint_commit_id = checkpoint;
        self.wals.retain(|_, reference| {
            reference
                .last_commit_id
                .is_none_or(|last| last > checkpoint)
        });
        Ok(())
    }

    fn set_next_ids(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let space = reader.u32()?;
        let policy = reader.u32()?;
        let bucket = reader.u32()?;
        let (segment, wal) = if reader.remaining() == 0 {
            (1, 1)
        } else {
            (reader.u64()?, reader.u64()?)
        };
        reader.finish()?;
        if space < self.next_space_id
            || policy < self.next_policy_id
            || bucket < self.next_bucket_id
            || segment < self.next_segment_id
            || wal < self.next_wal_id
            || space == 0
            || policy == 0
            || bucket == 0
            || segment == 0
            || wal == 0
        {
            return Err(corrupt(
                "legacy v1 next IDs moved backwards or reached zero",
            ));
        }
        self.next_space_id = space;
        self.next_policy_id = policy;
        self.next_bucket_id = bucket;
        self.next_segment_id = segment;
        self.next_wal_id = wal;
        Ok(())
    }

    fn compaction_commit(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let _commit_id = reader.u64()?;
        let input_count = reader.u32()? as usize;
        let output_count = reader.u32()? as usize;
        let minimum = input_count
            .checked_mul(8)
            .and_then(|size| {
                output_count
                    .checked_mul(72)
                    .and_then(|value| size.checked_add(value))
            })
            .ok_or_else(|| corrupt("legacy v1 compaction counts overflow"))?;
        let maximum = input_count
            .checked_mul(8)
            .and_then(|size| {
                output_count
                    .checked_mul(88)
                    .and_then(|value| size.checked_add(value))
            })
            .ok_or_else(|| corrupt("legacy v1 compaction counts overflow"))?;
        if input_count == 0 || !(minimum..=maximum).contains(&reader.remaining()) {
            return Err(corrupt("invalid legacy v1 compaction counts"));
        }
        let mut inputs = Vec::with_capacity(input_count);
        let mut input_ids = BTreeSet::new();
        for _ in 0..input_count {
            let id = reader.u64()?;
            if !input_ids.insert(id) || !self.segments.contains_key(&id) {
                return Err(corrupt("invalid legacy v1 compaction input"));
            }
            inputs.push(id);
        }
        let mut outputs = Vec::with_capacity(output_count);
        let mut output_ids = BTreeSet::new();
        for _ in 0..output_count {
            let descriptor = reader.segment_descriptor()?;
            if !output_ids.insert(descriptor.segment_id)
                || self.segments.contains_key(&descriptor.segment_id)
            {
                return Err(corrupt("invalid legacy v1 compaction output"));
            }
            outputs.push(descriptor);
        }
        reader.finish()?;
        if outputs.len() > 1 {
            return Err(corrupt(
                "legacy v1 compaction has multiple complete-domain outputs",
            ));
        }
        if let Some(output) = outputs.first() {
            let first = self
                .segments
                .get(&inputs[0])
                .ok_or_else(|| corrupt("missing legacy v1 compaction input"))?;
            let domain = (
                first.shard_id,
                first.policy_id,
                first.window_start_us,
                first.window_end_us,
            );
            if output.level == 0
                || outputs.iter().any(|item| {
                    (
                        item.shard_id,
                        item.policy_id,
                        item.window_start_us,
                        item.window_end_us,
                    ) != domain
                })
                || inputs.iter().any(|id| {
                    self.segments.get(id).is_none_or(|item| {
                        (
                            item.shard_id,
                            item.policy_id,
                            item.window_start_us,
                            item.window_end_us,
                        ) != domain
                    })
                })
            {
                return Err(corrupt("legacy v1 compaction crosses storage domains"));
            }
        }
        for input in inputs {
            self.segments.remove(&input);
        }
        for output in outputs {
            self.next_segment_id = next_u64_after(output.segment_id, self.next_segment_id)?;
            self.segments.insert(output.segment_id, output);
        }
        Ok(())
    }

    fn set_retention_cutoff(&mut self, payload: &[u8]) -> Result<()> {
        let mut reader = SliceReader::new(payload);
        let policy_id = reader.u32()?;
        let cutoff = reader.i64()?;
        reader.finish()?;
        let policy = self
            .policies
            .iter()
            .find(|policy| policy.id == policy_id)
            .ok_or_else(|| corrupt("legacy v1 retention references an unknown policy"))?;
        if policy.retention_us.is_none()
            || self
                .retention_cutoffs
                .get(&policy_id)
                .is_some_and(|current| cutoff < *current)
        {
            return Err(corrupt("invalid legacy v1 retention cutoff"));
        }
        self.retention_cutoffs.insert(policy_id, cutoff);
        Ok(())
    }
}

fn read_identity(path: &Path) -> Result<Identity> {
    let bytes = fs::read(path)?;
    if bytes.len() != IDENTITY_SIZE
        || &bytes[..8] != IDENTITY_MAGIC
        || read_u16(&bytes, 8) != FORMAT_VERSION
        || read_u16(&bytes, 10) as usize != IDENTITY_SIZE
        || read_u16(&bytes, 28) != FORMAT_VERSION
        || read_u16(&bytes, 30) != FORMAT_VERSION
        || read_u16(&bytes, 34) != 0
        || bytes[44..60].iter().any(|byte| *byte != 0)
        || read_u32(&bytes, 60) != crc32c::crc32c(&bytes[..60])
    {
        return Err(corrupt("invalid legacy v1 IDENTITY"));
    }
    let virtual_shard_count = read_u16(&bytes, 32);
    if !(1..=4096).contains(&virtual_shard_count) || !virtual_shard_count.is_power_of_two() {
        return Err(corrupt("invalid legacy v1 virtual shard count"));
    }
    Ok(Identity {
        storage_uuid: bytes[12..28]
            .try_into()
            .expect("fixed-size identity UUID slice"),
        virtual_shard_count,
        created_at_us: read_i64(&bytes, 36),
    })
}

fn read_current(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    if bytes.is_empty()
        || bytes.len() > MAX_CURRENT_BYTES
        || bytes.last() != Some(&b'\n')
        || bytes.iter().filter(|byte| **byte == b'\n').count() != 1
    {
        return Err(corrupt(
            "legacy v1 CURRENT must contain one name and newline",
        ));
    }
    let name = std::str::from_utf8(&bytes[..bytes.len() - 1])
        .map_err(|_| corrupt("legacy v1 CURRENT is not UTF-8"))?
        .to_owned();
    parse_manifest_name(&name)?;
    Ok(name)
}

fn parse_manifest_name(name: &str) -> Result<u64> {
    let digits = name
        .strip_prefix("MANIFEST-")
        .and_then(|rest| rest.strip_suffix(".tsm"))
        .filter(|digits| digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| corrupt("invalid legacy v1 manifest name"))?;
    digits
        .parse()
        .map_err(|_| corrupt("invalid legacy v1 manifest ID"))
}

fn replay_manifest(path: &Path, identity: Identity, manifest_id: u64) -> Result<ReplayState> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    if file_size < MANIFEST_HEADER_SIZE as u64 {
        return Err(corrupt("legacy v1 manifest is shorter than its header"));
    }
    let mut header = [0_u8; MANIFEST_HEADER_SIZE];
    file.read_exact(&mut header)?;
    if &header[..8] != MANIFEST_MAGIC
        || read_u16(&header, 8) != FORMAT_VERSION
        || read_u16(&header, 10) as usize != MANIFEST_HEADER_SIZE
        || header[44..60].iter().any(|byte| *byte != 0)
        || read_u32(&header, 60) != crc32c::crc32c(&header[..60])
        || header[12..28] != identity.storage_uuid
        || read_u64(&header, 28) != manifest_id
    {
        return Err(corrupt("invalid legacy v1 manifest header"));
    }
    let mut state = ReplayState::new();
    let mut offset = MANIFEST_HEADER_SIZE as u64;
    while offset < file_size {
        let remaining = file_size - offset;
        if remaining < RECORD_HEADER_SIZE as u64 {
            return Err(corrupt("torn legacy v1 manifest record header"));
        }
        let mut record = [0_u8; RECORD_HEADER_SIZE];
        file.read_exact(&mut record)?;
        if read_u32(&record, 16) != crc32c::crc32c(&record[..16]) {
            return Err(corrupt("legacy v1 manifest record header CRC mismatch"));
        }
        let total_size = read_u32(&record, 0) as usize;
        let edit_type = record[4];
        let version = record[5];
        let flags = read_u16(&record, 6);
        let payload_size = read_u32(&record, 8) as usize;
        if version != FORMAT_VERSION as u8
            || flags != 0
            || payload_size > MAX_PAYLOAD_BYTES
            || total_size != RECORD_HEADER_SIZE.saturating_add(payload_size)
        {
            return Err(corrupt("invalid legacy v1 manifest record header"));
        }
        if remaining < total_size as u64 {
            return Err(corrupt("torn legacy v1 manifest record payload"));
        }
        let mut payload = vec![0_u8; payload_size];
        file.read_exact(&mut payload)?;
        if crc32c::crc32c(&payload) != read_u32(&record, 12) {
            return Err(corrupt("legacy v1 manifest record payload CRC mismatch"));
        }
        state.apply(edit_type, &payload)?;
        offset = offset
            .checked_add(total_size as u64)
            .ok_or_else(|| corrupt("legacy v1 manifest offset overflow"))?;
    }
    Ok(state)
}

fn validate_physical_regions<'a>(
    root: &Path,
    descriptors: impl Iterator<Item = &'a SegmentDescriptor>,
) -> Result<()> {
    let mut regions = BTreeMap::<PathBuf, Vec<(u64, u64)>>::new();
    for descriptor in descriptors {
        let path = root.join(descriptor.relative_path());
        let physical_size = fs::metadata(&path)?.len();
        let end = descriptor
            .file_offset
            .checked_add(descriptor.file_size)
            .ok_or_else(|| corrupt("legacy v1 physical region overflow"))?;
        if end > physical_size
            || (descriptor.pack_id.is_none() && physical_size != descriptor.file_size)
        {
            return Err(corrupt("legacy v1 segment is outside its physical file"));
        }
        regions
            .entry(path)
            .or_default()
            .push((descriptor.file_offset, end));
    }
    for entries in regions.values_mut() {
        entries.sort_unstable();
        if entries.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(corrupt("legacy v1 logical segments overlap in a pack"));
        }
    }
    Ok(())
}

struct SliceReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt("truncated legacy v1 manifest payload"))?;
        let result = &self.bytes[self.position..end];
        self.position = end;
        Ok(result)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two-byte slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ))
    }

    fn name(&mut self) -> Result<String> {
        let length = self.u16()? as usize;
        if !(1..=128).contains(&length) {
            return Err(corrupt("invalid legacy v1 catalog name length"));
        }
        let name = std::str::from_utf8(self.take(length)?)
            .map_err(|_| corrupt("legacy v1 catalog name is not UTF-8"))?;
        if name.chars().any(char::is_control) {
            return Err(corrupt(
                "legacy v1 catalog name contains control characters",
            ));
        }
        Ok(name.to_owned())
    }

    fn profile(&mut self) -> Result<CompressionProfile> {
        let timestamp_codec = match self.u8()? {
            0 => TimestampCodec::RawInt64,
            1 => TimestampCodec::DeltaVarint,
            2 => TimestampCodec::DeltaOfDelta,
            _ => return Err(corrupt("unsupported legacy v1 timestamp codec")),
        };
        let value_codec = match self.u8()? {
            0 => ValueCodec::RawFloat64,
            1 => ValueCodec::GorillaXor,
            _ => return Err(corrupt("unsupported legacy v1 value codec")),
        };
        let block_codec = match self.u8()? {
            0 => BlockCodec::None,
            1 => BlockCodec::Gzip,
            2 => BlockCodec::Zstd,
            _ => return Err(corrupt("unsupported legacy v1 block codec")),
        };
        if self.u8()? != 0 {
            return Err(corrupt("unknown legacy v1 compression profile flags"));
        }
        let raw_level = self.u32()? as i32;
        let block_level = if raw_level == NIL_LEVEL {
            None
        } else {
            Some(
                u8::try_from(raw_level)
                    .map_err(|_| corrupt("invalid legacy v1 compression level"))?,
            )
        };
        CompressionProfile::new(timestamp_codec, value_codec, block_codec, block_level)
            .map_err(|error| corrupt(format!("invalid legacy v1 compression profile: {error}")))
    }

    fn segment_descriptor(&mut self) -> Result<SegmentDescriptor> {
        let segment_id = self.u64()?;
        let shard_id = self.u16()?;
        let policy_id = self.u32()?;
        let level = self.u8()?;
        let flags = self.u8()?;
        if !matches!(flags, 0 | 1) {
            return Err(corrupt("unknown legacy v1 segment descriptor flags"));
        }
        let descriptor = SegmentDescriptor {
            segment_id,
            shard_id,
            policy_id,
            level,
            window_start_us: self.i64()?,
            window_end_us: self.i64()?,
            min_timestamp_us: self.i64()?,
            max_timestamp_us: self.i64()?,
            min_commit_id: self.u64()?,
            max_commit_id: self.u64()?,
            file_size: u64::try_from(self.i64()?)
                .map_err(|_| corrupt("negative legacy v1 segment size"))?,
            pack_id: if flags == 1 { Some(self.u64()?) } else { None },
            file_offset: if flags == 1 {
                u64::try_from(self.i64()?)
                    .map_err(|_| corrupt("negative legacy v1 segment offset"))?
            } else {
                0
            },
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn finish(self) -> Result<()> {
        if self.position != self.bytes.len() {
            return Err(corrupt("trailing bytes in legacy v1 manifest payload"));
        }
        Ok(())
    }
}

fn required_span(value: i64) -> Result<i64> {
    if value == NIL_SPAN {
        Err(corrupt("required legacy v1 policy span uses nil sentinel"))
    } else {
        Ok(value)
    }
}

const fn optional_span(value: i64) -> Option<i64> {
    if value == NIL_SPAN { None } else { Some(value) }
}

fn next_u32_after(id: u32, current: u32) -> u32 {
    current.max(id.saturating_add(1))
}

fn next_u64_after(id: u64, current: u64) -> Result<u64> {
    let candidate = id
        .checked_add(1)
        .ok_or_else(|| corrupt("legacy v1 segment ID space exhausted"))?;
    Ok(current.max(candidate))
}
