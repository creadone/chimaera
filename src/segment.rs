//! Сегменты: описатели, открытие и общий фасад файлового чтения.

use crate::aggregate;
use crate::aggregate::Cursor as AggregateCursor;
use crate::cache::{BlockCache, MemoryClass, MemoryReservation};
use crate::catalog::CompressionProfile;
use crate::error::{Result, corrupt};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::layout::Domain;
use crate::query::{QueryBudget, QueryMemoryClass, QueryMetadataKind};
use crate::types::{AggregateResolution, AggregateState, SeriesKey, VersionedPoint};
use crate::wal::{put_i64, put_u32, put_u64, read_i64, read_u32, read_u64, sync_directory};
use index::validate_descriptor;
use metadata::{
    QueryMetadataResources, read_paged_metadata_for_query, read_segment_aggregates,
    read_segment_metadata, read_segment_signature,
};
use metadata_cache::MetadataCacheKey;
use segment_cursor::{SegmentCursor, TimestampFilter};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use pack_directory::OfflinePackDirectoryInfo;
pub(crate) use pack_directory::{DirectoryPagePool, OfflinePackDirectory, segments_crc32c};

mod codecs;
mod cursor;
mod index;
mod merge;
mod metadata;
mod metadata_cache;
mod pack;
mod read_plan;
mod segment_cursor;
#[cfg(test)]
mod tests;
pub(crate) use cursor::OperationCursor;
pub(crate) use cursor::PointBatch;
pub(crate) use cursor::RangedSliceCursor;
pub(crate) use cursor::SegmentReadMetrics;
pub(crate) use cursor::SliceCursor;
pub(crate) use merge::VisibleCursor;
pub(crate) use metadata_cache::MetadataCache;
pub(crate) use pack::SegmentPackResources;
pub(crate) use pack::SegmentWriteRequest;
pub(crate) use pack::SegmentWriteResult;
pub(crate) use pack::{prepare_segment_pack, sync_segment_pack_group};
pub(crate) use read_plan::QueryPlanReservation;
pub(crate) use read_plan::QueryReadPlan;
pub(crate) use read_plan::QuerySegmentRead;

mod pack_directory;

const SEGMENT_MAGIC: &[u8; 8] = b"TSSEG001";
const INDEX_MAGIC: &[u8; 8] = b"TSIDX001";
const PAGED_INDEX_MAGIC: &[u8; 8] = b"TSIDX002";
const INDEX_PAGE_MAGIC: &[u8; 8] = b"TSIPG001";
const FOOTER_MAGIC: &[u8; 8] = b"TSFTR001";
const BLOCK_MAGIC: &[u8; 4] = b"TSBL";
const HEADER_SIZE: usize = 112;
const BLOCK_HEADER_SIZE: usize = 88;
const INDEX_HEADER_SIZE: usize = 16;
const LEGACY_INDEX_ENTRY_SIZE: usize = 44;
const INDEX_ENTRY_SIZE: usize = 72;
const INDEX_FLAG_COUNT_SUM_UNAVAILABLE: u32 = 1;
const PAGED_INDEX_HEADER_SIZE: usize = 32;
const INDEX_PAGE_DIRECTORY_SIZE: usize = 56;
const INDEX_PAGE_HEADER_SIZE: usize = 16;
const TARGET_INDEX_PAGE_BLOCKS: usize = 8;
const INDEX_ROOT_STREAM_BUFFER_SIZE: usize = 4 * 1024;
const FOOTER_SIZE: usize = 64;
const TARGET_BLOCK_POINTS: usize = 4_096;
const MAX_INDEX_BYTES: usize = 256 * 1024 * 1024;
const MAX_STORED_BLOCK_OVERHEAD: usize = 64 * 1024;

fn duration_nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SegmentDescriptor {
    pub id: u64,
    pub level: u8,
    pub shard_id: u16,
    pub policy_id: u32,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
    pub relative_path: String,
    #[serde(default)]
    pub pack_id: Option<u64>,
    #[serde(default)]
    pub file_offset: u64,
    pub min_commit: u64,
    pub max_commit: u64,
    pub record_count: u64,
    pub file_size: u64,
    /// Число поколений compaction, через которые прошла эта логическая часть.
    /// Поле живёт только в manifest; двоичный формат segment v1 не меняется.
    #[serde(default)]
    pub rewrite_count: u32,
}

impl SegmentDescriptor {
    pub(crate) const fn domain(&self) -> Domain {
        Domain {
            shard_id: self.shard_id,
            policy_id: self.policy_id,
            window_start_us: self.window_start_us,
            window_end_us: self.window_end_us,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BlockIndex {
    series: SeriesKey,
    offset: u64,
    size: u32,
    min_timestamp_us: i64,
    max_timestamp_us: i64,
    count_sum: Option<CountSumSummary>,
}

#[derive(Clone, Copy, Debug)]
struct CountSumSummary {
    count: u64,
    raw_sum: f64,
    compensation: f64,
}

fn read_count_sum_summary(summary: CountSumSummary) -> Result<AggregateState> {
    AggregateState::from_count_sum_parts(summary.count, summary.raw_sum, summary.compensation)
}

#[derive(Clone, Copy, Debug)]
struct IndexPageDescriptor {
    first_series: SeriesKey,
    last_series: SeriesKey,
    offset: u64,
    length: u32,
    block_count: u32,
    crc32c: u32,
}

#[derive(Debug)]
struct PagedIndexRoot {
    pages: Box<[IndexPageDescriptor]>,
    block_count: u32,
    membership: MembershipFilter,
}

impl PagedIndexRoot {
    fn bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64)
            .saturating_add(
                u64::try_from(self.pages.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<IndexPageDescriptor>() as u64),
            )
            .saturating_add(self.membership.bytes())
    }
}

#[derive(Debug)]
struct MembershipFilter {
    words: Box<[u64]>,
    bit_mask: usize,
}

impl MembershipFilter {
    fn bytes_for_series(series_count: usize) -> Result<u64> {
        let bit_count = series_count
            .saturating_mul(10)
            .max(64)
            .checked_next_power_of_two()
            .ok_or_else(|| corrupt("segment membership filter size overflow"))?;
        Ok(u64::try_from(bit_count.div_ceil(64))
            .unwrap_or(u64::MAX)
            .saturating_mul(std::mem::size_of::<u64>() as u64))
    }

    fn from_index(index: &[BlockIndex]) -> Self {
        let series_count = index
            .iter()
            .enumerate()
            .filter(|(position, entry)| {
                *position == 0 || index[*position - 1].series != entry.series
            })
            .count();
        let bit_count = series_count.saturating_mul(10).max(64).next_power_of_two();
        let mut filter = Self {
            words: vec![0; bit_count.div_ceil(64)].into_boxed_slice(),
            bit_mask: bit_count - 1,
        };
        let mut previous = None;
        for entry in index {
            if previous == Some(entry.series) {
                continue;
            }
            filter.insert(entry.series);
            previous = Some(entry.series);
        }
        filter
    }

    fn from_words(words: Box<[u64]>) -> Result<Self> {
        if words.is_empty() || !words.len().is_power_of_two() {
            return Err(corrupt(
                "segment membership filter word count is not a power of two",
            ));
        }
        let bit_count = words
            .len()
            .checked_mul(u64::BITS as usize)
            .ok_or_else(|| corrupt("segment membership filter size overflows"))?;
        Ok(Self {
            words,
            bit_mask: bit_count - 1,
        })
    }

    fn insert(&mut self, series: SeriesKey) {
        for bit in membership_bits(series, self.bit_mask) {
            self.words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    fn contains(&self, series: SeriesKey) -> bool {
        membership_bits(series, self.bit_mask)
            .into_iter()
            .all(|bit| self.words[bit / 64] & (1_u64 << (bit % 64)) != 0)
    }

    fn bytes(&self) -> u64 {
        u64::try_from(self.words.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(std::mem::size_of::<u64>() as u64)
    }
}

fn membership_bits(series: SeriesKey, bit_mask: usize) -> [usize; 3] {
    let catalog = (u64::from(series.space_id) << 32) | u64::from(series.bucket_id);
    let first = mix64(catalog ^ series.key);
    let second = mix64(series.key.rotate_left(29) ^ catalog).max(1);
    [
        first as usize & bit_mask,
        first.wrapping_add(second) as usize & bit_mask,
        first.wrapping_add(second.wrapping_mul(2)) as usize & bit_mask,
    ]
}

fn mix64(input: u64) -> u64 {
    let mut value = input.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug)]
struct SegmentMetadata {
    index: Box<[BlockIndex]>,
    membership: MembershipFilter,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl SegmentMetadata {
    fn new(index: Vec<BlockIndex>) -> Self {
        let membership = MembershipFilter::from_index(&index);
        Self {
            index: index.into_boxed_slice(),
            membership,
            _reservation: None,
        }
    }

    fn bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64)
            .saturating_add(
                u64::try_from(self.index.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<BlockIndex>() as u64),
            )
            .saturating_add(self.membership.bytes())
    }

    fn estimated_bytes(block_count: usize, series_count: usize) -> Result<u64> {
        Ok((std::mem::size_of::<Self>() as u64)
            .saturating_add(
                u64::try_from(block_count)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<BlockIndex>() as u64),
            )
            .saturating_add(MembershipFilter::bytes_for_series(series_count)?))
    }
}

#[derive(Debug)]
pub(crate) struct Segment {
    descriptor: SegmentDescriptor,
    path: PathBuf,
    storage_signature: Option<[u8; 4]>,
    block_cache: Arc<BlockCache>,
    metadata_cache: Arc<MetadataCache>,
    metadata_load: Mutex<()>,
    file_pool: FilePermitPool,
    open_file_cache: Option<Arc<OpenFileCache>>,
    storage_uuid: [u8; 16],
    index_root: Option<PagedIndexRoot>,
    index_offset: u64,
    data_end: u64,
    _index_root_reservation: Option<MemoryReservation>,
}

#[derive(Clone, Copy)]
struct FooterMetadata {
    version: u16,
    index_offset: u64,
    index_length: u64,
    index_crc: u32,
    aggregate_length: u32,
    aggregate_crc: u32,
}

struct EncodedRegion {
    descriptor: SegmentDescriptor,
    index: Vec<BlockIndex>,
    index_root: PagedIndexRoot,
    index_offset: u64,
    data_end: u64,
    aggregate: Option<Vec<u8>>,
    storage_signature: [u8; 4],
}

struct OpenedSegmentIndex {
    storage_signature: Option<[u8; 4]>,
    root: Option<PagedIndexRoot>,
    index_offset: u64,
    data_end: u64,
    reservation: Option<MemoryReservation>,
}

impl Segment {
    fn from_written(
        root: &Path,
        encoded: EncodedRegion,
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        file_pool: FilePermitPool,
        open_file_cache: Option<Arc<OpenFileCache>>,
        storage_uuid: [u8; 16],
    ) -> Result<Arc<Self>> {
        let EncodedRegion {
            descriptor,
            index,
            index_root,
            index_offset,
            data_end,
            aggregate,
            storage_signature,
        } = encoded;
        let path = root.join(&descriptor.relative_path);
        let index_root_reservation = metadata_cache
            .governor
            .try_reserve_token(MemoryClass::MetadataCache, index_root.bytes());
        let index_root = index_root_reservation.as_ref().map(|_| index_root);
        let metadata = Arc::new(SegmentMetadata::new(index));
        let aggregate = Arc::new(aggregate::Section::new(aggregate));
        let _ = metadata_cache.insert(descriptor.id, metadata)?;
        let _ = metadata_cache.insert_aggregate(descriptor.id, aggregate)?;
        Ok(Arc::new(Self {
            descriptor,
            path,
            storage_signature: Some(storage_signature),
            block_cache,
            metadata_cache,
            metadata_load: Mutex::new(()),
            file_pool,
            open_file_cache,
            storage_uuid,
            index_root,
            index_offset,
            data_end,
            _index_root_reservation: index_root_reservation,
        }))
    }

    pub fn open(
        root: &Path,
        storage_uuid: [u8; 16],
        descriptor: SegmentDescriptor,
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        file_pool: FilePermitPool,
    ) -> Result<Arc<Self>> {
        Self::open_inner(
            root,
            storage_uuid,
            descriptor,
            block_cache,
            metadata_cache,
            file_pool,
            None,
            true,
        )
    }

    pub(crate) fn open_with_file_cache(
        root: &Path,
        storage_uuid: [u8; 16],
        descriptor: SegmentDescriptor,
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        file_pool: FilePermitPool,
        open_file_cache: Arc<OpenFileCache>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(
            root,
            storage_uuid,
            descriptor,
            block_cache,
            metadata_cache,
            file_pool,
            Some(open_file_cache),
            true,
        )
    }

    pub(crate) fn open_with_file_cache_without_resident_index(
        root: &Path,
        storage_uuid: [u8; 16],
        descriptor: SegmentDescriptor,
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        file_pool: FilePermitPool,
        open_file_cache: Arc<OpenFileCache>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(
            root,
            storage_uuid,
            descriptor,
            block_cache,
            metadata_cache,
            file_pool,
            Some(open_file_cache),
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_inner(
        root: &Path,
        storage_uuid: [u8; 16],
        descriptor: SegmentDescriptor,
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        file_pool: FilePermitPool,
        open_file_cache: Option<Arc<OpenFileCache>>,
        load_resident_index: bool,
    ) -> Result<Arc<Self>> {
        validate_descriptor(&descriptor)?;
        let path = root.join(&descriptor.relative_path);
        let opened_index = read_segment_signature(
            &path,
            storage_uuid,
            &descriptor,
            &file_pool,
            &metadata_cache.governor,
            load_resident_index,
        )?;
        Ok(Arc::new(Self {
            descriptor,
            path,
            storage_signature: opened_index.storage_signature,
            block_cache,
            metadata_cache,
            metadata_load: Mutex::new(()),
            file_pool,
            open_file_cache,
            storage_uuid,
            index_root: opened_index.root,
            index_offset: opened_index.index_offset,
            data_end: opened_index.data_end,
            _index_root_reservation: opened_index.reservation,
        }))
    }

    fn metadata(&self) -> Result<Arc<SegmentMetadata>> {
        if let Some(metadata) = self.metadata_cache.get(self.descriptor.id)? {
            return Ok(metadata);
        }
        let _load = self
            .metadata_load
            .lock()
            .map_err(|_| corrupt("segment metadata load mutex is poisoned"))?;
        if let Some(metadata) = self.metadata_cache.get(self.descriptor.id)? {
            return Ok(metadata);
        }
        let loaded = read_segment_metadata(
            &self.path,
            self.storage_uuid,
            &self.descriptor,
            &self.file_pool,
            None,
            true,
        )?;
        let metadata = self
            .metadata_cache
            .insert(self.descriptor.id, loaded.metadata)?;
        let _ = self
            .metadata_cache
            .insert_aggregate(self.descriptor.id, loaded.aggregate)?;
        Ok(metadata)
    }

    fn metadata_for_query(&self, query_budget: &QueryBudget) -> Result<Arc<SegmentMetadata>> {
        if let Some(metadata) =
            query_budget.local_metadata(self.descriptor.id, QueryMetadataKind::Segment)?
        {
            return Ok(metadata);
        }
        if let Some(metadata) = self.metadata_cache.get(self.descriptor.id)? {
            let bytes = metadata.bytes();
            return query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Segment,
                metadata,
                bytes,
                0,
            );
        }
        query_budget.check()?;
        let _load = crate::query::lock_for_query(&self.metadata_load, query_budget)?;
        query_budget.check()?;
        if let Some(metadata) =
            query_budget.local_metadata(self.descriptor.id, QueryMetadataKind::Segment)?
        {
            return Ok(metadata);
        }
        if let Some(metadata) = self.metadata_cache.get(self.descriptor.id)? {
            let bytes = metadata.bytes();
            return query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Segment,
                metadata,
                bytes,
                0,
            );
        }
        let load_started = Instant::now();
        let loaded = read_segment_metadata(
            &self.path,
            self.storage_uuid,
            &self.descriptor,
            &self.file_pool,
            Some(query_budget),
            false,
        )?;
        query_budget.record_metadata(load_started.elapsed());
        query_budget.record_file_open();
        query_budget.record_metadata_bytes(loaded.bytes_read);
        let metadata_bytes = loaded.metadata.bytes();
        let aggregate_bytes = loaded.aggregate.cache_bytes();
        let (metadata, metadata_cached) = self
            .metadata_cache
            .insert_with_status(self.descriptor.id, loaded.metadata)?;
        if metadata_cached {
            query_budget.release_memory_class(QueryMemoryClass::Metadata, loaded.metadata_reserved);
        }
        let (aggregate, aggregate_cached) = self
            .metadata_cache
            .insert_aggregate_with_status(self.descriptor.id, loaded.aggregate)?;
        if aggregate_cached {
            query_budget
                .release_memory_class(QueryMemoryClass::Metadata, loaded.aggregate_reserved);
        }
        let _ = query_budget.keep_local_metadata(
            self.descriptor.id,
            QueryMetadataKind::Aggregate,
            aggregate,
            aggregate_bytes,
            if aggregate_cached {
                0
            } else {
                loaded.aggregate_reserved
            },
        )?;
        query_budget.keep_local_metadata(
            self.descriptor.id,
            QueryMetadataKind::Segment,
            metadata,
            metadata_bytes,
            if metadata_cached {
                0
            } else {
                loaded.metadata_reserved
            },
        )
    }

    fn metadata_for_query_series(
        &self,
        series: &[SeriesKey],
        query_budget: &QueryBudget,
    ) -> Result<Arc<SegmentMetadata>> {
        if let Some(metadata) = query_budget.local_series_metadata(
            self.descriptor.id,
            QueryMetadataKind::Segment,
            series,
        )? {
            return Ok(metadata);
        }
        if let Some(metadata) = self.metadata_cache.get(self.descriptor.id)? {
            let bytes = metadata.bytes();
            return query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Segment,
                metadata,
                bytes,
                0,
            );
        }
        let load_started = Instant::now();
        let Some(loaded) = read_paged_metadata_for_query(
            self.storage_uuid,
            &self.descriptor,
            self.index_root.as_ref(),
            self.index_offset,
            self.data_end,
            series,
            &self.metadata_cache,
            QueryMetadataResources {
                path: &self.path,
                file_pool: &self.file_pool,
                open_file_cache: self.open_file_cache.as_deref(),
                query_budget,
            },
        )?
        else {
            return self.metadata_for_query(query_budget);
        };
        query_budget.record_metadata(load_started.elapsed());
        query_budget.record_metadata_bytes(loaded.bytes_read);
        let bytes = loaded.metadata.bytes();
        query_budget.keep_local_series_metadata(
            self.descriptor.id,
            QueryMetadataKind::Segment,
            series.to_vec(),
            loaded.metadata,
            bytes,
            loaded.metadata_reserved,
        )
    }

    fn aggregate_for_query(
        &self,
        series: SeriesKey,
        resolution: AggregateResolution,
        query_budget: &QueryBudget,
    ) -> Result<Arc<aggregate::Section>> {
        self.aggregates_for_query(&[series], resolution, query_budget)
    }

    pub(crate) fn prepare_aggregates_for_query(
        &self,
        series: &[SeriesKey],
        resolution: AggregateResolution,
        query_budget: &QueryBudget,
    ) -> Result<()> {
        self.aggregates_for_query(series, resolution, query_budget)
            .map(drop)
    }

    fn aggregates_for_query(
        &self,
        series: &[SeriesKey],
        resolution: AggregateResolution,
        query_budget: &QueryBudget,
    ) -> Result<Arc<aggregate::Section>> {
        let selected_key = match series {
            [series] => Some(MetadataCacheKey::AggregateSeries(
                self.descriptor.id,
                *series,
                resolution,
            )),
            _ => None,
        };
        if let Some(aggregate) = query_budget.local_series_metadata(
            self.descriptor.id,
            QueryMetadataKind::Aggregate,
            series,
        )? {
            return Ok(aggregate);
        }
        if let Some(aggregate) = self.metadata_cache.get_aggregate(self.descriptor.id)? {
            let bytes = aggregate.cache_bytes();
            return query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                aggregate,
                bytes,
                0,
            );
        }
        if let Some(key) = selected_key
            && let Some(aggregate) = self.metadata_cache.get_aggregate_entry(key)?
        {
            let bytes = aggregate.cache_bytes();
            return query_budget.keep_local_series_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                series.to_vec(),
                aggregate,
                bytes,
                0,
            );
        }
        query_budget.check()?;
        let _load = crate::query::lock_for_query(&self.metadata_load, query_budget)?;
        query_budget.check()?;
        if let Some(aggregate) = query_budget.local_series_metadata(
            self.descriptor.id,
            QueryMetadataKind::Aggregate,
            series,
        )? {
            return Ok(aggregate);
        }
        if let Some(aggregate) = self.metadata_cache.get_aggregate(self.descriptor.id)? {
            let bytes = aggregate.cache_bytes();
            return query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                aggregate,
                bytes,
                0,
            );
        }
        if let Some(key) = selected_key
            && let Some(aggregate) = self.metadata_cache.get_aggregate_entry(key)?
        {
            let bytes = aggregate.cache_bytes();
            return query_budget.keep_local_series_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                series.to_vec(),
                aggregate,
                bytes,
                0,
            );
        }
        let load_started = Instant::now();
        let (aggregate, bytes_read, reserved_bytes, cacheable) = read_segment_aggregates(
            self.storage_uuid,
            &self.descriptor,
            series,
            resolution,
            QueryMetadataResources {
                path: &self.path,
                file_pool: &self.file_pool,
                open_file_cache: self.open_file_cache.as_deref(),
                query_budget,
            },
        )?;
        query_budget.record_metadata(load_started.elapsed());
        query_budget.record_metadata_bytes(bytes_read);
        let bytes = aggregate.cache_bytes();
        if cacheable {
            let (aggregate, cached) = self
                .metadata_cache
                .insert_aggregate_with_status(self.descriptor.id, aggregate)?;
            if cached {
                query_budget.release_memory_class(QueryMemoryClass::Metadata, reserved_bytes);
            }
            query_budget.keep_local_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                aggregate,
                bytes,
                if cached { 0 } else { reserved_bytes },
            )
        } else {
            let (aggregate, cached) = if let Some(key) = selected_key {
                self.metadata_cache.insert_aggregate_entry(key, aggregate)?
            } else {
                (aggregate, false)
            };
            if cached {
                query_budget.release_memory_class(QueryMemoryClass::Metadata, reserved_bytes);
            }
            query_budget.keep_local_series_metadata(
                self.descriptor.id,
                QueryMetadataKind::Aggregate,
                series.to_vec(),
                aggregate,
                bytes,
                if cached { 0 } else { reserved_bytes },
            )
        }
    }

    #[must_use]
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    pub(crate) fn may_contain_any_series(&self, series: &[SeriesKey]) -> bool {
        series.iter().any(|series| {
            self.index_root
                .as_ref()
                .is_none_or(|root| root.membership.contains(*series))
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn series_keys(&self) -> Result<Vec<SeriesKey>> {
        let metadata = self.metadata()?;
        let mut series = Vec::new();
        for block in &metadata.index {
            if series.last() != Some(&block.series) {
                series.push(block.series);
            }
        }
        Ok(series)
    }

    pub(crate) fn visit_series_max_timestamps(
        &self,
        mut visit: impl FnMut(SeriesKey, i64) -> Result<()>,
    ) -> Result<()> {
        let metadata = self.metadata()?;
        let mut current = None::<(SeriesKey, i64)>;
        for block in &metadata.index {
            if let Some((series, maximum)) = current.as_mut()
                && *series == block.series
            {
                *maximum = (*maximum).max(block.max_timestamp_us);
            } else {
                if let Some((series, maximum)) =
                    current.replace((block.series, block.max_timestamp_us))
                {
                    visit(series, maximum)?;
                }
            }
        }
        if let Some((series, maximum)) = current {
            visit(series, maximum)?;
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn uses_profile(&self, profile: CompressionProfile) -> bool {
        self.storage_signature == Some(profile.storage_signature())
    }

    pub fn cursor(self: &Arc<Self>) -> Result<Box<dyn OperationCursor>> {
        let metadata = self.metadata()?;
        let blocks = (0..metadata.index.len()).collect();
        Ok(Box::new(SegmentCursor::new_indexed(
            Arc::clone(self),
            metadata,
            blocks,
            TimestampFilter::All,
            false,
            false,
            None,
            None,
        )?))
    }

    pub(crate) fn compaction_time_cursor(
        self: &Arc<Self>,
        from_us: i64,
        to_us: i64,
        metrics: Arc<SegmentReadMetrics>,
    ) -> Result<Option<Box<dyn OperationCursor>>> {
        let metadata = self.metadata()?;
        let blocks = metadata
            .index
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                (entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(SegmentCursor::new_indexed(
            Arc::clone(self),
            metadata,
            blocks,
            TimestampFilter::HalfOpen { from_us, to_us },
            false,
            false,
            None,
            Some(metrics),
        )?)))
    }

    pub(crate) fn verify_operations(
        self: &Arc<Self>,
        mut validate: impl FnMut(VersionedPoint) -> Result<()>,
    ) -> Result<u64> {
        let mut cursor = self.cursor()?;
        let mut previous = None;
        let mut count = 0_u64;
        while let Some(operation) = cursor.next_operation()? {
            if self
                .index_root
                .as_ref()
                .is_some_and(|root| !root.membership.contains(operation.key.series))
            {
                return Err(corrupt("segment membership filter excludes stored data"));
            }
            if previous.is_some_and(|previous| previous > operation) {
                return Err(corrupt("segment operations are not ordered"));
            }
            validate(operation)?;
            previous = Some(operation);
            count = count
                .checked_add(1)
                .ok_or_else(|| corrupt("segment operation count overflows"))?;
        }
        if count != self.descriptor.record_count {
            return Err(corrupt("decoded segment count does not match descriptor"));
        }
        Ok(count)
    }

    pub(crate) fn plan_query_series(
        self: &Arc<Self>,
        series_ranges: &[(SeriesKey, i64)],
        to_us: i64,
        query_budget: &QueryBudget,
        plan_reservation: &mut QueryPlanReservation,
    ) -> Result<Vec<(SeriesKey, QuerySegmentRead)>> {
        query_budget.record_index_root_consulted();
        let requested_series = series_ranges
            .iter()
            .map(|(series, _)| *series)
            .collect::<Vec<_>>();
        let metadata = self.metadata_for_query_series(&requested_series, query_budget)?;
        let mut result = Vec::new();
        for (series, from_us) in series_ranges {
            query_budget.check()?;
            let Some((first, end)) = self.series_index_range(&metadata, *series) else {
                continue;
            };
            let matching_blocks = metadata.index[first..end]
                .iter()
                .filter(|entry| {
                    entry.max_timestamp_us >= *from_us && entry.min_timestamp_us < to_us
                })
                .count();
            if matching_blocks != 0 {
                plan_reservation
                    .reserve_blocks(u64::try_from(matching_blocks).unwrap_or(u64::MAX))?;
                let blocks = metadata.index[first..end]
                    .iter()
                    .copied()
                    .filter(|entry| {
                        entry.max_timestamp_us >= *from_us && entry.min_timestamp_us < to_us
                    })
                    .collect::<Vec<_>>();
                result.push((
                    *series,
                    QuerySegmentRead {
                        segment: Arc::clone(self),
                        blocks,
                    },
                ));
            }
        }
        Ok(result)
    }

    pub(crate) fn query_series_block_count(
        &self,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        query_budget: &QueryBudget,
    ) -> Result<usize> {
        query_budget.check()?;
        let metadata = self.metadata_for_query_series(&[series], query_budget)?;
        let Some((first, end)) = self.series_index_range(&metadata, series) else {
            return Ok(0);
        };
        Ok(metadata.index[first..end]
            .iter()
            .filter(|entry| entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us)
            .count())
    }

    pub(crate) fn series_cursor_for_query(
        self: &Arc<Self>,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        query_budget: Arc<QueryBudget>,
    ) -> Result<Option<Box<dyn OperationCursor>>> {
        query_budget.check()?;
        let metadata = self.metadata_for_query(&query_budget)?;
        let Some((first, end)) = self.series_index_range(&metadata, series) else {
            return Ok(None);
        };
        let blocks = (first..end)
            .filter(|index| {
                let entry = metadata.index[*index];
                entry.max_timestamp_us >= from_us && entry.min_timestamp_us < to_us
            })
            .collect::<Vec<_>>();
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(SegmentCursor::new_indexed(
            Arc::clone(self),
            metadata,
            blocks,
            TimestampFilter::HalfOpen { from_us, to_us },
            true,
            false,
            Some(query_budget),
            None,
        )?)))
    }

    pub(crate) fn point_cursor_for_query(
        self: &Arc<Self>,
        series: SeriesKey,
        timestamp_us: i64,
        query_budget: Arc<QueryBudget>,
    ) -> Result<Option<Box<dyn OperationCursor>>> {
        let metadata = self.metadata_for_query_series(&[series], &query_budget)?;
        let Some((first, end)) = self.series_index_range(&metadata, series) else {
            return Ok(None);
        };
        let blocks = (first..end)
            .filter(|index| {
                let entry = metadata.index[*index];
                entry.min_timestamp_us <= timestamp_us && entry.max_timestamp_us >= timestamp_us
            })
            .collect::<Vec<_>>();
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(SegmentCursor::new_indexed(
            Arc::clone(self),
            metadata,
            blocks,
            TimestampFilter::Exact(timestamp_us),
            true,
            false,
            Some(query_budget),
            None,
        )?)))
    }

    pub(crate) fn contains_series_namespace(&self, space_id: u32, bucket_id: u32) -> Result<bool> {
        let metadata = self.metadata()?;
        Ok(metadata
            .index
            .iter()
            .any(|entry| entry.series.space_id == space_id && entry.series.bucket_id == bucket_id))
    }

    pub(crate) fn aggregate_cursor_for_query(
        &self,
        series: SeriesKey,
        resolution: AggregateResolution,
        from_us: i64,
        to_us: i64,
        query_budget: &QueryBudget,
    ) -> Result<Option<AggregateCursor>> {
        let aggregate = self.aggregate_for_query(series, resolution, query_budget)?;
        if aggregate.is_empty() {
            return Ok(None);
        }
        AggregateCursor::for_series(aggregate, series, resolution, from_us, to_us)
    }

    fn series_index_range(
        &self,
        metadata: &SegmentMetadata,
        series: SeriesKey,
    ) -> Option<(usize, usize)> {
        if !metadata.membership.contains(series) {
            self.metadata_cache.record_membership(true, false);
            return None;
        }
        let first = metadata
            .index
            .partition_point(|entry| entry.series < series);
        let end = metadata
            .index
            .partition_point(|entry| entry.series <= series);
        if first == end {
            self.metadata_cache.record_membership(false, true);
            None
        } else {
            Some((first, end))
        }
    }
}
