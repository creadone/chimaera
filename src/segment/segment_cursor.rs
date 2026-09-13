//! Потоковое чтение блоков сегмента с ограничениями запроса и кэша.

use super::cursor::{OperationCursor, SegmentReadMetrics};
use super::read_plan::QueryReadPlan;
use super::{BlockIndex, Segment, SegmentMetadata, TARGET_BLOCK_POINTS};
use crate::cache::{
    BlockCacheAdmission, BlockKey, BlockLoadDecision, BlockLoadWaitOutcome, CachedBlock,
};
use crate::error::{Result, corrupt};
use crate::file_pool::{FilePermit, FilePermitPool};
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::{PointKey, VersionedPoint};
use crate::v1::{OperationKind, decode_block};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::Instant;

pub(super) struct SegmentCursor {
    pub(super) segment: Arc<Segment>,
    pub(super) file: Option<File>,
    pub(super) file_permit: Option<FilePermit>,
    pub(super) blocks: SegmentCursorBlocks,
    pub(super) next_block: usize,
    pub(super) compressed: Vec<u8>,
    pub(super) decoded: Arc<[VersionedPoint]>,
    pub(super) cached_decoded: Option<Arc<CachedBlock>>,
    pub(super) decoded_position: usize,
    pub(super) timestamp_filter: TimestampFilter,
    pub(super) cache_admission: Option<BlockCacheAdmission>,
    pub(super) keep_file_open: bool,
    pub(super) query_budget: Option<Arc<QueryBudget>>,
    pub(super) read_metrics: Option<Arc<SegmentReadMetrics>>,
    pub(super) read_plan: Option<Arc<QueryReadPlan>>,
    pub(super) reserved_compressed_bytes: u64,
    pub(super) reserved_decoded_bytes: u64,
}

pub(super) enum SegmentCursorBlocks {
    Indexed {
        metadata: Arc<SegmentMetadata>,
        indices: Vec<usize>,
    },
    Planned(Vec<BlockIndex>),
}

impl SegmentCursorBlocks {
    pub(super) fn get(&self, position: usize) -> Option<BlockIndex> {
        match self {
            Self::Indexed { metadata, indices } => indices
                .get(position)
                .copied()
                .map(|index| metadata.index[index]),
            Self::Planned(blocks) => blocks.get(position).copied(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TimestampFilter {
    All,
    HalfOpen { from_us: i64, to_us: i64 },
    Exact(i64),
}

impl TimestampFilter {
    pub(super) const fn contains(self, timestamp_us: i64) -> bool {
        match self {
            Self::All => true,
            Self::HalfOpen { from_us, to_us } => timestamp_us >= from_us && timestamp_us < to_us,
            Self::Exact(expected) => timestamp_us == expected,
        }
    }
}

impl SegmentCursor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_indexed(
        segment: Arc<Segment>,
        metadata: Arc<SegmentMetadata>,
        indices: Vec<usize>,
        timestamp_filter: TimestampFilter,
        cache_blocks: bool,
        keep_file_open: bool,
        query_budget: Option<Arc<QueryBudget>>,
        read_metrics: Option<Arc<SegmentReadMetrics>>,
    ) -> Result<Self> {
        Ok(Self {
            segment,
            file: None,
            file_permit: None,
            blocks: SegmentCursorBlocks::Indexed { metadata, indices },
            next_block: 0,
            compressed: Vec::new(),
            decoded: Arc::from(Vec::<VersionedPoint>::new()),
            cached_decoded: None,
            decoded_position: 0,
            timestamp_filter,
            cache_admission: cache_blocks.then_some(BlockCacheAdmission::Always),
            keep_file_open,
            query_budget,
            read_metrics,
            read_plan: None,
            reserved_compressed_bytes: 0,
            reserved_decoded_bytes: 0,
        })
    }

    pub(super) fn new_planned(
        segment: Arc<Segment>,
        blocks: Vec<BlockIndex>,
        timestamp_filter: TimestampFilter,
        cache_admission: BlockCacheAdmission,
        query_budget: Option<Arc<QueryBudget>>,
        read_plan: Option<Arc<QueryReadPlan>>,
    ) -> Result<Self> {
        Ok(Self {
            segment,
            file: None,
            file_permit: None,
            blocks: SegmentCursorBlocks::Planned(blocks),
            next_block: 0,
            compressed: Vec::new(),
            decoded: Arc::from(Vec::<VersionedPoint>::new()),
            cached_decoded: None,
            decoded_position: 0,
            timestamp_filter,
            cache_admission: Some(cache_admission),
            keep_file_open: false,
            query_budget,
            read_metrics: None,
            read_plan,
            reserved_compressed_bytes: 0,
            reserved_decoded_bytes: 0,
        })
    }

    pub(super) fn load_block(&mut self) -> Result<bool> {
        self.release_compressed_memory();
        let Some(descriptor) = self.blocks.get(self.next_block) else {
            return Ok(false);
        };
        self.next_block += 1;
        if let Some(metrics) = &self.read_metrics {
            metrics.record_visit(&self.segment, descriptor.offset, u64::from(descriptor.size))?;
        }
        let cache_key = BlockKey::new(self.segment.descriptor.id, descriptor.offset);
        let mut load_leader = None;
        if self.cache_admission.is_some() {
            loop {
                let decoded_limit = u64::try_from(TARGET_BLOCK_POINTS)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<VersionedPoint>() as u64);
                let estimated_bytes = u64::from(descriptor.size).saturating_add(decoded_limit);
                match self
                    .segment
                    .block_cache
                    .acquire_for_load(cache_key, estimated_bytes)?
                {
                    BlockLoadDecision::Cached(block) => {
                        self.install_cached(block, false)?;
                        return Ok(true);
                    }
                    BlockLoadDecision::Leader(leader) => {
                        load_leader = Some(leader);
                        break;
                    }
                    BlockLoadDecision::Waiter(waiter) => {
                        match waiter.wait(self.query_budget.as_deref())? {
                            BlockLoadWaitOutcome::Loaded(decoded) => {
                                self.install_decoded(decoded, false)?;
                                return Ok(true);
                            }
                            BlockLoadWaitOutcome::Retry => {}
                        }
                    }
                    BlockLoadDecision::Independent => break,
                }
            }
        }
        if let Some(budget) = &self.query_budget {
            budget
                .reserve_memory_class(QueryMemoryClass::Compressed, u64::from(descriptor.size))?;
            self.reserved_compressed_bytes = u64::from(descriptor.size);
        }
        let physical_read = if let Some(read_plan) = &self.read_plan {
            self.compressed = read_plan.read_block(cache_key)?;
            false
        } else {
            let io_started = Instant::now();
            self.compressed.resize(descriptor.size as usize, 0);
            if self.file.is_none() {
                self.file_permit = Some(acquire_query_file_permit(
                    &self.segment.file_pool,
                    self.query_budget.as_deref(),
                )?);
                match File::open(&self.segment.path) {
                    Ok(file) => self.file = Some(file),
                    Err(error) => {
                        self.file_permit = None;
                        return Err(error.into());
                    }
                }
            }
            let read_result = if let Some(file) = self.file.as_mut() {
                file.seek(SeekFrom::Start(
                    self.segment.descriptor.file_offset + descriptor.offset,
                ))
                .and_then(|_| file.read_exact(&mut self.compressed))
            } else {
                unreachable!("segment cursor initialized a file before reading")
            };
            if !self.keep_file_open || read_result.is_err() {
                self.file = None;
                self.file_permit = None;
            }
            if let Some(query_budget) = &self.query_budget {
                query_budget.record_io(io_started.elapsed());
            }
            read_result?;
            true
        };
        if let Some(metrics) = &self.read_metrics {
            metrics.record_physical_read(u64::from(descriptor.size));
        }
        let decode_started = Instant::now();
        let operations = decode_block(&self.compressed)?;
        validate_decoded_block_against_index(&operations, descriptor, self.compressed.len())?;
        if let Some(metrics) = &self.read_metrics {
            metrics.record_decode(
                self.compressed.len() as u64,
                operations.len() as u64,
                decode_started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        let physical_decoded_bytes =
            (operations.len() as u64).saturating_mul(std::mem::size_of::<VersionedPoint>() as u64);
        let mut decoded = Vec::with_capacity(operations.len());
        for operation in operations {
            let (value_bits, is_delete) = match operation.kind {
                OperationKind::Put => (
                    operation
                        .value
                        .ok_or_else(|| corrupt("put operation is missing its value"))?
                        .to_bits(),
                    false,
                ),
                OperationKind::Delete => {
                    if operation.value.is_some() {
                        return Err(corrupt("delete operation has a value"));
                    }
                    (0, true)
                }
            };
            decoded.push(VersionedPoint {
                key: PointKey {
                    series: operation.series,
                    timestamp_us: operation.timestamp_us,
                },
                commit_id: operation.commit_id,
                ordinal: operation.ordinal,
                value_bits,
                is_delete,
            });
        }
        if let Some(query_budget) = &self.query_budget {
            query_budget.record_decode(decode_started.elapsed(), physical_decoded_bytes);
        }
        let decoded = Arc::from(decoded);
        if let Some(leader) = load_leader {
            let admission = self
                .cache_admission
                .ok_or_else(|| corrupt("coalesced block load has no cache admission policy"))?;
            let cached =
                self.segment
                    .block_cache
                    .complete_load(leader, Arc::clone(&decoded), admission)?;
            if let Some(block) = cached {
                self.install_cached(block, physical_read)?;
            } else {
                self.install_decoded(decoded, physical_read)?;
            }
        } else if let Some(admission) = self.cache_admission
            && let Some(block) = self.segment.block_cache.insert_with_admission(
                cache_key,
                Arc::clone(&decoded),
                admission,
            )?
        {
            self.install_cached(block, physical_read)?;
        } else {
            self.install_decoded(decoded, physical_read)?;
        }
        Ok(true)
    }

    pub(super) fn install_cached(
        &mut self,
        block: Arc<CachedBlock>,
        physical_read: bool,
    ) -> Result<()> {
        self.install_decoded(Arc::clone(block.points()), physical_read)?;
        self.cached_decoded = Some(block);
        Ok(())
    }

    pub(super) fn install_decoded(
        &mut self,
        decoded: Arc<[VersionedPoint]>,
        physical_read: bool,
    ) -> Result<()> {
        let decoded_bytes = u64::try_from(decoded.len())
            .ok()
            .and_then(|points| {
                points.checked_mul(u64::try_from(std::mem::size_of::<VersionedPoint>()).ok()?)
            })
            .ok_or_else(|| corrupt("decoded block memory size overflows u64"))?;
        self.release_decoded_memory();
        if let Some(budget) = &self.query_budget {
            budget.record_block(decoded_bytes, physical_read)?;
            budget.reserve_memory_class(QueryMemoryClass::Decoded, decoded_bytes)?;
            self.reserved_decoded_bytes = decoded_bytes;
        }
        self.decoded = decoded;
        self.decoded_position = 0;
        Ok(())
    }

    pub(super) fn release_decoded_memory(&mut self) {
        self.cached_decoded = None;
        self.decoded = Arc::from(Vec::<VersionedPoint>::new());
        if self.reserved_decoded_bytes != 0 {
            if let Some(budget) = &self.query_budget {
                budget.release_memory_class(QueryMemoryClass::Decoded, self.reserved_decoded_bytes);
            }
            self.reserved_decoded_bytes = 0;
        }
    }

    pub(super) fn release_compressed_memory(&mut self) {
        self.compressed = Vec::new();
        if self.reserved_compressed_bytes != 0 {
            if let Some(budget) = &self.query_budget {
                budget.release_memory_class(
                    QueryMemoryClass::Compressed,
                    self.reserved_compressed_bytes,
                );
            }
            self.reserved_compressed_bytes = 0;
        }
    }
}

pub(super) fn validate_decoded_block_against_index(
    operations: &[crate::v1::Operation],
    descriptor: BlockIndex,
    encoded_bytes: usize,
) -> Result<()> {
    if encoded_bytes != descriptor.size as usize
        || operations.first().is_none_or(|first| {
            first.series != descriptor.series || first.timestamp_us != descriptor.min_timestamp_us
        })
        || operations
            .last()
            .is_none_or(|last| last.timestamp_us != descriptor.max_timestamp_us)
    {
        return Err(corrupt("segment block does not match its index"));
    }
    Ok(())
}

pub(super) fn acquire_query_file_permit(
    file_pool: &FilePermitPool,
    query_budget: Option<&QueryBudget>,
) -> Result<FilePermit> {
    match query_budget {
        Some(budget) => file_pool.acquire_for_query(budget),
        None => file_pool.acquire(),
    }
}

impl Drop for SegmentCursor {
    fn drop(&mut self) {
        self.release_compressed_memory();
        self.release_decoded_memory();
    }
}

impl OperationCursor for SegmentCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        loop {
            while let Some(point) = self.decoded.get(self.decoded_position).copied() {
                self.decoded_position += 1;
                if !self.timestamp_filter.contains(point.key.timestamp_us) {
                    continue;
                }
                return Ok(Some(point));
            }
            if !self.load_block()? {
                self.release_decoded_memory();
                return Ok(None);
            }
        }
    }
}
