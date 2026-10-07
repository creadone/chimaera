//! Streaming segment blocks within query and cache budgets.

use crate::error::{Result, corrupt};
use crate::memory::cache::{
    BlockCacheAdmission, BlockKey, BlockLoadDecision, BlockLoadWaitOutcome, CachedBlock,
};
use crate::persistence::decode::decode_block;
use crate::persistence::files::{FilePermit, FilePermitPool};
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::segment::cursor::{OperationCursor, SegmentReadMetrics};
use crate::segment::{BlockIndex, Segment, SegmentMetadata, TARGET_BLOCK_POINTS};
use crate::types::{PointKey, VersionedPoint};
use std::fs::File;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct SegmentCursor {
    pub(in crate::segment) segment: Arc<Segment>,
    pub(in crate::segment) file: Option<File>,
    pub(in crate::segment) file_permit: Option<FilePermit>,
    pub(in crate::segment) blocks: SegmentCursorBlocks,
    pub(in crate::segment) next_block: usize,
    pub(in crate::segment) compressed: Vec<u8>,
    pub(in crate::segment) decoded: Arc<[VersionedPoint]>,
    pub(in crate::segment) cached_decoded: Option<Arc<CachedBlock>>,
    pub(in crate::segment) decoded_position: usize,
    pub(in crate::segment) timestamp_filter: TimestampFilter,
    pub(in crate::segment) cache_admission: Option<BlockCacheAdmission>,
    pub(in crate::segment) keep_file_open: bool,
    pub(in crate::segment) query_budget: Option<Arc<QueryBudget>>,
    pub(in crate::segment) read_metrics: Option<Arc<SegmentReadMetrics>>,
    pub(in crate::segment) reserved_compressed_bytes: u64,
    pub(in crate::segment) reserved_decoded_bytes: u64,
}

pub(in crate::segment) enum SegmentCursorBlocks {
    Indexed {
        metadata: Arc<SegmentMetadata>,
        indices: Vec<usize>,
    },
    Planned(Vec<BlockIndex>),
    Lazy(Box<crate::segment::read::LazyBlocks>),
}

impl SegmentCursorBlocks {
    pub(in crate::segment) fn get(&mut self, position: usize) -> Result<Option<BlockIndex>> {
        Ok(match self {
            Self::Indexed { metadata, indices } => indices
                .get(position)
                .copied()
                .map(|index| metadata.index[index]),
            Self::Planned(blocks) => blocks.get(position).copied(),
            Self::Lazy(blocks) => return blocks.get(position),
        })
    }
}

#[derive(Clone, Copy)]
pub(in crate::segment) enum TimestampFilter {
    All,
    HalfOpen { from_us: i64, to_us: i64 },
}

impl TimestampFilter {
    pub(in crate::segment) const fn contains(self, timestamp_us: i64) -> bool {
        match self {
            Self::All => true,
            Self::HalfOpen { from_us, to_us } => timestamp_us >= from_us && timestamp_us < to_us,
        }
    }
}

impl SegmentCursor {
    pub(in crate::segment) fn new_lazy(
        source: crate::segment::PartSource,
        series: crate::types::SeriesKey,
        from_us: i64,
        to_us: i64,
        admission: BlockCacheAdmission,
        budget: Arc<QueryBudget>,
    ) -> Result<Self> {
        let segment = Arc::clone(&source.segment);
        let blocks = crate::segment::read::LazyBlocks::new(
            source,
            series,
            from_us,
            to_us,
            Arc::clone(&budget),
        )?;
        let mut cursor = Self::new_planned(
            segment,
            Vec::new(),
            TimestampFilter::HalfOpen { from_us, to_us },
            admission,
            Some(budget),
        )?;
        cursor.blocks = SegmentCursorBlocks::Lazy(Box::new(blocks));
        Ok(cursor)
    }
    /// Advances across an already summarized interval without decoding its blocks.
    pub(crate) fn skip_before(&mut self, timestamp_us: i64) -> Result<()> {
        let TimestampFilter::HalfOpen { from_us, to_us } = self.timestamp_filter else {
            return Err(corrupt("group cursor requires a half-open range"));
        };
        self.timestamp_filter = TimestampFilter::HalfOpen {
            from_us: from_us.max(timestamp_us),
            to_us,
        };
        self.decoded_position = self.decoded_position.max(
            self.decoded
                .partition_point(|point| point.key.timestamp_us < timestamp_us),
        );
        if self.decoded_position == self.decoded.len() {
            self.release_decoded_memory();
            self.decoded_position = 0;
        }
        if let SegmentCursorBlocks::Lazy(blocks) = &mut self.blocks {
            blocks.skip_before(self.next_block, timestamp_us);
            return Ok(());
        }
        while self
            .blocks
            .get(self.next_block)?
            .is_some_and(|block| block.max_timestamp_us < timestamp_us)
        {
            self.next_block += 1;
        }
        Ok(())
    }

    pub(crate) fn next_before(&mut self, exclusive: i64) -> Result<Option<VersionedPoint>> {
        if let TimestampFilter::HalfOpen { from_us, to_us } = self.timestamp_filter
            && from_us >= to_us.min(exclusive)
        {
            return Ok(None);
        }
        loop {
            while let Some(point) = self.decoded.get(self.decoded_position).copied() {
                if point.key.timestamp_us >= exclusive {
                    return Ok(None);
                }
                self.decoded_position += 1;
                if self.timestamp_filter.contains(point.key.timestamp_us) {
                    return Ok(Some(point));
                }
            }
            if self
                .blocks
                .get(self.next_block)?
                .is_none_or(|block| block.min_timestamp_us >= exclusive)
            {
                return Ok(None);
            }
            if !self.load_block()? {
                return Ok(None);
            }
        }
    }

    /// Borrows the remaining points of one decoded block inside the group range.
    pub(crate) fn next_chunk_before(
        &mut self,
        exclusive: i64,
    ) -> Result<Option<&[VersionedPoint]>> {
        let TimestampFilter::HalfOpen { from_us, to_us } = self.timestamp_filter else {
            return Err(corrupt("group cursor requires a half-open range"));
        };
        let end_us = to_us.min(exclusive);
        if from_us >= end_us {
            return Ok(None);
        }
        loop {
            let remaining = &self.decoded[self.decoded_position..];
            let start = self.decoded_position
                + remaining.partition_point(|point| point.key.timestamp_us < from_us);
            let end = start
                + self.decoded[start..].partition_point(|point| point.key.timestamp_us < end_us);
            self.decoded_position = end;
            if start < end {
                return Ok(Some(&self.decoded[start..end]));
            }
            if end < self.decoded.len()
                || self
                    .blocks
                    .get(self.next_block)?
                    .is_none_or(|block| block.min_timestamp_us >= end_us)
                || !self.load_block()?
            {
                return Ok(None);
            }
        }
    }

    pub(crate) fn take_count_sum(
        &mut self,
        grouping: crate::types::Grouping,
        exclusive: i64,
    ) -> Result<Option<(i64, i64, crate::types::AggregateState)>> {
        if let TimestampFilter::HalfOpen { from_us, to_us } = self.timestamp_filter
            && from_us >= to_us.min(exclusive)
        {
            return Ok(None);
        }
        if self.decoded_position < self.decoded.len() {
            return Ok(None);
        }
        let Some(block) = self.blocks.get(self.next_block)? else {
            return Ok(None);
        };
        let Some(summary) = block.count_sum else {
            return Ok(None);
        };
        if !self.timestamp_filter.contains(block.min_timestamp_us)
            || !self.timestamp_filter.contains(block.max_timestamp_us)
            || block.max_timestamp_us >= exclusive
        {
            return Ok(None);
        }
        let (start, end) = grouping.bucket_bounds(block.min_timestamp_us)?;
        if block.max_timestamp_us >= end {
            return Ok(None);
        }
        let state = crate::segment::read_count_sum_summary(summary)?;
        if state.count == 0 {
            return Err(corrupt("nonempty block has empty count summary"));
        }
        self.next_block += 1;
        self.release_decoded_memory();
        self.decoded_position = 0;
        Ok(Some((start, end, state)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::segment) fn new_indexed(
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
            reserved_compressed_bytes: 0,
            reserved_decoded_bytes: 0,
        })
    }

    pub(in crate::segment) fn new_planned(
        segment: Arc<Segment>,
        blocks: Vec<BlockIndex>,
        timestamp_filter: TimestampFilter,
        cache_admission: BlockCacheAdmission,
        query_budget: Option<Arc<QueryBudget>>,
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
            reserved_compressed_bytes: 0,
            reserved_decoded_bytes: 0,
        })
    }

    pub(in crate::segment) fn load_block(&mut self) -> Result<bool> {
        let result = self.read_block();
        if result.is_err() {
            self.file = None;
            self.file_permit = None;
        }
        result
    }

    fn read_block(&mut self) -> Result<bool> {
        self.release_compressed_memory();
        let Some(descriptor) = self.blocks.get(self.next_block)? else {
            self.file = None;
            self.file_permit = None;
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
        let physical_read = {
            let io_started = Instant::now();
            self.compressed.resize(descriptor.size as usize, 0);
            if let (Some(cache), Some(budget)) = (&self.segment.open_file_cache, &self.query_budget)
            {
                let file = cache.get(&self.segment.path, budget)?;
                file.read_exact_at(
                    self.segment.descriptor.file_offset + descriptor.offset,
                    &mut self.compressed,
                    budget,
                )?;
            } else {
                if self.file.is_none() {
                    let retained = if self.keep_file_open {
                        self.segment.file_pool.try_acquire_retained()?
                    } else {
                        None
                    };
                    self.file_permit = Some(match retained {
                        Some(permit) => permit,
                        None => acquire_query_file_permit(
                            &self.segment.file_pool,
                            self.query_budget.as_deref(),
                        )?,
                    });
                    match File::open(&self.segment.path) {
                        Ok(file) => {
                            self.file = Some(file);
                            if let Some(budget) = &self.query_budget {
                                budget.record_file_open();
                            }
                        }
                        Err(error) => {
                            self.file_permit = None;
                            return Err(error.into());
                        }
                    }
                }
                let read_result = if let Some(file) = self.file.as_mut() {
                    let offset = self.segment.descriptor.file_offset + descriptor.offset;
                    #[cfg(unix)]
                    {
                        std::os::unix::fs::FileExt::read_exact_at(
                            file,
                            &mut self.compressed,
                            offset,
                        )
                    }
                    #[cfg(not(unix))]
                    {
                        file.seek(SeekFrom::Start(offset))
                            .and_then(|_| file.read_exact(&mut self.compressed))
                    }
                } else {
                    unreachable!("segment cursor initialized a file before reading")
                };
                if !self
                    .file_permit
                    .as_ref()
                    .is_some_and(FilePermit::is_retained)
                    || read_result.is_err()
                    || matches!(&self.blocks, SegmentCursorBlocks::Indexed { indices, .. }
                        if self.next_block == indices.len())
                {
                    self.file = None;
                    self.file_permit = None;
                }
                read_result?;
            }
            if let Some(query_budget) = &self.query_budget {
                query_budget.record_io(io_started.elapsed());
            }
            if let Some(budget) = &self.query_budget {
                budget.record_payload_bytes(u64::from(descriptor.size));
            }
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
            decoded.push(VersionedPoint {
                key: PointKey {
                    series: operation.series,
                    timestamp_us: operation.timestamp_us,
                },
                commit_id: operation.commit_id,
                value_bits: operation.value.to_bits(),
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

    pub(in crate::segment) fn install_cached(
        &mut self,
        block: Arc<CachedBlock>,
        physical_read: bool,
    ) -> Result<()> {
        self.install_decoded(Arc::clone(block.points()), physical_read)?;
        self.cached_decoded = Some(block);
        Ok(())
    }

    pub(in crate::segment) fn install_decoded(
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

    pub(in crate::segment) fn release_decoded_memory(&mut self) {
        self.cached_decoded = None;
        self.decoded = Arc::from(Vec::<VersionedPoint>::new());
        if self.reserved_decoded_bytes != 0 {
            if let Some(budget) = &self.query_budget {
                budget.release_memory_class(QueryMemoryClass::Decoded, self.reserved_decoded_bytes);
            }
            self.reserved_decoded_bytes = 0;
        }
    }

    pub(in crate::segment) fn release_compressed_memory(&mut self) {
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

pub(in crate::segment) fn validate_decoded_block_against_index(
    operations: &[crate::persistence::decode::Operation],
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

pub(in crate::segment) fn acquire_query_file_permit(
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
