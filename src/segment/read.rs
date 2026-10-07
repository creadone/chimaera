//! Physical block references and their query-owned memory reservation.
use super::{BlockIndex, OfflinePackDirectory, Segment};
use crate::error::{Result, corrupt};
use crate::memory::cache::BlockCacheAdmission;
use crate::query::{QueryBudget, QueryReservation};
use crate::segment::cursor::segment::SegmentCursor;
use crate::types::SeriesKey;
use std::sync::Arc;

pub(crate) struct PartSource {
    pub(crate) segment: Arc<Segment>,
    pub(crate) directory: Option<ReadDirectory>,
}

pub(crate) type ReadDirectory = Arc<OfflinePackDirectory>;

impl PartSource {
    pub(crate) fn segment(&self) -> &Arc<Segment> {
        &self.segment
    }

    pub(crate) fn into_group_cursor(
        self,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        admission: BlockCacheAdmission,
        budget: Arc<QueryBudget>,
    ) -> Result<SegmentCursor> {
        SegmentCursor::new_lazy(self, series, from_us, to_us, admission, budget)
    }
}

enum Blocks {
    Directory(super::directory::BlockCursor),
    Segment(super::metadata::BlockCursor),
}

pub(in crate::segment) struct LazyBlocks {
    blocks: Blocks,
    position: usize,
    pending: Option<BlockIndex>,
    loaded: bool,
    _memory: QueryReservation,
}

impl LazyBlocks {
    pub(in crate::segment) fn skip_before(&mut self, position: usize, timestamp_us: i64) {
        match &mut self.blocks {
            Blocks::Directory(cursor) => cursor.skip_before(timestamp_us),
            Blocks::Segment(cursor) => cursor.skip_before(timestamp_us),
        }
        if position != self.position
            || self
                .pending
                .is_some_and(|block| block.max_timestamp_us < timestamp_us)
        {
            self.position = position;
            self.pending = None;
            self.loaded = false;
        }
    }

    pub(in crate::segment) fn new(
        source: PartSource,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        budget: Arc<QueryBudget>,
    ) -> Result<Self> {
        let memory = budget.reserve_owned(std::mem::size_of::<Self>() as u64)?;
        // A published directory spans many parts. Restrict this part's block
        // lookup before visiting pages, not after decoding their segment IDs.
        // Keep the caller's grouping/summary bounds unchanged: these are only
        // the physical source bounds, with an inclusive stored maximum.
        let descriptor = source.segment.descriptor();
        let from_us = from_us.max(descriptor.min_timestamp_us);
        let to_us = to_us.min(descriptor.max_timestamp_us.saturating_add(1));
        let blocks = match source.directory {
            Some(directory) => Blocks::Directory(super::directory::BlockCursor::new(
                directory,
                source.segment.descriptor.id,
                series,
                from_us,
                to_us,
                budget,
            )?),
            None => Blocks::Segment(super::metadata::BlockCursor::new(
                source.segment,
                series,
                from_us,
                to_us,
                budget,
            )),
        };
        Ok(Self {
            blocks,
            position: 0,
            pending: None,
            loaded: false,
            _memory: memory,
        })
    }

    pub(in crate::segment) fn get(&mut self, position: usize) -> Result<Option<BlockIndex>> {
        if position != self.position {
            if position != self.position + 1 || !self.loaded {
                return Err(corrupt("streaming block cursor moved out of order"));
            }
            self.position = position;
            self.loaded = false;
        }
        if !self.loaded {
            self.pending = match &mut self.blocks {
                Blocks::Directory(cursor) => cursor.next_block()?,
                Blocks::Segment(cursor) => cursor.next_block()?,
            };
            self.loaded = true;
        }
        Ok(self.pending)
    }
}

// Includes each physical locator and its share of per-part lookup/vector overhead.
pub(super) const READ_BYTES_PER_BLOCK: u64 = 208;

pub(crate) struct BlockReadReservation {
    pub(super) query_budget: Arc<QueryBudget>,
    pub(super) bytes: u64,
}

impl BlockReadReservation {
    pub(crate) fn new(query_budget: Arc<QueryBudget>) -> Self {
        Self {
            query_budget,
            bytes: 0,
        }
    }

    pub(crate) fn reserve_source(&mut self) -> Result<()> {
        self.reserve_blocks(1)
    }

    pub(super) fn reserve_blocks(&mut self, blocks: u64) -> Result<()> {
        let bytes = blocks.saturating_mul(READ_BYTES_PER_BLOCK);
        self.query_budget.reserve_memory(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }
}

impl Drop for BlockReadReservation {
    fn drop(&mut self) {
        self.query_budget.release_memory(self.bytes);
    }
}
