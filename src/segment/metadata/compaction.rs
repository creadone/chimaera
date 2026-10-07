//! Job-owned metadata: reuse inputs without admitting cold history to query caches.

use super::read_segment_metadata;
use crate::error::{Result, corrupt};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use crate::segment::cursor::segment::TimestampFilter;
use crate::segment::{
    OperationCursor, Segment, SegmentCursor, SegmentMetadata, SegmentReadMetrics,
};
use crate::types::VersionedPoint;
use std::collections::BTreeMap;
use std::sync::Arc;

// Includes the lease, Arc headers and conservative BTree node slack per input.
pub(super) const ENTRY_BYTES: u64 = 1024;

struct Input {
    metadata: Arc<SegmentMetadata>,
    _memory: WorkMemoryReservation,
}

/// One coordinator job owns the registry. Cursors keep their input's charge
/// alive even if the job is dropped while pack workers still consume them.
/// The shared work limit bounds both entries and decoding; exhaustion fails
/// immediately because waiting could deadlock on this job's own reservations.
pub(crate) struct CompactionReader {
    inputs: BTreeMap<([u8; 16], u64), Arc<Input>>,
    work: Arc<WorkMemoryGovernor>,
    metrics: Arc<SegmentReadMetrics>,
}

impl CompactionReader {
    pub(crate) fn new(work: Arc<WorkMemoryGovernor>, metrics: Arc<SegmentReadMetrics>) -> Self {
        Self {
            inputs: BTreeMap::new(),
            work,
            metrics,
        }
    }

    fn input(&mut self, segment: &Segment) -> Result<Arc<Input>> {
        let key = (segment.storage_uuid, segment.descriptor.id);
        if let Some(input) = self.inputs.get(&key) {
            return Ok(Arc::clone(input));
        }
        let (metadata, memory) = if let Some(metadata) = segment.metadata_cache.get(key.1)? {
            // A shared hit is already charged globally. Also bound how much one
            // job can pin, so warm inputs cannot bypass its work limit.
            let memory = self.reserve(metadata.bytes().saturating_add(ENTRY_BYTES))?;
            (metadata, memory)
        } else {
            let loaded = read_segment_metadata(
                &segment.path,
                segment.storage_uuid,
                &segment.descriptor,
                &segment.file_pool,
                None,
                true,
                Some(&self.work),
            )?;
            crate::diagnostics::publication::add(
                crate::diagnostics::publication::Counter::SegmentMetadataReadBytes,
                usize::try_from(loaded.bytes_read).unwrap_or(usize::MAX),
            );
            crate::diagnostics::publication::add(
                crate::diagnostics::publication::Counter::SegmentHeaderReadBytes,
                loaded
                    .metadata
                    .index
                    .len()
                    .saturating_mul(crate::segment::BLOCK_HEADER_SIZE),
            );
            // Validation is unchanged. The merge only needs the decoded index;
            // it never consumes aggregate records from this section.
            drop(loaded.aggregate);
            let mut memory = loaded
                .work_memory
                .ok_or_else(|| corrupt("missing compaction metadata charge"))?;
            let retained = loaded.metadata.bytes().saturating_add(ENTRY_BYTES);
            memory.shrink(memory.bytes().saturating_sub(retained));
            (loaded.metadata, memory)
        };
        let input = Arc::new(Input {
            metadata,
            _memory: memory,
        });
        self.inputs.insert(key, Arc::clone(&input));
        Ok(input)
    }

    fn reserve(&self, bytes: u64) -> Result<WorkMemoryReservation> {
        self.work
            .try_reserve_background_with_headroom(WorkMemoryClass::Compaction, bytes, 0)?
            .ok_or_else(|| {
                crate::Error::ResourceLimit("compaction cursor exceeds work memory".into())
            })
    }

    pub(crate) fn cursor(
        &mut self,
        segment: &Arc<Segment>,
        from_us: i64,
        to_us: i64,
    ) -> Result<Option<Box<dyn OperationCursor>>> {
        let input = self.input(segment)?;
        let selected = || {
            input
                .metadata
                .index
                .iter()
                .enumerate()
                .filter_map(|(i, b)| {
                    (b.max_timestamp_us >= from_us && b.min_timestamp_us < to_us).then_some(i)
                })
        };
        let count = selected().count();
        if count == 0 {
            return Ok(None);
        }
        let memory = self.reserve(
            (count as u64)
                .saturating_mul(std::mem::size_of::<usize>() as u64)
                .saturating_add(std::mem::size_of::<Cursor>() as u64),
        )?;
        let mut blocks = Vec::with_capacity(count);
        blocks.extend(selected());
        let cursor = SegmentCursor::new_indexed(
            Arc::clone(segment),
            Arc::clone(&input.metadata),
            blocks,
            TimestampFilter::HalfOpen { from_us, to_us },
            false,
            true,
            None,
            Some(Arc::clone(&self.metrics)),
        )?;
        Ok(Some(Box::new(Cursor {
            cursor,
            _input: input,
            _memory: memory,
        })))
    }
}

struct Cursor {
    cursor: SegmentCursor,
    _input: Arc<Input>,
    _memory: WorkMemoryReservation,
}

impl OperationCursor for Cursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        self.cursor.next_operation()
    }
}
