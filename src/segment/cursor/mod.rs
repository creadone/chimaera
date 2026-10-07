//! Point batches, in-memory cursors, and file-read counters.

pub(super) mod segment;

use super::Segment;
use crate::error::{Result, corrupt, invalid};
use crate::memory::cache::MemoryReservation;
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use crate::types::{PointKey, SeriesKey, VersionedPoint};
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

pub(crate) trait OperationCursor: Send {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SegmentReadMetricsSnapshot {
    pub block_visits: u64,
    pub unique_blocks: u64,
    pub unique_block_bytes: u64,
    pub physical_blocks_read: u64,
    pub physical_bytes_read: u64,
    pub decoded_input_bytes: u64,
    pub decoded_points: u64,
    pub decode_nanos: u64,
}

pub(crate) struct SegmentReadMetrics {
    pub(super) block_visits: AtomicU64,
    unique_blocks: Mutex<UniqueBlocks>,
    pub(super) unique_block_bytes: AtomicU64,
    pub(super) physical_blocks_read: AtomicU64,
    pub(super) physical_bytes_read: AtomicU64,
    pub(super) decoded_input_bytes: AtomicU64,
    pub(super) decoded_points: AtomicU64,
    pub(super) decode_nanos: AtomicU64,
}

struct UniqueBlocks {
    blocks: HashSet<(u64, u64)>,
    memory: WorkMemoryReservation,
}

impl UniqueBlocks {
    fn insert(&mut self, key: (u64, u64)) -> Result<bool> {
        if self.blocks.contains(&key) {
            return Ok(false);
        }
        // Includes hash table slack and control bytes. Never wait while the
        // job already owns decode buffers: that could wait on itself forever.
        let bytes = (self.blocks.len().saturating_add(1).max(4) as u64).saturating_mul(64);
        if !self
            .memory
            .try_grow(bytes.saturating_sub(self.memory.bytes()))?
        {
            return Err(crate::error::Error::ResourceLimit(
                "compaction block diagnostics exceed work memory".into(),
            ));
        }
        Ok(self.blocks.insert(key))
    }
}

impl SegmentReadMetrics {
    pub(crate) fn new(governor: &Arc<WorkMemoryGovernor>) -> Result<Self> {
        Ok(Self {
            unique_blocks: Mutex::new(UniqueBlocks {
                blocks: HashSet::new(),
                memory: governor.reserve_background(WorkMemoryClass::Compaction, 0)?,
            }),
            block_visits: AtomicU64::new(0),
            unique_block_bytes: AtomicU64::new(0),
            physical_blocks_read: AtomicU64::new(0),
            physical_bytes_read: AtomicU64::new(0),
            decoded_input_bytes: AtomicU64::new(0),
            decoded_points: AtomicU64::new(0),
            decode_nanos: AtomicU64::new(0),
        })
    }

    pub(super) fn record_visit(
        &self,
        segment: &Segment,
        block_offset: u64,
        block_size: u64,
    ) -> Result<()> {
        self.block_visits.fetch_add(1, AtomicOrdering::Relaxed);
        let physical_id = segment.descriptor.pack_id.unwrap_or(segment.descriptor.id);
        let physical_offset = segment
            .descriptor
            .file_offset
            .checked_add(block_offset)
            .ok_or_else(|| corrupt("physical block offset overflows u64"))?;
        let inserted = self
            .unique_blocks
            .lock()
            .map_err(|_| corrupt("segment read metrics mutex is poisoned"))?
            .insert((physical_id, physical_offset))?;
        if inserted {
            self.unique_block_bytes
                .fetch_add(block_size, AtomicOrdering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn record_physical_read(&self, bytes: u64) {
        self.physical_blocks_read
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.physical_bytes_read
            .fetch_add(bytes, AtomicOrdering::Relaxed);
    }

    pub(super) fn record_decode(&self, input_bytes: u64, points: u64, elapsed_nanos: u64) {
        self.decoded_input_bytes
            .fetch_add(input_bytes, AtomicOrdering::Relaxed);
        self.decoded_points
            .fetch_add(points, AtomicOrdering::Relaxed);
        self.decode_nanos
            .fetch_add(elapsed_nanos, AtomicOrdering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Result<SegmentReadMetricsSnapshot> {
        Ok(SegmentReadMetricsSnapshot {
            block_visits: self.block_visits.load(AtomicOrdering::Relaxed),
            unique_blocks: u64::try_from(
                self.unique_blocks
                    .lock()
                    .map_err(|_| corrupt("segment read metrics mutex is poisoned"))?
                    .blocks
                    .len(),
            )
            .unwrap_or(u64::MAX),
            unique_block_bytes: self.unique_block_bytes.load(AtomicOrdering::Relaxed),
            physical_blocks_read: self.physical_blocks_read.load(AtomicOrdering::Relaxed),
            physical_bytes_read: self.physical_bytes_read.load(AtomicOrdering::Relaxed),
            decoded_input_bytes: self.decoded_input_bytes.load(AtomicOrdering::Relaxed),
            decoded_points: self.decoded_points.load(AtomicOrdering::Relaxed),
            decode_nanos: self.decode_nanos.load(AtomicOrdering::Relaxed),
        })
    }
}

#[derive(Debug)]
pub(crate) struct PointBatch {
    commit_id: u64,
    pub(super) points: Vec<VersionedPoint>,
    pub(super) _reservation: Option<Arc<MemoryReservation>>,
    min_timestamp_us: i64,
    max_timestamp_us: i64,
}

impl PointBatch {
    pub(crate) fn deletion(commit_id: u64, reservation: Arc<MemoryReservation>) -> Self {
        let mut batch = Self::new(Vec::new(), Some(reservation));
        batch.commit_id = commit_id;
        batch
    }

    pub(crate) const fn commit_id(&self) -> u64 {
        self.commit_id
    }

    /// Timestamp bounds and the commit barrier count toward the batch reservation.
    pub(crate) const METADATA_BYTES: u64 = std::mem::size_of::<(i64, i64, u64)>() as u64;

    pub(crate) fn managed(
        points: Vec<VersionedPoint>,
        reservation: Arc<MemoryReservation>,
    ) -> Self {
        Self::new(points, Some(reservation))
    }

    fn new(points: Vec<VersionedPoint>, reservation: Option<Arc<MemoryReservation>>) -> Self {
        // Points are sorted by (series, time), not by time across all series.
        // Inverted bounds make an empty batch disjoint from every range.
        let (min_timestamp_us, max_timestamp_us) =
            points
                .iter()
                .fold((i64::MAX, i64::MIN), |(min, max), point| {
                    let time = point.key.timestamp_us;
                    (min.min(time), max.max(time))
                });
        Self {
            commit_id: points.first().map_or(0, |p| p.commit_id),
            points,
            _reservation: reservation,
            min_timestamp_us,
            max_timestamp_us,
        }
    }

    pub(crate) fn overlaps_time(&self, from_us: i64, to_us: i64) -> bool {
        from_us < to_us && self.min_timestamp_us < to_us && self.max_timestamp_us >= from_us
    }
}

impl Deref for PointBatch {
    type Target = [VersionedPoint];

    fn deref(&self) -> &Self::Target {
        &self.points
    }
}

pub(crate) struct SliceCursor {
    pub(super) points: Arc<PointBatch>,
    pub(super) position: usize,
    pub(super) end: usize,
}

pub(crate) struct RangedSliceCursor {
    pub(super) points: Arc<PointBatch>,
    pub(super) ranges: Box<[(usize, usize)]>,
    pub(super) range_index: usize,
    pub(super) position: usize,
}

impl RangedSliceCursor {
    pub fn new(points: Arc<PointBatch>, ranges: Vec<(usize, usize)>) -> Result<Self> {
        let mut previous_end = 0_usize;
        if ranges.is_empty()
            || ranges.iter().any(|(start, end)| {
                let valid = *start < *end && *start >= previous_end && *end <= points.len();
                previous_end = *end;
                !valid
            })
        {
            return Err(invalid("invalid ranged slice cursor"));
        }
        let position = ranges[0].0;
        Ok(Self {
            points,
            ranges: ranges.into_boxed_slice(),
            range_index: 0,
            position,
        })
    }
}

impl SliceCursor {
    pub(crate) fn key_bounds(&self) -> Option<(PointKey, PointKey)> {
        if self.position >= self.end {
            return None;
        }
        Some((
            self.points.get(self.position)?.key,
            self.points.get(self.end - 1)?.key,
        ))
    }

    pub fn series(
        points: &Arc<PointBatch>,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
    ) -> Option<Self> {
        let start_key = PointKey {
            series,
            timestamp_us: from_us,
        };
        let end_key = PointKey {
            series,
            timestamp_us: to_us,
        };
        // Sorted batch endpoints reject unrelated series without binary searches
        // or touching the shared Arc count. No separate index is needed.
        if from_us >= to_us || points.last()?.key < start_key || points.first()?.key >= end_key {
            return None;
        }
        let position = points.partition_point(|point| point.key < start_key);
        let end = points.partition_point(|point| point.key < end_key);
        (position < end).then(|| Self {
            points: Arc::clone(points),
            position,
            end,
        })
    }

    pub fn point(points: Arc<PointBatch>, series: SeriesKey, timestamp_us: i64) -> Option<Self> {
        let key = PointKey {
            series,
            timestamp_us,
        };
        let position = points.partition_point(|point| point.key < key);
        let end = points.partition_point(|point| point.key <= key);
        (position < end).then_some(Self {
            points,
            position,
            end,
        })
    }
}

impl OperationCursor for SliceCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        if self.position >= self.end {
            return Ok(None);
        }
        let point = self.points[self.position];
        self.position += 1;
        Ok(Some(point))
    }
}

impl OperationCursor for RangedSliceCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        loop {
            let Some((start, end)) = self.ranges.get(self.range_index).copied() else {
                return Ok(None);
            };
            if self.position < start {
                self.position = start;
            }
            if self.position < end {
                let point = self.points[self.position];
                self.position += 1;
                return Ok(Some(point));
            }
            self.range_index += 1;
        }
    }
}
