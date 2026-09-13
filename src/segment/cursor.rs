//! Пакеты точек, курсоры памяти и счётчики файлового чтения.

use super::Segment;
use crate::cache::MemoryReservation;
use crate::error::{Result, corrupt, invalid};
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

#[derive(Default)]
pub(crate) struct SegmentReadMetrics {
    pub(super) block_visits: AtomicU64,
    pub(super) unique_blocks: Mutex<HashSet<(u64, u64)>>,
    pub(super) unique_block_bytes: AtomicU64,
    pub(super) physical_blocks_read: AtomicU64,
    pub(super) physical_bytes_read: AtomicU64,
    pub(super) decoded_input_bytes: AtomicU64,
    pub(super) decoded_points: AtomicU64,
    pub(super) decode_nanos: AtomicU64,
}

impl SegmentReadMetrics {
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
            .insert((physical_id, physical_offset));
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
    pub(super) points: Vec<VersionedPoint>,
    pub(super) _reservation: Option<Arc<MemoryReservation>>,
}

impl PointBatch {
    pub(crate) fn managed(
        points: Vec<VersionedPoint>,
        reservation: Arc<MemoryReservation>,
    ) -> Self {
        Self {
            points,
            _reservation: Some(reservation),
        }
    }

    pub(crate) fn for_each_requested_series(
        &self,
        requested: &[SeriesKey],
        mut visit: impl FnMut(SeriesKey, i64, i64),
    ) {
        let Some(first) = self.points.first() else {
            return;
        };
        let Some(last) = self.points.last() else {
            return;
        };
        let requested_start = requested.partition_point(|series| *series < first.key.series);
        let requested_end = requested.partition_point(|series| *series <= last.key.series);
        let mut position = 0_usize;
        for series in &requested[requested_start..requested_end] {
            while position < self.points.len() && self.points[position].key.series < *series {
                position += 1;
            }
            let start = position;
            while position < self.points.len() && self.points[position].key.series == *series {
                position += 1;
            }
            if start < position {
                visit(
                    *series,
                    self.points[start].key.timestamp_us,
                    self.points[position - 1].key.timestamp_us,
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn unmanaged(points: Arc<Vec<VersionedPoint>>) -> Self {
        Self {
            points: Arc::try_unwrap(points).unwrap_or_else(|points| (*points).clone()),
            _reservation: None,
        }
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
    pub(crate) fn remaining_points(&self) -> &[VersionedPoint] {
        &self.points[self.position..self.end]
    }

    pub(crate) fn key_bounds(&self) -> Option<(PointKey, PointKey)> {
        if self.position >= self.end {
            return None;
        }
        Some((
            self.points.get(self.position)?.key,
            self.points.get(self.end - 1)?.key,
        ))
    }

    pub(crate) fn remaining_len(&self) -> usize {
        self.end.saturating_sub(self.position)
    }

    #[cfg(test)]
    pub fn all(points: Arc<Vec<VersionedPoint>>) -> Self {
        let points = Arc::new(PointBatch::unmanaged(points));
        let end = points.len();
        Self {
            points,
            position: 0,
            end,
        }
    }

    pub fn series(
        points: Arc<PointBatch>,
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
        let position = points.partition_point(|point| point.key < start_key);
        let end = points.partition_point(|point| point.key < end_key);
        (position < end).then_some(Self {
            points,
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

    #[must_use]
    pub fn series_time_bounds(
        points: &[VersionedPoint],
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
    ) -> Option<(i64, i64)> {
        let start_key = PointKey {
            series,
            timestamp_us: from_us,
        };
        let end_key = PointKey {
            series,
            timestamp_us: to_us,
        };
        let start = points.partition_point(|point| point.key < start_key);
        let end = points.partition_point(|point| point.key < end_key);
        (start < end).then(|| {
            (
                points[start].key.timestamp_us,
                points[end - 1].key.timestamp_us,
            )
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
