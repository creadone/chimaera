//! Упорядоченное чтение пакетов памяти и проверка их непересечения.

use super::{BATCH_CONSOLIDATION_THRESHOLD, Batch, Metrics};
use crate::error::{Error, Result};
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::segment::{OperationCursor, SliceCursor, VisibleCursor};
use crate::types::{AggregateState, SeriesKey, VersionedPoint};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Одна серия внутри неизменяемого пакета активной памяти.
///
/// Границы вычисляются один раз при построении плана. Суточное планирование затем
/// проверяет пересечение простым сравнением вместо повторного двоичного поиска в
/// каждом пакете для каждого дня диапазона.
pub(super) struct QueryBatch {
    pub(super) batch: Batch,
    pub(super) minimum_timestamp_us: i64,
    pub(super) maximum_timestamp_us: i64,
}

impl QueryBatch {
    pub(super) fn intersects(&self, from_us: i64, to_us: i64) -> bool {
        self.maximum_timestamp_us >= from_us && self.minimum_timestamp_us < to_us
    }
}

// Almost all historical days precede the entire live tail. Reject them once,
// without walking every active batch for each candidate summary. Bounds only
// reject: gaps inside the envelope still use the exact per-batch predicate.
pub(super) struct QueryBatchOverlap<'a> {
    pub(super) batches: &'a [QueryBatch],
    pub(super) bounds: Option<(i64, i64)>,
}

impl<'a> QueryBatchOverlap<'a> {
    pub(super) fn new(batches: &'a [QueryBatch]) -> Self {
        let bounds = batches
            .iter()
            .map(|batch| (batch.minimum_timestamp_us, batch.maximum_timestamp_us))
            .reduce(|(minimum, maximum), (from, to)| (minimum.min(from), maximum.max(to)));
        Self { batches, bounds }
    }

    pub(super) fn intersects(&self, from_us: i64, to_us: i64) -> bool {
        self.bounds
            .is_some_and(|(minimum, maximum)| maximum >= from_us && minimum < to_us)
            && self
                .batches
                .iter()
                .any(|batch| batch.intersects(from_us, to_us))
    }
}

// Concatenation is safe only after proving disjoint key ranges. The outer
// VisibleCursor still resolves versions inside each batch at its MVCC snapshot.
pub(super) struct OrderedBatchCursor {
    pub(super) cursors: Vec<SliceCursor>,
    pub(super) position: usize,
    pub(super) reserved_bytes: u64,
    pub(super) budget: Arc<QueryBudget>,
}

impl OrderedBatchCursor {
    pub(super) fn only_visible_unique_puts(&self, read_commit: u64) -> Result<bool> {
        let mut previous = None;
        for cursor in &self.cursors {
            for chunk in cursor.remaining_points().chunks(256) {
                self.budget.check()?;
                for operation in chunk {
                    if operation.is_delete
                        || operation.commit_id > read_commit
                        || previous.is_some_and(|key| key >= operation.key)
                    {
                        return Ok(false);
                    }
                    previous = Some(operation.key);
                }
            }
        }
        Ok(true)
    }

    pub(super) fn append_aggregate(
        &self,
        state: &mut AggregateState,
        metrics: &Metrics,
    ) -> Result<()> {
        let count = self
            .cursors
            .iter()
            .map(|cursor| cursor.remaining_len() as u64)
            .sum();
        self.budget.record_operations(count)?;
        metrics
            .query_operations_scanned
            .fetch_add(count, Ordering::Relaxed);
        for cursor in &self.cursors {
            for chunk in cursor.remaining_points().chunks(256) {
                self.budget.check()?;
                for operation in chunk {
                    state.add(operation.key.timestamp_us, operation.put_value()?)?;
                }
            }
        }
        self.budget.check()
    }

    pub(super) fn try_new(
        mut cursors: Vec<SliceCursor>,
        budget: Arc<QueryBudget>,
    ) -> std::result::Result<Self, Vec<SliceCursor>> {
        if cursors.len() < BATCH_CONSOLIDATION_THRESHOLD {
            return Err(cursors);
        }
        cursors.sort_unstable_by_key(SliceCursor::key_bounds);
        if cursors.windows(2).any(|pair| {
            pair[0]
                .key_bounds()
                .zip(pair[1].key_bounds())
                .is_none_or(|((_, end), (start, _))| end >= start)
        }) {
            return Err(cursors);
        }
        let Some(bytes) = cursors
            .capacity()
            .checked_mul(std::mem::size_of::<SliceCursor>())
            .and_then(|bytes| u64::try_from(bytes).ok())
        else {
            return Err(cursors);
        };
        if !budget.try_reserve_memory_class(QueryMemoryClass::Decoded, bytes) {
            return Err(cursors);
        }
        Ok(Self {
            cursors,
            position: 0,
            reserved_bytes: bytes,
            budget,
        })
    }
}

impl OperationCursor for OrderedBatchCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        while let Some(cursor) = self.cursors.get_mut(self.position) {
            if let Some(operation) = cursor.next_operation()? {
                return Ok(Some(operation));
            }
            self.position += 1;
        }
        Ok(None)
    }
}

impl Drop for OrderedBatchCursor {
    fn drop(&mut self) {
        self.cursors = Vec::new();
        self.budget
            .release_memory_class(QueryMemoryClass::Decoded, self.reserved_bytes);
    }
}

struct ConsolidatedBatchCursor {
    points: Vec<VersionedPoint>,
    position: usize,
    reserved_bytes: u64,
    query_budget: Arc<QueryBudget>,
}

impl OperationCursor for ConsolidatedBatchCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        let Some(point) = self.points.get(self.position).copied() else {
            return Ok(None);
        };
        self.position += 1;
        Ok(Some(point))
    }
}

impl Drop for ConsolidatedBatchCursor {
    fn drop(&mut self) {
        self.query_budget
            .release_memory_class(QueryMemoryClass::Decoded, self.reserved_bytes);
    }
}

pub(super) fn append_batch_sources(
    sources: &mut Vec<Box<dyn OperationCursor>>,
    batches: &[QueryBatch],
    series: SeriesKey,
    range: (i64, i64),
    read_commit: u64,
    metrics: &Metrics,
    query_budget: &Arc<QueryBudget>,
) -> Result<()> {
    let (from_us, to_us) = range;
    let mut batch_cursors = Vec::<SliceCursor>::new();
    let mut maximum_points = 0_u64;
    for batch in batches {
        if batch.intersects(from_us, to_us)
            && let Some(cursor) =
                SliceCursor::series(Arc::clone(&batch.batch), series, from_us, to_us)
        {
            maximum_points = maximum_points
                .saturating_add(u64::try_from(cursor.remaining_len()).unwrap_or(u64::MAX));
            batch_cursors.push(cursor);
        }
    }
    let batch_cursors = match OrderedBatchCursor::try_new(batch_cursors, Arc::clone(query_budget)) {
        Ok(cursor) => {
            sources.push(Box::new(cursor));
            return Ok(());
        }
        Err(cursors) => cursors,
    };
    let mut batch_sources = batch_cursors
        .into_iter()
        .map(|cursor| Box::new(cursor) as Box<dyn OperationCursor>)
        .collect::<Vec<_>>();
    if batch_sources.len() < BATCH_CONSOLIDATION_THRESHOLD {
        sources.append(&mut batch_sources);
        return Ok(());
    }

    let reserved_bytes =
        maximum_points.saturating_mul(std::mem::size_of::<VersionedPoint>() as u64);
    let capacity = usize::try_from(maximum_points)
        .map_err(|_| Error::ResourceLimit("query batch consolidation exceeds usize".into()))?;
    query_budget.reserve_memory_class(QueryMemoryClass::Decoded, reserved_bytes)?;
    let mut visible = match VisibleCursor::new(batch_sources, read_commit) {
        Ok(visible) => visible,
        Err(error) => {
            query_budget.release_memory_class(QueryMemoryClass::Decoded, reserved_bytes);
            return Err(error);
        }
    };
    let mut points = Vec::with_capacity(capacity);
    let consolidation = (|| {
        while let Some(point) = visible.next_latest()? {
            points.push(point);
        }
        query_budget.record_operations(visible.operations_read())?;
        Ok(visible.operations_read())
    })();
    let scanned = match consolidation {
        Ok(scanned) => scanned,
        Err(error) => {
            query_budget.release_memory_class(QueryMemoryClass::Decoded, reserved_bytes);
            return Err(error);
        }
    };
    metrics
        .query_operations_scanned
        .fetch_add(scanned, Ordering::Relaxed);
    sources.push(Box::new(ConsolidatedBatchCursor {
        points,
        position: 0,
        reserved_bytes,
        query_budget: Arc::clone(query_budget),
    }));
    Ok(())
}
