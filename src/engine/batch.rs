//! Ordered traversal of in-memory batches and disjoint-range validation.

use super::BATCH_CONSOLIDATION_THRESHOLD;
use crate::error::Result;
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::segment::{OperationCursor, SliceCursor};
use crate::types::VersionedPoint;
use std::sync::Arc;

// Concatenation is safe only after proving disjoint key ranges. The outer
// VisibleCursor still resolves versions inside each batch at its MVCC snapshot.
pub(super) struct OrderedBatchCursor {
    pub(super) cursors: Vec<SliceCursor>,
    pub(super) position: usize,
    pub(super) reserved_bytes: u64,
    pub(super) budget: Arc<QueryBudget>,
}

impl OrderedBatchCursor {
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
