//! Слияние отсортированных курсоров и выбор видимой MVCC-версии.

use super::cursor::OperationCursor;
use crate::error::{Result, corrupt};
use crate::types::VersionedPoint;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

#[derive(Clone, Copy, Debug)]
pub(super) struct Head {
    pub(super) point: VersionedPoint,
    pub(super) source: usize,
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.point == other.point
    }
}

impl Eq for Head {}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        self.point
            .cmp(&other.point)
            .then_with(|| self.source.cmp(&other.source))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct MergedCursor {
    pub(super) sources: Vec<Box<dyn OperationCursor>>,
    pub(super) heap: BinaryHeap<Reverse<Head>>,
}

impl MergedCursor {
    pub fn new(mut sources: Vec<Box<dyn OperationCursor>>) -> Result<Self> {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (source, cursor) in sources.iter_mut().enumerate() {
            if let Some(point) = cursor.next_operation()? {
                heap.push(Reverse(Head { point, source }));
            }
        }
        Ok(Self { sources, heap })
    }

    pub(super) fn next_raw(&mut self) -> Result<Option<VersionedPoint>> {
        let Some(Reverse(head)) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(point) = self.sources[head.source].next_operation()? {
            self.heap.push(Reverse(Head {
                point,
                source: head.source,
            }));
        }
        Ok(Some(head.point))
    }
}

pub(crate) struct VisibleCursor {
    pub(super) merged: MergedCursor,
    pub(super) pending: Option<VersionedPoint>,
    pub(super) read_commit: u64,
    pub(super) operations_read: u64,
}

impl VisibleCursor {
    pub fn new(sources: Vec<Box<dyn OperationCursor>>, read_commit: u64) -> Result<Self> {
        Ok(Self {
            merged: MergedCursor::new(sources)?,
            pending: None,
            read_commit,
            operations_read: 0,
        })
    }

    #[must_use]
    pub const fn operations_read(&self) -> u64 {
        self.operations_read
    }

    pub fn next_latest(&mut self) -> Result<Option<VersionedPoint>> {
        let first = match self.pending.take() {
            Some(point) => point,
            None => match self.next_raw()? {
                Some(point) => point,
                None => return Ok(None),
            },
        };
        let key = first.key;
        let mut winner = (first.commit_id <= self.read_commit).then_some(first);
        loop {
            let Some(point) = self.next_raw()? else {
                return Ok(winner);
            };
            if point.key != key {
                self.pending = Some(point);
                return Ok(winner);
            }
            if point.commit_id <= self.read_commit {
                if let Some(current) = winner
                    && (point.commit_id, point.ordinal) == (current.commit_id, current.ordinal)
                    && point != current
                {
                    return Err(corrupt("conflicting operations share one point version"));
                }
                if winner.is_none_or(|current| {
                    (point.commit_id, point.ordinal) > (current.commit_id, current.ordinal)
                }) {
                    winner = Some(point);
                }
            }
        }
    }

    pub fn next_visible(&mut self) -> Result<Option<VersionedPoint>> {
        loop {
            match self.next_latest()? {
                Some(operation) if operation.is_delete => {}
                operation => return Ok(operation),
            }
        }
    }

    pub(super) fn next_raw(&mut self) -> Result<Option<VersionedPoint>> {
        let point = self.merged.next_raw()?;
        if point.is_some() {
            self.operations_read = self.operations_read.saturating_add(1);
        }
        Ok(point)
    }
}
