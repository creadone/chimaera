//! Sorted-cursor merging and snapshot commit filtering.

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
    previous: Option<crate::types::PointKey>,
    deletions: Option<std::sync::Arc<crate::deletion::Deletions>>,
    deletion_series: Option<crate::types::SeriesKey>,
    deletion_cutoff: u64,
    pub(super) read_commit: u64,
    pub(super) operations_read: u64,
}

impl VisibleCursor {
    pub(crate) fn release_sources(&mut self) {
        self.merged.sources = Vec::new();
        self.merged.heap = BinaryHeap::new();
        self.deletions = None;
    }

    pub fn new(sources: Vec<Box<dyn OperationCursor>>, read_commit: u64) -> Result<Self> {
        Ok(Self {
            merged: MergedCursor::new(sources)?,
            previous: None,
            deletions: None,
            deletion_series: None,
            deletion_cutoff: 0,
            read_commit,
            operations_read: 0,
        })
    }

    pub(crate) fn with_deletions(
        mut self,
        deletions: std::sync::Arc<crate::deletion::Deletions>,
    ) -> Self {
        if !deletions.entries.is_empty() {
            self.deletions = Some(deletions);
        }
        self
    }

    #[must_use]
    pub const fn operations_read(&self) -> u64 {
        self.operations_read
    }

    pub fn next_visible(&mut self) -> Result<Option<VersionedPoint>> {
        while let Some(point) = self.next_raw()? {
            if let Some(deletions) = &self.deletions {
                if self.deletion_series != Some(point.key.series) {
                    self.deletion_series = Some(point.key.series);
                    self.deletion_cutoff = deletions.cutoff(point.key.series);
                }
                // Erased incarnations may reuse timestamps. Discard them before
                // validating the strict ordering of the surviving incarnation.
                if point.commit_id <= self.deletion_cutoff {
                    continue;
                }
            }

            if self.previous.is_some_and(|key| point.key <= key) {
                return Err(corrupt(
                    "immutable sources contain a duplicate or decreasing point",
                ));
            }
            self.previous = Some(point.key);
            if point.commit_id <= self.read_commit {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }

    pub(super) fn next_raw(&mut self) -> Result<Option<VersionedPoint>> {
        let point = self.merged.next_raw()?;
        if point.is_some() {
            self.operations_read = self.operations_read.saturating_add(1);
        }
        Ok(point)
    }
}

pub(crate) fn filter_deletions(
    source: Box<dyn OperationCursor>,
    deletions: std::sync::Arc<crate::deletion::Deletions>,
) -> Box<dyn OperationCursor> {
    if deletions.entries.is_empty() {
        return source;
    }
    Box::new(DeletedCursor {
        source,
        deletions,
        series: None,
        cutoff: 0,
    })
}

struct DeletedCursor {
    source: Box<dyn OperationCursor>,
    deletions: std::sync::Arc<crate::deletion::Deletions>,
    series: Option<crate::types::SeriesKey>,
    cutoff: u64,
}
impl OperationCursor for DeletedCursor {
    fn next_operation(&mut self) -> Result<Option<VersionedPoint>> {
        while let Some(point) = self.source.next_operation()? {
            if self.series != Some(point.key.series) {
                self.series = Some(point.key.series);
                self.cutoff = self.deletions.cutoff(point.key.series);
            }
            if point.commit_id > self.cutoff {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }
}
