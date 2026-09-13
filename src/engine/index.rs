//! Индексы опубликованных сегментов и поколений чтения.

use crate::error::{Result, corrupt};
use crate::segment::{OfflinePackDirectory, Segment};
use crate::types::SeriesKey;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct PublishedSegmentIndex {
    pub(super) by_shard: BTreeMap<u16, Arc<ShardSegmentIndex>>,
}

pub(super) struct ShardSegmentIndex {
    pub(super) segments: Vec<Arc<Segment>>,
    pub(super) prefix_max_end_us: Vec<i64>,
}

pub(super) struct SegmentIndexSelection {
    pub(super) segments: Vec<Arc<Segment>>,
    pub(super) entries_examined: usize,
}

impl PublishedSegmentIndex {
    pub(super) fn build(segments: &[Arc<Segment>]) -> Self {
        let mut grouped = BTreeMap::<u16, Vec<Arc<Segment>>>::new();
        for segment in segments {
            grouped
                .entry(segment.descriptor().shard_id)
                .or_default()
                .push(Arc::clone(segment));
        }
        Self {
            by_shard: grouped
                .into_iter()
                .map(|(shard_id, segments)| {
                    (shard_id, Arc::new(ShardSegmentIndex::build(segments)))
                })
                .collect(),
        }
    }

    pub(super) fn with_added(&self, added: &[Arc<Segment>]) -> Self {
        self.with_replaced(&HashSet::new(), added)
    }

    pub(super) fn with_replaced(&self, removed_ids: &HashSet<u64>, added: &[Arc<Segment>]) -> Self {
        let mut affected_shards = added
            .iter()
            .map(|segment| segment.descriptor().shard_id)
            .collect::<BTreeSet<_>>();
        if !removed_ids.is_empty() {
            for (shard_id, shard) in &self.by_shard {
                if shard
                    .segments
                    .iter()
                    .any(|segment| removed_ids.contains(&segment.descriptor().id))
                {
                    affected_shards.insert(*shard_id);
                }
            }
        }
        let mut by_shard = self.by_shard.clone();
        for shard_id in affected_shards {
            let mut segments = by_shard
                .get(&shard_id)
                .map(|shard| {
                    shard
                        .segments
                        .iter()
                        .filter(|segment| !removed_ids.contains(&segment.descriptor().id))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            segments.extend(
                added
                    .iter()
                    .filter(|segment| segment.descriptor().shard_id == shard_id)
                    .cloned(),
            );
            if segments.is_empty() {
                by_shard.remove(&shard_id);
            } else {
                by_shard.insert(shard_id, Arc::new(ShardSegmentIndex::build(segments)));
            }
        }
        Self { by_shard }
    }

    pub(super) fn range(
        &self,
        shard_id: u16,
        from_us: i64,
        to_us: i64,
        series: &[SeriesKey],
    ) -> SegmentIndexSelection {
        self.by_shard
            .get(&shard_id)
            .map_or_else(SegmentIndexSelection::empty, |shard| {
                shard.range(from_us, to_us, series)
            })
    }

    pub(super) fn point(
        &self,
        shard_id: u16,
        series: SeriesKey,
        timestamp_us: i64,
    ) -> SegmentIndexSelection {
        self.by_shard
            .get(&shard_id)
            .map_or_else(SegmentIndexSelection::empty, |shard| {
                shard.point(series, timestamp_us)
            })
    }
}

impl ShardSegmentIndex {
    pub(super) fn build(mut segments: Vec<Arc<Segment>>) -> Self {
        segments.sort_unstable_by_key(|segment| {
            let descriptor = segment.descriptor();
            (
                descriptor.window_start_us,
                descriptor.window_end_us,
                descriptor.policy_id,
                descriptor.level,
                descriptor.id,
            )
        });
        let mut maximum = i64::MIN;
        let prefix_max_end_us = segments
            .iter()
            .map(|segment| {
                maximum = maximum.max(segment.descriptor().window_end_us);
                maximum
            })
            .collect();
        Self {
            segments,
            prefix_max_end_us,
        }
    }

    pub(super) fn range(
        &self,
        from_us: i64,
        to_us: i64,
        series: &[SeriesKey],
    ) -> SegmentIndexSelection {
        let end = self
            .segments
            .partition_point(|segment| segment.descriptor().window_start_us < to_us);
        let start = self.prefix_max_end_us[..end].partition_point(|end_us| *end_us <= from_us);
        let examined = &self.segments[start..end];
        SegmentIndexSelection {
            segments: examined
                .iter()
                .filter(|segment| {
                    segment.descriptor().domain().overlaps(from_us, to_us)
                        && segment.may_contain_any_series(series)
                })
                .cloned()
                .collect(),
            entries_examined: examined.len(),
        }
    }

    pub(super) fn point(&self, series: SeriesKey, timestamp_us: i64) -> SegmentIndexSelection {
        let end = self
            .segments
            .partition_point(|segment| segment.descriptor().window_start_us <= timestamp_us);
        let start = self.prefix_max_end_us[..end].partition_point(|end_us| *end_us <= timestamp_us);
        let examined = &self.segments[start..end];
        SegmentIndexSelection {
            segments: examined
                .iter()
                .filter(|segment| {
                    segment.descriptor().window_end_us > timestamp_us
                        && segment.may_contain_any_series(&[series])
                })
                .cloned()
                .collect(),
            entries_examined: examined.len(),
        }
    }
}

impl SegmentIndexSelection {
    pub(super) const fn empty() -> Self {
        Self {
            segments: Vec::new(),
            entries_examined: 0,
        }
    }
}

type ReadDirectoryScope = (u32, Option<u16>);

#[derive(Clone, Default)]
pub(super) struct PublishedReadDirectoryIndex {
    pub(super) by_scope: BTreeMap<ReadDirectoryScope, Arc<Vec<Arc<OfflinePackDirectory>>>>,
}

impl PublishedReadDirectoryIndex {
    pub(super) fn build(directories: &[Arc<OfflinePackDirectory>]) -> Result<Self> {
        let mut by_scope = BTreeMap::<ReadDirectoryScope, Vec<Arc<OfflinePackDirectory>>>::new();
        for directory in directories {
            let (policy_id, _, _) = directory
                .published_scope()
                .ok_or_else(|| corrupt("published read generation has no scope"))?;
            by_scope
                .entry((policy_id, directory.shard_scope()))
                .or_default()
                .push(Arc::clone(directory));
        }
        Ok(Self {
            by_scope: by_scope
                .into_iter()
                .map(|(policy_id, mut directories)| {
                    directories.sort_unstable_by_key(|directory| {
                        directory
                            .published_scope()
                            .map(|(_, start_us, _)| start_us)
                            .unwrap_or(i64::MIN)
                    });
                    (policy_id, Arc::new(directories))
                })
                .collect(),
        })
    }

    pub(super) fn range(
        &self,
        policy_id: u32,
        shards: &HashSet<u16>,
        from_us: i64,
        to_us: i64,
    ) -> Vec<Arc<OfflinePackDirectory>> {
        std::iter::once(None)
            .chain(shards.iter().copied().map(Some))
            .flat_map(|shard| self.scope_range((policy_id, shard), from_us, to_us))
            .collect()
    }

    fn scope_range(
        &self,
        scope: ReadDirectoryScope,
        from_us: i64,
        to_us: i64,
    ) -> Vec<Arc<OfflinePackDirectory>> {
        let Some(directories) = self.by_scope.get(&scope) else {
            return Vec::new();
        };
        let end = directories.partition_point(|directory| {
            directory
                .published_scope()
                .is_some_and(|(_, start_us, _)| start_us < to_us)
        });
        let start = directories[..end].partition_point(|directory| {
            directory
                .published_scope()
                .is_some_and(|(_, _, end_us)| end_us <= from_us)
        });
        directories[start..end].to_vec()
    }
}
