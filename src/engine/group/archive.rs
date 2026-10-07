//! Exact retained aggregates and the gap before archive publication.

use super::GroupStream;
use super::cursor::{AggregateGroupCursor, SeriesCursor, StoredAggregateCursor};
use crate::error::{Error, Result, corrupt};
use crate::segment::{BlockReadReservation, OperationCursor, PartSource, SliceCursor};
use crate::types::{AggregateResolution, GroupSource, Grouping, SeriesKey};
use std::sync::Arc;

impl GroupStream {
    pub(super) fn append_archives(
        &self,
        series: SeriesKey,
        shard: u16,
        policy: u32,
        raw_from: i64,
        cursors: &mut Vec<SeriesCursor>,
    ) -> Result<()> {
        if self.request.source != GroupSource::Auto
            || self
                .snapshot
                .retained_aggregate_resolutions
                .get(&series)
                .is_none_or(Vec::is_empty)
        {
            return Ok(());
        }
        let Some(cutoff) = self.snapshot.aggregate_retention_cutoffs.get(&series) else {
            return Ok(());
        };
        let from = self.request.from_us.max(*cutoff);
        let to = self.request.to_us.min(raw_from);
        if from >= to {
            return Ok(());
        }
        self.ensure_archived(series, shard, policy, from, to)?;
        for archive in &self.snapshot.aggregate_archives {
            self.query_metrics.check()?;
            let descriptor = archive.descriptor();
            if descriptor.deletion_epoch < self.snapshot.deletions.cutoff(series)
                || descriptor.policy_id != policy
                || descriptor.shard_id != shard
                || descriptor.coverage_from_us >= to
                || descriptor.coverage_to_us <= from
                || descriptor.minimum_series.is_some_and(|key| series < key)
                || descriptor.maximum_series.is_some_and(|key| series > key)
            {
                continue;
            }
            let from = from.max(descriptor.coverage_from_us);
            let to = to.min(descriptor.coverage_to_us);
            // Every chunk of one publication uses the same resolution. A chunk
            // can hold only part of a series/resolution, so None in its page
            // directory means no contribution, not a reason to change resolution.
            let resolution = resolution(self.request.grouping, from, to, &descriptor.resolutions)?;
            cursors.push(SeriesCursor::Aggregates(AggregateGroupCursor {
                series,
                cursors: vec![StoredAggregateCursor::PendingArchive {
                    archive: Arc::clone(archive),
                    series,
                    resolution,
                    from_us: from,
                    to_us: to,
                    budget: Arc::clone(&self.query_metrics),
                }],
                cursor_index: 0,
                pending: None,
                grouping: self.request.grouping,
                metrics: Arc::clone(&self.snapshot.metrics),
                query_metrics: Arc::clone(&self.query_metrics),
                aggregates: self.request.aggregates,
            }));
        }
        Ok(())
    }

    fn ensure_archived(
        &self,
        series: SeriesKey,
        shard: u16,
        policy: u32,
        from: i64,
        to: i64,
    ) -> Result<()> {
        let unavailable = || {
            Error::QueryUnavailable(
                "raw points have expired but their aggregate archive is not published yet".into(),
            )
        };
        // Archive publication and removal of its raw inputs share one root.
        // A visible expired point in this snapshot is therefore still pending.
        for batch in &self.snapshot.batches {
            self.query_metrics.check()?;
            if let Some(mut cursor) = SliceCursor::series(batch, series, from, to) {
                while let Some(point) = cursor.next_operation()? {
                    self.query_metrics.check()?;
                    if point.commit_id <= self.snapshot.commit_id
                        && !self.snapshot.deletions.hides(point)
                    {
                        return Err(unavailable());
                    }
                }
            }
        }
        for segment in self
            .snapshot
            .segments_by_shard
            .get(&shard)
            .into_iter()
            .flatten()
        {
            self.query_metrics.check()?;
            let descriptor = segment.descriptor();
            if descriptor.policy_id != policy
                || descriptor.min_commit > self.snapshot.commit_id
                || descriptor.max_timestamp_us < from
                || descriptor.min_timestamp_us >= to
                || !segment.may_contain_any_series(&[series])
            {
                continue;
            }
            let mut blocks = BlockReadReservation::new(Arc::clone(&self.query_metrics));
            blocks.reserve_source()?;
            let source = PartSource {
                segment: Arc::clone(segment),
                directory: self.snapshot.read_directory(descriptor),
            };
            let mut cursor = source.into_group_cursor(
                series,
                from,
                to,
                self.cache_admission,
                Arc::clone(&self.query_metrics),
            )?;
            while let Some(point) = cursor.next_operation()? {
                self.query_metrics.check()?;
                if point.commit_id <= self.snapshot.commit_id
                    && !self.snapshot.deletions.hides(point)
                {
                    return Err(unavailable());
                }
            }
        }
        Ok(())
    }
}

fn resolution(
    grouping: Grouping,
    from: i64,
    to: i64,
    available: &[AggregateResolution],
) -> Result<AggregateResolution> {
    available
        .iter()
        .rev()
        .copied()
        .find(|resolution| {
            let quantum = resolution.duration_us();
            let aligned = match grouping {
                Grouping::UtcDay => true,
                Grouping::Calendar(_) => grouping.is_utc_day(),
                Grouping::Fixed {
                    duration_us,
                    origin_us,
                } => duration_us % quantum == 0 && origin_us.rem_euclid(quantum) == 0,
            };
            aligned && from.rem_euclid(quantum) == 0 && to.rem_euclid(quantum) == 0
        })
        .or_else(|| available.first().copied())
        .ok_or_else(|| corrupt("archive has no aggregate resolutions"))
}
