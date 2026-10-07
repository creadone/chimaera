//! Open one series at a time; immutable parts contribute additive groups.
use super::cursor::{CombinedGroupCursor, SeriesCursor, SeriesGroupCursor};
use super::part::PartCursor;
use super::{GroupStream, ParallelSeriesPipeline};
use crate::engine::Snapshot;
use crate::error::Result;
use crate::memory::cache::BlockCacheAdmission;
use crate::persistence::layout::shard_for;
use crate::query::QueryBudget;
use crate::segment::{
    BlockReadReservation, OperationCursor, PartSource, SliceCursor, VisibleCursor,
};
use crate::types::{GroupRequest, GroupScope, GroupSource};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

impl GroupStream {
    pub(in crate::engine) fn new(snapshot: Snapshot, mut request: GroupRequest) -> Result<Self> {
        request.validate()?;
        let query_metrics = match &snapshot.memory {
            Some(memory) => Arc::clone(memory.budget()),
            None => QueryBudget::new_with_admission(
                request.options.clone(),
                Arc::clone(&snapshot.work_memory_governor),
            )?,
        };
        let request_memory =
            Some(query_metrics.reserve_owned((request.series.len() as u64).saturating_mul(32))?);
        request.series.sort_unstable();
        let parallel_daily = request.scope == GroupScope::PerKey
            && request.series.len() >= 32
            && snapshot.query_executor.is_parallel();
        let parallel_pipeline = parallel_daily
            .then(|| ParallelSeriesPipeline::new(snapshot.query_executor.worker_count));
        let cache_admission = if request.series.len() == 1 {
            BlockCacheAdmission::Always
        } else {
            BlockCacheAdmission::Repeated
        };
        Ok(Self {
            snapshot,
            request,
            query_metrics,
            request_memory,
            cache_admission,
            parallel_daily,
            parallel_pipeline,
            parallel_rows: VecDeque::new(),
            parallel_buffer_bytes: 0,
            series_index: 0,
            current: None,
            combined: None,
            combined_initialized: false,
            reported_query_resources: false,
            finished: false,
            failed: false,
        })
    }

    pub(super) fn open_next_series(&mut self, _keep_file_open: bool) -> Result<bool> {
        self.query_metrics.check()?;
        let Some(series) = self.request.series.get(self.series_index).copied() else {
            return Ok(false);
        };
        self.series_index += 1;
        let started = Instant::now();
        let from = self.request.from_us.max(
            self.snapshot
                .retention_cutoffs
                .get(&series)
                .copied()
                .unwrap_or(i64::MIN),
        );
        let to = self.request.to_us;
        let shard = shard_for(series, self.snapshot.virtual_shards);
        let policy = self
            .snapshot
            .series_policy_ids
            .get(&series)
            .copied()
            .unwrap_or(1);
        let auto = self.request.source == GroupSource::Auto;
        let mut reservation = BlockReadReservation::new(Arc::clone(&self.query_metrics));
        let mut reads = Vec::new();
        let mut examined = 0;
        let mut lookups = 0;
        if from < to {
            for segment in self
                .snapshot
                .segments_by_shard
                .get(&shard)
                .into_iter()
                .flatten()
            {
                self.query_metrics.check()?;
                examined += 1;
                let descriptor = segment.descriptor();
                if descriptor.max_commit <= self.snapshot.deletions.cutoff(series)
                    || descriptor.min_commit > self.snapshot.commit_id
                    || descriptor.max_timestamp_us < from
                    || descriptor.min_timestamp_us >= to
                    || !segment.may_contain_any_series(&[series])
                {
                    continue;
                }
                reservation.reserve_source()?;
                reads.push(PartSource {
                    segment: Arc::clone(segment),
                    directory: self.snapshot.read_directory(descriptor),
                });
                lookups += 1;
            }
        }
        self.query_metrics.record_part_reads(reads.len() as u64);
        let reservation = Arc::new(reservation);
        let memory = CombinedGroupCursor::reserve(
            reads
                .len()
                .saturating_add(self.snapshot.aggregate_archives.len())
                .saturating_add(1),
            &self.query_metrics,
        )?;
        let mut cursors =
            Vec::with_capacity(reads.len() + self.snapshot.aggregate_archives.len() + 1);
        for read in reads {
            cursors.push(SeriesCursor::Part(Box::new(PartCursor::new(
                read,
                series,
                from,
                to,
                self.request.grouping,
                self.request.aggregates,
                auto,
                self.snapshot.commit_id,
                self.snapshot.deletions.cutoff(series),
                self.cache_admission,
                Arc::clone(&self.query_metrics),
                Arc::clone(&self.snapshot.metrics),
                Arc::clone(&reservation),
            )?)));
        }
        let memory_started = Instant::now();
        let mut memory_cursors = 0;
        let _batch_memory = self.query_metrics.reserve_owned(
            (self.snapshot.batches.len() as u64)
                .saturating_mul((std::mem::size_of::<SliceCursor>() + 32) as u64),
        )?;
        if from < to {
            let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
            for batch in &self.snapshot.batches {
                if let Some(cursor) = SliceCursor::series(batch, series, from, to) {
                    memory_cursors += 1;
                    sources.push(Box::new(cursor));
                }
            }
            if !sources.is_empty() {
                cursors.push(SeriesCursor::Points(SeriesGroupCursor {
                    series,
                    visible: VisibleCursor::new(sources, self.snapshot.commit_id)?
                        .with_deletions(Arc::clone(&self.snapshot.deletions)),
                    pending: None,
                    grouping: self.request.grouping,
                    aggregates: self.request.aggregates,
                    metrics: Arc::clone(&self.snapshot.metrics),
                    query_metrics: Arc::clone(&self.query_metrics),
                    _memory: _batch_memory,
                    reported_operations: 0,
                    returned_points: 0,
                    reported_points: 0,
                }));
            }
        }
        self.query_metrics.record_memory_sources(
            if from < to {
                self.snapshot.batches.len()
            } else {
                0
            },
            memory_cursors,
            memory_started.elapsed(),
        );
        self.append_archives(series, shard, policy, from, &mut cursors)?;
        self.current = Some(SeriesCursor::Merged(CombinedGroupCursor::new(
            cursors,
            Some(series),
            memory,
        )));
        self.query_metrics
            .record_source_open(examined, lookups, started.elapsed());
        Ok(true)
    }
}
