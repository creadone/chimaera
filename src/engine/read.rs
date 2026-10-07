//! Public reads, MVCC snapshot capture, and raw point streaming.

use super::batch::OrderedBatchCursor;
use super::catalog::{advance_aggregate_retention_cutoff, advance_retention_cutoff};
use super::group::GroupStream;
use super::regulator::WriteRegulator;
use super::snapshots::SnapshotGuard;
use super::support::{budgeted_sources, duration_nanos, write_for_query};
use super::{Metrics, Snapshot, Storage};
use crate::catalog::SeriesRef;
use crate::error::{Result, invalid};
use crate::persistence::layout::shard_for;
use crate::persistence::lock::StorageLock;
use crate::query::{QueryBudget, QueryOptions, QueryStats};
use crate::segment::{OperationCursor, PartSource, Segment, SliceCursor, VisibleCursor};
use crate::types::{GroupRequest, GroupRow, Point, SeriesKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[cfg(feature = "read-diagnostics")]
use crate::diagnostics::query::timing::{Stage, stage};

/// Materialized grouping results from one snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupResult {
    pub read_commit_id: u64,
    pub rows: Vec<GroupRow>,
    pub stats: QueryStats,
}

/// Metadata from a completed streaming grouping query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupStreamResult {
    pub read_commit_id: u64,
    pub stats: QueryStats,
}

impl Storage {
    /// Capture a consistent snapshot and return a stream of time buckets.
    /// An iteration error ends the stream; previously emitted rows are not
    /// a complete query result. In particular, `QueryUnavailable` means
    /// the retained data cannot currently provide an exact answer.
    ///
    /// # Errors
    ///
    /// Returns invalid-query, segment-open, or decoding errors.
    pub fn group_stream(&self, mut request: GroupRequest) -> Result<GroupStream> {
        self.ensure_available()?;
        request.validate()?;
        request.options.set_default_deadline();
        request.options.check()?;
        let snapshot = self.capture_snapshot(
            &request.series,
            request.from_us,
            request.to_us,
            &request.options,
        )?;
        GroupStream::new(snapshot, request)
    }

    /// Materialize all nonempty time buckets from one snapshot.
    ///
    /// # Errors
    ///
    /// Returns query, read, or aggregate-overflow errors.
    /// `QueryUnavailable` means an archive is not ready or is too coarse;
    /// no partial result is returned on error.
    pub fn group(&self, request: GroupRequest) -> Result<GroupResult> {
        let mut stream = self.group_stream(request)?;
        let read_commit_id = stream.read_commit_id();
        let mut rows = Vec::new();
        let mut memory = stream
            .query_metrics
            .reserve_owned_class(crate::query::QueryMemoryClass::Results, 0)?;
        for row in stream.by_ref() {
            let row = row?;
            memory.push(&mut rows, row)?;
        }
        Ok(GroupResult {
            read_commit_id,
            rows,
            stats: stream.stats(),
        })
    }

    /// Pass time buckets to a consumer without materializing the result.
    ///
    /// # Errors
    ///
    /// Returns read errors or errors returned by the consumer.
    pub fn each_group<F>(&self, request: GroupRequest, mut consumer: F) -> Result<GroupStreamResult>
    where
        F: FnMut(GroupRow) -> Result<()>,
    {
        let mut stream = self.group_stream(request)?;
        let read_commit_id = stream.read_commit_id();
        for row in stream.by_ref() {
            consumer(row?)?;
        }
        Ok(GroupStreamResult {
            read_commit_id,
            stats: stream.stats(),
        })
    }

    /// Read a point at the commit ID captured when the read starts.
    ///
    /// # Errors
    ///
    /// Returns segment-open or data-corruption errors.
    pub fn point(&self, series: SeriesKey, timestamp_us: i64) -> Result<Option<Point>> {
        self.point_with_options(series, timestamp_us, QueryOptions::default())
    }

    /// Read a point with a deadline, cancellation, and query limits.
    ///
    /// # Errors
    /// Returns admission, query, segment, or snapshot-capture errors.
    pub fn point_with_options(
        &self,
        series: SeriesKey,
        timestamp_us: i64,
        options: QueryOptions,
    ) -> Result<Option<Point>> {
        let budget =
            QueryBudget::new_with_admission(options, Arc::clone(&self.inner.work_memory_governor))?;
        self.ensure_available()?;
        let _storage_lock = self.clone_storage_lock()?;
        let shard_id = shard_for(series, self.inner.options.virtual_shards);
        let observed_now_us = self.inner.options.clock.now_us();
        let mut memory = budget.reserve_owned(1024)?;
        let snapshot = {
            #[cfg(feature = "read-diagnostics")]
            let waiting = stage(Stage::SnapshotWait);
            let mut state = write_for_query(&self.inner.state, || budget.check())?;
            #[cfg(feature = "read-diagnostics")]
            drop(waiting);
            #[cfg(feature = "read-diagnostics")]
            let _held = stage(Stage::SnapshotHold);
            state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
            let now_us = state.maintenance_now_us;
            let retention_cutoff = state
                .catalog
                .registered_policy(series)
                .cloned()
                .and_then(|policy| advance_retention_cutoff(&mut state, &policy, now_us));
            if retention_cutoff.is_some_and(|cutoff| timestamp_us < cutoff) {
                return Ok(None);
            }
            let mut batches = Vec::new();
            for batch in state
                .active_batches
                .iter()
                .chain(state.frozen.iter().flat_map(|g| &g.batches))
            {
                memory.push(&mut batches, Arc::clone(batch))?;
            }
            let policy = state.catalog.registered_policy(series).map_or(1, |p| p.id);
            (
                state.visible_commit,
                batches,
                state.files.clone(),
                policy,
                Arc::clone(&state.deletions),
            )
        };
        // The root lease and memtable generation were captured under one lock.
        // Catalogue I/O and payload opens happen after releasing State.
        let file_view = snapshot.2;
        let selection_started = Instant::now();
        let selection = file_view.range(
            snapshot.3,
            shard_id,
            timestamp_us,
            timestamp_us.saturating_add(1),
            &[series],
            &mut memory,
        )?;
        self.record_segment_index_selection(
            1,
            selection.entries_examined,
            selection.segments.len(),
            selection_started.elapsed(),
        );
        let snapshot = (snapshot.0, snapshot.1, selection.segments, snapshot.4);
        memory
            .grow((snapshot.1.len().saturating_add(snapshot.2.len()) as u64).saturating_mul(256))?;
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        for batch in snapshot.1 {
            if let Some(cursor) = SliceCursor::point(batch, series, timestamp_us) {
                sources.push(Box::new(cursor));
            }
        }
        for segment in snapshot.2 {
            if let Some(cursor) =
                segment.point_cursor_for_query(series, timestamp_us, Arc::clone(&budget))?
            {
                sources.push(cursor);
            }
        }
        let mut visible = VisibleCursor::new(budgeted_sources(sources, &budget), snapshot.0)?
            .with_deletions(snapshot.3);
        let point = visible.next_visible()?;
        self.inner
            .metrics
            .query_operations_scanned
            .fetch_add(visible.operations_read(), Ordering::Relaxed);
        self.inner
            .metrics
            .query_points_returned
            .fetch_add(u64::from(point.is_some()), Ordering::Relaxed);
        match point {
            Some(point) => Ok(Some(Point {
                series: point.key.series,
                timestamp_us: point.key.timestamp_us,
                value: point.put_value()?,
            })),
            None => Ok(None),
        }
    }

    /// Read a named series point using a consistent current snapshot.
    ///
    /// # Errors
    ///
    /// Returns catalog, segment-open, or data-corruption errors.
    pub fn point_named(&self, series: &SeriesRef, timestamp_us: i64) -> Result<Option<Point>> {
        let series = self.resolve_series(series)?.0;
        self.point(series, timestamp_us)
    }

    /// Capture a consistent snapshot and stream visible points in
    /// `[from_us, to_us)` in ascending timestamp order.
    ///
    /// # Errors
    ///
    /// Returns errors for an empty or reversed range, segment-open failures, or
    /// data corruption.
    pub fn scan(&self, series: SeriesKey, from_us: i64, to_us: i64) -> Result<PointStream> {
        self.scan_with_options(series, from_us, to_us, QueryOptions::default())
    }

    /// Scan raw points with one deadline and cancellation for planning, I/O, and delivery.
    ///
    /// # Errors
    /// Returns range, deadline, cancellation, resource-limit, or read errors.
    pub fn scan_with_options(
        &self,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        mut options: QueryOptions,
    ) -> Result<PointStream> {
        options.set_default_deadline();
        options.check()?;
        self.ensure_available()?;
        if from_us >= to_us {
            return Err(invalid("scan range must be non-empty"));
        }
        let snapshot = self.capture_snapshot(&[series], from_us, to_us, &options)?;
        PointStream::new(snapshot, series, from_us, to_us, options)
    }

    /// Return a lazy point stream for a named series.
    ///
    /// # Errors
    ///
    /// Returns catalog, range, segment, or data-corruption errors.
    pub fn scan_named(&self, series: &SeriesRef, from_us: i64, to_us: i64) -> Result<PointStream> {
        let series = self.resolve_series(series)?.0;
        self.scan(series, from_us, to_us)
    }

    pub(super) fn capture_snapshot(
        &self,
        requested_series: &[SeriesKey],
        from_us: i64,
        to_us: i64,
        options: &QueryOptions,
    ) -> Result<Snapshot> {
        let storage_lock = self.clone_storage_lock()?;
        let budget = QueryBudget::new_with_admission(
            options.clone(),
            Arc::clone(&self.inner.work_memory_governor),
        )?;
        // Hash tables (including spare buckets), shard nodes, policy IDs and
        // temporary grouping collections. Bound this before allocating any of them.
        let mut memory =
            budget.reserve_owned((requested_series.len() as u64).saturating_mul(1024))?;
        let mut requested_series_by_shard = BTreeMap::<u16, Vec<SeriesKey>>::new();
        for series in requested_series {
            options.check()?;
            requested_series_by_shard
                .entry(shard_for(*series, self.inner.options.virtual_shards))
                .or_default()
                .push(*series);
        }
        let requested_shards = requested_series_by_shard
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        let observed_now_us = self.inner.options.clock.now_us();
        #[cfg(feature = "read-diagnostics")]
        let waiting = stage(Stage::SnapshotWait);
        let mut state = write_for_query(&self.inner.state, || options.check())?;
        #[cfg(feature = "read-diagnostics")]
        drop(waiting);
        #[cfg(feature = "read-diagnostics")]
        let _held = stage(Stage::SnapshotHold);
        state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
        let now_us = state.maintenance_now_us;
        let mut retention_cutoffs = HashMap::new();
        let mut aggregate_retention_cutoffs = HashMap::new();
        let mut retained_aggregate_resolutions = HashMap::new();
        let mut series_policy_ids = HashMap::new();
        for series in requested_series {
            options.check()?;
            if let Some(policy) = state.catalog.registered_policy(*series) {
                memory.grow(
                    (policy.retained_aggregate_resolutions.len() as u64)
                        .saturating_mul(
                            std::mem::size_of::<crate::types::AggregateResolution>() as u64
                        )
                        .saturating_mul(2),
                )?;
            }
            let policy = state.catalog.registered_policy(*series).cloned();
            series_policy_ids.insert(*series, policy.as_ref().map_or(1, |policy| policy.id));
            if let Some(policy) = policy {
                series_policy_ids.insert(*series, policy.id);
                if let Some(cutoff) = advance_retention_cutoff(&mut state, &policy, now_us) {
                    retention_cutoffs.insert(*series, cutoff);
                }
                if let Some(cutoff) =
                    advance_aggregate_retention_cutoff(&mut state, &policy, now_us)
                {
                    aggregate_retention_cutoffs.insert(*series, cutoff);
                    retained_aggregate_resolutions
                        .insert(*series, policy.retained_aggregate_resolutions.clone());
                }
            }
        }
        let mut batches = Vec::new();
        let mut pinned_memtable_generations = 0_usize;
        for generation in
            std::iter::once(&state.active_batches).chain(state.frozen.iter().map(|g| &g.batches))
        {
            let before = batches.len();
            for batch in generation {
                if batch.overlaps_time(from_us, to_us) {
                    memory.push(&mut batches, Arc::clone(batch))?;
                }
            }
            pinned_memtable_generations += usize::from(batches.len() != before);
        }
        let deletions = Arc::clone(&state.deletions);
        let commit_id = state.visible_commit;
        let file_view = state.files.clone();
        drop(state);
        #[cfg(feature = "read-diagnostics")]
        drop(_held);
        let selection_started = Instant::now();
        let mut segments_by_shard = BTreeMap::<u16, Vec<Arc<Segment>>>::new();
        let mut entries_examined = 0_usize;
        let mut candidates = 0_usize;
        for (shard_id, series) in &requested_series_by_shard {
            let policies = series
                .iter()
                .map(|key| series_policy_ids[key])
                .collect::<BTreeSet<_>>();
            for policy in policies {
                options.check()?;
                let selected_series = series
                    .iter()
                    .copied()
                    .filter(|key| series_policy_ids[key] == policy)
                    .collect::<Vec<_>>();
                let selection = file_view.range(
                    policy,
                    *shard_id,
                    from_us,
                    to_us,
                    &selected_series,
                    &mut memory,
                )?;
                entries_examined = entries_examined.saturating_add(selection.entries_examined);
                candidates = candidates.saturating_add(selection.segments.len());
                if !selection.segments.is_empty() {
                    segments_by_shard
                        .entry(*shard_id)
                        .or_default()
                        .extend(selection.segments);
                }
            }
        }
        self.record_segment_index_selection(
            requested_shards.len(),
            entries_examined,
            candidates,
            selection_started.elapsed(),
        );
        let pinned_segments: usize = segments_by_shard.values().map(Vec::len).sum();
        let mut aggregate_archives = Vec::new();
        for policy in series_policy_ids.values().copied().collect::<BTreeSet<_>>() {
            for shard in &requested_shards {
                file_view.archives(policy, *shard, from_us, to_us, Some(&budget), |d| {
                    memory.grow(1024)?;
                    memory.push(
                        &mut aggregate_archives,
                        Arc::new(crate::aggregate::archive::AggregateArchive::open_for_query(
                            &file_view.resources.root,
                            file_view.resources.uuid,
                            d,
                            file_view.resources.file_pool.clone(),
                            Some(&budget),
                        )?),
                    )
                })?;
            }
        }
        let guard = Some(Arc::new(self.inner.snapshot_tracker.register(
            pinned_memtable_generations,
            pinned_segments.saturating_add(aggregate_archives.len()),
        )?));
        Ok(Snapshot {
            deletions,
            file_view: Some(file_view),
            commit_id,
            virtual_shards: self.inner.options.virtual_shards,
            batches,
            segments_by_shard,
            retention_cutoffs,
            aggregate_retention_cutoffs,
            retained_aggregate_resolutions,
            series_policy_ids,
            aggregate_archives,
            metrics: Arc::clone(&self.inner.metrics),
            offline_pack_directory: self.inner.offline_pack_directory.clone(),
            guard,
            write_regulator: Some(Arc::clone(&self.inner.write_regulator)),
            query_executor: Arc::clone(&self.inner.query_executor),
            work_memory_governor: Arc::clone(&self.inner.work_memory_governor),
            storage_lock: Some(storage_lock),
            memory: Some(memory),
        })
    }

    fn record_segment_index_selection(
        &self,
        lookups: usize,
        entries_examined: usize,
        candidates: usize,
        duration: Duration,
    ) {
        self.inner.metrics.query_segment_index_lookups.fetch_add(
            u64::try_from(lookups).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner
            .metrics
            .query_segment_index_entries_examined
            .fetch_add(
                u64::try_from(entries_examined).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        self.inner.metrics.query_segment_index_candidates.fetch_add(
            u64::try_from(candidates).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner
            .metrics
            .query_segment_index_nanos
            .fetch_add(duration_nanos(duration), Ordering::Relaxed);
    }
}

/// A lazy stream of one series at a consistent snapshot.
pub struct PointStream {
    pub(super) read_commit_id: u64,
    pub(super) visible: VisibleCursor,
    pub(super) query_budget: Arc<QueryBudget>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) reported_operations: u64,
    pub(super) returned_points: u64,
    pub(super) reported_points: u64,
    pub(super) failed: bool,
    pub(super) _snapshot_guard: Option<Arc<SnapshotGuard>>,
    pub(super) write_regulator: Option<Arc<WriteRegulator>>,
    pub(super) read_nanos: u64,
    pub(super) _storage_lock: Option<Arc<StorageLock>>,
    source_memory: Option<crate::query::QueryReservation>,
    file_view: Option<super::files::View>,
}

impl PointStream {
    pub(super) fn new(
        mut snapshot: Snapshot,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        options: QueryOptions,
    ) -> Result<Self> {
        let started = Instant::now();
        let mut stream =
            Self::from_snapshot_with_options(&snapshot, series, from_us, to_us, options)?;
        stream.read_nanos = duration_nanos(started.elapsed());
        stream._snapshot_guard = snapshot.guard.take();
        stream._storage_lock = snapshot.storage_lock.take();
        Ok(stream)
    }

    pub(super) fn from_snapshot(
        snapshot: &Snapshot,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
    ) -> Result<Self> {
        Self::from_snapshot_with_options(snapshot, series, from_us, to_us, QueryOptions::default())
    }

    fn from_snapshot_with_options(
        snapshot: &Snapshot,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        options: QueryOptions,
    ) -> Result<Self> {
        let query_budget = match &snapshot.memory {
            Some(memory) => Arc::clone(memory.budget()),
            None => QueryBudget::new_with_admission(
                options,
                Arc::clone(&snapshot.work_memory_governor),
            )?,
        };
        let from_us = snapshot
            .retention_cutoffs
            .get(&series)
            .copied()
            .map_or(from_us, |cutoff| from_us.max(cutoff));
        let source_count = snapshot.batches.len().saturating_add(
            snapshot
                .segments_by_shard
                .values()
                .map(Vec::len)
                .sum::<usize>(),
        );
        let source_memory =
            query_budget.reserve_owned((source_count as u64).saturating_mul(256))?;
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        if from_us < to_us {
            let batch_cursors = snapshot
                .batches
                .iter()
                .filter_map(|batch| SliceCursor::series(batch, series, from_us, to_us))
                .collect::<Vec<_>>();
            match OrderedBatchCursor::try_new(batch_cursors, Arc::clone(&query_budget)) {
                Ok(cursor) => sources.push(Box::new(cursor)),
                Err(cursors) => sources.extend(
                    cursors
                        .into_iter()
                        .map(|cursor| Box::new(cursor) as Box<dyn OperationCursor>),
                ),
            }
            let shard_id = shard_for(series, snapshot.virtual_shards);
            let mut reads = Vec::new();
            let mut reads_memory = query_budget.reserve_owned(0)?;
            for segment in snapshot
                .segments_by_shard
                .get(&shard_id)
                .into_iter()
                .flatten()
            {
                query_budget.check()?;
                if segment.descriptor().min_commit > snapshot.commit_id
                    || segment.descriptor().max_timestamp_us < from_us
                    || segment.descriptor().min_timestamp_us >= to_us
                    || !segment.may_contain_any_series(&[series])
                {
                    continue;
                }
                reads_memory.push(
                    &mut reads,
                    PartSource {
                        segment: Arc::clone(segment),
                        directory: snapshot.read_directory(segment.descriptor()),
                    },
                )?;
            }
            for read in reads {
                sources.push(Box::new(read.into_group_cursor(
                    series,
                    from_us,
                    to_us,
                    crate::memory::cache::BlockCacheAdmission::Always,
                    Arc::clone(&query_budget),
                )?));
            }
        }
        Ok(Self {
            read_commit_id: snapshot.commit_id,
            visible: VisibleCursor::new(
                budgeted_sources(sources, &query_budget),
                snapshot.commit_id,
            )?
            .with_deletions(Arc::clone(&snapshot.deletions)),
            query_budget,
            metrics: Arc::clone(&snapshot.metrics),
            reported_operations: 0,
            returned_points: 0,
            reported_points: 0,
            failed: false,
            _snapshot_guard: None,
            write_regulator: snapshot.write_regulator.as_ref().map(Arc::clone),
            read_nanos: 0,
            _storage_lock: snapshot.storage_lock.as_ref().map(Arc::clone),
            source_memory: Some(source_memory),
            file_view: snapshot.file_view.clone(),
        })
    }

    /// Commit ID captured when the stream was created.
    #[must_use]
    pub const fn read_commit_id(&self) -> u64 {
        self.read_commit_id
    }

    fn release_sources(&mut self) {
        self.visible.release_sources();
        self.source_memory = None;
        self.file_view = None;
        self._snapshot_guard = None;
        self._storage_lock = None;
        let _ = self.query_budget.release_local_metadata();
        if Arc::strong_count(&self.query_budget) == 1 {
            self.query_budget.release_remaining_memory();
        }
    }

    pub(super) fn publish_metrics(&mut self) {
        let scanned = self
            .visible
            .operations_read()
            .saturating_sub(self.reported_operations);
        let returned = self.returned_points.saturating_sub(self.reported_points);
        self.metrics
            .query_operations_scanned
            .fetch_add(scanned, Ordering::Relaxed);
        self.metrics
            .query_points_returned
            .fetch_add(returned, Ordering::Relaxed);
        self.reported_operations = self.visible.operations_read();
        self.reported_points = self.returned_points;
    }
}

impl Iterator for PointStream {
    type Item = Result<Point>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let started = Instant::now();
        let result = match self
            .query_budget
            .check()
            .and_then(|()| self.visible.next_visible())
        {
            Ok(Some(point)) => {
                if let Err(error) = self
                    .query_budget
                    .record_points(1)
                    .and_then(|()| self.query_budget.record_result_row())
                {
                    self.failed = true;
                    self.publish_metrics();
                    self.release_sources();
                    return Some(Err(error));
                }
                self.returned_points = self.returned_points.saturating_add(1);
                self.publish_metrics();
                Some(point.put_value().map(|value| Point {
                    series: point.key.series,
                    timestamp_us: point.key.timestamp_us,
                    value,
                }))
            }
            Ok(None) => {
                self.failed = true;
                self.publish_metrics();
                None
            }
            Err(error) => {
                self.failed = true;
                self.publish_metrics();
                Some(Err(error))
            }
        };
        if result.as_ref().is_some_and(Result::is_err) {
            self.failed = true;
        }
        if self.failed {
            self.release_sources();
        }
        self.read_nanos = self
            .read_nanos
            .saturating_add(duration_nanos(started.elapsed()));
        result
    }
}

impl Drop for PointStream {
    fn drop(&mut self) {
        self.publish_metrics();
        if let Some(regulator) = &self.write_regulator {
            regulator.record_read(Duration::from_nanos(self.read_nanos), false);
        }
    }
}
