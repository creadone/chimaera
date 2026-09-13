//! Публичное чтение, фиксация MVCC-снимка и поток исходных точек.

use super::batch::OrderedBatchCursor;
use super::catalog::{advance_aggregate_retention_cutoff, advance_retention_cutoff};
use super::group::GroupStream;
use super::regulator::WriteRegulator;
use super::snapshots::SnapshotGuard;
use super::support::{budgeted_sources, duration_nanos, write_for_query};
use super::{Metrics, Snapshot, Storage};
use crate::catalog::{SeriesRef, WriteMode};
use crate::error::{Result, invalid};
use crate::layout::shard_for;
use crate::query::{QueryBudget, QueryOptions, QueryStats};
use crate::segment::{OperationCursor, Segment, SliceCursor, VisibleCursor};
use crate::storage_lock::StorageLock;
use crate::types::{GroupRequest, GroupRow, Point, SeriesKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Полностью материализованный результат группировки одного снимка.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupResult {
    pub read_commit_id: u64,
    pub rows: Vec<GroupRow>,
    pub stats: QueryStats,
}

/// Метаданные завершённой потоковой группировки.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupStreamResult {
    pub read_commit_id: u64,
    pub stats: QueryStats,
}

impl Storage {
    /// Фиксирует согласованный снимок и возвращает поток временных корзин.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку неверного запроса, открытия или декодирования сегмента.
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

    /// Материализует все непустые временные корзины одного снимка.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку запроса, чтения либо переполнения агрегата.
    pub fn group(&self, request: GroupRequest) -> Result<GroupResult> {
        let mut stream = self.group_stream(request)?;
        let read_commit_id = stream.read_commit_id();
        let mut rows = Vec::new();
        while let Some(row) = stream.next() {
            let row = row?;
            stream
                .query_metrics
                .reserve_memory(std::mem::size_of::<GroupRow>() as u64)?;
            rows.push(row);
        }
        Ok(GroupResult {
            read_commit_id,
            rows,
            stats: stream.stats(),
        })
    }

    /// Передаёт временные корзины потребителю без материализации результата.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку чтения или ошибку, которую вернул потребитель.
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

    /// Возвращает точку, видимую на commit ID, зафиксированном в начале чтения.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку открытия сегмента или повреждения данных.
    pub fn point(&self, series: SeriesKey, timestamp_us: i64) -> Result<Option<Point>> {
        self.point_with_options(series, timestamp_us, QueryOptions::default())
    }

    /// Точечное чтение с deadline, отменой и ограничениями запроса.
    ///
    /// # Errors
    /// Отказ admission, ошибка запроса, сегмента или WAL-снимка.
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
        let snapshot = {
            let mut state = write_for_query(&self.inner.state, || budget.check())?;
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
            let mut batches = state.active_batches.clone();
            for generation in &state.frozen {
                batches.extend(generation.batches.iter().cloned());
            }
            let selection_started = Instant::now();
            let selection = state.segment_index.point(shard_id, series, timestamp_us);
            let entries_examined = selection.entries_examined;
            let candidates = selection.segments.len();
            let segments = selection
                .segments
                .into_iter()
                .filter(|segment| {
                    let descriptor = segment.descriptor();
                    descriptor.min_commit <= state.visible_commit
                        && descriptor.min_timestamp_us <= timestamp_us
                        && descriptor.max_timestamp_us >= timestamp_us
                })
                .collect::<Vec<_>>();
            self.record_segment_index_selection(
                1,
                entries_examined,
                candidates,
                selection_started.elapsed(),
            );
            (state.visible_commit, batches, segments)
        };
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
        let mut visible = VisibleCursor::new(budgeted_sources(sources, &budget), snapshot.0)?;
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

    /// Возвращает точку именованного ряда на текущем согласованном снимке.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, открытия сегмента или повреждения данных.
    pub fn point_named(&self, series: &SeriesRef, timestamp_us: i64) -> Result<Option<Point>> {
        let series = self.resolve_series(series)?.0;
        self.point(series, timestamp_us)
    }

    /// Фиксирует согласованный снимок и возвращает поток видимых точек диапазона
    /// `[from_us, to_us)` по возрастанию timestamp.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для пустого диапазона, ошибки открытия сегмента или
    /// повреждения данных.
    pub fn scan(&self, series: SeriesKey, from_us: i64, to_us: i64) -> Result<PointStream> {
        self.scan_with_options(series, from_us, to_us, QueryOptions::default())
    }

    /// Raw scan с общим deadline и отменой для планирования, I/O и выдачи точек.
    ///
    /// # Errors
    /// Ошибка диапазона, deadline/cancellation, ограничения ресурсов или чтения.
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

    /// Возвращает ленивый поток точек именованного ряда.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку каталога, диапазона, сегмента или повреждения данных.
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
        let mut state = write_for_query(&self.inner.state, || options.check())?;
        state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
        let now_us = state.maintenance_now_us;
        let mut retention_cutoffs = HashMap::new();
        let mut aggregate_retention_cutoffs = HashMap::new();
        let mut retained_aggregate_resolutions = HashMap::new();
        let mut series_policy_ids = HashMap::new();
        let mut strictly_increasing_series = HashSet::new();
        for series in requested_series {
            options.check()?;
            let policy = state.catalog.registered_policy(*series).cloned();
            if let Some(policy) = policy {
                series_policy_ids.insert(*series, policy.id);
                if policy.write_mode == WriteMode::StrictlyIncreasing {
                    strictly_increasing_series.insert(*series);
                }
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
        let mut batches = state.active_batches.clone();
        for generation in &state.frozen {
            batches.extend(generation.batches.iter().cloned());
        }
        let selection_started = Instant::now();
        let mut segments_by_shard = BTreeMap::<u16, Vec<Arc<Segment>>>::new();
        let mut entries_examined = 0_usize;
        let mut candidates = 0_usize;
        for (shard_id, series) in &requested_series_by_shard {
            options.check()?;
            let selection = state.segment_index.range(*shard_id, from_us, to_us, series);
            entries_examined = entries_examined.saturating_add(selection.entries_examined);
            candidates = candidates.saturating_add(selection.segments.len());
            if !selection.segments.is_empty() {
                segments_by_shard.insert(*shard_id, selection.segments);
            }
        }
        self.record_segment_index_selection(
            requested_shards.len(),
            entries_examined,
            candidates,
            selection_started.elapsed(),
        );
        let pinned_memtable_generations =
            usize::from(!state.active_batches.is_empty()).saturating_add(state.frozen.len());
        let pinned_segments: usize = segments_by_shard.values().map(Vec::len).sum();
        let aggregate_archives = state
            .aggregate_archives
            .iter()
            .filter(|archive| {
                requested_shards.contains(&archive.descriptor().shard_id)
                    && archive.descriptor().coverage_from_us < to_us
                    && archive.descriptor().coverage_to_us > from_us
            })
            .cloned()
            .collect::<Vec<_>>();
        let published_read_directories = series_policy_ids
            .values()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .flat_map(|policy_id| {
                state.published_read_directory_index.range(
                    policy_id,
                    &requested_shards,
                    from_us,
                    to_us,
                )
            })
            .collect::<Vec<_>>();
        let guard = Some(Arc::new(self.inner.snapshot_tracker.register(
            pinned_memtable_generations,
            pinned_segments.saturating_add(aggregate_archives.len()),
        )?));
        Ok(Snapshot {
            commit_id: state.visible_commit,
            virtual_shards: self.inner.options.virtual_shards,
            batches,
            segments_by_shard,
            retention_cutoffs,
            aggregate_retention_cutoffs,
            retained_aggregate_resolutions,
            series_policy_ids,
            strictly_increasing_series,
            aggregate_archives,
            published_read_directories,
            metrics: Arc::clone(&self.inner.metrics),
            open_file_cache: Arc::clone(&self.inner.open_file_cache),
            offline_pack_directory: self.inner.offline_pack_directory.clone(),
            guard,
            write_regulator: Some(Arc::clone(&self.inner.write_regulator)),
            query_executor: Arc::clone(&self.inner.query_executor),
            work_memory_governor: Arc::clone(&self.inner.work_memory_governor),
            storage_lock: Some(storage_lock),
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

/// Ленивый поток точек одного ряда на согласованном снимке.
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
        let query_budget =
            QueryBudget::new_with_admission(options, Arc::clone(&snapshot.work_memory_governor))?;
        let from_us = snapshot
            .retention_cutoffs
            .get(&series)
            .copied()
            .map_or(from_us, |cutoff| from_us.max(cutoff));
        let mut sources: Vec<Box<dyn OperationCursor>> = Vec::new();
        if from_us < to_us {
            let batch_cursors = snapshot
                .batches
                .iter()
                .filter_map(|batch| SliceCursor::series(Arc::clone(batch), series, from_us, to_us))
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
            for segment in snapshot
                .segments_by_shard
                .get(&shard_id)
                .into_iter()
                .flatten()
            {
                if segment.descriptor().min_commit > snapshot.commit_id
                    || segment.descriptor().max_timestamp_us < from_us
                    || segment.descriptor().min_timestamp_us >= to_us
                {
                    continue;
                }
                if let Some(cursor) = segment.series_cursor_for_query(
                    series,
                    from_us,
                    to_us,
                    Arc::clone(&query_budget),
                )? {
                    sources.push(cursor);
                }
            }
        }
        Ok(Self {
            read_commit_id: snapshot.commit_id,
            visible: VisibleCursor::new(
                budgeted_sources(sources, &query_budget),
                snapshot.commit_id,
            )?,
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
        })
    }

    /// Commit ID, зафиксированный при создании потока.
    #[must_use]
    pub const fn read_commit_id(&self) -> u64 {
        self.read_commit_id
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
