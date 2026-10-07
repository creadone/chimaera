use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result, invalid};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor};

const DEFAULT_MAX_SCANNED_POINTS: u64 = 100_000_000;
const DEFAULT_MAX_DECODED_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_MAX_RESULT_ROWS: u64 = 10_000_000;
const DEFAULT_MAX_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_SERIES: usize = 100_000;

std::thread_local! {
    static THREAD_EXTERNAL_PHASE_NANOS: Cell<u64> = const { Cell::new(0) };
}

fn record_thread_external_phase(duration: Duration) {
    let nanos = duration_nanos(duration);
    THREAD_EXTERNAL_PHASE_NANOS.with(|total| total.set(total.get().saturating_add(nanos)));
}

pub(crate) fn thread_external_phase_duration() -> Duration {
    THREAD_EXTERNAL_PHASE_NANOS.with(|total| Duration::from_nanos(total.get()))
}

/// Shared cancellation signal for one or more queries.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a token that has not been cancelled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel every query using this token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Query class label. All classes currently share one bounded FIFO queue;
/// this label does not grant scheduling priority or a separate resource quota.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryPriority {
    Critical,
    #[default]
    Interactive,
    Bulk,
}

/// Deadline, cooperative cancellation, and hard limits for one query.
#[derive(Clone, Debug)]
pub struct QueryOptions {
    /// Query label; default: [`QueryPriority::Interactive`]. No scheduling preference yet.
    pub priority: QueryPriority,
    /// None sets a 30-second deadline at query creation. Pull streams check it
    /// during reads; an idle stream retains its snapshot until next() or Drop.
    pub deadline: Option<Instant>,
    /// Cooperative cancellation: call next() or drop an idle stream to release
    /// its resources after signalling the token.
    /// Default: `None`.
    pub cancellation: Option<CancellationToken>,
    /// Positive raw-point scan limit. Default: 100,000,000.
    pub max_scanned_points: u64,
    /// Positive decoded-byte limit. Default: 1 GiB; not an I/O byte counter.
    pub max_decoded_bytes: u64,
    /// Positive emitted-row limit. Default: 10,000,000.
    pub max_result_rows: u64,
    /// Positive per-query memory-accounting limit. Default: 256 MiB.
    /// The engine's shared budgets still apply; this is not reserved memory.
    pub max_memory_bytes: u64,
    /// Positive series limit. Default: 100,000.
    pub max_series: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            priority: QueryPriority::default(),
            deadline: None,
            cancellation: None,
            max_scanned_points: DEFAULT_MAX_SCANNED_POINTS,
            max_decoded_bytes: DEFAULT_MAX_DECODED_BYTES,
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_series: DEFAULT_MAX_SERIES,
        }
    }
}

impl QueryOptions {
    pub(crate) fn set_default_deadline(&mut self) {
        self.deadline.get_or_insert_with(|| {
            Instant::now() + crate::engine::lifecycle::admission::DEFAULT_WAIT_TIMEOUT
        });
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_scanned_points == 0
            || self.max_decoded_bytes == 0
            || self.max_result_rows == 0
            || self.max_memory_bytes == 0
            || self.max_series == 0
        {
            return Err(invalid("query limits must be positive"));
        }
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(Error::QueryCancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Error::QueryDeadlineExceeded);
        }
        Ok(())
    }
}

/// Read costs attributed to one grouping query.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct QueryStats {
    /// Raw point operations traversed before visibility filtering;
    /// charged against `QueryOptions::max_scanned_points`.
    pub operations_scanned: u64,
    pub operation_bytes_decoded: u64,
    pub points_scanned: u64,
    pub blocks_read: u64,
    pub physical_blocks_decoded: u64,
    pub physical_decoded_bytes: u64,
    pub decoded_bytes: u64,
    pub metadata_bytes_read: u64,
    pub metadata_local_hits: u64,
    pub metadata_local_pinned_bytes: u64,
    pub metadata_local_peak_pinned_bytes: u64,
    pub payload_bytes_read: u64,
    pub file_opens: u64,
    pub file_cache_hits: u64,
    pub file_cache_misses: u64,
    /// Reused segment objects, distinct from cached file descriptors.
    pub segment_handle_cache_hits: u64,
    /// Lookups without a matching object, including failed opens.
    pub segment_handle_cache_misses: u64,
    /// Misses whose initial cache slot held a different segment ID.
    pub segment_handle_cache_collisions: u64,
    /// Successful segment opens after a miss; includes first opens and reopens.
    pub segment_handle_opens: u64,
    /// Root loads while opening a segment object.
    pub index_root_loads: u64,
    /// Disk-root navigation when the segment has no resident root.
    pub index_root_fallback_reads: u64,
    /// Number of logical segment-index roots consulted.
    pub index_roots_consulted: u64,
    /// Number of index roots loaded by this query through file reads.
    pub index_roots_read: u64,
    /// Number of index leaf pages loaded by this query.
    pub index_pages_read: u64,
    /// Number of experimental shared-directory root consultations.
    pub pack_directory_roots_consulted: u64,
    /// Number of shared-directory pages read by this query.
    pub pack_directory_pages_read: u64,
    /// Number of shared-directory pages found in the bounded resident cache.
    pub pack_directory_page_cache_hits: u64,
    /// Number of direct block locators returned by the shared directory.
    pub pack_directory_extents_returned: u64,
    /// Number of page-fence comparisons during directory lookup.
    pub pack_directory_fences_examined: u64,
    /// Number of complete locator decodes, including repeated passes.
    pub pack_directory_locators_decoded: u64,
    pub coalesced_block_load_waits: u64,
    pub coalesced_block_load_wait_duration: Duration,
    pub used_exact_daily_aggregates: bool,
    pub result_rows: u64,
    pub peak_memory_bytes: u64,
    pub sources_peak_memory_bytes: u64,
    pub metadata_peak_memory_bytes: u64,
    pub compressed_input_peak_memory_bytes: u64,
    pub decoded_blocks_peak_memory_bytes: u64,
    pub worker_results_peak_memory_bytes: u64,
    pub aggregate_records_read: u64,
    pub aggregate_bytes_read: u64,
    pub segment_descriptors_examined: u64,
    pub series_index_lookups: u64,
    /// Number of physical parts opened for the selected series.
    pub part_reads: u64,
    /// Preparation of in-memory cursors, nested in source_open_duration.
    pub memory_source_open_duration: Duration,
    pub memory_batches_examined: u64,
    pub memory_cursors_created: u64,
    pub catalog_pages_visited: u64,
    pub catalog_pages_read: u64,
    pub catalog_page_decodes: u64,
    pub catalog_entries_decoded: u64,
    pub catalog_entries_examined: u64,
    /// Block addresses returned by catalogue searches, including other files' addresses.
    pub catalog_locators_examined: u64,
    pub source_open_duration: Duration,
    /// Cold metadata load and validation time, included in source opening and reading.
    pub metadata_duration: Duration,
    /// Encoded block read time, included in read_duration.
    pub io_duration: Duration,
    /// Block decoding time, included in read_duration.
    pub decode_duration: Duration,
    /// MVCC merging and raw-group accumulation time, included in read_duration.
    pub mvcc_duration: Duration,
    /// Bucket-state accumulation time, including obtaining visible points.
    pub aggregate_accumulation_duration: Duration,
    /// Time spent deriving aggregate values and constructing result rows.
    pub aggregate_finalization_duration: Duration,
    /// Sum of series-task waits from the first enqueue attempt to execution.
    /// Includes waiting for a queue slot. Tasks overlap, so this is not query wall time.
    pub executor_queue_duration: Duration,
    /// Sum of series-task execution times until their replies are published.
    /// Includes waits within tasks; neither CPU time nor a separate query phase.
    pub executor_active_duration: Duration,
    /// Series tasks completed by a worker, including errors and caught panics.
    pub executor_series_tasks: u64,
    /// Series-task queue wait only; included in `executor_queue_duration`.
    pub executor_series_queue_duration: Duration,
    /// Series-task execution only; included in `executor_active_duration`.
    pub executor_series_active_duration: Duration,
    /// Time from query start to the first result row.
    pub first_row_duration: Duration,
    pub read_duration: Duration,
}

pub(crate) struct QueryBudget {
    options: QueryOptions,
    cancelled: AtomicBool,
    operations_scanned: AtomicU64,
    points_scanned: AtomicU64,
    blocks_read: AtomicU64,
    physical_blocks_decoded: AtomicU64,
    physical_decoded_bytes: AtomicU64,
    decoded_bytes: AtomicU64,
    operation_bytes_decoded: AtomicU64,
    result_rows: AtomicU64,
    memory_bytes: AtomicU64,
    peak_memory_bytes: AtomicU64,
    memory_by_class: [AtomicU64; 5],
    peak_memory_by_class: [AtomicU64; 5],
    aggregate_records_read: AtomicU64,
    aggregate_bytes_read: AtomicU64,
    segment_descriptors_examined: AtomicU64,
    series_index_lookups: AtomicU64,
    part_reads: AtomicU64,
    memory_source_open_nanos: AtomicU64,
    memory_batches_examined: AtomicU64,
    memory_cursors_created: AtomicU64,
    catalog_pages_visited: AtomicU64,
    catalog_pages_read: AtomicU64,
    catalog_page_decodes: AtomicU64,
    catalog_entries_decoded: AtomicU64,
    catalog_entries_examined: AtomicU64,
    source_open_nanos: AtomicU64,
    metadata_nanos: AtomicU64,
    io_nanos: AtomicU64,
    decode_nanos: AtomicU64,
    mvcc_nanos: AtomicU64,
    aggregate_accumulation_nanos: AtomicU64,
    aggregate_finalization_nanos: AtomicU64,
    executor_series_tasks: AtomicU64,
    executor_series_queue_nanos: AtomicU64,
    executor_series_active_nanos: AtomicU64,
    first_row_nanos: AtomicU64,
    first_row_recorded: AtomicBool,
    read_nanos: AtomicU64,
    metadata_bytes_read: AtomicU64,
    metadata_local_hits: AtomicU64,
    metadata_local_pinned_bytes: AtomicU64,
    metadata_local_peak_pinned_bytes: AtomicU64,
    local_metadata: Mutex<HashMap<QueryMetadataKey, QueryLocalMetadata>>,
    payload_bytes_read: AtomicU64,
    file_opens: AtomicU64,
    file_cache_hits: AtomicU64,
    file_cache_misses: AtomicU64,
    segment_handle_cache_hits: AtomicU64,
    segment_handle_cache_misses: AtomicU64,
    segment_handle_cache_collisions: AtomicU64,
    segment_handle_opens: AtomicU64,
    index_root_loads: AtomicU64,
    index_root_fallback_reads: AtomicU64,
    index_roots_consulted: AtomicU64,
    index_roots_read: AtomicU64,
    index_pages_read: AtomicU64,
    pack_directory_roots_consulted: AtomicU64,
    pack_directory_pages_read: AtomicU64,
    pack_directory_page_cache_hits: AtomicU64,
    pack_directory_extents_returned: AtomicU64,
    pack_directory_fences_examined: AtomicU64,
    pack_directory_locators_decoded: AtomicU64,
    catalog_locators_examined: AtomicU64,
    coalesced_block_load_waits: AtomicU64,
    coalesced_block_load_wait_nanos: AtomicU64,
    used_exact_daily_aggregates: AtomicBool,
    started: Instant,
    work_memory: Option<Arc<WorkMemoryGovernor>>,
}

/// Memory owned by a query object; released when that object is dropped.
pub(crate) struct QueryReservation {
    budget: Arc<QueryBudget>,
    class: QueryMemoryClass,
    bytes: u64,
}

impl QueryReservation {
    pub(crate) fn budget(&self) -> &Arc<QueryBudget> {
        &self.budget
    }

    pub(crate) fn grow(&mut self, bytes: u64) -> Result<()> {
        self.budget.reserve_memory_class(self.class, bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub(crate) fn push<T>(&mut self, values: &mut Vec<T>, value: T) -> Result<()> {
        if values.len() == values.capacity() {
            let capacity = values.capacity().saturating_mul(2).max(4);
            self.grow(
                ((capacity - values.capacity()) as u64)
                    .saturating_mul(std::mem::size_of::<T>() as u64),
            )?;
            values.reserve_exact(capacity - values.len());
        }
        values.push(value);
        Ok(())
    }
}

impl Drop for QueryReservation {
    fn drop(&mut self) {
        self.budget.release_memory_class(self.class, self.bytes);
    }
}

impl QueryBudget {
    pub(crate) fn reserve_owned(self: &Arc<Self>, bytes: u64) -> Result<QueryReservation> {
        self.reserve_owned_class(QueryMemoryClass::Sources, bytes)
    }

    pub(crate) fn reserve_owned_class(
        self: &Arc<Self>,
        class: QueryMemoryClass,
        bytes: u64,
    ) -> Result<QueryReservation> {
        self.reserve_memory_class(class, bytes)?;
        Ok(QueryReservation {
            budget: Arc::clone(self),
            class,
            bytes,
        })
    }

    pub(crate) fn new_with_admission(
        options: QueryOptions,
        work_memory: Arc<WorkMemoryGovernor>,
    ) -> Result<Arc<Self>> {
        Self::new_inner(options, Some(work_memory))
    }

    fn new_inner(
        mut options: QueryOptions,
        work_memory: Option<Arc<WorkMemoryGovernor>>,
    ) -> Result<Arc<Self>> {
        options.deadline.get_or_insert_with(|| {
            Instant::now() + crate::engine::lifecycle::admission::DEFAULT_WAIT_TIMEOUT
        });
        options.validate()?;
        let budget = Arc::new(Self {
            options,
            cancelled: AtomicBool::new(false),
            operations_scanned: AtomicU64::new(0),
            points_scanned: AtomicU64::new(0),
            blocks_read: AtomicU64::new(0),
            physical_blocks_decoded: AtomicU64::new(0),
            physical_decoded_bytes: AtomicU64::new(0),
            decoded_bytes: AtomicU64::new(0),
            operation_bytes_decoded: AtomicU64::new(0),
            result_rows: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            peak_memory_bytes: AtomicU64::new(0),
            memory_by_class: std::array::from_fn(|_| AtomicU64::new(0)),
            peak_memory_by_class: std::array::from_fn(|_| AtomicU64::new(0)),
            aggregate_records_read: AtomicU64::new(0),
            aggregate_bytes_read: AtomicU64::new(0),
            segment_descriptors_examined: AtomicU64::new(0),
            series_index_lookups: AtomicU64::new(0),
            part_reads: AtomicU64::new(0),
            memory_source_open_nanos: AtomicU64::new(0),
            memory_batches_examined: AtomicU64::new(0),
            memory_cursors_created: AtomicU64::new(0),
            catalog_pages_visited: AtomicU64::new(0),
            catalog_pages_read: AtomicU64::new(0),
            catalog_page_decodes: AtomicU64::new(0),
            catalog_entries_decoded: AtomicU64::new(0),
            catalog_entries_examined: AtomicU64::new(0),
            source_open_nanos: AtomicU64::new(0),
            metadata_nanos: AtomicU64::new(0),
            io_nanos: AtomicU64::new(0),
            decode_nanos: AtomicU64::new(0),
            mvcc_nanos: AtomicU64::new(0),
            aggregate_accumulation_nanos: AtomicU64::new(0),
            aggregate_finalization_nanos: AtomicU64::new(0),
            executor_series_tasks: AtomicU64::new(0),
            executor_series_queue_nanos: AtomicU64::new(0),
            executor_series_active_nanos: AtomicU64::new(0),
            first_row_nanos: AtomicU64::new(0),
            first_row_recorded: AtomicBool::new(false),
            read_nanos: AtomicU64::new(0),
            metadata_bytes_read: AtomicU64::new(0),
            metadata_local_hits: AtomicU64::new(0),
            metadata_local_pinned_bytes: AtomicU64::new(0),
            metadata_local_peak_pinned_bytes: AtomicU64::new(0),
            local_metadata: Mutex::new(HashMap::new()),
            payload_bytes_read: AtomicU64::new(0),
            file_opens: AtomicU64::new(0),
            file_cache_hits: AtomicU64::new(0),
            file_cache_misses: AtomicU64::new(0),
            segment_handle_cache_hits: AtomicU64::new(0),
            segment_handle_cache_misses: AtomicU64::new(0),
            segment_handle_cache_collisions: AtomicU64::new(0),
            segment_handle_opens: AtomicU64::new(0),
            index_root_loads: AtomicU64::new(0),
            index_root_fallback_reads: AtomicU64::new(0),
            index_roots_consulted: AtomicU64::new(0),
            index_roots_read: AtomicU64::new(0),
            index_pages_read: AtomicU64::new(0),
            pack_directory_roots_consulted: AtomicU64::new(0),
            pack_directory_pages_read: AtomicU64::new(0),
            pack_directory_page_cache_hits: AtomicU64::new(0),
            pack_directory_extents_returned: AtomicU64::new(0),
            pack_directory_fences_examined: AtomicU64::new(0),
            pack_directory_locators_decoded: AtomicU64::new(0),
            catalog_locators_examined: AtomicU64::new(0),
            coalesced_block_load_waits: AtomicU64::new(0),
            coalesced_block_load_wait_nanos: AtomicU64::new(0),
            used_exact_daily_aggregates: AtomicBool::new(false),
            started: Instant::now(),
            work_memory,
        });
        budget.check()?;
        Ok(budget)
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(Error::QueryCancelled);
        }
        self.options.check()
    }

    pub(crate) fn check_waiting_for(&self, reason: crate::OverloadReason) -> Result<()> {
        self.check().map_err(|error| match error {
            Error::QueryDeadlineExceeded => Error::Overloaded(reason),
            other => other,
        })
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn record_operations(&self, count: u64) -> Result<()> {
        self.check()?;
        add_bounded(
            &self.operations_scanned,
            count,
            self.options.max_scanned_points,
            "query scanned-point limit exceeded",
        )?;
        Ok(())
    }

    pub(crate) fn record_points(&self, count: u64) -> Result<()> {
        add_checked(
            &self.points_scanned,
            count,
            "query visible-point counter overflow",
        )?;
        Ok(())
    }

    pub(crate) fn record_block(&self, decoded_bytes: u64, physical_read: bool) -> Result<()> {
        self.check()?;
        add_bounded(
            &self.decoded_bytes,
            decoded_bytes,
            self.options.max_decoded_bytes,
            "query decoded-byte limit exceeded",
        )?;
        add_checked(
            &self.operation_bytes_decoded,
            decoded_bytes,
            "query operation-byte counter overflow",
        )?;
        if physical_read {
            add_checked(&self.blocks_read, 1, "query block counter overflow")?;
        }
        Ok(())
    }

    pub(crate) fn record_metadata_bytes(&self, bytes: u64) {
        add_saturating(&self.metadata_bytes_read, bytes);
    }

    pub(crate) fn keep_local_metadata<T>(
        &self,
        segment_id: u64,
        kind: QueryMetadataKind,
        value: Arc<T>,
        bytes: u64,
        reserved_bytes: u64,
    ) -> Result<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        #[cfg(feature = "read-diagnostics")]
        let _timing = crate::diagnostics::query::timing::summary::stage(
            crate::diagnostics::query::timing::summary::Stage::Pin,
        );
        let key = QueryMetadataKey { segment_id, kind };
        let mut state = self
            .local_metadata
            .lock()
            .map_err(|_| Error::Corruption("query-local metadata mutex is poisoned".into()))?;
        if let Some(entry) = state.get(&key)
            && entry.series.is_none()
        {
            let existing = Arc::clone(&entry.value).downcast::<T>().map_err(|_| {
                Error::Corruption("query-local metadata entry has the wrong type".into())
            })?;
            drop(state);
            self.release_memory_class(QueryMemoryClass::Metadata, reserved_bytes);
            return Ok(existing);
        }
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        let previous = state.insert(
            key,
            QueryLocalMetadata {
                value: erased,
                bytes,
                reserved_bytes,
                series: None,
            },
        );
        let previous_reserved = previous.as_ref().map_or(0, |entry| entry.reserved_bytes);
        if let Some(previous) = previous {
            self.metadata_local_pinned_bytes
                .fetch_sub(previous.bytes, Ordering::Relaxed);
        }
        let pinned = self
            .metadata_local_pinned_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.metadata_local_peak_pinned_bytes
            .fetch_max(pinned, Ordering::Relaxed);
        drop(state);
        self.release_memory_class(QueryMemoryClass::Metadata, previous_reserved);
        Ok(value)
    }

    pub(crate) fn local_series_metadata<T>(
        &self,
        segment_id: u64,
        kind: QueryMetadataKind,
        requested: &[crate::types::SeriesKey],
    ) -> Result<Option<Arc<T>>>
    where
        T: Any + Send + Sync,
    {
        #[cfg(feature = "read-diagnostics")]
        let _timing = crate::diagnostics::query::timing::summary::stage(
            crate::diagnostics::query::timing::summary::Stage::Local,
        );
        let state = self
            .local_metadata
            .lock()
            .map_err(|_| Error::Corruption("query-local metadata mutex is poisoned".into()))?;
        let Some(entry) = state.get(&QueryMetadataKey { segment_id, kind }) else {
            return Ok(None);
        };
        if let Some(covered) = &entry.series
            && !requested
                .iter()
                .all(|series| covered.binary_search(series).is_ok())
        {
            return Ok(None);
        }
        let value = Arc::clone(&entry.value).downcast::<T>().map_err(|_| {
            Error::Corruption("query-local metadata entry has the wrong type".into())
        })?;
        add_saturating(&self.metadata_local_hits, 1);
        #[cfg(feature = "read-diagnostics")]
        crate::diagnostics::query::timing::summary::hit(
            crate::diagnostics::query::timing::summary::Hit::Local,
        );
        Ok(Some(value))
    }

    pub(crate) fn keep_local_series_metadata<T>(
        &self,
        segment_id: u64,
        kind: QueryMetadataKind,
        mut series: Vec<crate::types::SeriesKey>,
        value: Arc<T>,
        bytes: u64,
        reserved_bytes: u64,
    ) -> Result<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        #[cfg(feature = "read-diagnostics")]
        let _timing = crate::diagnostics::query::timing::summary::stage(
            crate::diagnostics::query::timing::summary::Stage::Pin,
        );
        series.sort_unstable();
        series.dedup();
        let key = QueryMetadataKey { segment_id, kind };
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        let mut state = self
            .local_metadata
            .lock()
            .map_err(|_| Error::Corruption("query-local metadata mutex is poisoned".into()))?;
        if let Some(previous) = state.get(&key)
            && (previous.series.is_none()
                || previous.series.as_ref().is_some_and(|covered| {
                    series
                        .iter()
                        .all(|requested| covered.binary_search(requested).is_ok())
                }))
        {
            let existing = Arc::clone(&previous.value).downcast::<T>().map_err(|_| {
                Error::Corruption("query-local metadata entry has the wrong type".into())
            })?;
            drop(state);
            self.release_memory_class(QueryMemoryClass::Metadata, reserved_bytes);
            return Ok(existing);
        }
        let previous = state.insert(
            key,
            QueryLocalMetadata {
                value: erased,
                bytes,
                reserved_bytes,
                series: Some(series.into_boxed_slice()),
            },
        );
        let previous_reserved = previous.as_ref().map_or(0, |entry| entry.reserved_bytes);
        if let Some(previous) = previous {
            self.metadata_local_pinned_bytes
                .fetch_sub(previous.bytes, Ordering::Relaxed);
        }
        let pinned = self
            .metadata_local_pinned_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.metadata_local_peak_pinned_bytes
            .fetch_max(pinned, Ordering::Relaxed);
        drop(state);
        self.release_memory_class(QueryMemoryClass::Metadata, previous_reserved);
        Ok(value)
    }

    pub(crate) fn release_local_metadata(&self) -> Result<()> {
        let mut state = self
            .local_metadata
            .lock()
            .map_err(|_| Error::Corruption("query-local metadata mutex is poisoned".into()))?;
        let reserved_bytes = state
            .drain()
            .map(|(_, entry)| entry.reserved_bytes)
            .fold(0_u64, u64::saturating_add);
        self.metadata_local_pinned_bytes.store(0, Ordering::Relaxed);
        drop(state);
        self.release_memory_class(QueryMemoryClass::Metadata, reserved_bytes);
        Ok(())
    }

    pub(crate) fn release_remaining_memory(&self) {
        for class in [
            QueryMemoryClass::Sources,
            QueryMemoryClass::Metadata,
            QueryMemoryClass::Compressed,
            QueryMemoryClass::Decoded,
            QueryMemoryClass::Results,
        ] {
            let bytes = self.memory_by_class[class.index()].load(Ordering::Relaxed);
            if bytes != 0 {
                self.release_memory_class(class, bytes);
            }
        }
    }

    pub(crate) fn record_payload_bytes(&self, bytes: u64) {
        add_saturating(&self.payload_bytes_read, bytes);
    }

    pub(crate) fn record_file_open(&self) {
        add_saturating(&self.file_opens, 1);
    }

    pub(crate) fn record_file_cache_hit(&self) {
        add_saturating(&self.file_cache_hits, 1);
    }

    pub(crate) fn record_file_cache_miss(&self) {
        add_saturating(&self.file_cache_misses, 1);
    }

    pub(crate) fn record_index_root_consulted(&self) {
        add_saturating(&self.index_roots_consulted, 1);
    }

    pub(crate) fn record_segment_handle_cache_hit(&self) {
        add_saturating(&self.segment_handle_cache_hits, 1);
    }

    pub(crate) fn record_segment_handle_cache_miss(&self, collision: bool) {
        add_saturating(&self.segment_handle_cache_misses, 1);
        if collision {
            add_saturating(&self.segment_handle_cache_collisions, 1);
        }
    }

    pub(crate) fn record_segment_handle_open(&self) {
        add_saturating(&self.segment_handle_opens, 1);
    }

    pub(crate) fn record_index_root_load(&self) {
        add_saturating(&self.index_root_loads, 1);
        add_saturating(&self.index_roots_read, 1);
    }

    pub(crate) fn record_index_root_fallback_read(&self) {
        add_saturating(&self.index_root_fallback_reads, 1);

        add_saturating(&self.index_roots_read, 1);
    }

    pub(crate) fn record_index_pages_read(&self, pages: u64) {
        add_saturating(&self.index_pages_read, pages);
    }

    pub(crate) fn record_pack_directory_root_consulted(&self) {
        add_saturating(&self.pack_directory_roots_consulted, 1);
    }

    pub(crate) fn record_pack_directory_pages_read(&self, pages: u64) {
        add_saturating(&self.pack_directory_pages_read, pages);
    }

    pub(crate) fn record_pack_directory_page_cache_hit(&self) {
        add_saturating(&self.pack_directory_page_cache_hits, 1);
    }

    pub(crate) fn record_pack_directory_locators_decoded(&self, count: u64) {
        add_saturating(&self.pack_directory_locators_decoded, count);
    }

    pub(crate) fn record_pack_directory_fences_examined(&self, count: u64) {
        add_saturating(&self.pack_directory_fences_examined, count);
    }

    pub(crate) fn record_pack_directory_extents(&self, extents: u64) {
        add_saturating(&self.pack_directory_extents_returned, extents);
    }

    pub(crate) fn record_block_load_wait(&self, duration: Duration) {
        add_saturating(&self.coalesced_block_load_waits, 1);
        add_saturating(
            &self.coalesced_block_load_wait_nanos,
            duration_nanos(duration),
        );
    }

    pub(crate) fn mark_exact_daily_aggregates(&self) -> bool {
        !self
            .used_exact_daily_aggregates
            .swap(true, Ordering::AcqRel)
    }

    pub(crate) fn try_reserve_memory_class(&self, class: QueryMemoryClass, bytes: u64) -> bool {
        if self.check().is_err() {
            return false;
        }
        let reserved =
            self.memory_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.options.max_memory_bytes)
                });
        if let Ok(previous) = reserved {
            if let Some(work_memory) = &self.work_memory {
                match work_memory.try_reserve(class.work_class(), bytes) {
                    Ok(true) => {}
                    Ok(false) | Err(_) => {
                        self.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
                        return false;
                    }
                }
            }
            self.peak_memory_bytes
                .fetch_max(previous.saturating_add(bytes), Ordering::Relaxed);
            self.record_class_reservation(class, bytes);
            true
        } else {
            false
        }
    }

    pub(crate) fn record_aggregate_records(&self, records: u64, decoded_bytes: u64) -> Result<()> {
        self.check()?;
        add_bounded(
            &self.decoded_bytes,
            decoded_bytes,
            self.options.max_decoded_bytes,
            "query decoded-byte limit exceeded",
        )?;
        add_checked(
            &self.aggregate_bytes_read,
            decoded_bytes,
            "query aggregate-byte counter overflow",
        )?;
        add_checked(
            &self.aggregate_records_read,
            records,
            "query aggregate counter overflow",
        )?;
        Ok(())
    }

    pub(crate) fn record_result_row(&self) -> Result<()> {
        self.check()?;
        add_bounded(
            &self.result_rows,
            1,
            self.options.max_result_rows,
            "query result-row limit exceeded",
        )?;
        if !self.first_row_recorded.swap(true, Ordering::AcqRel) {
            self.first_row_nanos
                .store(duration_nanos(self.started.elapsed()), Ordering::Relaxed);
        }
        Ok(())
    }

    pub(crate) fn reserve_memory(&self, bytes: u64) -> Result<()> {
        self.reserve_memory_class(QueryMemoryClass::Sources, bytes)
    }

    pub(crate) fn reserve_memory_class(&self, class: QueryMemoryClass, bytes: u64) -> Result<()> {
        #[cfg(feature = "read-diagnostics")]
        let _timing = crate::diagnostics::query::timing::summary::stage(
            crate::diagnostics::query::timing::summary::Stage::Memory,
        );
        self.check()?;
        let current = add_bounded(
            &self.memory_bytes,
            bytes,
            self.options.max_memory_bytes,
            "query memory limit exceeded",
        )?;
        if let Some(work_memory) = &self.work_memory {
            match work_memory.try_reserve(class.work_class(), bytes) {
                Ok(true) => {}
                Ok(false) => {
                    self.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    return Err(Error::Overloaded(crate::OverloadReason::WorkMemory));
                }
                Err(error) => {
                    self.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    return Err(error);
                }
            }
        }
        self.peak_memory_bytes.fetch_max(current, Ordering::Relaxed);
        self.record_class_reservation(class, bytes);
        Ok(())
    }

    pub(crate) fn record_memory_sources(&self, batches: usize, cursors: usize, elapsed: Duration) {
        add_saturating(&self.memory_batches_examined, batches as u64);
        add_saturating(&self.memory_cursors_created, cursors as u64);
        add_saturating(&self.memory_source_open_nanos, duration_nanos(elapsed));
    }

    pub(crate) fn record_catalog_page(&self, read: bool) {
        add_saturating(&self.catalog_pages_visited, 1);
        add_saturating(&self.catalog_pages_read, u64::from(read));
    }

    pub(crate) fn record_catalog_entries(&self, entries: usize) {
        add_saturating(&self.catalog_entries_examined, entries as u64);
    }

    pub(crate) fn record_catalog_decode(&self, entries: usize) {
        add_saturating(&self.catalog_page_decodes, 1);
        add_saturating(&self.catalog_entries_decoded, entries as u64);
    }

    pub(crate) fn record_source_open(
        &self,
        descriptors_examined: u64,
        series_index_lookups: u64,
        duration: Duration,
    ) {
        add_saturating(&self.segment_descriptors_examined, descriptors_examined);
        add_saturating(&self.series_index_lookups, series_index_lookups);
        add_saturating(&self.source_open_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_part_reads(&self, count: u64) {
        add_saturating(&self.part_reads, count);
    }

    pub(crate) fn record_metadata(&self, duration: Duration) {
        record_thread_external_phase(duration);
        add_saturating(&self.metadata_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_io(&self, duration: Duration) {
        record_thread_external_phase(duration);
        add_saturating(&self.io_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_decode(&self, duration: Duration, decoded_bytes: u64) {
        record_thread_external_phase(duration);
        add_saturating(&self.decode_nanos, duration_nanos(duration));
        add_saturating(&self.physical_blocks_decoded, 1);
        add_saturating(&self.physical_decoded_bytes, decoded_bytes);
    }

    pub(crate) fn record_mvcc(&self, duration: Duration) {
        add_saturating(&self.mvcc_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_aggregate_accumulation(&self, duration: Duration) {
        add_saturating(&self.aggregate_accumulation_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_aggregate_finalization(&self, duration: Duration) {
        add_saturating(&self.aggregate_finalization_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_executor(&self, queue: Duration, active: Duration) {
        add_saturating(&self.executor_series_queue_nanos, duration_nanos(queue));
        add_saturating(&self.executor_series_active_nanos, duration_nanos(active));
        add_saturating(&self.executor_series_tasks, 1);
    }

    pub(crate) fn io_duration(&self) -> Duration {
        Duration::from_nanos(self.io_nanos.load(Ordering::Relaxed))
    }

    pub(crate) fn decode_duration(&self) -> Duration {
        Duration::from_nanos(self.decode_nanos.load(Ordering::Relaxed))
    }

    pub(crate) fn record_read(&self, duration: Duration) {
        add_saturating(&self.read_nanos, duration_nanos(duration));
    }

    pub(crate) fn release_memory(&self, bytes: u64) {
        self.release_memory_class(QueryMemoryClass::Sources, bytes);
    }

    pub(crate) fn release_memory_class(&self, class: QueryMemoryClass, bytes: u64) {
        let result =
            self.memory_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_sub(bytes)
                });
        debug_assert!(result.is_ok(), "query memory accounting underflow");
        let class_result = self.memory_by_class[class.index()].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_sub(bytes),
        );
        debug_assert!(
            class_result.is_ok(),
            "query class memory accounting underflow"
        );
        if let Some(work_memory) = &self.work_memory {
            work_memory.release(class.work_class(), bytes);
        }
    }

    fn record_class_reservation(&self, class: QueryMemoryClass, bytes: u64) {
        let current = self.memory_by_class[class.index()]
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.peak_memory_by_class[class.index()].fetch_max(current, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> QueryStats {
        // Use the same samples for each class and the totals. Replies synchronize
        // completed workers; snapshots taken while work runs remain best-effort.

        let series_queue = self.executor_series_queue_nanos.load(Ordering::Relaxed);
        let series_active = self.executor_series_active_nanos.load(Ordering::Relaxed);
        QueryStats {
            operations_scanned: self.operations_scanned.load(Ordering::Relaxed),
            operation_bytes_decoded: self.operation_bytes_decoded.load(Ordering::Relaxed),
            points_scanned: self.points_scanned.load(Ordering::Relaxed),
            blocks_read: self.blocks_read.load(Ordering::Relaxed),
            physical_blocks_decoded: self.physical_blocks_decoded.load(Ordering::Relaxed),
            physical_decoded_bytes: self.physical_decoded_bytes.load(Ordering::Relaxed),
            decoded_bytes: self.decoded_bytes.load(Ordering::Relaxed),
            metadata_bytes_read: self.metadata_bytes_read.load(Ordering::Relaxed),
            metadata_local_hits: self.metadata_local_hits.load(Ordering::Relaxed),
            metadata_local_pinned_bytes: self.metadata_local_pinned_bytes.load(Ordering::Relaxed),
            metadata_local_peak_pinned_bytes: self
                .metadata_local_peak_pinned_bytes
                .load(Ordering::Relaxed),
            payload_bytes_read: self.payload_bytes_read.load(Ordering::Relaxed),
            file_opens: self.file_opens.load(Ordering::Relaxed),
            file_cache_hits: self.file_cache_hits.load(Ordering::Relaxed),
            file_cache_misses: self.file_cache_misses.load(Ordering::Relaxed),
            segment_handle_cache_hits: self.segment_handle_cache_hits.load(Ordering::Relaxed),
            segment_handle_cache_misses: self.segment_handle_cache_misses.load(Ordering::Relaxed),
            segment_handle_cache_collisions: self
                .segment_handle_cache_collisions
                .load(Ordering::Relaxed),
            segment_handle_opens: self.segment_handle_opens.load(Ordering::Relaxed),
            index_root_loads: self.index_root_loads.load(Ordering::Relaxed),
            index_root_fallback_reads: self.index_root_fallback_reads.load(Ordering::Relaxed),
            index_roots_consulted: self.index_roots_consulted.load(Ordering::Relaxed),
            index_roots_read: self.index_roots_read.load(Ordering::Relaxed),
            index_pages_read: self.index_pages_read.load(Ordering::Relaxed),
            pack_directory_roots_consulted: self
                .pack_directory_roots_consulted
                .load(Ordering::Relaxed),
            pack_directory_pages_read: self.pack_directory_pages_read.load(Ordering::Relaxed),
            pack_directory_page_cache_hits: self
                .pack_directory_page_cache_hits
                .load(Ordering::Relaxed),
            pack_directory_extents_returned: self
                .pack_directory_extents_returned
                .load(Ordering::Relaxed),
            pack_directory_locators_decoded: self
                .pack_directory_locators_decoded
                .load(Ordering::Relaxed),
            catalog_locators_examined: self.catalog_locators_examined.load(Ordering::Relaxed),
            pack_directory_fences_examined: self
                .pack_directory_fences_examined
                .load(Ordering::Relaxed),
            coalesced_block_load_waits: self.coalesced_block_load_waits.load(Ordering::Relaxed),
            coalesced_block_load_wait_duration: Duration::from_nanos(
                self.coalesced_block_load_wait_nanos.load(Ordering::Relaxed),
            ),
            used_exact_daily_aggregates: self.used_exact_daily_aggregates.load(Ordering::Relaxed),
            result_rows: self.result_rows.load(Ordering::Relaxed),
            peak_memory_bytes: self.peak_memory_bytes.load(Ordering::Relaxed),
            sources_peak_memory_bytes: self.peak_memory_by_class[QueryMemoryClass::Sources.index()]
                .load(Ordering::Relaxed),
            metadata_peak_memory_bytes: self.peak_memory_by_class
                [QueryMemoryClass::Metadata.index()]
            .load(Ordering::Relaxed),
            compressed_input_peak_memory_bytes: self.peak_memory_by_class
                [QueryMemoryClass::Compressed.index()]
            .load(Ordering::Relaxed),
            decoded_blocks_peak_memory_bytes: self.peak_memory_by_class
                [QueryMemoryClass::Decoded.index()]
            .load(Ordering::Relaxed),
            worker_results_peak_memory_bytes: self.peak_memory_by_class
                [QueryMemoryClass::Results.index()]
            .load(Ordering::Relaxed),
            aggregate_records_read: self.aggregate_records_read.load(Ordering::Relaxed),
            aggregate_bytes_read: self.aggregate_bytes_read.load(Ordering::Relaxed),
            segment_descriptors_examined: self.segment_descriptors_examined.load(Ordering::Relaxed),
            series_index_lookups: self.series_index_lookups.load(Ordering::Relaxed),
            part_reads: self.part_reads.load(Ordering::Relaxed),
            memory_source_open_duration: Duration::from_nanos(
                self.memory_source_open_nanos.load(Ordering::Relaxed),
            ),
            memory_batches_examined: self.memory_batches_examined.load(Ordering::Relaxed),
            memory_cursors_created: self.memory_cursors_created.load(Ordering::Relaxed),
            catalog_pages_visited: self.catalog_pages_visited.load(Ordering::Relaxed),
            catalog_pages_read: self.catalog_pages_read.load(Ordering::Relaxed),
            catalog_page_decodes: self.catalog_page_decodes.load(Ordering::Relaxed),
            catalog_entries_decoded: self.catalog_entries_decoded.load(Ordering::Relaxed),
            catalog_entries_examined: self.catalog_entries_examined.load(Ordering::Relaxed),
            source_open_duration: Duration::from_nanos(
                self.source_open_nanos.load(Ordering::Relaxed),
            ),
            metadata_duration: Duration::from_nanos(self.metadata_nanos.load(Ordering::Relaxed)),
            io_duration: self.io_duration(),
            decode_duration: self.decode_duration(),
            mvcc_duration: Duration::from_nanos(self.mvcc_nanos.load(Ordering::Relaxed)),
            aggregate_accumulation_duration: Duration::from_nanos(
                self.aggregate_accumulation_nanos.load(Ordering::Relaxed),
            ),
            aggregate_finalization_duration: Duration::from_nanos(
                self.aggregate_finalization_nanos.load(Ordering::Relaxed),
            ),
            executor_queue_duration: Duration::from_nanos(series_queue),
            executor_active_duration: Duration::from_nanos(series_active),
            executor_series_tasks: self.executor_series_tasks.load(Ordering::Relaxed),
            executor_series_queue_duration: Duration::from_nanos(series_queue),
            executor_series_active_duration: Duration::from_nanos(series_active),
            first_row_duration: Duration::from_nanos(self.first_row_nanos.load(Ordering::Relaxed)),
            read_duration: Duration::from_nanos(self.read_nanos.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum QueryMetadataKind {
    Aggregate,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct QueryMetadataKey {
    segment_id: u64,
    kind: QueryMetadataKind,
}

struct QueryLocalMetadata {
    value: Arc<dyn Any + Send + Sync>,
    bytes: u64,
    reserved_bytes: u64,
    series: Option<Box<[crate::types::SeriesKey]>>,
}

impl Drop for QueryBudget {
    fn drop(&mut self) {
        let Some(work_memory) = &self.work_memory else {
            return;
        };
        for class in [
            QueryMemoryClass::Sources,
            QueryMemoryClass::Metadata,
            QueryMemoryClass::Compressed,
            QueryMemoryClass::Decoded,
            QueryMemoryClass::Results,
        ] {
            let bytes = self.memory_by_class[class.index()].load(Ordering::Relaxed);
            work_memory.release(class.work_class(), bytes);
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueryMemoryClass {
    Sources,
    Metadata,
    Compressed,
    Decoded,
    Results,
}

impl QueryMemoryClass {
    const fn index(self) -> usize {
        match self {
            Self::Sources => 0,
            Self::Metadata => 1,
            Self::Compressed => 2,
            Self::Decoded => 3,
            Self::Results => 4,
        }
    }

    const fn work_class(self) -> WorkMemoryClass {
        match self {
            Self::Sources => WorkMemoryClass::QuerySources,
            Self::Metadata => WorkMemoryClass::QueryMetadata,
            Self::Compressed => WorkMemoryClass::QueryCompressed,
            Self::Decoded => WorkMemoryClass::QueryDecoded,
            Self::Results => WorkMemoryClass::QueryResults,
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn add_saturating(target: &AtomicU64, amount: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn add_bounded(target: &AtomicU64, amount: u64, limit: u64, message: &'static str) -> Result<u64> {
    target
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(amount).filter(|next| *next <= limit)
        })
        .map(|previous| previous + amount)
        .map_err(|_| Error::ResourceLimit(message.into()))
}

fn add_checked(target: &AtomicU64, amount: u64, message: &'static str) -> Result<u64> {
    target
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(amount)
        })
        .map(|previous| previous + amount)
        .map_err(|_| Error::ResourceLimit(message.into()))
}

pub(crate) fn lock_for_query<'a, T>(
    mutex: &'a Mutex<T>,
    budget: &QueryBudget,
) -> Result<std::sync::MutexGuard<'a, T>> {
    loop {
        budget.check()?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(crate::error::corrupt("query metadata mutex is poisoned"));
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(crate::engine::lifecycle::admission::WAIT_SLICE)
            }
        }
    }
}
