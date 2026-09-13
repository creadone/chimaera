use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::admission::{WorkMemoryClass, WorkMemoryGovernor};
use crate::error::{Error, Result, invalid};

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

/// Совместный сигнал отмены одного или нескольких запросов.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Создаёт неотменённый token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Отменяет все запросы, использующие этот token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Класс запроса для общего runtime. Текущий локальный executor обслуживает
/// классы в одной ограниченной FIFO-очереди; отдельные квоты вводятся в P3.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QueryPriority {
    Critical,
    #[default]
    Interactive,
    Bulk,
}

/// Deadline, отмена и жёсткие пределы ресурсов одного запроса.
#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub priority: QueryPriority,
    /// None задаёт 30 секунд от начала каждого конкретного запроса.
    pub deadline: Option<Instant>,
    pub cancellation: Option<CancellationToken>,
    pub max_scanned_points: u64,
    pub max_decoded_bytes: u64,
    pub max_result_rows: u64,
    pub max_memory_bytes: u64,
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
        self.deadline
            .get_or_insert_with(|| Instant::now() + crate::lifecycle::DEFAULT_WAIT_TIMEOUT);
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

/// Расход чтения, принадлежащий одному запросу группировки.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct QueryStats {
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
    /// Число логических корней segment index, к которым обратился planner.
    pub index_roots_consulted: u64,
    /// Число корней index, физически прочитанных запросом с диска.
    pub index_roots_read: u64,
    /// Число leaf-страниц index, физически прочитанных запросом.
    pub index_pages_read: u64,
    /// Число обращений к корню экспериментального общего directory.
    pub pack_directory_roots_consulted: u64,
    /// Число страниц общего directory, физически прочитанных запросом.
    pub pack_directory_pages_read: u64,
    /// Число страниц общего directory, найденных в bounded resident cache.
    pub pack_directory_page_cache_hits: u64,
    /// Число прямых block locator, возвращённых общим directory.
    pub pack_directory_extents_returned: u64,
    pub prefetched_blocks: u64,
    pub prefetch_hits: u64,
    pub unused_prefetched_blocks: u64,
    pub coalesced_block_load_waits: u64,
    pub coalesced_block_load_wait_duration: Duration,
    pub used_exact_daily_aggregates: bool,
    pub result_rows: u64,
    pub peak_memory_bytes: u64,
    pub plan_peak_memory_bytes: u64,
    pub metadata_peak_memory_bytes: u64,
    pub compressed_input_peak_memory_bytes: u64,
    pub decoded_blocks_peak_memory_bytes: u64,
    pub worker_results_peak_memory_bytes: u64,
    pub aggregate_records_read: u64,
    pub aggregate_bytes_read: u64,
    pub aggregate_base_point_lookups: u64,
    pub segment_descriptors_examined: u64,
    pub series_index_lookups: u64,
    /// Ссылки `(series, logical segment)` в сыром плане после aggregate pruning.
    pub planned_segment_reads: u64,
    pub unique_planned_segments: u64,
    pub unique_planned_files: u64,
    pub planned_block_references: u64,
    pub unique_planned_blocks: u64,
    pub planning_duration: Duration,
    pub memtable_planning_duration: Duration,
    pub segment_planning_duration: Duration,
    pub aggregate_planning_duration: Duration,
    /// Время холодной загрузки и проверки метаданных, входящее в planning/read.
    pub metadata_duration: Duration,
    /// Время чтения закодированных блоков данных, входящее в read_duration.
    pub io_duration: Duration,
    /// Время декодирования блоков данных, входящее в read_duration.
    pub decode_duration: Duration,
    /// Время MVCC-слияния и накопления сырых групп, входящее в read_duration.
    pub mvcc_duration: Duration,
    /// Время накопления общего состояния корзин, включая получение видимых точек.
    pub aggregate_accumulation_duration: Duration,
    /// Время вычисления производных значений и создания строк результата.
    pub aggregate_finalization_duration: Duration,
    /// Сумма ожиданий Planning и Series от первой попытки постановки до запуска.
    /// Включает ожидание свободного слота. Это не время запроса: задачи параллельны.
    pub executor_queue_duration: Duration,
    /// Суммарное время работы Planning и Series до публикации ответа.
    /// Включает ожидания внутри задачи; это не CPU time и не отдельная фаза запроса.
    pub executor_active_duration: Duration,
    /// Завершённые работником Planning, включая ошибки и перехваченные паники.
    pub executor_planning_tasks: u64,
    /// Ожидание только Planning; входит в `executor_queue_duration`.
    pub executor_planning_queue_duration: Duration,
    /// Работа только Planning; входит в `executor_active_duration`.
    pub executor_planning_active_duration: Duration,
    /// Завершённые работником Series, включая ошибки и перехваченные паники.
    pub executor_series_tasks: u64,
    /// Ожидание только Series; входит в `executor_queue_duration`.
    pub executor_series_queue_duration: Duration,
    /// Работа только Series; входит в `executor_active_duration`.
    pub executor_series_active_duration: Duration,
    /// Время от начала запроса до первой строки результата.
    pub first_row_duration: Duration,
    pub read_duration: Duration,
}

#[derive(Clone, Copy)]
pub(crate) enum ExecutorTaskKind {
    Planning,
    Series,
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
    aggregate_base_point_lookups: AtomicU64,
    segment_descriptors_examined: AtomicU64,
    series_index_lookups: AtomicU64,
    planned_segment_reads: AtomicU64,
    unique_planned_segments: AtomicU64,
    unique_planned_files: AtomicU64,
    planned_block_references: AtomicU64,
    unique_planned_blocks: AtomicU64,
    planning_nanos: AtomicU64,
    memtable_planning_nanos: AtomicU64,
    segment_planning_nanos: AtomicU64,
    aggregate_planning_nanos: AtomicU64,
    metadata_nanos: AtomicU64,
    io_nanos: AtomicU64,
    decode_nanos: AtomicU64,
    mvcc_nanos: AtomicU64,
    aggregate_accumulation_nanos: AtomicU64,
    aggregate_finalization_nanos: AtomicU64,
    executor_planning_tasks: AtomicU64,
    executor_planning_queue_nanos: AtomicU64,
    executor_planning_active_nanos: AtomicU64,
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
    index_roots_consulted: AtomicU64,
    index_roots_read: AtomicU64,
    index_pages_read: AtomicU64,
    pack_directory_roots_consulted: AtomicU64,
    pack_directory_pages_read: AtomicU64,
    pack_directory_page_cache_hits: AtomicU64,
    pack_directory_extents_returned: AtomicU64,
    prefetched_blocks: AtomicU64,
    prefetch_hits: AtomicU64,
    coalesced_block_load_waits: AtomicU64,
    coalesced_block_load_wait_nanos: AtomicU64,
    used_exact_daily_aggregates: AtomicBool,
    started: Instant,
    work_memory: Option<Arc<WorkMemoryGovernor>>,
}

impl QueryBudget {
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
        options
            .deadline
            .get_or_insert_with(|| Instant::now() + crate::lifecycle::DEFAULT_WAIT_TIMEOUT);
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
            aggregate_base_point_lookups: AtomicU64::new(0),
            segment_descriptors_examined: AtomicU64::new(0),
            series_index_lookups: AtomicU64::new(0),
            planned_segment_reads: AtomicU64::new(0),
            unique_planned_segments: AtomicU64::new(0),
            unique_planned_files: AtomicU64::new(0),
            planned_block_references: AtomicU64::new(0),
            unique_planned_blocks: AtomicU64::new(0),
            planning_nanos: AtomicU64::new(0),
            memtable_planning_nanos: AtomicU64::new(0),
            segment_planning_nanos: AtomicU64::new(0),
            aggregate_planning_nanos: AtomicU64::new(0),
            metadata_nanos: AtomicU64::new(0),
            io_nanos: AtomicU64::new(0),
            decode_nanos: AtomicU64::new(0),
            mvcc_nanos: AtomicU64::new(0),
            aggregate_accumulation_nanos: AtomicU64::new(0),
            aggregate_finalization_nanos: AtomicU64::new(0),
            executor_planning_tasks: AtomicU64::new(0),
            executor_planning_queue_nanos: AtomicU64::new(0),
            executor_planning_active_nanos: AtomicU64::new(0),
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
            index_roots_consulted: AtomicU64::new(0),
            index_roots_read: AtomicU64::new(0),
            index_pages_read: AtomicU64::new(0),
            pack_directory_roots_consulted: AtomicU64::new(0),
            pack_directory_pages_read: AtomicU64::new(0),
            pack_directory_page_cache_hits: AtomicU64::new(0),
            pack_directory_extents_returned: AtomicU64::new(0),
            prefetched_blocks: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
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

    pub(crate) fn local_metadata<T>(
        &self,
        segment_id: u64,
        kind: QueryMetadataKind,
    ) -> Result<Option<Arc<T>>>
    where
        T: Any + Send + Sync,
    {
        let state = self
            .local_metadata
            .lock()
            .map_err(|_| Error::Corruption("query-local metadata mutex is poisoned".into()))?;
        let Some(entry) = state.get(&QueryMetadataKey { segment_id, kind }) else {
            return Ok(None);
        };
        if entry.series.is_some() {
            return Ok(None);
        }
        let value = Arc::clone(&entry.value).downcast::<T>().map_err(|_| {
            Error::Corruption("query-local metadata entry has the wrong type".into())
        })?;
        add_saturating(&self.metadata_local_hits, 1);
        Ok(Some(value))
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
            QueryMemoryClass::Plan,
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

    pub(crate) fn record_physical_block(&self) {
        add_saturating(&self.blocks_read, 1);
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

    pub(crate) fn record_index_root_read(&self) {
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

    pub(crate) fn record_pack_directory_extents(&self, extents: u64) {
        add_saturating(&self.pack_directory_extents_returned, extents);
    }

    pub(crate) fn record_prefetched_block(&self) {
        add_saturating(&self.prefetched_blocks, 1);
    }

    pub(crate) fn record_prefetch_hit(&self) {
        add_saturating(&self.prefetch_hits, 1);
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

    pub(crate) fn prefetch_memory_limit(&self) -> u64 {
        (self.options.max_memory_bytes / 8).min(2 * 1024 * 1024)
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

    pub(crate) fn record_aggregate_base_point_lookup(&self) {
        add_saturating(&self.aggregate_base_point_lookups, 1);
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
        self.reserve_memory_class(QueryMemoryClass::Plan, bytes)
    }

    pub(crate) fn reserve_memory_class(&self, class: QueryMemoryClass, bytes: u64) -> Result<()> {
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

    pub(crate) fn record_plan(
        &self,
        descriptors_examined: u64,
        series_index_lookups: u64,
        duration: Duration,
    ) {
        add_saturating(&self.segment_descriptors_examined, descriptors_examined);
        add_saturating(&self.series_index_lookups, series_index_lookups);
        add_saturating(&self.planning_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_planned_segment_reads(&self, count: u64) {
        add_saturating(&self.planned_segment_reads, count);
    }

    pub(crate) fn record_read_plan(
        &self,
        unique_segments: u64,
        unique_files: u64,
        block_references: u64,
        unique_blocks: u64,
    ) {
        self.unique_planned_segments
            .store(unique_segments, Ordering::Relaxed);
        self.unique_planned_files
            .store(unique_files, Ordering::Relaxed);
        self.planned_block_references
            .store(block_references, Ordering::Relaxed);
        self.unique_planned_blocks
            .store(unique_blocks, Ordering::Relaxed);
    }

    pub(crate) fn record_memtable_planning(&self, duration: Duration) {
        add_saturating(&self.memtable_planning_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_segment_planning(&self, duration: Duration) {
        add_saturating(&self.segment_planning_nanos, duration_nanos(duration));
    }

    pub(crate) fn record_aggregate_planning(&self, duration: Duration) {
        add_saturating(&self.aggregate_planning_nanos, duration_nanos(duration));
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

    pub(crate) fn record_executor(
        &self,
        kind: ExecutorTaskKind,
        queue: Duration,
        active: Duration,
    ) {
        let (tasks, queue_nanos, active_nanos) = match kind {
            ExecutorTaskKind::Planning => (
                &self.executor_planning_tasks,
                &self.executor_planning_queue_nanos,
                &self.executor_planning_active_nanos,
            ),
            ExecutorTaskKind::Series => (
                &self.executor_series_tasks,
                &self.executor_series_queue_nanos,
                &self.executor_series_active_nanos,
            ),
        };
        add_saturating(queue_nanos, duration_nanos(queue));
        add_saturating(active_nanos, duration_nanos(active));
        add_saturating(tasks, 1);
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
        self.release_memory_class(QueryMemoryClass::Plan, bytes);
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
        let planning_queue = self.executor_planning_queue_nanos.load(Ordering::Relaxed);
        let planning_active = self.executor_planning_active_nanos.load(Ordering::Relaxed);
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
            prefetched_blocks: self.prefetched_blocks.load(Ordering::Relaxed),
            prefetch_hits: self.prefetch_hits.load(Ordering::Relaxed),
            unused_prefetched_blocks: self
                .prefetched_blocks
                .load(Ordering::Relaxed)
                .saturating_sub(self.prefetch_hits.load(Ordering::Relaxed)),
            coalesced_block_load_waits: self.coalesced_block_load_waits.load(Ordering::Relaxed),
            coalesced_block_load_wait_duration: Duration::from_nanos(
                self.coalesced_block_load_wait_nanos.load(Ordering::Relaxed),
            ),
            used_exact_daily_aggregates: self.used_exact_daily_aggregates.load(Ordering::Relaxed),
            result_rows: self.result_rows.load(Ordering::Relaxed),
            peak_memory_bytes: self.peak_memory_bytes.load(Ordering::Relaxed),
            plan_peak_memory_bytes: self.peak_memory_by_class[QueryMemoryClass::Plan.index()]
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
            aggregate_base_point_lookups: self.aggregate_base_point_lookups.load(Ordering::Relaxed),
            segment_descriptors_examined: self.segment_descriptors_examined.load(Ordering::Relaxed),
            series_index_lookups: self.series_index_lookups.load(Ordering::Relaxed),
            planned_segment_reads: self.planned_segment_reads.load(Ordering::Relaxed),
            unique_planned_segments: self.unique_planned_segments.load(Ordering::Relaxed),
            unique_planned_files: self.unique_planned_files.load(Ordering::Relaxed),
            planned_block_references: self.planned_block_references.load(Ordering::Relaxed),
            unique_planned_blocks: self.unique_planned_blocks.load(Ordering::Relaxed),
            planning_duration: Duration::from_nanos(self.planning_nanos.load(Ordering::Relaxed)),
            memtable_planning_duration: Duration::from_nanos(
                self.memtable_planning_nanos.load(Ordering::Relaxed),
            ),
            segment_planning_duration: Duration::from_nanos(
                self.segment_planning_nanos.load(Ordering::Relaxed),
            ),
            aggregate_planning_duration: Duration::from_nanos(
                self.aggregate_planning_nanos.load(Ordering::Relaxed),
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
            executor_queue_duration: Duration::from_nanos(
                planning_queue.saturating_add(series_queue),
            ),
            executor_active_duration: Duration::from_nanos(
                planning_active.saturating_add(series_active),
            ),
            executor_planning_tasks: self.executor_planning_tasks.load(Ordering::Relaxed),
            executor_planning_queue_duration: Duration::from_nanos(planning_queue),
            executor_planning_active_duration: Duration::from_nanos(planning_active),
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
    Segment,
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
            QueryMemoryClass::Plan,
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
    Plan,
    Metadata,
    Compressed,
    Decoded,
    Results,
}

impl QueryMemoryClass {
    const fn index(self) -> usize {
        match self {
            Self::Plan => 0,
            Self::Metadata => 1,
            Self::Compressed => 2,
            Self::Decoded => 3,
            Self::Results => 4,
        }
    }

    const fn work_class(self) -> WorkMemoryClass {
        match self {
            Self::Plan => WorkMemoryClass::QueryPlan,
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
                std::thread::sleep(crate::lifecycle::WAIT_SLICE)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_metrics_sum_each_task_class_once_and_saturate() -> Result<()> {
        let budget = QueryBudget::new_with_admission(
            QueryOptions::default(),
            WorkMemoryGovernor::new(1024),
        )?;
        std::thread::scope(|scope| {
            for kind in [ExecutorTaskKind::Planning, ExecutorTaskKind::Series] {
                let budget = &budget;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        budget.record_executor(
                            kind,
                            Duration::from_nanos(7),
                            Duration::from_nanos(11),
                        );
                    }
                });
            }
        });
        let stats = budget.stats();
        assert_eq!(stats.executor_planning_tasks, 1000);
        assert_eq!(stats.executor_series_tasks, 1000);
        assert_eq!(
            stats.executor_planning_queue_duration,
            Duration::from_nanos(7000)
        );
        assert_eq!(
            stats.executor_series_active_duration,
            Duration::from_nanos(11000)
        );
        assert_eq!(stats.executor_queue_duration, Duration::from_nanos(14000));
        assert_eq!(stats.executor_active_duration, Duration::from_nanos(22000));
        budget
            .executor_planning_tasks
            .store(u64::MAX, Ordering::Relaxed);
        budget.record_executor(ExecutorTaskKind::Planning, Duration::MAX, Duration::MAX);
        budget.record_executor(
            ExecutorTaskKind::Planning,
            Duration::from_nanos(1),
            Duration::from_nanos(1),
        );
        let stats = budget.stats();
        assert_eq!(stats.executor_planning_tasks, u64::MAX);
        assert_eq!(
            stats.executor_queue_duration,
            Duration::from_nanos(u64::MAX)
        );
        assert_eq!(
            stats.executor_active_duration,
            Duration::from_nanos(u64::MAX)
        );
        Ok(())
    }

    #[test]
    fn shared_file_or_metadata_lock_observes_query_deadline() -> Result<()> {
        let mutex = Mutex::new(());
        let guard = mutex.lock().expect("owner");
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                deadline: Some(Instant::now() + Duration::from_millis(20)),
                ..QueryOptions::default()
            },
            WorkMemoryGovernor::new(1024),
        )?;
        let started = Instant::now();
        assert!(matches!(
            lock_for_query(&mutex, &budget),
            Err(Error::QueryDeadlineExceeded)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(guard);
        Ok(())
    }

    #[test]
    fn shared_work_memory_is_bounded_classified_and_released() -> Result<()> {
        let governor = WorkMemoryGovernor::new(64);
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                max_memory_bytes: 64,
                ..QueryOptions::default()
            },
            Arc::clone(&governor),
        )?;
        budget.reserve_memory_class(QueryMemoryClass::Plan, 16)?;
        budget.reserve_memory_class(QueryMemoryClass::Compressed, 24)?;
        assert!(
            budget
                .reserve_memory_class(QueryMemoryClass::Decoded, 32)
                .is_err()
        );
        let stats = budget.stats();
        assert_eq!(stats.plan_peak_memory_bytes, 16);
        assert_eq!(stats.compressed_input_peak_memory_bytes, 24);
        assert_eq!(governor.stats()?.used_bytes, 40);
        drop(budget);
        assert_eq!(governor.stats()?.used_bytes, 0);
        Ok(())
    }

    #[test]
    fn query_local_metadata_distinguishes_partial_and_full_coverage() -> Result<()> {
        let governor = WorkMemoryGovernor::new(1_024);
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                max_memory_bytes: 1_024,
                ..QueryOptions::default()
            },
            Arc::clone(&governor),
        )?;
        let first = crate::types::SeriesKey::new(1, 1, 1);
        let second = crate::types::SeriesKey::new(1, 1, 2);

        budget.reserve_memory_class(QueryMemoryClass::Metadata, 100)?;
        budget.keep_local_series_metadata(
            7,
            QueryMetadataKind::Aggregate,
            vec![first],
            Arc::new(11_u64),
            100,
            100,
        )?;
        assert!(
            budget
                .local_metadata::<u64>(7, QueryMetadataKind::Aggregate)?
                .is_none()
        );
        assert_eq!(
            *budget
                .local_series_metadata::<u64>(7, QueryMetadataKind::Aggregate, &[first])?
                .ok_or_else(|| Error::Corruption("missing partial metadata".into()))?,
            11
        );

        budget.reserve_memory_class(QueryMemoryClass::Metadata, 200)?;
        budget.keep_local_metadata(7, QueryMetadataKind::Aggregate, Arc::new(22_u64), 200, 200)?;
        assert_eq!(budget.stats().metadata_local_pinned_bytes, 200);
        assert_eq!(governor.stats()?.used_bytes, 200);
        for series in [first, second] {
            assert_eq!(
                *budget
                    .local_series_metadata::<u64>(7, QueryMetadataKind::Aggregate, &[series],)?
                    .ok_or_else(|| Error::Corruption("missing full metadata".into()))?,
                22
            );
        }

        budget.reserve_memory_class(QueryMemoryClass::Metadata, 50)?;
        let retained = budget.keep_local_series_metadata(
            7,
            QueryMetadataKind::Aggregate,
            vec![second],
            Arc::new(33_u64),
            50,
            50,
        )?;
        assert_eq!(*retained, 22);
        assert_eq!(governor.stats()?.used_bytes, 200);
        budget.release_local_metadata()?;
        assert_eq!(governor.stats()?.used_bytes, 0);
        Ok(())
    }
}
