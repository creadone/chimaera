//! Настройки открытия и ограничения фоновой работы.

use super::{
    APPROXIMATE_POINT_BYTES, DEFAULT_AGGREGATE_ARCHIVE_CHUNK_BYTES, DEFAULT_HISTORY_SEAL_LAG,
    DEFAULT_HISTORY_SEGMENT_TARGET_BYTES, DEFAULT_HISTORY_WINDOW, UNMANAGED_FILE_RESERVE,
};
use crate::clock::{Clock, SystemClock};
use crate::error::{Result, invalid};
use crate::layout::{DEFAULT_PARTITION_SPAN_US, DEFAULT_VIRTUAL_SHARDS, validate_layout};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Ограничения памяти, очередей и фоновых работников ядра.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub create_if_missing: bool,
    pub checkpoint_on_close: bool,
    /// Экспериментальный read-only directory для точного immutable checkpoint.
    pub offline_pack_directory: Option<PathBuf>,
    pub virtual_shards: u16,
    pub partition_span_us: i64,
    pub memtable_max_bytes: u64,
    pub block_cache_max_bytes: u64,
    pub metadata_cache_max_bytes: u64,
    pub block_load_max_in_flight: usize,
    pub block_load_max_waiters: usize,
    pub block_load_max_bytes: u64,
    pub memory_max_bytes: u64,
    pub file_descriptor_max: usize,
    pub minimum_free_disk_bytes: u64,
    pub compaction_output_percent: u16,
    pub write_queue_capacity: usize,
    pub flush_queue_capacity: usize,
    /// Общий пул упаковки L0 и независимых выходов слияния.
    pub flush_worker_count: usize,
    pub flush_pack_queue_capacity: usize,
    pub publication_queue_capacity: usize,
    pub compaction_queue_capacity: usize,
    /// Полный отступ от наибольшего наблюдавшегося timestamp до закрытой истории.
    /// `None` отключает физический L2.
    pub history_seal_lag: Option<Duration>,
    /// Базовая временная сетка для адаптивных закрытых частей.
    pub history_window: Duration,
    /// Мягкий предел размера одного закрытого сегмента.
    pub history_segment_target_bytes: u64,
    /// Число исторических L0 одной области, после которого запускается repair.
    pub history_rewrite_trigger: usize,
    /// Максимальный размер одной независимо проверяемой части aggregate archive.
    pub aggregate_archive_chunk_bytes: u64,
    /// Отдельный ограниченный хвост исторических переписываний.
    pub history_compaction_queue_capacity: usize,
    pub wal_checkpoint_bytes: u64,
    pub checkpoint_interval: Duration,
    pub cold_flush_idle_interval: Duration,
    pub l0_compaction_trigger: usize,
    pub l0_slowdown_trigger: usize,
    pub l0_stop_trigger: usize,
    pub read_latency_slowdown_threshold: Duration,
    pub wide_read_latency_slowdown_threshold: Duration,
    pub write_pacing_max_delay: Duration,
    pub clock: Arc<dyn Clock>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            checkpoint_on_close: true,
            offline_pack_directory: None,
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
            partition_span_us: DEFAULT_PARTITION_SPAN_US,
            memtable_max_bytes: 32 * 1024 * 1024,
            block_cache_max_bytes: 64 * 1024 * 1024,
            metadata_cache_max_bytes: 64 * 1024 * 1024,
            block_load_max_in_flight: 64,
            block_load_max_waiters: 1_024,
            block_load_max_bytes: 64 * 1024 * 1024,
            memory_max_bytes: 160 * 1024 * 1024,
            file_descriptor_max: 128,
            minimum_free_disk_bytes: 512 * 1024 * 1024,
            compaction_output_percent: 125,
            write_queue_capacity: 64,
            flush_queue_capacity: 2,
            flush_worker_count: 4,
            flush_pack_queue_capacity: 8,
            publication_queue_capacity: 4,
            compaction_queue_capacity: 2,
            history_seal_lag: Some(DEFAULT_HISTORY_SEAL_LAG),
            history_window: DEFAULT_HISTORY_WINDOW,
            history_segment_target_bytes: DEFAULT_HISTORY_SEGMENT_TARGET_BYTES,
            history_rewrite_trigger: 4,
            aggregate_archive_chunk_bytes: DEFAULT_AGGREGATE_ARCHIVE_CHUNK_BYTES,
            history_compaction_queue_capacity: 1,
            wal_checkpoint_bytes: 512 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(5 * 60),
            cold_flush_idle_interval: Duration::from_secs(1),
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 16,
            l0_stop_trigger: 32,
            read_latency_slowdown_threshold: Duration::from_millis(250),
            wide_read_latency_slowdown_threshold: Duration::from_secs(2),
            write_pacing_max_delay: Duration::from_millis(50),
            clock: Arc::new(SystemClock),
        }
    }
}

impl OpenOptions {
    pub(super) fn validate(&self) -> Result<()> {
        validate_layout(self.virtual_shards, self.partition_span_us)?;
        if self.memtable_max_bytes < APPROXIMATE_POINT_BYTES
            || self.block_cache_max_bytes == 0
            || self.metadata_cache_max_bytes == 0
            || self.block_load_max_in_flight == 0
            || self.block_load_max_waiters == 0
            || self.block_load_max_bytes == 0
            || self.memory_max_bytes < APPROXIMATE_POINT_BYTES
            || self.file_descriptor_max <= self.flush_worker_count + UNMANAGED_FILE_RESERVE
            || !(100..=400).contains(&self.compaction_output_percent)
            || self.write_queue_capacity == 0
            || self.flush_queue_capacity == 0
            || self.flush_worker_count == 0
            || self.flush_worker_count > 32
            || self.flush_pack_queue_capacity == 0
            || self.publication_queue_capacity == 0
            || self.compaction_queue_capacity == 0
            || self.history_window.is_zero()
            || self.history_window.as_micros() > i64::MAX as u128
            || self
                .history_seal_lag
                .is_some_and(|lag| lag.is_zero() || lag.as_micros() > i64::MAX as u128)
            || self.history_segment_target_bytes < 1024 * 1024
            || self.history_rewrite_trigger < 2
            || self.aggregate_archive_chunk_bytes < crate::aggregate::MIN_SECTION_SIZE as u64
            || self.aggregate_archive_chunk_bytes > crate::aggregate::MAX_BYTES as u64
            || self.history_rewrite_trigger >= self.l0_stop_trigger
            || self.history_compaction_queue_capacity == 0
            || self.wal_checkpoint_bytes == 0
            || self.checkpoint_interval.is_zero()
            || self.l0_compaction_trigger < 2
            || self.l0_compaction_trigger >= self.l0_slowdown_trigger
            || self.l0_slowdown_trigger >= self.l0_stop_trigger
            || self.read_latency_slowdown_threshold.is_zero()
            || self.wide_read_latency_slowdown_threshold.is_zero()
            || self.write_pacing_max_delay.is_zero()
            || self.write_pacing_max_delay > Duration::from_secs(1)
        {
            return Err(invalid("open options contain a zero or unsafe limit"));
        }
        Ok(())
    }
}

/// Выбор полного или порогового прохода compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionOptions {
    pub full: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self { full: true }
    }
}
