//! Open settings and limits for background work.

use super::{
    APPROXIMATE_POINT_BYTES, DEFAULT_AGGREGATE_ARCHIVE_CHUNK_BYTES, DEFAULT_HISTORY_SEAL_LAG,
    DEFAULT_HISTORY_SEGMENT_TARGET_BYTES, DEFAULT_HISTORY_WINDOW, UNMANAGED_FILE_RESERVE,
};
use crate::engine::clock::{Clock, SystemClock};
use crate::error::{Result, invalid};
use crate::persistence::layout::{
    DEFAULT_PARTITION_SPAN_US, DEFAULT_VIRTUAL_SHARDS, validate_layout,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Storage layout, memory, queue, and worker limits.
///
/// Defaults below are library settings, not a benchmark profile. Byte limits use
/// binary units (1 MiB = 1,048,576 bytes). Invalid combinations fail at open.
/// Managed memory and working-memory governors share these settings; their limits
/// are accounting bounds, not additive reservations or a process RSS cap.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Create an absent database. Default: `true`.
    pub create_if_missing: bool,
    /// Checkpoint during normal close. Default: `true`; disabling leaves WAL for recovery.
    pub checkpoint_on_close: bool,
    /// Experimental read-only directory for one exact immutable checkpoint.
    /// Default: `None`. Not a general read-only replica mode.
    pub offline_pack_directory: Option<PathBuf>,
    /// Persistent shard count: a power of two in 1..=4096. Default: 64.
    pub virtual_shards: u16,
    /// Positive default time-partition span in microseconds. Default: 30 days.
    /// Persisted layout must match on reopen; registered policies have their own span.
    pub partition_span_us: i64,
    /// Flush threshold in bytes, at least one internal point. Default: 32 MiB.
    pub memtable_max_bytes: u64,
    /// Positive decoded-block cache budget in bytes. Default: 64 MiB.
    pub block_cache_max_bytes: u64,
    /// Combined cached directory/index/aggregate metadata, including retained entries.
    /// Positive byte budget. Default: 64 MiB.
    pub metadata_cache_max_bytes: u64,
    /// Positive concurrent block-load limit. Default: 64.
    pub block_load_max_in_flight: usize,
    /// Positive block-load waiter limit. Default: 1024.
    pub block_load_max_waiters: usize,
    /// Positive byte budget for in-flight block loading. Default: 64 MiB.
    pub block_load_max_bytes: u64,
    /// Managed memory budget in bytes. Default: 160 MiB. Must leave enough reserve
    /// for at least one internal point; cache and query limits do not override it.
    pub memory_max_bytes: u64,
    /// Engine FD budget. Default: 128; must exceed `flush_worker_count + 8`.
    /// The host application needs additional OS descriptor headroom.
    pub file_descriptor_max: usize,
    /// Free-space reserve for WAL append, compaction and repack admission.
    /// WAL checks include the encoded frame size; zero disables its guard.
    /// Queued tickets can fail before WAL with InsufficientDiskSpace. Already
    /// appended frames still sync/publish; flush/checkpoint can use the reserve.
    /// This does not reserve filesystem blocks against concurrent writers.
    /// Default: 512 MiB.
    pub minimum_free_disk_bytes: u64,
    /// Reclaim shared packs with at least 75% dead bytes and 1 MiB to return.
    /// Default: `true`.
    pub retention_repack: bool,
    /// Time grid for scheduling background raw-retention rewrites; zero is immediate.
    /// Read/write visibility stays exact. Explicit compact and archive policies bypass
    /// this grid. Queueing and held snapshots can delay file reclamation further.
    /// Default: zero. A nonzero value must represent 1..=i64::MAX microseconds.
    pub retention_rewrite_interval: Duration,
    /// Maximum compressed bytes copied before yielding the compaction coordinator.
    /// Range: 64 KiB..=32 MiB. Default: 32 MiB.
    pub repack_step_bytes: u64,
    /// Output-space estimate as a percentage of compaction input, 100..=400.
    /// Default: 125; this is admission headroom, not a compression-ratio promise.
    pub compaction_output_percent: u16,
    /// Positive writer command-queue capacity. Default: 64; memory also limits admission.
    pub write_queue_capacity: usize,
    /// Positive frozen-generation queue capacity. Default: 2.
    pub flush_queue_capacity: usize,
    /// Shared pack-writing pool for L0 and independent compaction outputs.
    /// Range: 1..=32. Default: 4.
    pub flush_worker_count: usize,
    /// Shared executor for multi-series reads, 1..=32; defaults to 1.
    /// Its bounded queue holds at most twice this many series tasks; query memory
    /// and deadlines still use the shared governors. With 1, each client reads
    /// serially; separate clients can still run concurrently.
    pub query_worker_count: usize,
    /// Positive pack-job queue capacity. Default: 8.
    pub flush_pack_queue_capacity: usize,
    /// Positive durable-publication queue capacity. Default: 4.
    pub publication_queue_capacity: usize,
    /// Positive active-compaction queue capacity. Default: 2.
    pub compaction_queue_capacity: usize,
    /// Full lag from the largest observed timestamp to sealed history.
    /// `None` disables physical L2.
    /// Default: one hour. A configured lag must be nonzero and fit in i64 microseconds.
    pub history_seal_lag: Option<Duration>,
    /// L2 time window; its segments, merges, and packs do not cross window boundaries.
    /// Default: one hour; must represent 1..=i64::MAX microseconds.
    pub history_window: Duration,
    /// Soft target size of one sealed segment.
    /// At least 1 MiB. Default: 64 MiB.
    pub history_segment_target_bytes: u64,
    /// Number of historical L0 parts that triggers consolidation.
    /// Also the minimum number of small L2 segments for bounded consolidation.
    /// Default: 4; at least 2 and below `l0_stop_trigger`.
    pub history_rewrite_trigger: usize,
    /// Maximum size of one independently verifiable aggregate-archive chunk.
    /// Range: 184 bytes..=64 MiB. Default: 8 MiB.
    pub aggregate_archive_chunk_bytes: u64,
    /// Separate bounded queue for historical rewrites.
    /// Positive capacity. Default: 1.
    pub history_compaction_queue_capacity: usize,
    /// Positive uncovered-WAL byte threshold for checkpoint scheduling. Default: 512 MiB.
    pub wal_checkpoint_bytes: u64,
    /// Nonzero periodic checkpoint interval. Default: 300 seconds.
    pub checkpoint_interval: Duration,
    /// Idle cold-write age that requests a checkpoint. Default: one second.
    /// Zero makes it eligible on the next maintenance poll; it does not disable it.
    pub cold_flush_idle_interval: Duration,
    /// L0 consolidation threshold, at least 2 and below `l0_slowdown_trigger`.
    /// Default: 4.
    pub l0_compaction_trigger: usize,
    /// L0 write-slowdown threshold, below `l0_stop_trigger`. Default: 16.
    pub l0_slowdown_trigger: usize,
    /// L0 write-stop threshold. Default: 32.
    pub l0_stop_trigger: usize,
    /// Nonzero ordinary-read latency threshold for write pacing. Default: 250 ms.
    pub read_latency_slowdown_threshold: Duration,
    /// Nonzero wide-read latency threshold for write pacing. Default: two seconds.
    pub wide_read_latency_slowdown_threshold: Duration,
    /// Maximum pacing delay, greater than zero and at most one second. Default: 50 ms.
    pub write_pacing_max_delay: Duration,
    /// Trusted clock for time policies and retention. Default: [`SystemClock`].
    /// A custom clock changes data expiry; it is not just a diagnostics timestamp.
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
            retention_repack: true,
            retention_rewrite_interval: Duration::ZERO,
            repack_step_bytes: 32 * 1024 * 1024,
            compaction_output_percent: 125,
            write_queue_capacity: 64,
            flush_queue_capacity: 2,
            flush_worker_count: 4,
            query_worker_count: 1,
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
    // Two memtables allow a flush handoff. Queue bytes are capped at a quarter
    // of managed memory: admission, not queue slots, bounds accepted batches.
    pub(super) fn write_memory_reserve(&self) -> u64 {
        let batch = (super::MAX_BATCH_POINTS as u64).saturating_mul(APPROXIMATE_POINT_BYTES);
        let queue = batch
            .saturating_mul(self.write_queue_capacity as u64)
            .min(self.memory_max_bytes / 4);
        self.memtable_max_bytes
            .saturating_mul(2)
            .saturating_add(queue)
            .min(self.memory_max_bytes / 2)
    }

    pub(super) fn validate(&self) -> Result<()> {
        validate_layout(self.virtual_shards, self.partition_span_us)?;
        if self.memtable_max_bytes < APPROXIMATE_POINT_BYTES
            || self.block_cache_max_bytes == 0
            || self.metadata_cache_max_bytes == 0
            || !(64 * 1024..=32 * 1024 * 1024).contains(&self.repack_step_bytes)
            || (!self.retention_rewrite_interval.is_zero()
                && self.retention_rewrite_interval.as_micros() == 0)
            || self.retention_rewrite_interval.as_micros() > i64::MAX as u128
            || self.block_load_max_in_flight == 0
            || self.block_load_max_waiters == 0
            || self.block_load_max_bytes == 0
            || self.write_memory_reserve() < APPROXIMATE_POINT_BYTES
            || self.file_descriptor_max <= self.flush_worker_count + UNMANAGED_FILE_RESERVE
            || !(100..=400).contains(&self.compaction_output_percent)
            || self.write_queue_capacity == 0
            || self.flush_queue_capacity == 0
            || self.flush_worker_count == 0
            || self.flush_worker_count > 32
            || !(1..=32).contains(&self.query_worker_count)
            || self.flush_pack_queue_capacity == 0
            || self.publication_queue_capacity == 0
            || self.compaction_queue_capacity == 0
            || self.history_window.as_micros() == 0
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

/// Select a full or threshold-driven compaction pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionOptions {
    pub full: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self { full: true }
    }
}
