//! Общее состояние Storage, каналы фоновых ролей и публичный фасад движка.

use crate::admission::WorkMemoryGovernor;
use crate::aggregate_archive::AggregateArchive;
use crate::cache::{BlockCache, MemoryGovernor, MemoryReservation};
use crate::catalog::{
    BucketDefinition, CatalogSnapshot, PolicySelector, SpaceDefinition, StoragePolicy,
    StoragePolicyOptions,
};
use crate::error::{Error, Result, corrupt};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::layout::Domain;
use crate::lifecycle::{WriteAdmission, WriteReply};
use crate::segment::{
    MetadataCache, OfflinePackDirectory, PointBatch, Segment, SegmentWriteRequest,
};
use crate::snapshot::SnapshotImage;
use crate::storage_lock::StorageLock;
use crate::types::{AggregateResolution, DAY_US, Durability, SeriesKey, WriteOperation};
use crossbeam_channel::Sender;
use executor::QueryExecutor;
use index::{PublishedReadDirectoryIndex, PublishedSegmentIndex};
use lifecycle::shutdown_inner;
use publication::UnpublishedArtifacts;
use regulator::WriteRegulator;
use snapshots::{SnapshotGuard, SnapshotTracker};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use support::DrainState;

#[cfg(test)]
use crossbeam_channel::Receiver;

mod admin;
mod admission;
mod batch;
mod catalog;
mod compaction;
mod compaction_ranges;
mod compaction_selection;
mod exchange;
mod executor;
mod flush;
mod group;
mod group_cursor;
mod hybrid;
mod index;
mod lifecycle;
mod maintenance;
mod options;
mod planning;
mod publication;
mod read;
mod read_directory;
mod reclamation;
mod recovery;
mod regulator;
mod snapshots;
mod stats;
mod support;
#[cfg(test)]
mod tests;
mod writer;

pub use group::GroupStream;
pub use options::{CompactionOptions, OpenOptions};
pub use read::{GroupResult, GroupStreamResult, PointStream};
pub use stats::{HistoryWatermarkStats, L1CompactionDebtStats, StorageStats};

const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;
const APPROXIMATE_POINT_BYTES: u64 = 48;
// Includes the key/value payload, hash-table control bytes, and spare capacity.
const APPROXIMATE_SERIES_WATERMARK_BYTES: u64 = 64;
const MAX_COMPACTION_DOMAINS: usize = 16;
// macOS commonly starts processes with a 256-FD soft limit. Leave half of that
// budget for the WAL, output packs, manifest publication, queries, and metrics.
const MAX_COMPACTION_INPUT_SEGMENTS: usize = 128;
const ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS: usize = 16;
const ACTIVE_COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const L1_OVERLAP_REPAIR_MAX_INPUT_BYTES: u64 = 128 * 1024 * 1024;
const ACTIVE_L1_CONSOLIDATION_TRIGGER: usize = 8;
const ACTIVE_L1_DELTA_BASE_RATIO: u64 = 1;
const COMPACTION_OUTPUT_SPAN_US: i64 = 10 * 60 * 1_000_000;
const MAX_COMPACTION_OUTPUT_RANGES: usize = 64;
const DEFAULT_HISTORY_SEAL_LAG: Duration = Duration::from_secs(60 * 60);
const DEFAULT_HISTORY_WINDOW: Duration = Duration::from_secs(60 * 60);
const READ_DIRECTORY_EPOCH_US: i64 = 30 * DAY_US;
const DEFAULT_HISTORY_SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_AGGREGATE_ARCHIVE_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const MAX_HYBRID_QUERY_DAYS: usize = 4_096;
const UNMANAGED_FILE_RESERVE: usize = 8;
const GROUP_COMMIT_MAX_BATCHES: usize = 64;
const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(200);
const READ_LATENCY_WINDOW: usize = 128;
const READ_LATENCY_WIDE_WINDOW: usize = 16;
const READ_LATENCY_MIN_SAMPLES: usize = 32;
const READ_LATENCY_WIDE_MIN_SAMPLES: usize = 3;
const WRITE_PRESSURE_STREAK: u8 = 3;
const MAX_QUERY_WORKERS: usize = 4;
const QUERY_WORK_WAIT: Duration = Duration::from_millis(10);
const BATCH_CONSOLIDATION_THRESHOLD: usize = 16;

#[derive(Clone)]
struct CachedL1Debt {
    stats: L1CompactionDebtStats,
    oldest_eligible: Option<Instant>,
    eligible_since: HashMap<u64, Instant>,
}

type Batch = Arc<PointBatch>;

#[derive(Debug)]
struct Generation {
    id: u64,
    maximum_commit: u64,
    batches: Vec<Batch>,
    bytes: u64,
    wal_paths: Vec<PathBuf>,
}

struct FlushSlice {
    batch: Batch,
    start: usize,
    end: usize,
}

struct State {
    visible_commit: u64,
    checkpoint_commit: u64,
    active_batches: Vec<Batch>,
    active_bytes: u64,
    frozen: Vec<Arc<Generation>>,
    segments: Vec<Arc<Segment>>,
    published_read_directories: Vec<Arc<OfflinePackDirectory>>,
    published_read_directory_index: PublishedReadDirectoryIndex,
    segment_published_at: HashMap<u64, Instant>,
    l1_debt: BTreeMap<Domain, CachedL1Debt>,
    aggregate_archives: Vec<Arc<AggregateArchive>>,
    segment_index: PublishedSegmentIndex,
    catalog: CatalogSnapshot,
    maintenance_now_us: i64,
    retention_cutoffs: BTreeMap<u32, i64>,
    aggregate_retention_cutoffs: BTreeMap<u32, i64>,
    event_watermarks: BTreeMap<u32, i64>,
    wal_bytes_total: u64,
    checkpoint_wal_bytes: u64,
    last_cold_write: Option<(u64, Instant)>,
}

#[derive(Default)]
struct Metrics {
    publisher_phase: AtomicU64,
    shutdown_phase: AtomicU64,
    read_directory_build_attempts: AtomicU64,
    read_directory_build_resource_skips: AtomicU64,
    read_directory_build_nanos: AtomicU64,
    read_directory_build_max_nanos: AtomicU64,
    read_directory_build_current_segments: AtomicU64,
    read_directory_build_max_segments: AtomicU64,
    compaction_current_input_bytes: AtomicU64,
    writes: AtomicU64,
    written_operations: AtomicU64,
    written_points: AtomicU64,
    deleted_points: AtomicU64,
    strict_series_watermarks: AtomicU64,
    wal_bytes: AtomicU64,
    wal_syncs: AtomicU64,
    wal_sync_nanos: AtomicU64,
    group_commit_groups: AtomicU64,
    group_commit_batches: AtomicU64,
    flushes: AtomicU64,
    flush_nanos: AtomicU64,
    flush_bytes: AtomicU64,
    compactions: AtomicU64,
    compacted_domains: AtomicU64,
    compaction_nanos: AtomicU64,
    compaction_bytes_read: AtomicU64,
    compaction_bytes_written: AtomicU64,
    compaction_physical_bytes_written: AtomicU64,
    compaction_block_visits: AtomicU64,
    compaction_unique_blocks: AtomicU64,
    compaction_unique_block_bytes: AtomicU64,
    compaction_physical_blocks_read: AtomicU64,
    compaction_physical_bytes_read: AtomicU64,
    compaction_decoded_input_bytes: AtomicU64,
    compaction_decoded_points: AtomicU64,
    compaction_decode_nanos: AtomicU64,
    active_compaction_physical_bytes_read: AtomicU64,
    active_compaction_physical_bytes_written: AtomicU64,
    active_compaction_rewrite_passes: AtomicU64,
    active_compaction_jobs: AtomicU64,
    l1_overlap_repair_jobs: AtomicU64,
    l1_overlap_repair_input_bytes: AtomicU64,
    l1_overlap_repair_max_input_bytes: AtomicU64,
    active_compaction_max_input_bytes: AtomicU64,
    active_compaction_max_output_bytes: AtomicU64,
    active_compaction_max_fan_in: AtomicU64,
    sealed_compaction_physical_bytes_read: AtomicU64,
    sealed_compaction_physical_bytes_written: AtomicU64,
    sealed_compaction_rewrite_passes: AtomicU64,
    sealed_compaction_jobs: AtomicU64,
    sealed_compaction_max_input_bytes: AtomicU64,
    sealed_compaction_max_output_bytes: AtomicU64,
    sealed_compaction_max_fan_in: AtomicU64,
    compaction_fair_handoffs: AtomicU64,
    manifest_publications: AtomicU64,
    manifest_encode_nanos: AtomicU64,
    manifest_write_nanos: AtomicU64,
    manifest_fsync_nanos: AtomicU64,
    manifest_lock_wait_nanos: AtomicU64,
    manifest_expired_segments: AtomicU64,
    manifest_expired_bytes: AtomicU64,
    write_backpressure: AtomicU64,
    write_backpressure_nanos: AtomicU64,
    flush_backpressure: AtomicU64,
    flush_backpressure_nanos: AtomicU64,
    background_errors: AtomicU64,
    background_checkpoints: AtomicU64,
    background_checkpoint_nanos: AtomicU64,
    foreground_maintenance_waits: AtomicU64,
    foreground_maintenance_wait_nanos: AtomicU64,
    write_pacing_events: AtomicU64,
    write_pacing_nanos: AtomicU64,
    memory_backpressure: AtomicU64,
    write_queue_memory_backpressure: AtomicU64,
    read_directory_build_memory_backpressure: AtomicU64,
    read_directory_root_memory_backpressure: AtomicU64,
    compaction_space_rejections: AtomicU64,
    last_compaction_required_bytes: AtomicU64,
    query_operations_scanned: AtomicU64,
    query_operation_bytes_decoded: AtomicU64,
    query_metadata_bytes_read: AtomicU64,
    query_payload_bytes_read: AtomicU64,
    query_blocks_read: AtomicU64,
    query_physical_blocks_decoded: AtomicU64,
    query_physical_decoded_bytes: AtomicU64,
    query_file_opens: AtomicU64,
    query_prefetched_blocks: AtomicU64,
    query_prefetch_hits: AtomicU64,
    query_planned_segment_reads: AtomicU64,
    query_segment_index_lookups: AtomicU64,
    query_segment_index_entries_examined: AtomicU64,
    query_segment_index_candidates: AtomicU64,
    query_segment_index_nanos: AtomicU64,
    query_peak_memory_bytes: AtomicU64,
    query_exact_daily_aggregate_queries: AtomicU64,
    query_aggregate_records_read: AtomicU64,
    query_points_returned: AtomicU64,
    recovery_nanos: AtomicU64,
    recovery_manifest_nanos: AtomicU64,
    recovery_descriptors_nanos: AtomicU64,
    recovery_read_directory_pages: AtomicU64,
    recovery_read_directory_bytes: AtomicU64,
    recovery_wal_scan_nanos: AtomicU64,
    recovery_wal_replay_nanos: AtomicU64,
    recovery_read_ready_nanos: AtomicU64,
    recovery_write_ready_nanos: AtomicU64,
    retired_segment_groups: AtomicU64,
    retired_segment_bytes: AtomicU64,
    retired_backlog_nanos: AtomicU64,
}

struct WorkerHandles {
    maintenance: JoinHandle<()>,
    writer: JoinHandle<()>,
    flush: JoinHandle<()>,
    flush_pack: Vec<JoinHandle<()>>,
    publisher: JoinHandle<()>,
    compaction: JoinHandle<()>,
}

struct Inner {
    root: PathBuf,
    storage_uuid: [u8; 16],
    options: OpenOptions,
    state: RwLock<State>,
    lifecycle: AtomicU8,
    admission: Arc<WriteAdmission>,
    drain_state: Mutex<DrainState>,
    shutdown_wait: Mutex<()>,
    shutdown_complete: Condvar,
    shutdown_failure: Mutex<Option<String>>,
    degraded: Mutex<Option<String>>,
    metrics: Arc<Metrics>,
    block_cache: Arc<BlockCache>,
    metadata_cache: Arc<MetadataCache>,
    memory_governor: Arc<MemoryGovernor>,
    directory_page_pool: Arc<crate::segment::DirectoryPagePool>,
    work_memory_governor: Arc<WorkMemoryGovernor>,
    file_pool: FilePermitPool,
    open_file_cache: Arc<OpenFileCache>,
    offline_pack_directory: Option<Arc<OfflinePackDirectory>>,
    retired_read_directories: Mutex<Vec<Arc<OfflinePackDirectory>>>,
    snapshot_tracker: Arc<SnapshotTracker>,
    write_regulator: Arc<WriteRegulator>,
    query_executor: Arc<QueryExecutor>,
    next_segment_id: AtomicU64,
    write_tx: Sender<WriteCommand>,
    flush_tx: Sender<FlushCommand>,
    flush_pack_tx: Sender<FlushPackCommand>,
    publish_tx: Sender<PublishCommand>,
    compaction_tx: Sender<CompactionCommand>,
    history_compaction_tx: Sender<CompactionCommand>,
    maintenance_tx: Sender<MaintenanceCommand>,
    workers: Mutex<Option<WorkerHandles>>,
    storage_lock: Mutex<Option<Arc<StorageLock>>>,
}

/// Открытая база. Клоны используют те же ограниченные очереди и снимки.
#[derive(Clone)]
pub struct Storage {
    _clients: Arc<ClientOwner>,
    inner: Arc<Inner>,
}

struct ClientOwner {
    inner: Weak<Inner>,
}

impl Drop for ClientOwner {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade()
            && inner.lifecycle.load(Ordering::Acquire) == OPEN
        {
            let _ = shutdown_inner(&inner);
        }
    }
}

enum WriteCommand {
    Write {
        operations: Vec<WriteOperation>,
        durability: Durability,
        memory_reservation: MemoryReservation,
        reply: WriteReply,
    },
    Checkpoint {
        reply: Sender<Result<u64>>,
    },
    Flush {
        reply: Sender<Result<u64>>,
    },
    Snapshot {
        reply: Sender<Result<SnapshotImage>>,
    },
    CreateBucket {
        space: String,
        name: String,
        policy: PolicySelector,
        created_at_us: i64,
        reply: Sender<Result<BucketDefinition>>,
    },
    Stop {
        reply: Sender<Result<()>>,
    },
}

enum FlushCommand {
    Generation(Arc<Generation>),
    Barrier {
        barrier_commit: u64,
        wal_bytes_through_commit: u64,
        checkpoint: bool,
        reply: Sender<Result<u64>>,
    },
    Stop,
}

enum FlushPackCommand {
    Write {
        pack_id: u64,
        shard_id: u16,
        requests: Vec<SegmentWriteRequest>,
        reply: Sender<Result<FlushPackOutput>>,
    },
    Stop,
}

struct FlushPackOutput {
    segments: Vec<crate::segment::SegmentWriteResult>,
    unpublished: UnpublishedArtifacts,
}

enum PublishCommand {
    #[cfg(test)]
    Pause {
        entered: Sender<()>,
        release: Receiver<()>,
    },
    CreateSpace {
        name: String,
        created_at_us: i64,
        reply: Sender<Result<SpaceDefinition>>,
    },
    CreatePolicy {
        options: StoragePolicyOptions,
        reply: Sender<Result<StoragePolicy>>,
    },
    CreateBucket {
        space: String,
        name: String,
        policy: PolicySelector,
        created_at_us: i64,
        reply: Sender<Result<BucketDefinition>>,
    },
    Flush {
        generation: Arc<Generation>,
        segments: Vec<Arc<Segment>>,
        unpublished: UnpublishedArtifacts,
        reply: Sender<Result<()>>,
    },
    Compaction {
        inputs: Vec<Arc<Segment>>,
        outputs: Vec<Arc<Segment>>,
        new_archives: Vec<Arc<AggregateArchive>>,
        expired_archives: Vec<Arc<AggregateArchive>>,
        unpublished: UnpublishedArtifacts,
        reply: Sender<Result<()>>,
    },
    Checkpoint {
        commit_id: u64,
        wal_bytes_through_commit: u64,
        reply: Sender<Result<u64>>,
    },
    CaptureSnapshot {
        checkpoint_commit: u64,
        reply: Sender<Result<SnapshotImage>>,
    },
    RefreshReadDirectory {
        reply: Sender<Result<Option<u64>>>,
    },
    Stop {
        reply: Sender<RetiredStorageFiles>,
    },
}

type RetiredStorageFiles = (Vec<Vec<Arc<Segment>>>, Vec<Vec<Arc<AggregateArchive>>>);

enum CompactionCommand {
    Trigger {
        force: bool,
        reply: Option<Sender<Result<u64>>>,
    },
    Stop,
}

enum MaintenanceCommand {
    Wake,
    Stop,
}

struct Snapshot {
    commit_id: u64,
    virtual_shards: u16,
    batches: Vec<Batch>,
    segments_by_shard: BTreeMap<u16, Vec<Arc<Segment>>>,
    retention_cutoffs: HashMap<SeriesKey, i64>,
    aggregate_retention_cutoffs: HashMap<SeriesKey, i64>,
    retained_aggregate_resolutions: HashMap<SeriesKey, Vec<AggregateResolution>>,
    series_policy_ids: HashMap<SeriesKey, u32>,
    strictly_increasing_series: HashSet<SeriesKey>,
    aggregate_archives: Vec<Arc<AggregateArchive>>,
    published_read_directories: Vec<Arc<OfflinePackDirectory>>,
    metrics: Arc<Metrics>,
    open_file_cache: Arc<OpenFileCache>,
    offline_pack_directory: Option<Arc<OfflinePackDirectory>>,
    guard: Option<Arc<SnapshotGuard>>,
    write_regulator: Option<Arc<WriteRegulator>>,
    query_executor: Arc<QueryExecutor>,
    work_memory_governor: Arc<WorkMemoryGovernor>,
    storage_lock: Option<Arc<StorageLock>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TimeRange {
    from_us: i64,
    to_us: i64,
}

impl TimeRange {
    fn for_segment(segment: &Segment) -> Result<Self> {
        Self::for_descriptor(segment.descriptor())
    }

    fn for_descriptor(descriptor: &crate::segment::SegmentDescriptor) -> Result<Self> {
        Ok(Self {
            from_us: descriptor.min_timestamp_us,
            to_us: descriptor
                .max_timestamp_us
                .checked_add(1)
                .ok_or_else(|| corrupt("segment time range overflows i64"))?,
        })
    }

    const fn overlaps(self, other: Self) -> bool {
        self.from_us < other.to_us && self.to_us > other.from_us
    }
}

fn degrade(inner: &Inner, error: &Error) {
    inner
        .metrics
        .background_errors
        .fetch_add(1, Ordering::Relaxed);
    if let Ok(mut degraded) = inner.degraded.lock() {
        degraded.get_or_insert_with(|| error.to_string());
    }
}

#[cfg(test)]
mod pack_group_tests;
