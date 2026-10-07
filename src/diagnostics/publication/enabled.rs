use super::{Counter, Stage, Timer};
use serde::Serialize;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

mod slow;

const STAGES: [&str; 36] = [
    "flush",
    "compaction",
    "other",
    "queue_flush",
    "queue_compaction",
    "builder_metadata",
    "builder_encode",
    "builder_write_and_reencode",
    "builder_sync",
    "persist_manifest",
    "apply_state",
    "catalog_prepare",
    "catalog_sync",
    "reclaim",
    "stats",
    "selection",
    "priority",
    "selection_snapshot_wait",
    "selection_snapshot_hold",
    "selection_active_ready",
    "selection_active_empty",
    "selection_active_error",
    "selection_historical_ready",
    "selection_historical_empty",
    "selection_historical_error",
    "historical_sealing",
    "historical_fallback",
    "publication_summary",
    "publication_eligibility",
    "publication_validation",
    "wal_sync",
    "wal_sync_all",
    "writer_visibility_wait",
    "publisher_catalog_wait",
    "publisher_state_wait",
    "manifest_sync_and_commit",
];
const COUNTERS: [&str; 22] = [
    "locators",
    "pages",
    "build_work_bytes_peak",
    "segment_metadata_read_bytes",
    "segment_header_read_bytes",
    "directory_read_bytes",
    "selection_active_domains",
    "selection_historical_domains",
    "selection_active_opened",
    "selection_historical_opened",
    "selection_active_l2",
    "selection_historical_l2",
    "historical_fallback_descriptors",
    "catalog_summary_nodes",
    "catalog_summary_records",
    "publication_history_descriptors",
    "publication_eligibility_descriptors",
    "publication_validation_descriptors",
    "segment_handle_cache_hits",
    "segment_handle_cache_misses",
    "segment_handle_cache_collisions",
    "segment_handle_opens",
];

#[derive(Default)]
struct Metric {
    calls: AtomicU64,
    nanos: AtomicU64,
    max_nanos: AtomicU64,
}
pub(crate) struct Collector {
    started: Instant,
    slow: slow::Events,
    stages: [Metric; STAGES.len()],
    counters: [AtomicU64; COUNTERS.len()],
    compaction: [super::compaction::Metrics; 3],
}
impl Collector {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            slow: slow::Events::new(),
            stages: std::array::from_fn(|_| Metric::default()),
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            compaction: std::array::from_fn(|_| super::compaction::Metrics::default()),
        })
    }
    pub(super) fn record(&self, stage: Stage, started: Instant, elapsed: Duration) {
        let metric = &self.stages[stage as usize];
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        metric.calls.fetch_add(1, Ordering::Relaxed);
        metric.nanos.fetch_add(nanos, Ordering::Relaxed);
        metric.max_nanos.fetch_max(nanos, Ordering::Relaxed);
        self.slow.record(
            stage,
            started.saturating_duration_since(self.started),
            nanos,
        );
    }

    pub(crate) fn record_compaction(
        &self,
        kind: super::compaction::Kind,
        sample: Option<&super::compaction::Sample>,
        elapsed: Duration,
    ) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.compaction[kind as usize].record(sample, nanos);
    }
}

/// Fixed-cardinality diagnostic handle for one Storage.
pub struct Session(pub(crate) Arc<Collector>);
#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    /// Monotonic time since this session started, sampled before its counters.
    pub elapsed_ns: u64,
    pub slow_events: slow::Report,
    pub stages: Vec<StageReport>,
    pub counters: Vec<CounterReport>,
    pub compaction: Vec<super::CompactionReport>,
}
#[derive(Debug, Serialize)]
pub struct StageReport {
    pub name: &'static str,
    pub calls: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}
#[derive(Debug, Serialize)]
pub struct CounterReport {
    pub name: &'static str,
    pub value: u64,
}
impl Session {
    /// Read completed timers and current counters, without an atomic snapshot.
    /// Counters may include running work whose timers have not finished yet.
    #[must_use]
    pub fn snapshot(&self) -> Report {
        Report {
            schema_version: 11,
            elapsed_ns: u64::try_from(self.0.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            slow_events: self.0.slow.snapshot(),
            compaction: super::compaction::snapshot(&self.0.compaction),
            stages: STAGES
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let metric = &self.0.stages[i];
                    StageReport {
                        name,
                        calls: metric.calls.load(Ordering::Relaxed),
                        total_ns: metric.nanos.load(Ordering::Relaxed),
                        max_ns: metric.max_nanos.load(Ordering::Relaxed),
                    }
                })
                .collect(),
            counters: COUNTERS
                .iter()
                .enumerate()
                .map(|(i, name)| CounterReport {
                    name,
                    value: self.0.counters[i].load(Ordering::Relaxed),
                })
                .collect(),
        }
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<Collector>>> = const { RefCell::new(None) };
}
pub(crate) struct Scope {
    previous: Option<Arc<Collector>>,
    // A thread-local scope must be restored on the thread that entered it.
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}
pub(crate) fn enter(collector: Option<&Arc<Collector>>) -> Scope {
    Scope {
        previous: CURRENT.with(|current| current.replace(collector.cloned())),
        _thread: std::marker::PhantomData,
    }
}
pub(crate) fn queued(collector: Option<&Arc<Collector>>) -> Option<Instant> {
    collector.map(|_| Instant::now())
}
pub(crate) fn record_elapsed(name: Stage, started: Option<Instant>) {
    if let Some(started) = started {
        CURRENT.with(|current| {
            if let Some(collector) = current.borrow().as_ref() {
                collector.record(name, started, started.elapsed());
            }
        });
    }
}
pub(super) fn timer(name: Stage) -> Timer {
    Timer {
        inner: CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .map(|collector| (Arc::clone(collector), name, Instant::now()))
        }),
    }
}
pub(super) fn add(name: Counter, value: usize) {
    CURRENT.with(|current| {
        if let Some(collector) = current.borrow().as_ref() {
            let value = u64::try_from(value).unwrap_or(u64::MAX);
            let counter = &collector.counters[name as usize];
            if matches!(name, Counter::BuildWorkBytesPeak) {
                counter.fetch_max(value, Ordering::Relaxed);
            } else {
                counter.fetch_add(value, Ordering::Relaxed);
            }
        }
    });
}
