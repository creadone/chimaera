//! Bounded attribution of completed compaction jobs, not exclusive retention overhead.
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Ordinary,
    Retention,
    Mixed,
}
const KINDS: [Kind; 3] = [Kind::Ordinary, Kind::Retention, Kind::Mixed];

#[derive(Default)]
pub(crate) struct Sample {
    pub groups: u64,
    pub input_bytes: u64,
    pub expired_input_bytes: u64,
    pub crossing_input_bytes: u64,
    pub live_input_bytes: u64,
    pub l0_input_bytes: u64,
    pub physical_read_bytes: u64,
    pub written_bytes: u64,
    pub archive_written_bytes: u64,
    pub decoded_points: u64,
    pub archive_ns: u64,
    pub pack_wait_ns: u64,
    pub publication_wait_ns: u64,
}
#[derive(Default)]
pub(super) struct Metrics {
    completed: AtomicU64,
    failed: AtomicU64,
    failed_ns: AtomicU64,
    elapsed_ns: AtomicU64,
    max_ns: AtomicU64,
    groups: AtomicU64,
    input_bytes: AtomicU64,
    expired_input_bytes: AtomicU64,
    crossing_input_bytes: AtomicU64,
    live_input_bytes: AtomicU64,
    l0_input_bytes: AtomicU64,
    physical_read_bytes: AtomicU64,
    written_bytes: AtomicU64,
    archive_written_bytes: AtomicU64,
    decoded_points: AtomicU64,
    archive_ns: AtomicU64,
    pack_wait_ns: AtomicU64,
    publication_wait_ns: AtomicU64,
}

/// Completed job totals. A retention job may also merge live data.
/// Mixed jobs contain both retention and ordinary groups; no proportional attribution.
/// Input buckets count whole segment sizes, not expired/live points within a segment.
/// Physical reads count segment block reads (including OS-cache hits), not device I/O.
/// Writes include raw segments and archives, excluding catalogs, WAL and repacks.
/// Elapsed time starts after selection. Pack/publication waits include execution;
/// these and archive time are nested within elapsed time. Failed work has only
/// a count and duration; its partial I/O is not included in completed totals.
#[derive(Debug, Serialize)]
pub struct Report {
    pub kind: &'static str,
    pub completed: u64,
    pub failed: u64,
    pub failed_ns: u64,
    pub elapsed_ns: u64,
    pub max_ns: u64,
    pub groups: u64,
    pub input_bytes: u64,
    pub expired_input_bytes: u64,
    pub crossing_input_bytes: u64,
    pub live_input_bytes: u64,
    pub l0_input_bytes: u64,
    pub physical_read_bytes: u64,
    pub written_bytes: u64,
    pub archive_written_bytes: u64,
    pub decoded_points: u64,
    pub archive_ns: u64,
    pub pack_wait_ns: u64,
    pub publication_wait_ns: u64,
}
impl Metrics {
    pub(super) fn record(&self, sample: Option<&Sample>, elapsed_ns: u64) {
        let Some(sample) = sample else {
            self.failed.fetch_add(1, Ordering::Relaxed);
            self.failed_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
            return;
        };
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.elapsed_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        self.max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
        self.groups.fetch_add(sample.groups, Ordering::Relaxed);
        self.input_bytes
            .fetch_add(sample.input_bytes, Ordering::Relaxed);
        self.expired_input_bytes
            .fetch_add(sample.expired_input_bytes, Ordering::Relaxed);
        self.crossing_input_bytes
            .fetch_add(sample.crossing_input_bytes, Ordering::Relaxed);
        self.live_input_bytes
            .fetch_add(sample.live_input_bytes, Ordering::Relaxed);
        self.l0_input_bytes
            .fetch_add(sample.l0_input_bytes, Ordering::Relaxed);
        self.physical_read_bytes
            .fetch_add(sample.physical_read_bytes, Ordering::Relaxed);
        self.written_bytes
            .fetch_add(sample.written_bytes, Ordering::Relaxed);
        self.archive_written_bytes
            .fetch_add(sample.archive_written_bytes, Ordering::Relaxed);
        self.decoded_points
            .fetch_add(sample.decoded_points, Ordering::Relaxed);
        self.archive_ns
            .fetch_add(sample.archive_ns, Ordering::Relaxed);
        self.pack_wait_ns
            .fetch_add(sample.pack_wait_ns, Ordering::Relaxed);
        self.publication_wait_ns
            .fetch_add(sample.publication_wait_ns, Ordering::Relaxed);
    }
    fn snapshot(&self, kind: Kind) -> Report {
        Report {
            kind: match kind {
                Kind::Ordinary => "ordinary",
                Kind::Retention => "retention",
                Kind::Mixed => "mixed",
            },
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            failed_ns: self.failed_ns.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
            groups: self.groups.load(Ordering::Relaxed),
            input_bytes: self.input_bytes.load(Ordering::Relaxed),
            expired_input_bytes: self.expired_input_bytes.load(Ordering::Relaxed),
            crossing_input_bytes: self.crossing_input_bytes.load(Ordering::Relaxed),
            live_input_bytes: self.live_input_bytes.load(Ordering::Relaxed),
            l0_input_bytes: self.l0_input_bytes.load(Ordering::Relaxed),
            physical_read_bytes: self.physical_read_bytes.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
            archive_written_bytes: self.archive_written_bytes.load(Ordering::Relaxed),
            decoded_points: self.decoded_points.load(Ordering::Relaxed),
            archive_ns: self.archive_ns.load(Ordering::Relaxed),
            pack_wait_ns: self.pack_wait_ns.load(Ordering::Relaxed),
            publication_wait_ns: self.publication_wait_ns.load(Ordering::Relaxed),
        }
    }
}
pub(super) fn snapshot(metrics: &[Metrics; 3]) -> Vec<Report> {
    KINDS
        .into_iter()
        .map(|kind| metrics[kind as usize].snapshot(kind))
        .collect()
}
