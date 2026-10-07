//! Per-Storage state-lock costs and sampled holders. Fixed owner cardinality;
//! at most 128 slow call sites, no event log or background thread.
//! Hold timers end before unlocking; statistics are updated after unlocking.
//! Concurrent snapshots and holder observations are approximate, not atomic.

use serde::Serialize;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const OWNERS: [&str; 13] = [
    "query",
    "stats",
    "writer",
    "admission",
    "publication",
    "directory",
    "compaction",
    "maintenance",
    "reclamation",
    "catalog",
    "recovery",
    "flush",
    "other",
];
pub const SLOTS: usize = OWNERS.len() * 2;
const SITE_LIMIT: usize = 128;

fn owner(file: &str) -> usize {
    if file.contains("/publication/directory/") {
        return 5;
    }
    if file.contains("/publication/") {
        return 4;
    }
    if file.contains("/compaction/") {
        return 6;
    }
    match file.rsplit('/').next().unwrap_or(file) {
        "read.rs" => 0,
        "stats.rs" => 1,
        "writer.rs" => 2,
        "admission.rs" => 3,
        "maintenance.rs" => 7,
        "reclamation.rs" => 8,
        "catalog.rs" => 9,
        "recovery.rs" => 10,
        "flush.rs" => 11,
        _ => 12,
    }
}

#[derive(Default)]
struct Metric {
    active: AtomicU64,
    calls: AtomicU64,
    wait_ns: AtomicU64,
    hold_ns: AtomicU64,
    max_hold_ns: AtomicU64,
    hold_ge_1ms: AtomicU64,
    hold_ge_10ms: AtomicU64,
    hold_ge_50ms: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Site {
    pub file: &'static str,
    pub line: u32,
    pub slot: usize,
    pub calls: u64,
    pub hold_ns: u64,
    pub max_hold_ns: u64,
}

pub(crate) struct Collector {
    metrics: [Metric; SLOTS],
    sites: Mutex<BTreeMap<(&'static str, u32, usize), Site>>,
    omitted_sites: AtomicU64,
}

impl Collector {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            metrics: std::array::from_fn(|_| Metric::default()),
            sites: Mutex::new(BTreeMap::new()),
            omitted_sites: AtomicU64::new(0),
        })
    }

    pub(crate) fn blockers(&self) -> [u64; SLOTS] {
        std::array::from_fn(|i| self.metrics[i].active.load(Ordering::Relaxed))
    }

    fn finish(&self, slot: usize, location: &'static Location<'static>, wait: u64, hold: u64) {
        let metric = &self.metrics[slot];
        metric.calls.fetch_add(1, Ordering::Relaxed);
        metric.wait_ns.fetch_add(wait, Ordering::Relaxed);
        metric.hold_ns.fetch_add(hold, Ordering::Relaxed);
        metric.max_hold_ns.fetch_max(hold, Ordering::Relaxed);
        for (threshold, count) in [
            (1_000_000, &metric.hold_ge_1ms),
            (10_000_000, &metric.hold_ge_10ms),
            (50_000_000, &metric.hold_ge_50ms),
        ] {
            if hold >= threshold {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
        if hold < 1_000_000 {
            return;
        }
        // Never hold the actual state lock while updating the slow-site table.
        if let Ok(mut sites) = self.sites.lock() {
            let key = (location.file(), location.line(), slot);
            if sites.len() == SITE_LIMIT && !sites.contains_key(&key) {
                self.omitted_sites.fetch_add(1, Ordering::Relaxed);
                return;
            }
            let site = sites.entry(key).or_insert_with(|| Site {
                file: key.0,
                line: key.1,
                slot,
                ..Site::default()
            });
            site.calls = site.calls.saturating_add(1);
            site.hold_ns = site.hold_ns.saturating_add(hold);
            site.max_hold_ns = site.max_hold_ns.max(hold);
        }
    }
}

pub struct Session(pub(crate) Arc<Collector>);

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub owners: Vec<Owner>,
    pub slow_sites: Vec<Site>,
    pub omitted_slow_site_observations: u64,
}

#[derive(Debug, Serialize)]
pub struct Owner {
    pub slot: usize,
    pub owner: &'static str,
    pub mode: &'static str,
    pub active: u64,
    pub calls: u64,
    pub wait_ns: u64,
    pub hold_ns: u64,
    pub max_hold_ns: u64,
    pub hold_ge_1ms: u64,
    pub hold_ge_10ms: u64,
    pub hold_ge_50ms: u64,
}

impl Session {
    /// Non-atomic snapshot. After Storage::close all completed holders are counted.
    #[must_use]
    pub fn snapshot(&self) -> Report {
        Report {
            schema_version: 1,
            owners: self
                .0
                .metrics
                .iter()
                .enumerate()
                .map(|(slot, m)| Owner {
                    slot,
                    owner: OWNERS[slot / 2],
                    mode: if slot % 2 == 0 { "read" } else { "write" },
                    active: m.active.load(Ordering::Relaxed),
                    calls: m.calls.load(Ordering::Relaxed),
                    wait_ns: m.wait_ns.load(Ordering::Relaxed),
                    hold_ns: m.hold_ns.load(Ordering::Relaxed),
                    max_hold_ns: m.max_hold_ns.load(Ordering::Relaxed),
                    hold_ge_1ms: m.hold_ge_1ms.load(Ordering::Relaxed),
                    hold_ge_10ms: m.hold_ge_10ms.load(Ordering::Relaxed),
                    hold_ge_50ms: m.hold_ge_50ms.load(Ordering::Relaxed),
                })
                .collect(),
            slow_sites: self
                .0
                .sites
                .lock()
                .map(|s| s.values().cloned().collect())
                .unwrap_or_default(),
            omitted_slow_site_observations: self.0.omitted_sites.load(Ordering::Relaxed),
        }
    }
}

struct Held {
    collector: Arc<Collector>,
    slot: usize,
    location: &'static Location<'static>,
    wait_ns: u64,
    started: Instant,
}

pub(crate) struct Guard<G> {
    value: Option<G>,
    held: Option<Held>,
}

impl<G> Guard<G> {
    #[track_caller]
    pub(crate) fn new(
        value: G,
        collector: Option<&Arc<Collector>>,
        waiting: Option<Instant>,
        write: bool,
    ) -> Self {
        let location = Location::caller();
        let held = collector.map(|collector| {
            let started = Instant::now();
            let slot = owner(location.file()) * 2 + usize::from(write);
            collector.metrics[slot]
                .active
                .fetch_add(1, Ordering::Relaxed);
            Held {
                collector: Arc::clone(collector),
                slot,
                location,
                wait_ns: waiting.map_or(0, |t| nanos(started.duration_since(t))),
                started,
            }
        });
        Self {
            value: Some(value),
            held,
        }
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl<G: Deref> Deref for Guard<G> {
    type Target = G::Target;
    fn deref(&self) -> &Self::Target {
        self.value.as_ref().expect("live state guard").deref()
    }
}
impl<G: DerefMut> DerefMut for Guard<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.as_mut().expect("live state guard").deref_mut()
    }
}
impl<G> Drop for Guard<G> {
    fn drop(&mut self) {
        let elapsed = self.held.as_ref().map(|h| nanos(h.started.elapsed()));
        if let Some(h) = &self.held {
            h.collector.metrics[h.slot]
                .active
                .fetch_sub(1, Ordering::Relaxed);
        }
        drop(self.value.take());
        if let (Some(h), Some(hold)) = (&self.held, elapsed) {
            h.collector.finish(h.slot, h.location, h.wait_ns, hold);
        }
    }
}
