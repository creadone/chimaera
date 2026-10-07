//! Nested timings for the persisted-summary lookup in an immutable part.
//!
//! All samples are inclusive. `load` contains file/region/root/page work;
//! cache waits and memory admission are subsets of their callers. I/O measures
//! read calls, including the OS page cache, not physical disk service time.

use super::Sample;
use serde::Serialize;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Report {
    pub local: Sample,
    pub shared: Sample,
    pub cache_wait: Sample,
    pub load_wait: Sample,
    pub load: Sample,
    pub insert: Sample,
    pub pin: Sample,
    pub cursor: Sample,
    pub file: Sample,
    pub permit_wait: Sample,
    pub region: Sample,
    pub root: Sample,
    pub search: Sample,
    pub pages: Sample,
    pub decode: Sample,
    pub memory: Sample,
    pub read: Sample,
    pub local_hits: u64,
    pub shared_hits: u64,
    pub prepared_hits: u64,
    /// Requested bytes, including a request which subsequently fails.
    pub read_bytes: u64,
    /// Distinct segment roots loaded in this measured calling-thread scope.
    pub root_unique: u64,
    pub root_repeated: u64,
    /// Loads beyond the bounded identity set; uniqueness is unknown for these.
    pub root_untracked: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Local,
    Shared,
    CacheWait,
    LoadWait,
    Load,
    Insert,
    Pin,
    Cursor,
    File,
    PermitWait,
    Region,
    Root,
    Search,
    Pages,
    Decode,
    Memory,
    Read,
}

pub(crate) enum Hit {
    Local,
    Shared,
}

thread_local! {
    static REPORT: Cell<Option<Report>> = const { Cell::new(None) };
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ROOTS: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
}

struct Restore(Option<Report>, bool, HashSet<u64>);

impl Drop for Restore {
    fn drop(&mut self) {
        REPORT.set(self.0);
        ACTIVE.set(self.1);
        ROOTS.with_borrow_mut(|roots| *roots = std::mem::take(&mut self.2));
    }
}

pub(super) fn measure<T>(run: impl FnOnce() -> T) -> (T, Report) {
    let restore = Restore(
        REPORT.replace(Some(Report::default())),
        ACTIVE.replace(false),
        ROOTS.take(),
    );
    let result = run();
    let report = REPORT.take().unwrap_or_default();
    drop(restore);
    (result, report)
}

pub(super) struct Scope(bool);

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.set(self.0);
    }
}

pub(super) fn enter() -> Scope {
    Scope(ACTIVE.replace(true))
}

pub(crate) struct Timer(Option<(Stage, Instant)>);

pub(crate) fn stage(stage: Stage) -> Timer {
    Timer(ACTIVE.get().then(|| (stage, Instant::now())))
}

pub(crate) fn hit(hit: Hit) {
    if ACTIVE.get()
        && let Some(mut report) = REPORT.get()
    {
        let counter = match hit {
            Hit::Local => &mut report.local_hits,
            Hit::Shared => &mut report.shared_hits,
        };
        *counter = counter.saturating_add(1);
        REPORT.set(Some(report));
    }
}

pub(crate) fn read_bytes(bytes: usize) {
    if ACTIVE.get()
        && let Some(mut report) = REPORT.get()
    {
        report.read_bytes = report.read_bytes.saturating_add(bytes as u64);
        REPORT.set(Some(report));
    }
}

pub(crate) fn root_read(segment_id: u64) {
    if ACTIVE.get()
        && let Some(mut report) = REPORT.get()
    {
        ROOTS.with_borrow_mut(|roots| {
            if roots.contains(&segment_id) {
                report.root_repeated += 1;
            } else if roots.len() < 4096 {
                roots.insert(segment_id);
                report.root_unique += 1;
            } else {
                report.root_untracked += 1;
            }
        });
        REPORT.set(Some(report));
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((stage, started)) = self.0
            && let Some(mut report) = REPORT.get()
        {
            let sample = match stage {
                Stage::Local => &mut report.local,
                Stage::Shared => &mut report.shared,
                Stage::CacheWait => &mut report.cache_wait,
                Stage::LoadWait => &mut report.load_wait,
                Stage::Load => &mut report.load,
                Stage::Insert => &mut report.insert,
                Stage::Pin => &mut report.pin,
                Stage::Cursor => &mut report.cursor,
                Stage::File => &mut report.file,
                Stage::PermitWait => &mut report.permit_wait,
                Stage::Region => &mut report.region,
                Stage::Root => &mut report.root,
                Stage::Search => &mut report.search,
                Stage::Pages => &mut report.pages,
                Stage::Decode => &mut report.decode,
                Stage::Memory => &mut report.memory,
                Stage::Read => &mut report.read,
            };
            sample.calls = sample.calls.saturating_add(1);
            sample.duration = sample.duration.saturating_add(started.elapsed());
            REPORT.set(Some(report));
        }
    }
}
