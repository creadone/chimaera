//! Calling-thread timings for snapshot acquisition and immutable reads. No events or cross-thread state.

use serde::Serialize;
use std::cell::Cell;
use std::time::{Duration, Instant};

pub mod summary;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Sample {
    pub calls: u64,
    pub duration: Duration,
}

/// Calling-thread scopes, excluding worker execution. With state diagnostics,
/// snapshot_sleep is a subset of snapshot_wait; do not add those durations.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Report {
    pub availability: Sample,
    pub directory_lock: Sample,
    pub snapshot_wait: Sample,
    pub snapshot_hold: Sample,
    /// Opening summaries for one immutable part.
    pub summary_open: Sample,
    /// Advancing the summary and raw boundary cursors.
    pub summary_next: Sample,
    /// Breakdown restricted to the calling thread inside `summary_open`.
    pub summary: summary::Report,
    #[cfg(feature = "state-diagnostics")]
    pub snapshot_sleep: Sample,
    #[cfg(feature = "state-diagnostics")]
    pub state_attempts: u64,
    #[cfg(feature = "state-diagnostics")]
    pub blocker_samples: u64,
    #[cfg(feature = "state-diagnostics")]
    pub no_observed_holder: u64,
    #[cfg(feature = "state-diagnostics")]
    pub state_blockers: [u64; crate::diagnostics::state::SLOTS],
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Availability,
    DirectoryLock,
    SnapshotWait,
    SnapshotHold,
    SummaryOpen,
    SummaryNext,
    #[cfg(feature = "state-diagnostics")]
    SnapshotSleep,
}

#[cfg(feature = "state-diagnostics")]
pub(crate) fn attempt() {
    if let Some(mut report) = REPORT.get() {
        report.state_attempts = report.state_attempts.saturating_add(1);
        REPORT.set(Some(report));
    }
}

#[cfg(feature = "state-diagnostics")]
pub(crate) fn blocked(holders: Option<[u64; crate::diagnostics::state::SLOTS]>) {
    if let (Some(mut report), Some(holders)) = (REPORT.get(), holders) {
        report.blocker_samples = report.blocker_samples.saturating_add(1);
        if holders.iter().all(|count| *count == 0) {
            report.no_observed_holder = report.no_observed_holder.saturating_add(1);
        }
        for (count, active) in report.state_blockers.iter_mut().zip(holders) {
            if active != 0 {
                *count = count.saturating_add(1);
            }
        }
        REPORT.set(Some(report));
    }
}

thread_local! {
    static REPORT: Cell<Option<Report>> = const { Cell::new(None) };
}

struct Restore(Option<Report>);

impl Drop for Restore {
    fn drop(&mut self) {
        REPORT.set(self.0);
    }
}

/// Measure synchronous calls, restoring any outer scope even during unwinding.
/// Nested measurements are isolated. Normal calls leave the collector disabled.
pub fn measure<T>(run: impl FnOnce() -> T) -> (T, Report) {
    let restore = Restore(REPORT.replace(Some(Report::default())));
    let (result, summary) = summary::measure(run);
    let mut report = REPORT.take().unwrap_or_default();
    report.summary = summary;
    drop(restore);
    (result, report)
}

pub(crate) struct Timer {
    measurement: Option<(Stage, Instant)>,
    _summary: Option<summary::Scope>,
}

pub(crate) fn stage(stage: Stage) -> Timer {
    let measurement = REPORT.get().map(|_| (stage, Instant::now()));
    Timer {
        measurement,
        _summary: (measurement.is_some() && matches!(stage, Stage::SummaryOpen))
            .then(summary::enter),
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((stage, started)) = self.measurement
            && let Some(mut report) = REPORT.get()
        {
            let sample = match stage {
                Stage::Availability => &mut report.availability,
                Stage::DirectoryLock => &mut report.directory_lock,
                Stage::SnapshotWait => &mut report.snapshot_wait,
                Stage::SnapshotHold => &mut report.snapshot_hold,
                Stage::SummaryOpen => &mut report.summary_open,
                Stage::SummaryNext => &mut report.summary_next,
                #[cfg(feature = "state-diagnostics")]
                Stage::SnapshotSleep => &mut report.snapshot_sleep,
            };
            sample.calls = sample.calls.saturating_add(1);
            sample.duration = sample.duration.saturating_add(started.elapsed());
            REPORT.set(Some(report));
        }
    }
}
