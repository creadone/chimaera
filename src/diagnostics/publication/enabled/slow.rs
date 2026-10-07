//! Recent completed waits, owned by one diagnostic session. No I/O under the lock.
use super::{STAGES, Stage};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

const CAPACITY: usize = 256;
const THRESHOLD_NS: u64 = 10_000_000;

pub(super) struct Events(Mutex<State>);

struct State {
    completed: u64,
    overwritten: u64,
    entries: VecDeque<Event>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Event {
    /// Completion order, for deduplicating consecutive snapshots.
    pub sequence: u64,
    pub stage: &'static str,
    /// Offset from the diagnostic session's monotonic start.
    pub start_ns: u64,
    pub elapsed_ns: u64,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub capacity: usize,
    pub threshold_ns: u64,
    pub completed: u64,
    pub overwritten: u64,
    pub events: Vec<Event>,
}

impl Events {
    pub(super) fn new() -> Self {
        Self(Mutex::new(State {
            completed: 0,
            overwritten: 0,
            entries: VecDeque::with_capacity(CAPACITY),
        }))
    }

    pub(super) fn record(&self, stage: Stage, start: Duration, elapsed_ns: u64) {
        if elapsed_ns < THRESHOLD_NS
            || !matches!(
                stage,
                Stage::WalSync
                    | Stage::WalSyncAll
                    | Stage::WriterVisibilityWait
                    | Stage::PublisherCatalogWait
                    | Stage::PublisherStateWait
                    | Stage::ManifestSyncAndCommit
                    | Stage::QueueFlush
                    | Stage::QueueCompaction
            )
        {
            return;
        }
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.completed = state.completed.saturating_add(1);
        if state.entries.len() == CAPACITY {
            state.entries.pop_front();
            state.overwritten = state.overwritten.saturating_add(1);
        }
        let sequence = state.completed;
        state.entries.push_back(Event {
            sequence,
            stage: STAGES[stage as usize],
            start_ns: u64::try_from(start.as_nanos()).unwrap_or(u64::MAX),
            elapsed_ns,
        });
    }

    pub(super) fn snapshot(&self) -> Report {
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        Report {
            capacity: CAPACITY,
            threshold_ns: THRESHOLD_NS,
            completed: state.completed,
            overwritten: state.overwritten,
            events: state.entries.iter().cloned().collect(),
        }
    }
}
