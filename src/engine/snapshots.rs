//! Учёт времени жизни и ресурсов MVCC-снимков.

use super::support::lock;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct ActiveSnapshot {
    started: Instant,
    memtable_generations: usize,
    segments: usize,
}

#[derive(Default)]
struct SnapshotTrackerState {
    next_id: u64,
    active: HashMap<u64, ActiveSnapshot>,
}

#[derive(Default)]
pub(super) struct SnapshotTracker {
    state: Mutex<SnapshotTrackerState>,
}

impl SnapshotTracker {
    pub(super) fn register(
        self: &Arc<Self>,
        memtable_generations: usize,
        segments: usize,
    ) -> Result<SnapshotGuard> {
        let mut state = lock(&self.state)?;
        state.next_id = state.next_id.saturating_add(1);
        let id = state.next_id;
        state.active.insert(
            id,
            ActiveSnapshot {
                started: Instant::now(),
                memtable_generations,
                segments,
            },
        );
        Ok(SnapshotGuard {
            tracker: Arc::clone(self),
            id,
        })
    }

    pub(super) fn stats(&self) -> Result<(usize, Duration, usize, usize)> {
        let state = lock(&self.state)?;
        let now = Instant::now();
        Ok((
            state.active.len(),
            state
                .active
                .values()
                .map(|snapshot| now.saturating_duration_since(snapshot.started))
                .max()
                .unwrap_or_default(),
            state
                .active
                .values()
                .map(|snapshot| snapshot.memtable_generations)
                .sum(),
            state
                .active
                .values()
                .map(|snapshot| snapshot.segments)
                .sum(),
        ))
    }
}

pub(super) struct SnapshotGuard {
    pub(super) tracker: Arc<SnapshotTracker>,
    pub(super) id: u64,
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.tracker.state.lock() {
            state.active.remove(&self.id);
        }
    }
}
