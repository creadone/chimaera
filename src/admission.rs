use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Result, corrupt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkMemoryClass {
    QueryPlan,
    QueryMetadata,
    QueryCompressed,
    QueryDecoded,
    QueryResults,
    Flush,
    Compaction,
    AggregateBuilder,
    Recovery,
}

const CLASS_COUNT: usize = 9;

impl WorkMemoryClass {
    const fn index(self) -> usize {
        match self {
            Self::QueryPlan => 0,
            Self::QueryMetadata => 1,
            Self::QueryCompressed => 2,
            Self::QueryDecoded => 3,
            Self::QueryResults => 4,
            Self::Flush => 5,
            Self::Compaction => 6,
            Self::AggregateBuilder => 7,
            Self::Recovery => 8,
        }
    }
}

#[derive(Default)]
struct State {
    used: u64,
    peak: u64,
    by_class: [u64; CLASS_COUNT],
    peak_by_class: [u64; CLASS_COUNT],
    waits: u64,
    wait_nanos: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WorkMemoryStats {
    pub maximum_bytes: u64,
    pub used_bytes: u64,
    pub peak_bytes: u64,
    pub query_plan_bytes: u64,
    pub query_plan_peak_bytes: u64,
    pub query_metadata_bytes: u64,
    pub query_metadata_peak_bytes: u64,
    pub query_compressed_bytes: u64,
    pub query_compressed_peak_bytes: u64,
    pub query_decoded_bytes: u64,
    pub query_decoded_peak_bytes: u64,
    pub query_results_bytes: u64,
    pub query_results_peak_bytes: u64,
    pub flush_bytes: u64,
    pub flush_peak_bytes: u64,
    pub compaction_bytes: u64,
    pub compaction_peak_bytes: u64,
    pub aggregate_builder_bytes: u64,
    pub aggregate_builder_peak_bytes: u64,
    pub recovery_bytes: u64,
    pub recovery_peak_bytes: u64,
    pub unbudgeted_large_allocations: u64,
    pub waits: u64,
    pub wait_nanos: u64,
}

pub(crate) struct WorkMemoryGovernor {
    maximum_bytes: u64,
    state: Mutex<State>,
    available: Condvar,
}

pub(crate) struct WorkMemoryReservation {
    governor: Arc<WorkMemoryGovernor>,
    class: WorkMemoryClass,
    bytes: u64,
}

impl WorkMemoryGovernor {
    pub(crate) fn new(maximum_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            maximum_bytes,
            state: Mutex::new(State::default()),
            available: Condvar::new(),
        })
    }

    pub(crate) fn try_reserve(&self, class: WorkMemoryClass, bytes: u64) -> Result<bool> {
        if bytes == 0 {
            return Ok(true);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("work memory governor mutex is poisoned"))?;
        let Some(next) = state.used.checked_add(bytes) else {
            return Ok(false);
        };
        if next > self.maximum_bytes {
            return Ok(false);
        }
        reserve_locked(&mut state, class, bytes, next);
        Ok(true)
    }

    pub(crate) fn reserve_background(
        self: &Arc<Self>,
        class: WorkMemoryClass,
        requested_bytes: u64,
    ) -> Result<WorkMemoryReservation> {
        if requested_bytes > self.maximum_bytes {
            return Err(crate::error::Error::ResourceLimit(format!(
                "background work requests {requested_bytes} bytes above the {} byte work-memory limit",
                self.maximum_bytes
            )));
        }
        let bytes = requested_bytes;
        if bytes == 0 {
            return Ok(WorkMemoryReservation {
                governor: Arc::clone(self),
                class,
                bytes: 0,
            });
        }
        let started = Instant::now();
        let mut waited = false;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("work memory governor mutex is poisoned"))?;
        loop {
            let fits = state
                .used
                .checked_add(bytes)
                .is_some_and(|next| next <= self.maximum_bytes);
            if fits {
                let next = state.used + bytes;
                reserve_locked(&mut state, class, bytes, next);
                if waited {
                    state.waits = state.waits.saturating_add(1);
                    state.wait_nanos = state
                        .wait_nanos
                        .saturating_add(duration_nanos(started.elapsed()));
                }
                return Ok(WorkMemoryReservation {
                    governor: Arc::clone(self),
                    class,
                    bytes,
                });
            }
            waited = true;
            state = self
                .available
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| corrupt("work memory governor mutex is poisoned"))?
                .0;
        }
    }

    pub(crate) fn try_reserve_background_with_headroom(
        self: &Arc<Self>,
        class: WorkMemoryClass,
        bytes: u64,
        headroom_bytes: u64,
    ) -> Result<Option<WorkMemoryReservation>> {
        if bytes.saturating_add(headroom_bytes) > self.maximum_bytes {
            return Ok(None);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("work memory governor mutex is poisoned"))?;
        let Some(next) = state.used.checked_add(bytes) else {
            return Ok(None);
        };
        if next.saturating_add(headroom_bytes) > self.maximum_bytes {
            return Ok(None);
        }
        reserve_locked(&mut state, class, bytes, next);
        Ok(Some(WorkMemoryReservation {
            governor: Arc::clone(self),
            class,
            bytes,
        }))
    }

    pub(crate) fn reserve_background_up_to(
        self: &Arc<Self>,
        class: WorkMemoryClass,
        maximum_bytes: u64,
        minimum_bytes: u64,
    ) -> Result<WorkMemoryReservation> {
        if minimum_bytes == 0 || minimum_bytes > maximum_bytes {
            return Err(corrupt("invalid bounded work-memory reservation"));
        }
        if minimum_bytes > self.maximum_bytes {
            return Err(crate::error::Error::ResourceLimit(format!(
                "background work requests at least {minimum_bytes} bytes above the {} byte work-memory limit",
                self.maximum_bytes
            )));
        }
        let maximum_bytes = maximum_bytes.min(self.maximum_bytes);
        let started = Instant::now();
        let mut waited = false;
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("work memory governor mutex is poisoned"))?;
        loop {
            let available = self.maximum_bytes.saturating_sub(state.used);
            if available >= minimum_bytes {
                let bytes = available.min(maximum_bytes);
                let next = state.used.saturating_add(bytes);
                reserve_locked(&mut state, class, bytes, next);
                if waited {
                    state.waits = state.waits.saturating_add(1);
                    state.wait_nanos = state
                        .wait_nanos
                        .saturating_add(duration_nanos(started.elapsed()));
                }
                return Ok(WorkMemoryReservation {
                    governor: Arc::clone(self),
                    class,
                    bytes,
                });
            }
            waited = true;
            state = self
                .available
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| corrupt("work memory governor mutex is poisoned"))?
                .0;
        }
    }

    pub(crate) fn release(&self, class: WorkMemoryClass, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            state.used = state.used.saturating_sub(bytes);
            let slot = &mut state.by_class[class.index()];
            *slot = slot.saturating_sub(bytes);
            self.available.notify_all();
        }
    }

    pub(crate) fn stats(&self) -> Result<WorkMemoryStats> {
        let state = self
            .state
            .lock()
            .map_err(|_| corrupt("work memory governor mutex is poisoned"))?;
        Ok(WorkMemoryStats {
            maximum_bytes: self.maximum_bytes,
            used_bytes: state.used,
            peak_bytes: state.peak,
            query_plan_bytes: state.by_class[WorkMemoryClass::QueryPlan.index()],
            query_plan_peak_bytes: state.peak_by_class[WorkMemoryClass::QueryPlan.index()],
            query_metadata_bytes: state.by_class[WorkMemoryClass::QueryMetadata.index()],
            query_metadata_peak_bytes: state.peak_by_class[WorkMemoryClass::QueryMetadata.index()],
            query_compressed_bytes: state.by_class[WorkMemoryClass::QueryCompressed.index()],
            query_compressed_peak_bytes: state.peak_by_class
                [WorkMemoryClass::QueryCompressed.index()],
            query_decoded_bytes: state.by_class[WorkMemoryClass::QueryDecoded.index()],
            query_decoded_peak_bytes: state.peak_by_class[WorkMemoryClass::QueryDecoded.index()],
            query_results_bytes: state.by_class[WorkMemoryClass::QueryResults.index()],
            query_results_peak_bytes: state.peak_by_class[WorkMemoryClass::QueryResults.index()],
            flush_bytes: state.by_class[WorkMemoryClass::Flush.index()],
            flush_peak_bytes: state.peak_by_class[WorkMemoryClass::Flush.index()],
            compaction_bytes: state.by_class[WorkMemoryClass::Compaction.index()],
            compaction_peak_bytes: state.peak_by_class[WorkMemoryClass::Compaction.index()],
            aggregate_builder_bytes: state.by_class[WorkMemoryClass::AggregateBuilder.index()],
            aggregate_builder_peak_bytes: state.peak_by_class
                [WorkMemoryClass::AggregateBuilder.index()],
            recovery_bytes: state.by_class[WorkMemoryClass::Recovery.index()],
            recovery_peak_bytes: state.peak_by_class[WorkMemoryClass::Recovery.index()],
            unbudgeted_large_allocations: 0,
            waits: state.waits,
            wait_nanos: state.wait_nanos,
        })
    }
}

fn reserve_locked(state: &mut State, class: WorkMemoryClass, bytes: u64, next: u64) {
    state.used = next;
    state.peak = state.peak.max(next);
    let index = class.index();
    state.by_class[index] = state.by_class[index].saturating_add(bytes);
    state.peak_by_class[index] = state.peak_by_class[index].max(state.by_class[index]);
}

impl Drop for WorkMemoryReservation {
    fn drop(&mut self) {
        self.governor.release(self.class, self.bytes);
    }
}

impl WorkMemoryReservation {
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_share_one_hard_limit_and_track_peaks() -> Result<()> {
        let governor = WorkMemoryGovernor::new(100);
        assert!(governor.try_reserve(WorkMemoryClass::QueryPlan, 40)?);
        assert!(governor.try_reserve(WorkMemoryClass::QueryDecoded, 60)?);
        assert!(!governor.try_reserve(WorkMemoryClass::QueryResults, 1)?);
        governor.release(WorkMemoryClass::QueryPlan, 40);
        let stats = governor.stats()?;
        assert_eq!(stats.peak_bytes, 100);
        assert_eq!(stats.query_plan_peak_bytes, 40);
        assert_eq!(stats.query_decoded_peak_bytes, 60);
        Ok(())
    }

    #[test]
    fn oversized_background_work_is_rejected_before_reservation() -> Result<()> {
        let governor = WorkMemoryGovernor::new(100);
        let error = match governor.reserve_background(WorkMemoryClass::Compaction, 101) {
            Ok(_) => panic!("oversized background job must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, crate::Error::ResourceLimit(_)));
        assert_eq!(governor.stats()?.used_bytes, 0);
        Ok(())
    }
}
