use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::error::{Result, corrupt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkMemoryClass {
    QuerySources,
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
            Self::QuerySources => 0,
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
    waiters: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WorkMemoryStats {
    pub maximum_bytes: u64,
    pub used_bytes: u64,
    pub peak_bytes: u64,
    pub query_sources_bytes: u64,
    pub query_sources_peak_bytes: u64,
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
            state = self.wait_for_memory(state)?;
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
            state = self.wait_for_memory(state)?;
        }
    }

    fn wait_for_memory<'a>(
        &self,
        mut state: MutexGuard<'a, State>,
    ) -> Result<MutexGuard<'a, State>> {
        // Register under the same mutex as release, before Condvar atomically
        // unlocks it. Keep the registration until reacquiring it, including
        // timeout/spurious wakes, so release cannot miss an entering waiter.
        state.waiters += 1;
        match self
            .available
            .wait_timeout(state, Duration::from_millis(10))
        {
            Ok((mut state, _)) => {
                state.waiters -= 1;
                Ok(state)
            }
            Err(error) => {
                let (mut state, _) = error.into_inner();
                state.waiters -= 1;
                Err(corrupt("work memory governor mutex is poisoned"))
            }
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
            if state.waiters != 0 {
                // Different waiters can require different amounts. Wake all
                // registered waiters; notifying just one could select a job
                // which still cannot fit while another could make progress.
                self.available.notify_all();
            }
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
            query_sources_bytes: state.by_class[WorkMemoryClass::QuerySources.index()],
            query_sources_peak_bytes: state.peak_by_class[WorkMemoryClass::QuerySources.index()],
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
    pub(crate) fn shrink(&mut self, bytes: u64) {
        let released = bytes.min(self.bytes);
        self.bytes -= released;
        self.governor.release(self.class, released);
    }

    pub(crate) fn try_grow(&mut self, bytes: u64) -> Result<bool> {
        if !self.governor.try_reserve(self.class, bytes)? {
            return Ok(false);
        }
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(true)
    }

    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
