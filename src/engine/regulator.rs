//! Write pacing driven by read latency, memory, and compaction debt.

use super::support::{duration_nanos, lock};
use super::{
    READ_LATENCY_MIN_SAMPLES, READ_LATENCY_WIDE_MIN_SAMPLES, READ_LATENCY_WIDE_WINDOW,
    READ_LATENCY_WINDOW, WRITE_PRESSURE_STREAK,
};
use crate::error::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const PACING_L0: u8 = 1;
const PACING_COMPACTION_QUEUE: u8 = 1 << 1;
const PACING_MEMORY: u8 = 1 << 2;
pub(super) const PACING_READ_LATENCY: u8 = 1 << 3;
// Soft L0 pacing is charged per volume, so splitting a write into smaller
// batches does not multiply its steady-state delay. Hard-stop checks remain
// in the writer; memory/read-latency protection keeps its per-call floor.
const DEBT_PACING_BYTES: u64 = 256 * 1024;

#[derive(Default)]
struct WriteRegulatorState {
    read_latency_single_nanos: VecDeque<u64>,
    read_latency_wide_nanos: VecDeque<u64>,
    last_single_read: Option<Instant>,
    last_wide_read: Option<Instant>,
    pressure_streak: u8,
    active_since: Option<Instant>,
    last_regulation: Option<Instant>,
    accumulated_active_nanos: u64,
    current_reasons: u8,
    recoveries: u64,
    l0_events: u64,
    compaction_queue_events: u64,
    memory_events: u64,
    read_latency_events: u64,
    last_memory_backpressure_events: u64,
    memory_pressure_ttl: u8,
}

pub(super) struct WriteRegulator {
    pub(super) read_latency_threshold: Duration,
    pub(super) wide_read_latency_threshold: Duration,
    pub(super) maximum_delay: Duration,
    state: Mutex<WriteRegulatorState>,
}

pub(super) struct Regulation {
    pub(super) delay: Duration,
}

pub(super) struct WritePressure {
    pub(super) incoming_bytes: u64,
    pub(super) l0_segments: usize,
    pub(super) l0_compaction_trigger: usize,
    pub(super) l0_slowdown_trigger: usize,
    pub(super) compaction_queue_depth: usize,
    pub(super) compaction_queue_capacity: usize,
    pub(super) managed_non_cache_bytes: u64,
    pub(super) managed_memory_max_bytes: u64,
    pub(super) memory_backpressure_events: u64,
}

pub(super) struct WriteRegulatorStats {
    pub(super) active: bool,
    pub(super) reasons: u8,
    pub(super) active_duration: Duration,
    pub(super) recoveries: u64,
    pub(super) l0_events: u64,
    pub(super) compaction_queue_events: u64,
    pub(super) memory_events: u64,
    pub(super) read_latency_events: u64,
    pub(super) read_latency_samples: usize,
    pub(super) read_latency_p95: Duration,
    pub(super) read_latency_single_samples: usize,
    pub(super) read_latency_single_p95: Duration,
    pub(super) read_latency_wide_samples: usize,
    pub(super) read_latency_wide_p95: Duration,
}

impl WriteRegulator {
    pub(super) fn new(
        read_latency_threshold: Duration,
        wide_read_latency_threshold: Duration,
        maximum_delay: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_latency_threshold,
            wide_read_latency_threshold,
            maximum_delay,
            state: Mutex::new(WriteRegulatorState::default()),
        })
    }

    pub(super) fn record_read(&self, duration: Duration, wide: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let now = Instant::now();
        expire_read_latencies(&mut state, now, self.read_latency_sample_ttl());
        if wide {
            if state.read_latency_wide_nanos.len() == READ_LATENCY_WIDE_WINDOW {
                state.read_latency_wide_nanos.pop_front();
            }
            state
                .read_latency_wide_nanos
                .push_back(duration_nanos(duration));
            state.last_wide_read = Some(now);
        } else {
            if state.read_latency_single_nanos.len() == READ_LATENCY_WINDOW {
                state.read_latency_single_nanos.pop_front();
            }
            state
                .read_latency_single_nanos
                .push_back(duration_nanos(duration));
            state.last_single_read = Some(now);
        }
    }

    pub(super) fn regulate(&self, pressure: WritePressure) -> Result<Regulation> {
        let mut state = lock(&self.state)?;
        let now = Instant::now();
        expire_read_latencies(&mut state, now, self.read_latency_sample_ttl());
        state.last_regulation = Some(now);
        let single_p95 = read_latency_percentile(&state.read_latency_single_nanos, 95);
        let wide_p95 = read_latency_percentile(&state.read_latency_wide_nanos, 95);
        let single_latency_pressure = state.read_latency_single_nanos.len()
            >= READ_LATENCY_MIN_SAMPLES
            && single_p95 >= self.read_latency_threshold;
        let wide_latency_pressure = state.read_latency_wide_nanos.len()
            >= READ_LATENCY_WIDE_MIN_SAMPLES
            && wide_p95 >= self.wide_read_latency_threshold;
        let mut reasons = 0_u8;
        if pressure.l0_segments > pressure.l0_compaction_trigger {
            reasons |= PACING_L0;
        }
        // Queue entries are recheck signals, not independent work items.
        // Only amplify pressure when there is actual L0 debt to consume.
        if pressure.l0_segments > pressure.l0_compaction_trigger
            && pressure.compaction_queue_depth >= pressure.compaction_queue_capacity
        {
            reasons |= PACING_COMPACTION_QUEUE;
        }
        if pressure.memory_backpressure_events > state.last_memory_backpressure_events {
            state.last_memory_backpressure_events = pressure.memory_backpressure_events;
            state.memory_pressure_ttl = 16;
        } else {
            state.memory_pressure_ttl = state.memory_pressure_ttl.saturating_sub(1);
        }
        if pressure.managed_non_cache_bytes.saturating_mul(100)
            >= pressure.managed_memory_max_bytes.saturating_mul(70)
            || state.memory_pressure_ttl != 0
        {
            reasons |= PACING_MEMORY;
        }
        if single_latency_pressure || wide_latency_pressure {
            reasons |= PACING_READ_LATENCY;
        }

        if reasons == 0 {
            state.pressure_streak = 0;
            state.current_reasons = 0;
            finish_regulation_period(&mut state, now);
            return Ok(Regulation {
                delay: Duration::ZERO,
            });
        }

        state.pressure_streak = state.pressure_streak.saturating_add(1);
        if state.pressure_streak < WRITE_PRESSURE_STREAK {
            return Ok(Regulation {
                delay: Duration::ZERO,
            });
        }
        state.current_reasons = reasons;
        state.active_since.get_or_insert(now);

        let mut debt_delay_ms = 0_u64;
        if reasons & PACING_L0 != 0 {
            state.l0_events = state.l0_events.saturating_add(1);
            let debt = pressure
                .l0_segments
                .saturating_sub(pressure.l0_compaction_trigger);
            debt_delay_ms = debt_delay_ms.saturating_add(u64::try_from(debt).unwrap_or(u64::MAX));
            if pressure.l0_segments >= pressure.l0_slowdown_trigger {
                debt_delay_ms = debt_delay_ms.saturating_add(
                    u64::try_from(pressure.l0_segments - pressure.l0_slowdown_trigger + 1)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2),
                );
            }
        }
        if reasons & PACING_COMPACTION_QUEUE != 0 {
            state.compaction_queue_events = state.compaction_queue_events.saturating_add(1);
            debt_delay_ms = debt_delay_ms.saturating_add(
                u64::try_from(pressure.compaction_queue_depth)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2),
            );
        }
        let debt_delay_nanos = Duration::from_millis(debt_delay_ms)
            .as_nanos()
            .saturating_mul(u128::from(pressure.incoming_bytes))
            .div_ceil(u128::from(DEBT_PACING_BYTES));
        let debt_delay = Duration::from_nanos(u64::try_from(debt_delay_nanos).unwrap_or(u64::MAX));
        let mut delay_ms = 0_u64;
        if reasons & PACING_MEMORY != 0 {
            state.memory_events = state.memory_events.saturating_add(1);
            let percent = pressure
                .managed_non_cache_bytes
                .saturating_mul(100)
                .checked_div(pressure.managed_memory_max_bytes.max(1))
                .unwrap_or(100);
            delay_ms = delay_ms.saturating_add(percent.saturating_sub(69).max(1));
        }
        if reasons & PACING_READ_LATENCY != 0 {
            state.read_latency_events = state.read_latency_events.saturating_add(1);
            let single_ratio = if single_latency_pressure {
                duration_nanos(single_p95)
                    .saturating_div(duration_nanos(self.read_latency_threshold).max(1))
            } else {
                0
            };
            let wide_ratio = if wide_latency_pressure {
                duration_nanos(wide_p95)
                    .saturating_div(duration_nanos(self.wide_read_latency_threshold).max(1))
            } else {
                0
            };
            let ratio = single_ratio.max(wide_ratio);
            delay_ms = delay_ms.saturating_add(ratio.saturating_mul(ratio).clamp(1, 50));
        }
        Ok(Regulation {
            delay: debt_delay
                .saturating_add(Duration::from_millis(delay_ms))
                .min(self.maximum_delay),
        })
    }

    pub(super) fn stats(&self) -> Result<WriteRegulatorStats> {
        let mut state = lock(&self.state)?;
        let now = Instant::now();
        expire_read_latencies(&mut state, now, self.read_latency_sample_ttl());
        if state
            .last_regulation
            .is_some_and(|last| now.saturating_duration_since(last) >= self.activity_ttl())
        {
            state.pressure_streak = 0;
            state.current_reasons = 0;
            let finished_at = state.last_regulation.unwrap_or(now);
            finish_regulation_period(&mut state, finished_at);
        }
        let current = state.active_since.map_or(0, |started| {
            duration_nanos(now.saturating_duration_since(started))
        });
        let read_latency_single_p95 = read_latency_percentile(&state.read_latency_single_nanos, 95);
        let read_latency_wide_p95 = read_latency_percentile(&state.read_latency_wide_nanos, 95);
        Ok(WriteRegulatorStats {
            active: state.active_since.is_some(),
            reasons: state.current_reasons,
            active_duration: Duration::from_nanos(
                state.accumulated_active_nanos.saturating_add(current),
            ),
            recoveries: state.recoveries,
            l0_events: state.l0_events,
            compaction_queue_events: state.compaction_queue_events,
            memory_events: state.memory_events,
            read_latency_events: state.read_latency_events,
            read_latency_samples: state
                .read_latency_single_nanos
                .len()
                .saturating_add(state.read_latency_wide_nanos.len()),
            read_latency_p95: read_latency_single_p95.max(read_latency_wide_p95),
            read_latency_single_samples: state.read_latency_single_nanos.len(),
            read_latency_single_p95,
            read_latency_wide_samples: state.read_latency_wide_nanos.len(),
            read_latency_wide_p95,
        })
    }

    fn read_latency_sample_ttl(&self) -> Duration {
        self.read_latency_threshold
            .saturating_mul(20)
            .max(Duration::from_millis(100))
            .min(Duration::from_secs(5))
    }

    fn activity_ttl(&self) -> Duration {
        self.maximum_delay
            .saturating_mul(4)
            .max(Duration::from_millis(100))
            .min(Duration::from_secs(1))
    }
}

fn expire_read_latencies(state: &mut WriteRegulatorState, now: Instant, ttl: Duration) {
    if state
        .last_single_read
        .is_some_and(|last| now.saturating_duration_since(last) >= ttl)
    {
        state.read_latency_single_nanos.clear();
        state.last_single_read = None;
    }
    if state
        .last_wide_read
        .is_some_and(|last| now.saturating_duration_since(last) >= ttl)
    {
        state.read_latency_wide_nanos.clear();
        state.last_wide_read = None;
    }
}

fn finish_regulation_period(state: &mut WriteRegulatorState, finished_at: Instant) {
    if let Some(started) = state.active_since.take() {
        state.accumulated_active_nanos =
            state
                .accumulated_active_nanos
                .saturating_add(duration_nanos(
                    finished_at.saturating_duration_since(started),
                ));
        state.recoveries = state.recoveries.saturating_add(1);
    }
}

fn read_latency_percentile(samples: &VecDeque<u64>, percentile: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let mut ordered = samples.iter().copied().collect::<Vec<_>>();
    ordered.sort_unstable();
    let index = (ordered.len() - 1).saturating_mul(percentile).div_ceil(100);
    Duration::from_nanos(ordered[index.min(ordered.len() - 1)])
}

pub(super) fn pacing_reason_text(reasons: u8) -> Option<String> {
    let mut names = Vec::with_capacity(4);
    if reasons & PACING_L0 != 0 {
        names.push("l0_debt");
    }
    if reasons & PACING_COMPACTION_QUEUE != 0 {
        names.push("compaction_queue");
    }
    if reasons & PACING_MEMORY != 0 {
        names.push("memory");
    }
    if reasons & PACING_READ_LATENCY != 0 {
        names.push("read_latency");
    }
    (!names.is_empty()).then(|| names.join(","))
}
