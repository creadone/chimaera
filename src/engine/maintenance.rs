//! Периодические checkpoint, холодный сброс и обслуживание хранения.

use super::support::{duration_nanos, lock, read};
use super::{Inner, MaintenanceCommand, OPEN, WriteCommand, degrade};
use crate::error::Error;
use crossbeam_channel::{Receiver, RecvTimeoutError, bounded};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub(super) fn maintenance_loop(inner: Arc<Inner>, receiver: Receiver<MaintenanceCommand>) {
    let poll_interval = inner
        .options
        .checkpoint_interval
        .min(if inner.options.cold_flush_idle_interval.is_zero() {
            Duration::from_millis(1)
        } else {
            inner.options.cold_flush_idle_interval
        })
        .min(Duration::from_millis(100))
        .max(Duration::from_millis(1));
    let mut observed_checkpoint = read(&inner.state)
        .map(|state| state.checkpoint_commit)
        .unwrap_or_default();
    let mut checkpoint_at = Instant::now();

    loop {
        match receiver.recv_timeout(poll_interval) {
            Ok(MaintenanceCommand::Stop) | Err(RecvTimeoutError::Disconnected) => break,
            Ok(MaintenanceCommand::Wake) | Err(RecvTimeoutError::Timeout) => {}
        }
        if inner.lifecycle.load(Ordering::Acquire) != OPEN {
            continue;
        }
        if lock(&inner.degraded).is_ok_and(|degraded| degraded.is_some()) {
            continue;
        }
        let now = Instant::now();
        let state = match read(&inner.state) {
            Ok(state) => state,
            Err(error) => {
                degrade(&inner, &error);
                continue;
            }
        };
        if state.checkpoint_commit != observed_checkpoint {
            observed_checkpoint = state.checkpoint_commit;
            checkpoint_at = now;
        }
        let uncheckpointed_wal_bytes = state
            .wal_bytes_total
            .saturating_sub(state.checkpoint_wal_bytes);
        let checkpoint_due = state.visible_commit > state.checkpoint_commit
            && (uncheckpointed_wal_bytes >= inner.options.wal_checkpoint_bytes
                || now.duration_since(checkpoint_at) >= inner.options.checkpoint_interval
                || state.last_cold_write.is_some_and(|(_, written_at)| {
                    now.duration_since(written_at) >= inner.options.cold_flush_idle_interval
                }));
        drop(state);
        if !checkpoint_due {
            continue;
        }

        let started = Instant::now();
        let (reply_tx, reply_rx) = bounded(1);
        if inner
            .write_tx
            .send(WriteCommand::Checkpoint { reply: reply_tx })
            .is_err()
        {
            if inner.lifecycle.load(Ordering::Acquire) == OPEN {
                degrade(&inner, &Error::WorkerStopped);
            }
            continue;
        }
        match reply_rx.recv() {
            Ok(Ok(commit_id)) => {
                observed_checkpoint = commit_id;
                checkpoint_at = Instant::now();
                inner
                    .metrics
                    .background_checkpoints
                    .fetch_add(1, Ordering::Relaxed);
                inner
                    .metrics
                    .background_checkpoint_nanos
                    .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
            }
            Ok(Err(error)) => degrade(&inner, &error),
            Err(_) if inner.lifecycle.load(Ordering::Acquire) == OPEN => {
                degrade(&inner, &Error::WorkerStopped);
            }
            Err(_) => {}
        }
    }
}
