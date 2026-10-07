//! Serialized writer: WAL, group commit, visibility, and freezing.

#[cfg(feature = "pipeline-trace")]
use crate::diagnostics::trace::{Stage, Token};

use super::catalog::{storage_domain, validate_registered_write_times, validate_strict_writes};
use super::regulator::WritePressure;
use super::support::{available_disk_bytes, duration_nanos};
use super::{
    APPROXIMATE_POINT_BYTES, APPROXIMATE_SERIES_WATERMARK_BYTES, FlushCommand,
    GROUP_COMMIT_MAX_BATCHES, GROUP_COMMIT_MAX_DELAY, Generation, Inner, MaintenanceCommand,
    PublishCommand, WriteCommand, degrade,
};
use crate::admin::snapshot::SnapshotImage;
use crate::catalog::DataTemperature;
use crate::diagnostics::publication::{self as cost, Stage as CostStage};
use crate::engine::lifecycle::admission::{
    DEFAULT_WAIT_TIMEOUT, OverloadReason, WAIT_SLICE, WriteReply,
};
use crate::error::{Error, Result, corrupt, invalid};
use crate::memory::cache::{MemoryClass, MemoryReservation};
use crate::persistence::wal::WalWriter;
use crate::segment::PointBatch;
use crate::types::{Point, PointKey, SeriesKey, VersionedPoint, WriteReceipt};
use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError, TrySendError, bounded};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

pub(super) struct StrictWriteState {
    pub(super) watermarks: HashMap<SeriesKey, i64>,
    pub(super) memory: MemoryReservation,
}

pub(super) struct WriterCommitState {
    pub(super) next_commit: u64,
    pub(super) strict: StrictWriteState,
}

pub(super) fn writer_loop(
    inner: Arc<Inner>,
    receiver: Receiver<WriteCommand>,
    mut wal: WalWriter,
    mut next_wal_id: u64,
    mut active_wal_paths: Vec<PathBuf>,
    mut commit_state: WriterCommitState,
) {
    active_wal_paths.push(wal.path().to_path_buf());
    let mut next_generation = 1_u64;
    let recovery_flush_needed = inner.state.read().is_ok_and(|state| {
        state.active_bytes >= inner.options.memtable_max_bytes
            || inner.memory_governor.stats().1 > inner.memory_governor.maximum_bytes()
    });
    if recovery_flush_needed
        && let Err(error) = freeze_active(
            &inner,
            &mut wal,
            &mut next_wal_id,
            &mut next_generation,
            &mut active_wal_paths,
        )
    {
        degrade(&inner, &error);
    }
    let mut pending_command = None;
    loop {
        if let Err(error) = prune_expired_watermarks(&inner, &mut commit_state.strict) {
            degrade(&inner, &error);
        }
        let command = match pending_command.take() {
            Some(command) => command,
            None => match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(command) => command,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            },
        };
        #[cfg(feature = "publication-diagnostics")]
        let _diagnostic_scope = cost::enter(inner.publication_diagnostics.get());
        match command {
            WriteCommand::Delete {
                keys,
                memory_reservation,
                reply,
            } => {
                let result = publish_deletion(
                    &inner,
                    &mut wal,
                    &mut commit_state,
                    &keys,
                    memory_reservation,
                );
                let flush = result.is_ok() && !keys.is_empty();
                if wal.is_failed()
                    && let Err(error) = &result
                {
                    degrade(&inner, error);
                }
                let _ = reply.send(result);
                drop(keys);
                if flush
                    && let Err(error) = freeze_active(
                        &inner,
                        &mut wal,
                        &mut next_wal_id,
                        &mut next_generation,
                        &mut active_wal_paths,
                    )
                {
                    degrade(&inner, &error);
                }
            }

            WriteCommand::PruneWatermarks { reply } => {
                let _ = reply.send(prune_expired_watermarks(&inner, &mut commit_state.strict));
            }
            WriteCommand::Write {
                operations,
                memory_reservation,
                reply,
            } => {
                #[cfg(feature = "pipeline-trace")]
                reply.trace.mark(Stage::WriterReceived);
                let mut group = vec![(operations, memory_reservation, reply)];
                let deadline = Instant::now() + GROUP_COMMIT_MAX_DELAY;
                while group.len() < GROUP_COMMIT_MAX_BATCHES {
                    let next = match receiver.try_recv() {
                        Ok(command) => Ok(command),
                        Err(TryRecvError::Empty) => {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                break;
                            }
                            receiver.recv_timeout(remaining)
                        }
                        Err(TryRecvError::Disconnected) => break,
                    };
                    match next {
                        Ok(WriteCommand::Write {
                            operations,
                            memory_reservation,
                            reply,
                        }) => {
                            #[cfg(feature = "pipeline-trace")]
                            reply.trace.mark(Stage::WriterReceived);
                            group.push((operations, memory_reservation, reply));
                        }
                        Ok(command) => {
                            pending_command = Some(command);
                            break;
                        }
                        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                    }
                }
                process_group_commit(
                    &inner,
                    &mut wal,
                    &mut commit_state,
                    &mut next_wal_id,
                    &mut next_generation,
                    &mut active_wal_paths,
                    group,
                );
            }
            WriteCommand::Checkpoint { reply } => {
                let (barrier_commit, wal_bytes_through_commit) = match inner.state.read() {
                    Ok(state) => (state.visible_commit, state.wal_bytes_total),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        continue;
                    }
                };
                let freeze_result = freeze_active(
                    &inner,
                    &mut wal,
                    &mut next_wal_id,
                    &mut next_generation,
                    &mut active_wal_paths,
                );
                if let Err(error) = freeze_result {
                    let _ = reply.send(Err(error));
                    continue;
                }
                if inner
                    .flush_tx
                    .send(FlushCommand::Barrier {
                        barrier_commit,
                        wal_bytes_through_commit,
                        checkpoint: true,
                        reply,
                    })
                    .is_err()
                {
                    break;
                }
            }
            WriteCommand::Flush { reply } => {
                let (barrier_commit, wal_bytes_through_commit) = match inner.state.read() {
                    Ok(state) => (state.visible_commit, state.wal_bytes_total),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        continue;
                    }
                };
                let freeze_result = freeze_active(
                    &inner,
                    &mut wal,
                    &mut next_wal_id,
                    &mut next_generation,
                    &mut active_wal_paths,
                );
                if let Err(error) = freeze_result {
                    let _ = reply.send(Err(error));
                    continue;
                }
                if inner
                    .flush_tx
                    .send(FlushCommand::Barrier {
                        barrier_commit,
                        wal_bytes_through_commit,
                        checkpoint: false,
                        reply,
                    })
                    .is_err()
                {
                    break;
                }
            }
            WriteCommand::Snapshot { reply } => {
                let outcome = capture_snapshot_at_writer_barrier(
                    &inner,
                    &mut wal,
                    &mut next_wal_id,
                    &mut next_generation,
                    &mut active_wal_paths,
                );
                let _ = reply.send(outcome);
            }
            WriteCommand::CreateBucket {
                space,
                name,
                policy,
                created_at_us,
                reply,
            } => {
                let (publish_reply, publish_result) = bounded(1);
                let outcome = if inner
                    .publish_tx
                    .send(PublishCommand::CreateBucket {
                        space,
                        name,
                        policy,
                        created_at_us,
                        reply: publish_reply,
                    })
                    .is_err()
                {
                    Err(Error::WorkerStopped)
                } else {
                    publish_result.recv().unwrap_or(Err(Error::WorkerStopped))
                };
                let _ = reply.send(outcome);
            }
            WriteCommand::Stop { reply } => {
                let result = sync_wal_all(&inner, &mut wal);
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn prune_expired_watermarks(inner: &Inner, strict: &mut StrictWriteState) -> Result<()> {
    if !inner.state.read()?.watermark_prune_pending {
        return Ok(());
    }
    let mut state = inner.state.write()?;
    let previous = strict.watermarks.len();
    strict.watermarks.retain(|series, timestamp| {
        state
            .catalog
            .registered_policy(*series)
            .and_then(|policy| state.durable_retention_cutoffs.get(&policy.id))
            .is_none_or(|cutoff| *timestamp >= *cutoff)
    });
    if strict.watermarks.len() != previous {
        // Release table capacity before returning its reservation.
        strict.watermarks.shrink_to_fit();
        strict.memory.shrink(
            ((previous - strict.watermarks.len()) as u64)
                .saturating_mul(APPROXIMATE_SERIES_WATERMARK_BYTES),
        );
        inner
            .metrics
            .strict_series_watermarks
            .store(strict.watermarks.len() as u64, Ordering::Relaxed);
    }
    state.watermark_prune_pending = false;
    Ok(())
}

fn process_group_commit(
    inner: &Inner,
    wal: &mut WalWriter,
    commit_state: &mut WriterCommitState,
    next_wal_id: &mut u64,
    next_generation: &mut u64,
    active_wal_paths: &mut Vec<PathBuf>,
    group: Vec<(Vec<Point>, MemoryReservation, WriteReply)>,
) {
    let mut prepared = Vec::with_capacity(group.len());
    let mut group = group.into_iter();
    while let Some((operations, memory_reservation, reply)) = group.next() {
        let reservation = Arc::new(memory_reservation.reclassify(MemoryClass::Memtable));
        let outcome = prepare_write(
            inner,
            wal,
            commit_state,
            operations,
            reservation,
            #[cfg(feature = "pipeline-trace")]
            &reply.trace,
        );
        match outcome {
            Ok(write) => prepared.push((write, reply)),
            Err(error) if wal.is_failed() => {
                degrade(inner, &error);
                let message = format!("group WAL append failed: {error}");
                for (_, prepared_reply) in prepared.drain(..) {
                    let _ = prepared_reply.send(Err(corrupt(message.clone())));
                }
                let _ = reply.send(Err(error));
                for (_, _, remaining_reply) in group {
                    let _ = remaining_reply.send(Err(corrupt(message.clone())));
                }
                return;
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }
    if prepared.is_empty() {
        return;
    }
    #[cfg(feature = "pipeline-trace")]
    for (_, reply) in &prepared {
        reply.trace.mark(Stage::WalSyncStarted);
    }
    if let Err(error) = sync_wal(inner, wal) {
        degrade(inner, &error);
        let message = format!("group WAL sync failed: {error}");
        let mut prepared = prepared.into_iter();
        if let Some((_, first_reply)) = prepared.next() {
            let _ = first_reply.send(Err(error));
        }
        for (_, reply) in prepared {
            let _ = reply.send(Err(corrupt(message.clone())));
        }
        return;
    }
    inner
        .metrics
        .group_commit_groups
        .fetch_add(1, Ordering::Relaxed);
    inner.metrics.group_commit_batches.fetch_add(
        u64::try_from(prepared.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    #[cfg(feature = "pipeline-trace")]
    for (_, reply) in &prepared {
        reply.trace.mark(Stage::WalDurable);
    }
    let mut published_any = false;
    for (write, reply) in prepared {
        let completed = publish_prepared_write(inner, write);
        #[cfg(feature = "pipeline-trace")]
        if completed.is_ok() {
            reply.trace.mark(Stage::Visible);
        }
        published_any |= completed.is_ok();
        let _ = reply.send(completed);
    }
    if published_any {
        let _ = inner.maintenance_tx.try_send(MaintenanceCommand::Wake);
    }
    let must_flush = published_any
        && inner
            .state
            .read()
            .is_ok_and(|state| state.active_bytes >= inner.options.memtable_max_bytes);
    if must_flush
        && let Err(error) =
            freeze_active(inner, wal, next_wal_id, next_generation, active_wal_paths)
    {
        degrade(inner, &error);
    }
}

fn capture_snapshot_at_writer_barrier(
    inner: &Inner,
    wal: &mut WalWriter,
    next_wal_id: &mut u64,
    next_generation: &mut u64,
    active_wal_paths: &mut Vec<PathBuf>,
) -> Result<SnapshotImage> {
    let (barrier_commit, wal_bytes_through_commit) = {
        let state = inner.state.read()?;
        (state.visible_commit, state.wal_bytes_total)
    };
    freeze_active(inner, wal, next_wal_id, next_generation, active_wal_paths)?;
    let (checkpoint_tx, checkpoint_rx) = bounded(1);
    inner
        .flush_tx
        .send(FlushCommand::Barrier {
            barrier_commit,
            wal_bytes_through_commit,
            checkpoint: true,
            reply: checkpoint_tx,
        })
        .map_err(|_| Error::WorkerStopped)?;
    let checkpoint_commit = checkpoint_rx.recv().map_err(|_| Error::WorkerStopped)??;
    let (capture_tx, capture_rx) = bounded(1);
    inner
        .publish_tx
        .send(PublishCommand::CaptureSnapshot {
            checkpoint_commit,
            reply: capture_tx,
        })
        .map_err(|_| Error::WorkerStopped)?;
    capture_rx.recv().map_err(|_| Error::WorkerStopped)?
}

fn sync_wal(inner: &Inner, wal: &mut WalWriter) -> Result<()> {
    let started = Instant::now();
    let result = {
        let _timer = cost::stage(CostStage::WalSync);
        wal.sync()
    };
    inner
        .metrics
        .wal_sync_nanos
        .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
    if result.is_ok() {
        inner.metrics.wal_syncs.fetch_add(1, Ordering::Relaxed);
    }
    result
}

fn sync_wal_all(inner: &Inner, wal: &mut WalWriter) -> Result<()> {
    let started = Instant::now();
    let result = {
        let _timer = cost::stage(CostStage::WalSyncAll);
        wal.sync_all()
    };
    inner
        .metrics
        .wal_sync_nanos
        .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
    if result.is_ok() {
        inner.metrics.wal_syncs.fetch_add(1, Ordering::Relaxed);
    }
    result
}

struct PreparedWrite {
    operations: Vec<VersionedPoint>,
    memory_reservation: Arc<MemoryReservation>,
    observed_now_us: i64,
    contains_cold_write: bool,
    wal_bytes: u64,
    receipt: WriteReceipt,
}

fn ensure_wal_space(inner: &Inner, frame_bytes: u64) -> Result<()> {
    let reserve = inner.options.minimum_free_disk_bytes;
    if reserve == 0 {
        return Ok(());
    }
    // The single writer checks each frame, including later batches in a group.
    // This is admission, not a filesystem reservation: concurrent users of the
    // disk can still exhaust it. Flush/checkpoint may use the remaining reserve.
    let available = available_disk_bytes(&inner.root)?;
    let required = reserve.saturating_add(frame_bytes);
    if available < required {
        inner
            .metrics
            .wal_space_rejections
            .fetch_add(1, Ordering::Relaxed);
        return Err(Error::InsufficientDiskSpace {
            available,
            required,
        });
    }
    Ok(())
}

fn prepare_write(
    inner: &Inner,
    wal: &mut WalWriter,
    commit_state: &mut WriterCommitState,
    writes: Vec<Point>,
    memory_reservation: Arc<MemoryReservation>,
    #[cfg(feature = "pipeline-trace")] trace: &Token,
) -> Result<PreparedWrite> {
    #[cfg(feature = "pipeline-trace")]
    trace.mark(Stage::PrepareStarted);
    let observed_now_us = inner.options.clock.now_us();
    validate_registered_write_times(inner, &writes, observed_now_us)?;
    let strict_updates = validate_strict_writes(&writes, &commit_state.strict.watermarks)?;
    apply_l0_backpressure(inner, &writes)?;
    let contains_cold_write = {
        let state = inner.state.read()?;
        let now_us = state.maintenance_now_us.max(observed_now_us);
        writes.iter().any(|operation| {
            state
                .catalog
                .registered_policy(operation.series)
                .is_some_and(|policy| {
                    policy.temperature(
                        operation.timestamp_us,
                        now_us,
                        state.retention_cutoffs.get(&policy.id).copied(),
                    ) == DataTemperature::Cold
                })
        })
    };
    let commit_id = commit_state
        .next_commit
        .checked_add(1)
        .ok_or_else(|| invalid("commit ID exhausted"))?;
    let mut operations = Vec::with_capacity(writes.len());
    let point_count =
        u32::try_from(writes.len()).map_err(|_| invalid("batch point count overflow"))?;
    for point in writes {
        operations.push(VersionedPoint {
            key: PointKey {
                series: point.series,
                timestamp_us: point.timestamp_us,
            },
            commit_id,
            value_bits: point.value.to_bits(),
        });
    }
    let new_strict_series = strict_updates
        .iter()
        .filter(|(series, _)| !commit_state.strict.watermarks.contains_key(series))
        .count();
    let added_watermark_bytes = u64::try_from(new_strict_series)
        .unwrap_or(u64::MAX)
        .saturating_mul(APPROXIMATE_SERIES_WATERMARK_BYTES);
    ensure_wal_space(inner, WalWriter::point_frame_bytes(operations.len())?)?;
    if !commit_state.strict.memory.try_grow(added_watermark_bytes) {
        return Err(Error::ResourceLimit(
            "strict series watermarks exceed the managed memory limit".into(),
        ));
    }
    #[cfg(feature = "pipeline-trace")]
    {
        if trace.is_sampled()
            && let Some(point) = operations.last()
            && let Ok(state) = inner.state.read()
            && let Ok(domain) = storage_domain(
                &state.catalog,
                point.key.series,
                point.key.timestamp_us,
                inner.options.virtual_shards,
                inner.options.partition_span_us,
            )
        {
            trace.bind(commit_id, domain);
        }
        trace.mark(Stage::WalAppendStarted);
    }
    let wal_bytes = match wal.append(commit_id, &operations) {
        Ok(bytes) => bytes,
        Err(error) => {
            commit_state.strict.memory.shrink(added_watermark_bytes);
            return Err(error);
        }
    };
    commit_state.next_commit = commit_id;
    #[cfg(feature = "pipeline-trace")]
    trace.mark(Stage::WalAppended);
    for (series, timestamp_us) in strict_updates {
        commit_state.strict.watermarks.insert(series, timestamp_us);
    }
    inner.metrics.strict_series_watermarks.store(
        u64::try_from(commit_state.strict.watermarks.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    operations.sort_unstable();
    let operation_count =
        u32::try_from(operations.len()).map_err(|_| invalid("batch too large"))?;
    Ok(PreparedWrite {
        operations,
        memory_reservation,
        observed_now_us,
        contains_cold_write,
        wal_bytes,
        receipt: WriteReceipt {
            commit_id,
            operation_count,
            point_count,
        },
    })
}

fn publish_prepared_write(inner: &Inner, prepared_write: PreparedWrite) -> Result<WriteReceipt> {
    let PreparedWrite {
        operations,
        memory_reservation,
        observed_now_us,
        contains_cold_write,
        wal_bytes,
        receipt,
    } = prepared_write;
    let bytes = operations.len() as u64 * APPROXIMATE_POINT_BYTES + PointBatch::METADATA_BYTES;
    // Compute batch bounds before taking the publication lock.
    let batch = Arc::new(PointBatch::managed(operations, memory_reservation));
    {
        let mut state = {
            let _timer = cost::stage(CostStage::WriterVisibilityWait);
            inner.state.write()?
        };
        for operation in batch.iter() {
            let policy_id = state
                .catalog
                .storage_layout(operation.key.series)
                .map_or(1, |(policy_id, _)| policy_id);
            state
                .event_watermarks
                .entry(policy_id)
                .and_modify(|watermark| {
                    *watermark = (*watermark).max(operation.key.timestamp_us);
                })
                .or_insert(operation.key.timestamp_us);
        }
        state.active_batches.push(batch);
        state.active_bytes = state.active_bytes.saturating_add(bytes);
        state.visible_commit = receipt.commit_id;
        state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
        state.wal_bytes_total = state.wal_bytes_total.saturating_add(wal_bytes);
        if contains_cold_write {
            state.last_cold_write = Some((receipt.commit_id, Instant::now()));
        }
    }
    inner.metrics.writes.fetch_add(1, Ordering::Relaxed);
    inner
        .metrics
        .written_operations
        .fetch_add(u64::from(receipt.operation_count), Ordering::Relaxed);
    inner
        .metrics
        .written_points
        .fetch_add(u64::from(receipt.point_count), Ordering::Relaxed);
    inner
        .metrics
        .wal_bytes
        .fetch_add(wal_bytes, Ordering::Relaxed);
    Ok(receipt)
}

fn apply_l0_backpressure(inner: &Inner, operations: &[Point]) -> Result<()> {
    let deadline = Instant::now() + DEFAULT_WAIT_TIMEOUT;
    let mut domains = HashSet::with_capacity(operations.len());
    let catalog = inner.state.read()?.catalog.clone();
    for operation in operations {
        domains.insert(storage_domain(
            &catalog,
            operation.series,
            operation.timestamp_us,
            inner.options.virtual_shards,
            inner.options.partition_span_us,
        )?);
    }
    let mut waiting = None;
    let result = (|| loop {
        if Instant::now() >= deadline {
            return Err(Error::Overloaded(OverloadReason::CompactionDebt));
        }
        let maximum = {
            let files = inner.state.read()?.files.clone();
            let mut maximum = 0;
            for domain in &domains {
                maximum = maximum.max(files.l0_count(*domain)?);
            }
            maximum
        };
        if maximum < inner.options.l0_stop_trigger {
            return Ok(maximum);
        }
        if waiting.is_none() {
            waiting = Some(Instant::now());
            inner
                .metrics
                .foreground_maintenance_waits
                .fetch_add(1, Ordering::Relaxed);
        }
        // Wait for this write's actual L0 condition, not an entire background
        // command. Publication may relieve pressure while history is still busy.
        super::publication::wake_compaction(&inner.compaction_tx);
        super::publication::wake_compaction(&inner.history_compaction_tx);
        thread::sleep(WAIT_SLICE.min(deadline.saturating_duration_since(Instant::now())));
    })();
    if let Some(started) = waiting {
        inner
            .metrics
            .foreground_maintenance_wait_nanos
            .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
    }
    let maximum = result?;
    let (
        _,
        _,
        managed_write_queue_bytes,
        managed_memtable_bytes,
        managed_series_watermark_bytes,
        _,
        _,
    ) = inner.memory_governor.stats();
    let regulation = inner.write_regulator.regulate(WritePressure {
        incoming_bytes: u64::try_from(operations.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(APPROXIMATE_POINT_BYTES),
        l0_segments: maximum,
        l0_compaction_trigger: inner.options.l0_compaction_trigger,
        l0_slowdown_trigger: inner.options.l0_slowdown_trigger,
        compaction_queue_depth: inner
            .compaction_tx
            .len()
            .saturating_add(inner.history_compaction_tx.len()),
        compaction_queue_capacity: inner
            .options
            .compaction_queue_capacity
            .saturating_add(inner.options.history_compaction_queue_capacity),
        managed_non_cache_bytes: managed_write_queue_bytes
            .saturating_add(managed_memtable_bytes)
            .saturating_add(managed_series_watermark_bytes),
        managed_memory_max_bytes: inner.memory_governor.maximum_bytes(),
        memory_backpressure_events: inner.metrics.memory_backpressure.load(Ordering::Relaxed),
    })?;
    if !regulation.delay.is_zero() {
        inner
            .metrics
            .write_pacing_events
            .fetch_add(1, Ordering::Relaxed);
        thread::sleep(regulation.delay);
        inner
            .metrics
            .write_pacing_nanos
            .fetch_add(duration_nanos(regulation.delay), Ordering::Relaxed);
    }
    Ok(())
}

fn freeze_active(
    inner: &Inner,
    wal: &mut WalWriter,
    next_wal_id: &mut u64,
    next_generation: &mut u64,
    active_wal_paths: &mut Vec<PathBuf>,
) -> Result<()> {
    let (maximum_commit, has_batches) = {
        let state = inner.state.read()?;
        (state.visible_commit, !state.active_batches.is_empty())
    };
    if !has_batches {
        return Ok(());
    }
    sync_wal_all(inner, wal)?;
    // Offline verify/salvage hold the same directory lock while copying the
    // durable image. Do not move HEAD past the WAL set they are inspecting.
    let wal_lock = crate::persistence::lock::StorageLock::wal_exclusive(&inner.root)?;
    let new_wal = match WalWriter::create(
        &inner.root.join("wal"),
        inner.storage_uuid,
        *next_wal_id,
        maximum_commit.saturating_add(1),
    ) {
        Ok(wal) => wal,
        Err(error) => {
            // HEAD may have been renamed before a failed directory sync. Do
            // not keep appending to the preceding WAL after an uncertain switch.
            degrade(inner, &error);
            return Err(error);
        }
    };
    drop(wal_lock);
    *next_wal_id = next_wal_id.saturating_add(1);
    let old_wal_paths = std::mem::replace(active_wal_paths, vec![new_wal.path().to_path_buf()]);
    *wal = new_wal;
    let generation = {
        let mut state = inner.state.write()?;
        let batches = std::mem::take(&mut state.active_batches);
        let bytes = std::mem::take(&mut state.active_bytes);
        let generation = Arc::new(Generation {
            id: *next_generation,
            maximum_commit,
            batches,
            bytes,
            wal_paths: old_wal_paths,
        });
        *next_generation = next_generation.saturating_add(1);
        state.frozen.push(Arc::clone(&generation));
        generation
    };
    #[cfg(feature = "pipeline-trace")]
    if let Some(trace) = inner.pipeline_trace.get() {
        trace.each(|sample| sample.freeze(maximum_commit, generation.id));
    }
    let command = FlushCommand::Generation(generation);
    match inner.flush_tx.try_send(command) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(command)) => {
            inner
                .metrics
                .flush_backpressure
                .fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            let result = inner
                .flush_tx
                .send(command)
                .map_err(|_| Error::WorkerStopped);
            inner
                .metrics
                .flush_backpressure_nanos
                .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
            result
        }
        Err(TrySendError::Disconnected(_)) => Err(Error::WorkerStopped),
    }
}

fn publish_deletion(
    inner: &Inner,
    wal: &mut WalWriter,
    commit: &mut WriterCommitState,
    keys: &[SeriesKey],
    reservation: MemoryReservation,
) -> Result<WriteReceipt> {
    let _guard = super::support::lock(&inner.deletion_lock)?;
    if keys.is_empty() {
        return Ok(WriteReceipt {
            commit_id: inner.state.read()?.visible_commit,
            operation_count: 0,
            point_count: 0,
        });
    }
    let id = commit
        .next_commit
        .checked_add(1)
        .ok_or_else(|| invalid("commit ID exhausted"))?;
    let previous = Arc::clone(&inner.state.read()?.deletions);
    let next = previous.updated(keys, id, &inner.memory_governor)?;
    ensure_wal_space(inner, WalWriter::deletion_frame_bytes(keys.len())?)?;
    let bytes = wal.append_deletions(id, keys)?;
    sync_wal(inner, wal)?;
    let batch = Arc::new(PointBatch::deletion(
        id,
        Arc::new(reservation.reclassify(MemoryClass::Memtable)),
    ));
    commit.next_commit = id;
    let mut removed = 0;
    for key in keys {
        removed += usize::from(commit.strict.watermarks.remove(key).is_some());
    }
    if removed != 0 {
        commit.strict.watermarks.shrink_to_fit();
        commit
            .strict
            .memory
            .shrink(removed as u64 * APPROXIMATE_SERIES_WATERMARK_BYTES);
    }
    {
        let mut state = inner.state.write()?;
        state.deletions = next;
        state.active_batches.push(batch);
        state.active_bytes += PointBatch::METADATA_BYTES;
        state.visible_commit = id;
        state.wal_bytes_total = state.wal_bytes_total.saturating_add(bytes);
    }
    inner
        .metrics
        .strict_series_watermarks
        .store(commit.strict.watermarks.len() as u64, Ordering::Relaxed);
    inner.metrics.writes.fetch_add(1, Ordering::Relaxed);
    inner
        .metrics
        .written_operations
        .fetch_add(keys.len() as u64, Ordering::Relaxed);
    inner.metrics.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    inner
        .metrics
        .group_commit_groups
        .fetch_add(1, Ordering::Relaxed);
    inner
        .metrics
        .group_commit_batches
        .fetch_add(1, Ordering::Relaxed);

    Ok(WriteReceipt {
        commit_id: id,
        operation_count: keys.len() as u32,
        point_count: 0,
    })
}
