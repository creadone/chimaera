//! Последовательный writer: WAL, group commit, видимость и freeze.

use super::catalog::{storage_domain, validate_registered_write_times, validate_strict_writes};
use super::regulator::WritePressure;
use super::support::{
    compaction_admission_error, duration_nanos, read, wait_for_compaction, write,
};
use super::{
    APPROXIMATE_POINT_BYTES, APPROXIMATE_SERIES_WATERMARK_BYTES, CompactionCommand, FlushCommand,
    GROUP_COMMIT_MAX_BATCHES, GROUP_COMMIT_MAX_DELAY, Generation, Inner, MaintenanceCommand,
    PublishCommand, WriteCommand, degrade,
};
use crate::cache::{MemoryClass, MemoryReservation};
use crate::catalog::DataTemperature;
use crate::error::{Error, Result, corrupt, invalid};
use crate::layout::Domain;
use crate::lifecycle::{DEFAULT_WAIT_TIMEOUT, OverloadReason, WriteReply};
use crate::segment::PointBatch;
use crate::snapshot::SnapshotImage;
use crate::types::{Durability, PointKey, SeriesKey, VersionedPoint, WriteOperation, WriteReceipt};
use crate::wal::WalWriter;
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
    let recovery_flush_needed = read(&inner.state).is_ok_and(|state| {
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
        let command = match pending_command.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };
        match command {
            WriteCommand::Write {
                operations,
                durability,
                memory_reservation,
                reply,
            } => {
                if durability == Durability::GroupCommit {
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
                                durability: Durability::GroupCommit,
                                memory_reservation,
                                reply,
                            }) => group.push((operations, memory_reservation, reply)),
                            Ok(command) => {
                                pending_command = Some(command);
                                break;
                            }
                            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                                break;
                            }
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
                    continue;
                }
                let reservation = Arc::new(memory_reservation.reclassify(MemoryClass::Memtable));
                let outcome = process_write(
                    &inner,
                    &mut wal,
                    &mut commit_state,
                    operations,
                    durability,
                    reservation,
                );
                if let Err(error) = outcome.as_ref()
                    && wal.is_failed()
                {
                    degrade(&inner, error);
                }
                let completed = outcome.is_ok();
                let must_flush = outcome.as_ref().ok().is_some_and(|_| {
                    read(&inner.state)
                        .is_ok_and(|state| state.active_bytes >= inner.options.memtable_max_bytes)
                });
                let _ = reply.send(outcome);
                if completed {
                    let _ = inner.maintenance_tx.try_send(MaintenanceCommand::Wake);
                }
                if must_flush
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
            WriteCommand::Checkpoint { reply } => {
                let (barrier_commit, wal_bytes_through_commit) = match read(&inner.state) {
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
                let (barrier_commit, wal_bytes_through_commit) = match read(&inner.state) {
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

fn process_group_commit(
    inner: &Inner,
    wal: &mut WalWriter,
    commit_state: &mut WriterCommitState,
    next_wal_id: &mut u64,
    next_generation: &mut u64,
    active_wal_paths: &mut Vec<PathBuf>,
    group: Vec<(Vec<WriteOperation>, MemoryReservation, WriteReply)>,
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
            Durability::GroupCommit,
            reservation,
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
    let mut published_any = false;
    for (write, reply) in prepared {
        let completed = publish_prepared_write(inner, write);
        published_any |= completed.is_ok();
        let _ = reply.send(completed);
    }
    if published_any {
        let _ = inner.maintenance_tx.try_send(MaintenanceCommand::Wake);
    }
    let must_flush = published_any
        && read(&inner.state)
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
        let state = read(&inner.state)?;
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

fn process_write(
    inner: &Inner,
    wal: &mut WalWriter,
    commit_state: &mut WriterCommitState,
    writes: Vec<WriteOperation>,
    durability: Durability,
    memory_reservation: Arc<MemoryReservation>,
) -> Result<WriteReceipt> {
    let write = prepare_write(
        inner,
        wal,
        commit_state,
        writes,
        durability,
        memory_reservation,
    )?;
    if durability != Durability::Async {
        sync_wal(inner, wal)?;
    }
    publish_prepared_write(inner, write)
}

fn sync_wal(inner: &Inner, wal: &mut WalWriter) -> Result<()> {
    let started = Instant::now();
    let result = wal.sync();
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
    let result = wal.sync_all();
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
    delete_count: u32,
}

fn prepare_write(
    inner: &Inner,
    wal: &mut WalWriter,
    commit_state: &mut WriterCommitState,
    writes: Vec<WriteOperation>,
    durability: Durability,
    memory_reservation: Arc<MemoryReservation>,
) -> Result<PreparedWrite> {
    let observed_now_us = inner.options.clock.now_us();
    validate_registered_write_times(inner, &writes, observed_now_us)?;
    let strict_updates = validate_strict_writes(inner, &writes, &commit_state.strict.watermarks)?;
    apply_l0_backpressure(inner, &writes)?;
    let contains_cold_write = {
        let state = read(&inner.state)?;
        let now_us = state.maintenance_now_us.max(observed_now_us);
        writes.iter().any(|operation| {
            state
                .catalog
                .registered_policy(operation.series())
                .is_some_and(|policy| {
                    policy.temperature(
                        operation.timestamp_us(),
                        now_us,
                        state.retention_cutoffs.get(&policy.id).copied(),
                    ) == DataTemperature::Cold
                })
        })
    };
    commit_state.next_commit = commit_state
        .next_commit
        .checked_add(1)
        .ok_or_else(|| invalid("commit ID exhausted"))?;
    let commit_id = commit_state.next_commit;
    let mut operations = Vec::with_capacity(writes.len());
    let mut point_count = 0_u32;
    let mut delete_count = 0_u32;
    for (index, operation) in writes.into_iter().enumerate() {
        let (series, timestamp_us, value_bits, is_delete) = match operation {
            WriteOperation::Put(point) => {
                point_count = point_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("batch point count overflow"))?;
                (
                    point.series,
                    point.timestamp_us,
                    point.value.to_bits(),
                    false,
                )
            }
            WriteOperation::Delete {
                series,
                timestamp_us,
            } => {
                delete_count = delete_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("batch delete count overflow"))?;
                (series, timestamp_us, 0, true)
            }
        };
        operations.push(VersionedPoint {
            key: PointKey {
                series,
                timestamp_us,
            },
            commit_id,
            ordinal: u32::try_from(index).map_err(|_| invalid("batch ordinal overflow"))?,
            value_bits,
            is_delete,
        });
    }
    let new_strict_series = strict_updates
        .iter()
        .filter(|(series, _)| !commit_state.strict.watermarks.contains_key(series))
        .count();
    let added_watermark_bytes = u64::try_from(new_strict_series)
        .unwrap_or(u64::MAX)
        .saturating_mul(APPROXIMATE_SERIES_WATERMARK_BYTES);
    if !commit_state.strict.memory.try_grow(added_watermark_bytes) {
        return Err(Error::ResourceLimit(
            "strict series watermarks exceed the managed memory limit".into(),
        ));
    }
    let wal_bytes = match wal.append(commit_id, &operations, durability) {
        Ok(bytes) => bytes,
        Err(error) => {
            commit_state.strict.memory.shrink(added_watermark_bytes);
            return Err(error);
        }
    };
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
            durable: durability != Durability::Async,
        },
        delete_count,
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
        delete_count,
    } = prepared_write;
    let bytes = operations.len() as u64 * APPROXIMATE_POINT_BYTES;
    {
        let mut state = write(&inner.state)?;
        for operation in &operations {
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
        state.active_batches.push(Arc::new(PointBatch::managed(
            operations,
            memory_reservation,
        )));
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
        .deleted_points
        .fetch_add(u64::from(delete_count), Ordering::Relaxed);
    inner
        .metrics
        .wal_bytes
        .fetch_add(wal_bytes, Ordering::Relaxed);
    Ok(receipt)
}

fn apply_l0_backpressure(inner: &Inner, operations: &[WriteOperation]) -> Result<()> {
    let deadline = Instant::now() + DEFAULT_WAIT_TIMEOUT;
    let mut domains = HashSet::with_capacity(operations.len());
    let catalog = read(&inner.state)?.catalog.clone();
    for operation in operations {
        domains.insert(storage_domain(
            &catalog,
            operation.series(),
            operation.timestamp_us(),
            inner.options.virtual_shards,
            inner.options.partition_span_us,
        )?);
    }
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Overloaded(OverloadReason::CompactionDebt));
        }
        let maximum = {
            let state = read(&inner.state)?;
            let mut counts = HashMap::<Domain, usize>::new();
            for segment in &state.segments {
                if segment.descriptor().level == 0 {
                    let domain = segment.descriptor().domain();
                    if domains.contains(&domain) {
                        *counts.entry(domain).or_default() += 1;
                    }
                }
            }
            counts.values().copied().max().unwrap_or(0)
        };
        if maximum >= inner.options.l0_stop_trigger {
            let started = Instant::now();
            inner
                .metrics
                .foreground_maintenance_waits
                .fetch_add(1, Ordering::Relaxed);
            let (reply_tx, reply_rx) = bounded(1);
            inner
                .compaction_tx
                .send_timeout(
                    CompactionCommand::Trigger {
                        force: false,
                        reply: Some(reply_tx),
                    },
                    deadline.saturating_duration_since(Instant::now()),
                )
                .map_err(compaction_admission_error)?;
            wait_for_compaction(&reply_rx, deadline)?;
            let (history_reply_tx, history_reply_rx) = bounded(1);
            inner
                .history_compaction_tx
                .send_timeout(
                    CompactionCommand::Trigger {
                        force: false,
                        reply: Some(history_reply_tx),
                    },
                    deadline.saturating_duration_since(Instant::now()),
                )
                .map_err(compaction_admission_error)?;
            wait_for_compaction(&history_reply_rx, deadline)?;
            inner
                .metrics
                .foreground_maintenance_wait_nanos
                .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
            continue;
        }
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
        return Ok(());
    }
}

fn freeze_active(
    inner: &Inner,
    wal: &mut WalWriter,
    next_wal_id: &mut u64,
    next_generation: &mut u64,
    active_wal_paths: &mut Vec<PathBuf>,
) -> Result<()> {
    let (maximum_commit, has_batches) = {
        let state = read(&inner.state)?;
        (state.visible_commit, !state.active_batches.is_empty())
    };
    if !has_batches {
        return Ok(());
    }
    sync_wal_all(inner, wal)?;
    let new_wal = WalWriter::create(
        &inner.root.join("wal"),
        inner.storage_uuid,
        *next_wal_id,
        maximum_commit.saturating_add(1),
    )?;
    *next_wal_id = next_wal_id.saturating_add(1);
    let old_wal_paths = std::mem::replace(active_wal_paths, vec![new_wal.path().to_path_buf()]);
    *wal = new_wal;
    let generation = {
        let mut state = write(&inner.state)?;
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
