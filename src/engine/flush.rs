//! Координатор сброса поколений памяти и работники упаковки.

use super::catalog::{compression_profile_for_domain, storage_domain};
use super::compaction_selection::sealed_before_for_policy;
use super::publication::UnpublishedArtifacts;
use super::support::{duration_nanos, lock, read};
use super::{
    ACTIVE_COMPACTION_MAX_INPUT_BYTES, APPROXIMATE_POINT_BYTES, FlushCommand, FlushPackCommand,
    FlushPackOutput, FlushSlice, Generation, Inner, PublishCommand, degrade,
};
use crate::admission::WorkMemoryClass;
use crate::catalog::CatalogSnapshot;
use crate::error::{Error, Result, corrupt};
use crate::layout::Domain;
use crate::segment::{
    OperationCursor, RangedSliceCursor, SegmentPackResources, SegmentWriteRequest,
    prepare_segment_pack, sync_segment_pack_group,
};
use crossbeam_channel::{Receiver, bounded};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub(super) fn flush_loop(inner: Arc<Inner>, receiver: Receiver<FlushCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            FlushCommand::Generation(generation) => {
                let started = Instant::now();
                let outcome = flush_generation(&inner, Arc::clone(&generation));
                if let Ok(bytes) = outcome {
                    inner.metrics.flushes.fetch_add(1, Ordering::Relaxed);
                    inner
                        .metrics
                        .flush_bytes
                        .fetch_add(bytes, Ordering::Relaxed);
                    inner
                        .metrics
                        .flush_nanos
                        .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
                } else if let Err(error) = outcome {
                    degrade(&inner, &error);
                }
            }
            FlushCommand::Barrier {
                barrier_commit,
                wal_bytes_through_commit,
                checkpoint,
                reply,
            } => {
                let degraded = lock(&inner.degraded)
                    .map(|degraded| degraded.clone())
                    .and_then(|degraded| match degraded {
                        Some(message) => Err(corrupt(format!("storage is degraded: {message}"))),
                        None => Ok(()),
                    });
                if let Err(error) = degraded {
                    let _ = reply.send(Err(error));
                } else if checkpoint {
                    if inner
                        .publish_tx
                        .send(PublishCommand::Checkpoint {
                            commit_id: barrier_commit,
                            wal_bytes_through_commit,
                            reply,
                        })
                        .is_err()
                    {
                        break;
                    }
                } else {
                    let _ = reply.send(Ok(barrier_commit));
                }
            }
            FlushCommand::Stop => break,
        }
    }
}

fn flush_generation(inner: &Inner, generation: Arc<Generation>) -> Result<u64> {
    let work_memory = inner
        .work_memory_governor
        .reserve_background(WorkMemoryClass::Flush, generation.bytes)?;
    let mut domains = BTreeMap::<(Domain, bool), Vec<FlushSlice>>::new();
    let (catalog, maintenance_now_us, event_watermarks) = {
        let state = read(&inner.state)?;
        (
            state.catalog.clone(),
            state.maintenance_now_us,
            state.event_watermarks.clone(),
        )
    };
    for batch in &generation.batches {
        let mut batch_ranges = BTreeMap::<(Domain, bool), Vec<(usize, usize)>>::new();
        let mut range_start = 0_usize;
        let mut current_region = None;
        for (index, point) in batch.iter().enumerate() {
            let region = storage_domain(
                &catalog,
                point.key.series,
                point.key.timestamp_us,
                inner.options.virtual_shards,
                inner.options.partition_span_us,
            )?;
            let historical = sealed_before_for_policy(
                &inner.options,
                &catalog,
                &event_watermarks,
                region.policy_id,
            )
            .is_some_and(|before| point.key.timestamp_us < before);
            let region = (region, historical);
            if current_region != Some(region) {
                if let Some(previous) = current_region {
                    batch_ranges
                        .entry(previous)
                        .or_default()
                        .push((range_start, index));
                }
                current_region = Some(region);
                range_start = index;
            }
        }
        if let Some(region) = current_region {
            batch_ranges
                .entry(region)
                .or_default()
                .push((range_start, batch.len()));
        }
        for (region, ranges) in batch_ranges {
            let maximum_points =
                usize::try_from(ACTIVE_COMPACTION_MAX_INPUT_BYTES / APPROXIMATE_POINT_BYTES)
                    .unwrap_or(usize::MAX)
                    .max(1);
            for (mut start, end) in ranges {
                while start < end {
                    let slice_end = start.saturating_add(maximum_points).min(end);
                    domains.entry(region).or_default().push(FlushSlice {
                        batch: Arc::clone(batch),
                        start,
                        end: slice_end,
                    });
                    start = slice_end;
                }
            }
        }
    }
    let mut requests = BTreeMap::<(u16, u32, bool), Vec<SegmentWriteRequest>>::new();
    for ((domain, historical), slices) in domains {
        let mut sources = Vec::<Box<dyn OperationCursor>>::new();
        let mut source_bytes = 0_u64;
        for slice in slices {
            let slice_bytes = u64::try_from(slice.end.saturating_sub(slice.start))
                .unwrap_or(u64::MAX)
                .saturating_mul(APPROXIMATE_POINT_BYTES);
            if !sources.is_empty()
                && source_bytes.saturating_add(slice_bytes) > ACTIVE_COMPACTION_MAX_INPUT_BYTES
            {
                push_flush_request(
                    inner,
                    &catalog,
                    maintenance_now_us,
                    domain,
                    historical,
                    std::mem::take(&mut sources),
                    &mut requests,
                );
                source_bytes = 0;
            }
            sources.push(Box::new(RangedSliceCursor::new(
                slice.batch,
                vec![(slice.start, slice.end)],
            )?));
            source_bytes = source_bytes.saturating_add(slice_bytes);
        }
        if !sources.is_empty() {
            push_flush_request(
                inner,
                &catalog,
                maintenance_now_us,
                domain,
                historical,
                sources,
                &mut requests,
            );
        }
    }
    let mut segments = Vec::new();
    let mut unpublished = UnpublishedArtifacts::default();
    let mut bytes = 0_u64;
    let request_count = requests.len();
    let (reply_tx, reply_rx) = bounded(request_count.max(1));
    for ((shard_id, _policy_id, _historical), shard_requests) in requests {
        let pack_id = shard_requests
            .first()
            .map(|request| request.segment_id)
            .ok_or_else(|| corrupt("empty shard pack request"))?;
        inner
            .flush_pack_tx
            .send(FlushPackCommand::Write {
                pack_id,
                shard_id,
                requests: shard_requests,
                reply: reply_tx.clone(),
            })
            .map_err(|_| Error::WorkerStopped)?;
    }
    drop(reply_tx);
    let mut first_error = None;
    for _ in 0..request_count {
        let result = reply_rx.recv().map_err(|_| Error::WorkerStopped)?;
        match result {
            Ok(mut written) => {
                unpublished.absorb(&mut written.unpublished);
                for result in written.segments {
                    bytes = bytes.saturating_add(result.bytes_written);
                    segments.push(result.segment);
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    // Pack workers have consumed every input cursor. The generation keeps its
    // managed-memory reservation until MVCC/publication release it, but packing
    // scratch must be returned before waiting for the publisher: it may need the
    // same work budget to build a read directory for an earlier compaction.
    drop(work_memory);
    if request_count != 0 {
        sync_segment_pack_group(&inner.root)?;
    }
    let (reply_tx, reply_rx) = bounded(1);
    inner
        .publish_tx
        .send(PublishCommand::Flush {
            generation,
            segments,
            unpublished,
            reply: reply_tx,
        })
        .map_err(|_| Error::WorkerStopped)?;
    reply_rx.recv().map_err(|_| Error::WorkerStopped)??;
    Ok(bytes)
}

fn push_flush_request(
    inner: &Inner,
    catalog: &CatalogSnapshot,
    maintenance_now_us: i64,
    domain: Domain,
    historical: bool,
    sources: Vec<Box<dyn OperationCursor>>,
    requests: &mut BTreeMap<(u16, u32, bool), Vec<SegmentWriteRequest>>,
) {
    let segment_id = inner.next_segment_id.fetch_add(1, Ordering::Relaxed);
    requests
        .entry((domain.shard_id, domain.policy_id, historical))
        .or_default()
        .push(SegmentWriteRequest {
            segment_id,
            level: 0,
            domain,
            profile: compression_profile_for_domain(catalog, domain, maintenance_now_us, 0),
            rewrite_count: 0,
            sources,
        });
}

pub(super) fn flush_pack_loop(inner: Arc<Inner>, receiver: Receiver<FlushPackCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            FlushPackCommand::Write {
                pack_id,
                shard_id,
                requests,
                reply,
            } => {
                let result = prepare_segment_pack(
                    &inner.root,
                    inner.storage_uuid,
                    pack_id,
                    shard_id,
                    requests,
                    SegmentPackResources {
                        block_cache: &inner.block_cache,
                        metadata_cache: &inner.metadata_cache,
                        file_pool: &inner.file_pool,
                        open_file_cache: Some(&inner.open_file_cache),
                        work_memory: Some(&inner.work_memory_governor),
                    },
                )
                .map(|(segments, pack_guard)| {
                    let mut unpublished = UnpublishedArtifacts::default();
                    for result in &segments {
                        unpublished.push(result.segment.path().to_path_buf());
                    }
                    pack_guard.commit();
                    FlushPackOutput {
                        segments,
                        unpublished,
                    }
                });
                let _ = reply.send(result);
            }
            FlushPackCommand::Stop => break,
        }
    }
}
