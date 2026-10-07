//! Compaction coordination and publication of merged files.

mod deletion;
#[cfg(feature = "publication-diagnostics")]
mod diagnostics;
pub(in crate::engine) mod history;
mod jobs;
mod repack;
mod temporal;

pub(super) mod ranges;
pub(super) mod selection;

#[cfg(feature = "pipeline-trace")]
use crate::diagnostics::trace::Stage;

use super::catalog::{
    advance_retention_cutoff, compression_profile_for_domain, sealed_compression_profile,
};
use super::publication::UnpublishedArtifacts;
use super::support::{ActivityGauge, available_disk_bytes, duration_nanos, non_empty_cursor};
use super::{
    CompactionCommand, FlushPackCommand, Inner, MAX_COMPACTION_DOMAINS,
    MAX_COMPACTION_INPUT_SEGMENTS, PublishCommand, TimeRange, degrade,
};
use crate::aggregate::archive::{AggregateArchive, AggregateArchiveWrite};
use crate::catalog::StoragePolicy;
use crate::engine::compaction::ranges::compaction_output_ranges;
use crate::engine::compaction::selection::{
    CompactionClass, CompactionGroup, CompactionSelection, is_sealed_descriptor,
    sealed_before_for_policy, select_active_l1_consolidation, select_compaction_group,
    select_l1_overlap_repair, weighted_compaction_order,
};
use crate::error::{Error, Result, corrupt, invalid};
use crate::memory::work::WorkMemoryClass;
use crate::persistence::layout::Domain;
use crate::segment::{
    CompactionReader, OperationCursor, SegmentReadMetrics, SegmentWriteRequest, VisibleCursor,
    sync_segment_pack_group,
};
use crate::types::{DAY_US, VersionedPoint};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// Owned by the coordinator, constant space, reset when the engine closes.
// A previous history job funds a bounded active burst only under L0 pressure.
// No job is preempted; at most eight active jobs precede one history job.
#[derive(Default)]
struct TurnBudget {
    history: Duration,
}

const MAX_ACTIVE_JOBS_PER_TURN: usize = 8;

#[derive(Clone, Copy)]
enum CompactionStep {
    Idle,
    Published(Option<u64>),
}

pub(super) fn compaction_loop(
    inner: Arc<Inner>,
    active_receiver: Receiver<CompactionCommand>,
    historical_receiver: Receiver<CompactionCommand>,
) {
    let mut active_stopped = false;
    let mut historical_stopped = false;
    let mut repacker = repack::Repacker::default();
    let mut cleaner = deletion::Cleaner::default();
    let mut budget = TurnBudget::default();
    while !active_stopped || !historical_stopped {
        crossbeam_channel::select! {
            recv(active_receiver) -> command => {
                match command {
                    Ok(CompactionCommand::Trigger { force, reply }) => {
                        let outcome = if force {
                            compact_all_until_stable(&inner, &mut repacker, &mut cleaner)
                        } else {
                            compact_turn(&inner, false, CompactionClass::Active, &mut budget, &mut repacker, &mut cleaner)
                        };
                        if outcome.is_err() {
                            repacker.cancel(&inner);
                        }
                        finish_compaction_command(&inner, reply, outcome);
                    }

                    Ok(CompactionCommand::Stop) | Err(_) => active_stopped = true,
                }
            }
            recv(historical_receiver) -> command => {
                match command {
                    Ok(CompactionCommand::Trigger { force, reply }) => {
                        let outcome = compact_turn(
                            &inner,
                            force,
                            CompactionClass::Historical,
                            &mut budget,
                            &mut repacker,
                            &mut cleaner,
                        );
                        if outcome.is_err() {
                            repacker.cancel(&inner);
                        }
                        finish_compaction_command(&inner, reply, outcome);
                    }

                    Ok(CompactionCommand::Stop) | Err(_) => historical_stopped = true,
                }
            }
        }
    }
    repacker.cancel(&inner);
}

fn finish_compaction_command(
    inner: &Inner,
    reply: Option<Sender<Result<u64>>>,
    outcome: Result<u64>,
) {
    if let Some(reply) = reply {
        let _ = reply.send(outcome);
    } else if let Err(Error::InsufficientDiskSpace { .. }) = outcome {
        // Capacity pressure is observable and retryable; writes and reads remain valid.
    } else if let Err(error) = outcome {
        degrade(inner, &error);
    }
}

fn compact_all_until_stable(
    inner: &Inner,
    repacker: &mut repack::Repacker,
    cleaner: &mut deletion::Cleaner,
) -> Result<u64> {
    let mut last_segment_id = 0_u64;
    loop {
        let active = compact_once(inner, true, CompactionClass::Active)?;
        if let CompactionStep::Published(Some(segment_id)) = active {
            last_segment_id = last_segment_id.max(segment_id);
        }
        let historical = compact_once(inner, true, CompactionClass::Historical)?;
        if let CompactionStep::Published(Some(segment_id)) = historical {
            last_segment_id = last_segment_id.max(segment_id);
        }
        let cleaned = cleaner.step(inner)?;
        repacker.step(inner)?;
        if !cleaned
            && matches!(active, CompactionStep::Idle)
            && matches!(historical, CompactionStep::Idle)
        {
            let (reply_tx, reply_rx) = bounded(1);
            inner
                .publish_tx
                .send(PublishCommand::CollectFiles { reply: reply_tx })
                .map_err(|_| Error::WorkerStopped)?;
            reply_rx.recv().map_err(|_| Error::WorkerStopped)??;
            return Ok(last_segment_id);
        }
    }
}

fn active_pressure(inner: &Inner) -> Result<bool> {
    use crate::persistence::catalog::records::{self, DomainEntry};
    let files = inner.state.read()?.files.clone();
    let mut pressure = false;
    files.visit_kind(records::PENDING, |_, summary: DomainEntry| {
        // Match the writer's first soft-pacing condition. Waiting for the
        // stronger slowdown threshold leaves already-throttled writes behind
        // another expensive historical job, even though active work is ready.
        pressure |= summary.counts[0] > inner.options.l0_compaction_trigger as u64;
        Ok(())
    })?;
    Ok(pressure)
}

fn compact_turn(
    inner: &Inner,
    force: bool,
    class: CompactionClass,
    budget: &mut TurnBudget,
    repacker: &mut repack::Repacker,
    cleaner: &mut deletion::Cleaner,
) -> Result<u64> {
    let mut last_segment_id = 0;
    let mut progressed = false;
    let (first, second) = if active_pressure(inner)? {
        (CompactionClass::Active, CompactionClass::Historical)
    } else {
        weighted_compaction_order(inner, class)?
    };
    for current_class in [first, second] {
        let started = Instant::now();
        for job in 0..MAX_ACTIVE_JOBS_PER_TURN {
            let step = compact_once(inner, force && current_class == class, current_class)?;
            if matches!(step, CompactionStep::Idle) {
                break;
            }
            progressed = true;
            if current_class != class {
                inner
                    .metrics
                    .compaction_fair_handoffs
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let CompactionStep::Published(Some(segment_id)) = step {
                last_segment_id = last_segment_id.max(segment_id);
            }
            if current_class == CompactionClass::Historical {
                budget.history = started.elapsed();
                break;
            }
            if job + 1 == MAX_ACTIVE_JOBS_PER_TURN
                || started.elapsed() >= budget.history
                || !active_pressure(inner)?
            {
                break;
            }
        }
    }
    // Keep cleanup advancing even when ordinary work never becomes idle.
    let cleaned = cleaner.step(inner)?;
    repacker.step(inner)?;
    if progressed || cleaned {
        // Coalesced wakes retain no inputs and never displace commands with replies.
        // Return to the receiver after every bounded turn, including this wake.
        super::publication::wake_compaction(&inner.compaction_tx);
        super::publication::wake_compaction(&inner.history_compaction_tx);
    }
    Ok(last_segment_id)
}

fn compact_once(inner: &Inner, force: bool, class: CompactionClass) -> Result<CompactionStep> {
    #[cfg(feature = "publication-diagnostics")]
    let _diagnostic_scope =
        crate::diagnostics::publication::enter(inner.publication_diagnostics.get());
    let Some(jobs::Plan {
        deletions,
        file_view: _file_view,
        memory: _selection_memory,
        archive_memory: _archive_memory,
        groups,
        catalog,
        maintenance_now_us,
        retention_cutoffs,
        aggregate_retention_cutoffs,
        existing_archives,
        expired_archives,
    }) = jobs::select(inner, force, class)?
    else {
        return Ok(CompactionStep::Idle);
    };
    #[cfg(feature = "pipeline-trace")]
    for group in &groups {
        inner.trace_inputs(&group.inputs, Stage::CompactionSelected);
    }
    let started = Instant::now();
    #[cfg(feature = "publication-diagnostics")]
    let mut diagnostic = diagnostics::Job::start(
        inner,
        &groups,
        &retention_cutoffs,
        !expired_archives.is_empty(),
        started,
    );
    let input_bytes = groups
        .iter()
        .flat_map(|group| &group.inputs)
        .map(|segment| segment.descriptor().file_size)
        .sum::<u64>();
    let repair_input_bytes = groups
        .iter()
        .filter(|group| group.repair_overlap)
        .flat_map(|group| &group.inputs)
        .fold(0_u64, |sum, segment| {
            sum.saturating_add(segment.descriptor().file_size)
        });
    let input_fan_in = groups
        .iter()
        .map(|group| group.inputs.len())
        .max()
        .unwrap_or(0) as u64;
    let _current_inputs =
        ActivityGauge::enter(&inner.metrics.compaction_current_input_bytes, input_bytes);
    let active_decode_bytes = groups
        .iter()
        .map(|group| group.inputs.len() as u64)
        .sum::<u64>()
        .saturating_mul(4_096)
        .saturating_mul(std::mem::size_of::<VersionedPoint>() as u64)
        .saturating_mul(2);
    let mut work_memory = inner.work_memory_governor.reserve_background(
        WorkMemoryClass::Compaction,
        active_decode_bytes.min(input_bytes),
    )?;
    ensure_compaction_space(inner, input_bytes)?;
    let segment_read_metrics = Arc::new(SegmentReadMetrics::new(&inner.work_memory_governor)?);
    let mut reader = CompactionReader::new(
        Arc::clone(&inner.work_memory_governor),
        Arc::clone(&segment_read_metrics),
    );
    let sealed_work = class == CompactionClass::Historical
        || groups.iter().all(|group| {
            group.sealed_before_us.is_some_and(|before| {
                group
                    .inputs
                    .iter()
                    .all(|segment| is_sealed_descriptor(segment.descriptor(), before))
            })
        });
    let window_us = inner.options.history_window.as_micros() as i64;
    let mut packed_requests = BTreeMap::<(u16, u32, Option<i64>), Vec<SegmentWriteRequest>>::new();
    let mut inputs = Vec::new();
    let mut new_archives = Vec::new();
    let mut unpublished = UnpublishedArtifacts::default();
    let mut compacted_domains = 0_u64;
    for group in groups {
        compacted_domains = compacted_domains.saturating_add(1);
        let domain = group.domain;
        let retention_cutoff = retention_cutoffs.get(&domain.policy_id).copied();
        if class == CompactionClass::Active
            && let Some(policy) = catalog.policy_by_id(domain.policy_id)
            && let Some(raw_cutoff) = retention_cutoff
            && let Some(aggregate_cutoff) =
                aggregate_retention_cutoffs.get(&domain.policy_id).copied()
        {
            #[cfg(feature = "publication-diagnostics")]
            let archive_started = diagnostic.as_ref().map(|_| Instant::now());
            let archives = build_retention_archives(
                inner,
                &group,
                policy,
                raw_cutoff,
                aggregate_cutoff,
                &existing_archives,
                &deletions,
                &mut reader,
                &mut unpublished,
            )?;
            #[cfg(feature = "publication-diagnostics")]
            if let (Some(diagnostic), Some(started)) = (&mut diagnostic, archive_started) {
                diagnostic.sample.archive_ns = diagnostic
                    .sample
                    .archive_ns
                    .saturating_add(duration_nanos(started.elapsed()));
            }
            new_archives.extend(archives);
        }
        let output_ranges =
            compaction_output_ranges(&group, inner.options.history_segment_target_bytes)?
                .into_iter()
                .filter_map(|mut range| {
                    range.from_us =
                        retention_cutoff.map_or(range.from_us, |cutoff| range.from_us.max(cutoff));
                    (range.from_us < range.to_us).then_some(range)
                })
                .collect();
        let output_ranges = temporal::split(
            &group,
            output_ranges,
            window_us,
            &mut reader,
            &mut work_memory,
        )?;
        let mut opened_sources = vec![false; group.inputs.len()];
        for range in output_ranges {
            let from_us = range.from_us;
            let mut sources = Vec::<Box<dyn OperationCursor>>::with_capacity(group.inputs.len());
            for (index, segment) in group.inputs.iter().enumerate() {
                let segment_range = TimeRange::for_segment(segment)?;
                if !segment_range.overlaps(range) {
                    continue;
                }
                // Requests retain the first decoded block until pack workers
                // consume them. Splitting one input into many windows must not
                // multiply these buffers outside the background work budget.
                let cursor_bytes = 4096 * std::mem::size_of::<VersionedPoint>() as u64 * 2;
                if std::mem::replace(&mut opened_sources[index], true)
                    && !work_memory.try_grow(cursor_bytes)?
                {
                    return Err(Error::ResourceLimit(
                        "compaction window cursors exceed work memory".into(),
                    ));
                }
                if let Some(source) = reader.cursor(segment, from_us, range.to_us)?
                    && let Some(source) = non_empty_cursor(crate::segment::filter_deletions(
                        source,
                        Arc::clone(&deletions),
                    ))?
                {
                    sources.push(source);
                }
            }
            if !sources.is_empty() {
                let segment_id = inner.next_segment_id.fetch_add(1, Ordering::Relaxed);
                let sealed = group
                    .sealed_before_us
                    .is_some_and(|before| range.to_us <= before);
                let profile = if sealed {
                    sealed_compression_profile(&catalog, domain.policy_id)
                } else {
                    compression_profile_for_domain(&catalog, domain, maintenance_now_us, 1)
                };
                packed_requests
                    .entry((
                        domain.shard_id,
                        domain.policy_id,
                        sealed.then(|| from_us.div_euclid(window_us)),
                    ))
                    .or_default()
                    .push(SegmentWriteRequest {
                        #[cfg(feature = "pipeline-trace")]
                        trace: inner.trace_region(
                            domain,
                            from_us,
                            range.to_us,
                            None,
                            &group.inputs,
                            if sealed { 2 } else { 1 },
                        ),
                        segment_id,
                        level: if sealed { 2 } else { 1 },
                        domain,
                        profile,
                        rewrite_count: group
                            .inputs
                            .iter()
                            .map(|segment| segment.descriptor().rewrite_count)
                            .max()
                            .unwrap_or(0)
                            .saturating_add(1),
                        sources,
                    });
            }
        }
        inputs.extend(group.inputs);
    }
    #[cfg(feature = "pipeline-trace")]
    for requests in packed_requests.values() {
        for request in requests {
            request
                .trace
                .mark(Stage::FlushPackQueued, Stage::CompactionPackQueued);
        }
    }
    let expected_output_count = packed_requests.values().map(Vec::len).sum::<usize>();
    #[cfg(feature = "publication-diagnostics")]
    let pack_started = diagnostic.as_ref().map(|_| Instant::now());
    let request_count = packed_requests.len();
    let (pack_reply_tx, pack_reply_rx) = bounded(request_count.max(1));
    for ((shard_id, _policy_id, _sealed), shard_requests) in packed_requests {
        let pack_id = shard_requests
            .first()
            .map(|request| request.segment_id)
            .ok_or_else(|| corrupt("empty compaction shard request"))?;
        inner
            .flush_pack_tx
            .send(FlushPackCommand::Write {
                pack_id,
                shard_id,
                requests: shard_requests,
                reply: pack_reply_tx.clone(),
            })
            .map_err(|_| Error::WorkerStopped)?;
    }
    drop(pack_reply_tx);
    let mut outputs = Vec::with_capacity(expected_output_count);
    let mut output_bytes = new_archives
        .iter()
        .map(|archive| archive.descriptor().file_size)
        .sum::<u64>();
    #[cfg(feature = "publication-diagnostics")]
    if let Some(diagnostic) = &mut diagnostic {
        diagnostic.sample.archive_written_bytes = output_bytes;
    }
    let mut last_segment_id = new_archives
        .iter()
        .map(|archive| archive.descriptor().id)
        .max();
    let mut first_error = None;
    for _ in 0..request_count {
        match pack_reply_rx.recv().map_err(|_| Error::WorkerStopped)? {
            Ok(mut written) => {
                unpublished.absorb(&mut written.unpublished);
                for result in written.segments {
                    let segment_id = result.segment.descriptor().id;
                    last_segment_id = Some(
                        last_segment_id.map_or(segment_id, |current: u64| current.max(segment_id)),
                    );
                    output_bytes = output_bytes.saturating_add(result.bytes_written);
                    outputs.push(result.segment);
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
    #[cfg(feature = "publication-diagnostics")]
    if let (Some(diagnostic), Some(started)) = (&mut diagnostic, pack_started) {
        diagnostic.sample.pack_wait_ns = duration_nanos(started.elapsed());
    }
    // All decoded compaction inputs have been consumed by the pack workers. Release
    // their reservation before the publisher budgets the read-directory build.
    drop(work_memory);
    drop(reader);
    if request_count != 0 {
        sync_segment_pack_group(&inner.root)?;
    }
    let segment_read_metrics = segment_read_metrics.snapshot()?;
    let (expired_segment_count, expired_segment_bytes) =
        inputs
            .iter()
            .fold((0_u64, 0_u64), |(count, bytes), segment| {
                if retention_cutoffs
                    .get(&segment.descriptor().policy_id)
                    .is_some_and(|cutoff| segment.descriptor().max_timestamp_us < *cutoff)
                {
                    (
                        count.saturating_add(1),
                        bytes.saturating_add(segment.descriptor().file_size),
                    )
                } else {
                    (count, bytes)
                }
            });
    #[cfg(feature = "pipeline-trace")]
    inner.trace_outputs(&outputs, Some(Stage::CompactionPublishQueued));
    let (reply_tx, reply_rx) = bounded(1);
    #[cfg(feature = "publication-diagnostics")]
    let publication_started = diagnostic.as_ref().map(|_| Instant::now());
    inner
        .publish_tx
        .send(PublishCommand::Compaction {
            #[cfg(feature = "publication-diagnostics")]
            queued: crate::diagnostics::publication::queued(inner.publication_diagnostics.get()),
            inputs,
            outputs,
            new_archives,
            expired_archives,
            unpublished,
            reply: reply_tx,
        })
        .map_err(|_| Error::WorkerStopped)?;
    reply_rx.recv().map_err(|_| Error::WorkerStopped)??;
    #[cfg(feature = "publication-diagnostics")]
    if let (Some(diagnostic), Some(started)) = (&mut diagnostic, publication_started) {
        diagnostic.sample.publication_wait_ns = duration_nanos(started.elapsed());
    }
    inner
        .metrics
        .manifest_expired_segments
        .fetch_add(expired_segment_count, Ordering::Relaxed);
    inner
        .metrics
        .manifest_expired_bytes
        .fetch_add(expired_segment_bytes, Ordering::Relaxed);
    if repair_input_bytes > 0 {
        inner
            .metrics
            .l1_overlap_repair_jobs
            .fetch_add(1, Ordering::Relaxed);
        inner
            .metrics
            .l1_overlap_repair_input_bytes
            .fetch_add(repair_input_bytes, Ordering::Relaxed);
        inner
            .metrics
            .l1_overlap_repair_max_input_bytes
            .fetch_max(repair_input_bytes, Ordering::Relaxed);
    }
    inner.metrics.compactions.fetch_add(1, Ordering::Relaxed);
    inner
        .metrics
        .compacted_domains
        .fetch_add(compacted_domains, Ordering::Relaxed);
    inner
        .metrics
        .compaction_nanos
        .fetch_add(duration_nanos(started.elapsed()), Ordering::Relaxed);
    inner
        .metrics
        .compaction_bytes_read
        .fetch_add(input_bytes, Ordering::Relaxed);
    inner
        .metrics
        .compaction_bytes_written
        .fetch_add(output_bytes, Ordering::Relaxed);
    inner
        .metrics
        .compaction_physical_bytes_written
        .fetch_add(output_bytes, Ordering::Relaxed);
    inner
        .metrics
        .compaction_block_visits
        .fetch_add(segment_read_metrics.block_visits, Ordering::Relaxed);
    inner
        .metrics
        .compaction_unique_blocks
        .fetch_add(segment_read_metrics.unique_blocks, Ordering::Relaxed);
    inner
        .metrics
        .compaction_unique_block_bytes
        .fetch_add(segment_read_metrics.unique_block_bytes, Ordering::Relaxed);
    inner
        .metrics
        .compaction_physical_blocks_read
        .fetch_add(segment_read_metrics.physical_blocks_read, Ordering::Relaxed);
    inner
        .metrics
        .compaction_physical_bytes_read
        .fetch_add(segment_read_metrics.physical_bytes_read, Ordering::Relaxed);
    inner
        .metrics
        .compaction_decoded_input_bytes
        .fetch_add(segment_read_metrics.decoded_input_bytes, Ordering::Relaxed);
    inner
        .metrics
        .compaction_decoded_points
        .fetch_add(segment_read_metrics.decoded_points, Ordering::Relaxed);
    inner
        .metrics
        .compaction_decode_nanos
        .fetch_add(segment_read_metrics.decode_nanos, Ordering::Relaxed);
    if sealed_work {
        inner
            .metrics
            .sealed_compaction_jobs
            .fetch_add(1, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_max_input_bytes
            .fetch_max(input_bytes, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_max_output_bytes
            .fetch_max(output_bytes, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_max_fan_in
            .fetch_max(input_fan_in, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_physical_bytes_read
            .fetch_add(segment_read_metrics.physical_bytes_read, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_physical_bytes_written
            .fetch_add(output_bytes, Ordering::Relaxed);
        inner
            .metrics
            .sealed_compaction_rewrite_passes
            .fetch_add(compacted_domains, Ordering::Relaxed);
    } else {
        inner
            .metrics
            .active_compaction_jobs
            .fetch_add(1, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_max_input_bytes
            .fetch_max(input_bytes, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_max_output_bytes
            .fetch_max(output_bytes, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_max_fan_in
            .fetch_max(input_fan_in, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_physical_bytes_read
            .fetch_add(segment_read_metrics.physical_bytes_read, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_physical_bytes_written
            .fetch_add(output_bytes, Ordering::Relaxed);
        inner
            .metrics
            .active_compaction_rewrite_passes
            .fetch_add(compacted_domains, Ordering::Relaxed);
    }
    #[cfg(feature = "publication-diagnostics")]
    if let Some(diagnostic) = &mut diagnostic {
        diagnostic.sample.physical_read_bytes = segment_read_metrics.physical_bytes_read;
        diagnostic.sample.written_bytes = output_bytes;
        diagnostic.sample.decoded_points = segment_read_metrics.decoded_points;
        diagnostic.completed = true;
    }
    Ok(CompactionStep::Published(last_segment_id))
}

#[allow(clippy::too_many_arguments)]
fn build_retention_archives(
    inner: &Inner,
    group: &CompactionGroup,
    policy: &StoragePolicy,
    raw_cutoff: i64,
    aggregate_cutoff: i64,
    existing_archives: &[Arc<AggregateArchive>],
    deletions: &Arc<crate::deletion::Deletions>,
    reader: &mut CompactionReader,
    unpublished: &mut UnpublishedArtifacts,
) -> Result<Vec<Arc<AggregateArchive>>> {
    if policy.retained_aggregate_resolutions.is_empty() {
        return Ok(Vec::new());
    }
    let domain = group.domain;
    let covered_to = existing_archives
        .iter()
        .filter(|archive| {
            let descriptor = archive.descriptor();
            descriptor.shard_id == domain.shard_id
                && descriptor.policy_id == domain.policy_id
                && descriptor.window_start_us == domain.window_start_us
                && descriptor.window_end_us == domain.window_end_us
        })
        .map(|archive| archive.descriptor().coverage_to_us)
        .max()
        .unwrap_or(domain.window_start_us);
    let coverage_from_us = domain.window_start_us.max(aggregate_cutoff).max(covered_to);
    let coverage_to_us = domain.window_end_us.min(raw_cutoff);
    if coverage_from_us >= coverage_to_us {
        return Ok(Vec::new());
    }
    if coverage_from_us.rem_euclid(DAY_US) != 0 || coverage_to_us.rem_euclid(DAY_US) != 0 {
        return Err(corrupt(
            "aggregate retention coverage is not UTC-day aligned",
        ));
    }
    let mut sources = Vec::<Box<dyn OperationCursor>>::with_capacity(group.inputs.len());
    for segment in &group.inputs {
        if let Some(source) = reader.cursor(segment, coverage_from_us, coverage_to_us)?
            && let Some(source) = non_empty_cursor(crate::segment::filter_deletions(
                source,
                Arc::clone(deletions),
            ))?
        {
            sources.push(source);
        }
    }
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let coverage_domain = Domain {
        shard_id: domain.shard_id,
        policy_id: domain.policy_id,
        window_start_us: coverage_from_us,
        window_end_us: coverage_to_us,
    };
    let mut builder = crate::aggregate::Builder::with_work_memory(
        coverage_domain,
        policy.retained_aggregate_resolutions.clone(),
        Arc::clone(&inner.work_memory_governor),
    );
    let mut visible = VisibleCursor::new(sources, u64::MAX)?;
    while let Some(point) = visible.next_visible()? {
        builder.add(point)?;
    }
    let maximum_bytes = usize::try_from(inner.options.aggregate_archive_chunk_bytes)
        .map_err(|_| invalid("aggregate archive chunk size exceeds usize"))?;
    let mut first_id = None;
    let mut archives = Vec::new();
    let result = builder.finish_chunks(maximum_bytes, |chunk_index, chunk_count, chunk| {
        let chunk_count_u32 = u32::try_from(chunk_count)
            .map_err(|_| invalid("aggregate archive has too many chunks"))?;
        let first_id = *first_id.get_or_insert_with(|| {
            inner
                .next_segment_id
                .fetch_add(chunk_count as u64, Ordering::Relaxed)
        });
        let archive = AggregateArchive::write(
            &inner.root,
            inner.storage_uuid,
            AggregateArchiveWrite {
                deletion_epoch: deletions.epoch,
                id: first_id.saturating_add(chunk_index as u64),
                domain,
                coverage_from_us,
                coverage_to_us,
                resolutions: policy.retained_aggregate_resolutions.clone(),
                section: chunk.bytes,
                minimum_series: chunk.minimum_series,
                maximum_series: chunk.maximum_series,
                chunk_index: chunk_index as u32,
                chunk_count: chunk_count_u32,
            },
            inner.file_pool.clone(),
        )?;
        unpublished.push(archive.path().to_path_buf());
        archives.push(archive);
        Ok(())
    });
    result?;
    Ok(archives)
}

fn ensure_compaction_space(inner: &Inner, input_bytes: u64) -> Result<()> {
    let estimated_output = (u128::from(input_bytes)
        * u128::from(inner.options.compaction_output_percent))
    .div_ceil(100)
    .min(u128::from(u64::MAX)) as u64;
    let required = inner
        .options
        .minimum_free_disk_bytes
        .saturating_add(estimated_output);
    inner
        .metrics
        .last_compaction_required_bytes
        .store(required, Ordering::Relaxed);
    let available = available_disk_bytes(&inner.root)?;
    if available < required {
        inner
            .metrics
            .compaction_space_rejections
            .fetch_add(1, Ordering::Relaxed);
        return Err(Error::InsufficientDiskSpace {
            available,
            required,
        });
    }
    Ok(())
}
