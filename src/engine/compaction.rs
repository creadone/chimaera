//! Координатор слияния, выполнение выбранных групп и архивирование retention.

use super::catalog::{
    advance_retention_cutoff, compression_profile_for_domain, sealed_compression_profile,
};
use super::compaction_ranges::compaction_output_ranges;
use super::compaction_selection::{
    CompactionClass, CompactionGroup, CompactionSelection, is_sealed_descriptor,
    sealed_before_for_policy, select_active_l1_consolidation, select_compaction_group,
    select_l1_overlap_repair, weighted_compaction_order,
};
use super::publication::UnpublishedArtifacts;
use super::support::{
    ActivityGauge, available_disk_bytes, duration_nanos, non_empty_cursor, write,
};
use super::{
    CompactionCommand, FlushPackCommand, Inner, MAX_COMPACTION_DOMAINS,
    MAX_COMPACTION_INPUT_SEGMENTS, PublishCommand, TimeRange, degrade,
};
use crate::admission::WorkMemoryClass;
use crate::aggregate_archive::{AggregateArchive, AggregateArchiveWrite};
use crate::catalog::StoragePolicy;
use crate::error::{Error, Result, corrupt, invalid};
use crate::layout::Domain;
use crate::segment::{
    OperationCursor, Segment, SegmentReadMetrics, SegmentWriteRequest, VisibleCursor,
    sync_segment_pack_group,
};
use crate::types::{DAY_US, VersionedPoint};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

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
    while !active_stopped || !historical_stopped {
        crossbeam_channel::select! {
            recv(active_receiver) -> command => {
                match command {
                    Ok(CompactionCommand::Trigger { force, reply }) => {
                        let outcome = if force {
                            compact_all_until_stable(&inner)
                        } else {
                            compact_class_fairly(&inner, false, CompactionClass::Active)
                        };
                        finish_compaction_command(&inner, reply, outcome);
                    }
                    Ok(CompactionCommand::Stop) | Err(_) => active_stopped = true,
                }
            }
            recv(historical_receiver) -> command => {
                match command {
                    Ok(CompactionCommand::Trigger { force, reply }) => {
                        let outcome = compact_class_fairly(
                            &inner,
                            force,
                            CompactionClass::Historical,
                        );
                        finish_compaction_command(&inner, reply, outcome);
                    }
                    Ok(CompactionCommand::Stop) | Err(_) => historical_stopped = true,
                }
            }
        }
    }
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

fn compact_all_until_stable(inner: &Inner) -> Result<u64> {
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
        if matches!(active, CompactionStep::Idle) && matches!(historical, CompactionStep::Idle) {
            let (reply_tx, reply_rx) = bounded(1);
            inner
                .publish_tx
                .send(PublishCommand::RefreshReadDirectory { reply: reply_tx })
                .map_err(|_| Error::WorkerStopped)?;
            if let Some(generation_id) = reply_rx.recv().map_err(|_| Error::WorkerStopped)?? {
                last_segment_id = last_segment_id.max(generation_id);
            }
            return Ok(last_segment_id);
        }
    }
}

fn compact_class_fairly(inner: &Inner, force: bool, class: CompactionClass) -> Result<u64> {
    let mut last_segment_id = 0;
    loop {
        let (first, second) = weighted_compaction_order(inner, class)?;
        let mut requested = CompactionStep::Idle;
        for current_class in [first, second] {
            let step = compact_once(inner, force && current_class == class, current_class)?;
            if current_class == class {
                requested = step;
            } else if let CompactionStep::Published(segment_id) = step {
                inner
                    .metrics
                    .compaction_fair_handoffs
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(segment_id) = segment_id {
                    last_segment_id = last_segment_id.max(segment_id);
                }
            }
            if let CompactionStep::Published(Some(segment_id)) = step {
                last_segment_id = last_segment_id.max(segment_id);
            }
        }
        if matches!(requested, CompactionStep::Idle) {
            return Ok(last_segment_id);
        }
    }
}

fn compact_once(inner: &Inner, force: bool, class: CompactionClass) -> Result<CompactionStep> {
    // Reserve every fourth active pass for eligible L1 below the L0 slowdown
    // threshold. Continuous L0 arrivals must not starve consolidation forever.
    let l1_turn = class == CompactionClass::Active
        && inner.metrics.active_compaction_jobs.load(Ordering::Relaxed) % 4 == 3;
    let observed_now_us = inner.options.clock.now_us();
    let (
        groups,
        catalog,
        maintenance_now_us,
        retention_cutoffs,
        aggregate_retention_cutoffs,
        existing_archives,
        expired_archives,
    ) = {
        let mut state = write(&inner.state)?;
        state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
        let policies = state.catalog.policies().to_vec();
        let maintenance_now_us = state.maintenance_now_us;
        for policy in &policies {
            advance_retention_cutoff(&mut state, policy, maintenance_now_us);
        }
        let mut by_domain = BTreeMap::<Domain, Vec<Arc<Segment>>>::new();
        for segment in &state.segments {
            by_domain
                .entry(segment.descriptor().domain())
                .or_default()
                .push(Arc::clone(segment));
        }
        let mut eligible_by_shard = BTreeMap::<u16, Vec<(CompactionGroup, usize)>>::new();
        for (domain, segments) in &by_domain {
            let sealed_before_us = sealed_before_for_policy(
                &inner.options,
                &state.catalog,
                &state.event_watermarks,
                domain.policy_id,
            );
            let l0_count = segments
                .iter()
                .filter(|segment| {
                    let descriptor = segment.descriptor();
                    if descriptor.level != 0 {
                        return false;
                    }
                    let historical = sealed_before_us
                        .is_some_and(|before| is_sealed_descriptor(descriptor, before));
                    match class {
                        CompactionClass::Active => !historical,
                        CompactionClass::Historical => historical,
                    }
                })
                .count();
            let retention_needed = class == CompactionClass::Active
                && state
                    .retention_cutoffs
                    .get(&domain.policy_id)
                    .is_some_and(|cutoff| {
                        segments
                            .iter()
                            .any(|segment| segment.descriptor().min_timestamp_us < *cutoff)
                    });
            let target_profile = compression_profile_for_domain(
                &state.catalog,
                *domain,
                state.maintenance_now_us,
                1,
            );
            let profile_rewrite_needed = class == CompactionClass::Active
                && l0_count == 0
                && segments.iter().any(|segment| {
                    segment.descriptor().level == 1 && !segment.uses_profile(target_profile)
                });
            let seal_needed = class == CompactionClass::Historical
                && sealed_before_us.is_some_and(|before| {
                    segments.iter().any(|segment| {
                        segment.descriptor().level == 1
                            && is_sealed_descriptor(segment.descriptor(), before)
                    })
                });
            let trigger = match class {
                CompactionClass::Active => inner.options.l0_compaction_trigger,
                CompactionClass::Historical => inner.options.history_rewrite_trigger,
            };
            let compact_l0 = l0_count > 0 && (force || l0_count >= trigger);
            let archive_retention = state
                .catalog
                .policy_by_id(domain.policy_id)
                .is_some_and(|policy| policy.aggregate_retention_us.is_some());
            let group = if retention_needed && archive_retention {
                Some(CompactionGroup {
                    domain: *domain,
                    inputs: segments.clone(),
                    focus_ranges: Vec::new(),
                    sealed_before_us,
                    coalesce_disjoint: false,
                    repair_overlap: false,
                })
            } else if class == CompactionClass::Active
                && !retention_needed
                && !profile_rewrite_needed
                && let Some(repair) = select_l1_overlap_repair(*domain, segments, sealed_before_us)
            {
                Some(repair)
            } else if l1_turn
                && !retention_needed
                && !profile_rewrite_needed
                && l0_count < inner.options.l0_slowdown_trigger
                && let Some(consolidation) =
                    select_active_l1_consolidation(*domain, segments, sealed_before_us, false)?
            {
                Some(consolidation)
            } else if retention_needed || profile_rewrite_needed || seal_needed || compact_l0 {
                select_compaction_group(
                    *domain,
                    segments,
                    CompactionSelection {
                        compact_l0,
                        class,
                        sealed_before_us,
                        retention_cutoff: state.retention_cutoffs.get(&domain.policy_id).copied(),
                        rewrite_profile: profile_rewrite_needed,
                        target_profile,
                        include_overlapping_levels: retention_needed
                            || profile_rewrite_needed
                            || seal_needed
                            || class == CompactionClass::Historical,
                    },
                )?
            } else if class == CompactionClass::Active {
                select_active_l1_consolidation(*domain, segments, sealed_before_us, false)?
            } else {
                None
            };
            if let Some(group) = group {
                eligible_by_shard
                    .entry(domain.shard_id)
                    .or_default()
                    .push((group, l0_count));
            }
        }
        let mut repair_selected = false;
        for domains in eligible_by_shard.values_mut() {
            domains.retain(|(group, _)| {
                if !group.repair_overlap {
                    return true;
                }
                if repair_selected {
                    return false;
                }
                repair_selected = true;
                true
            });
        }
        let mut eligible_shards = eligible_by_shard
            .into_iter()
            .filter(|(_, domains)| !domains.is_empty())
            .collect::<Vec<_>>();
        eligible_shards.sort_by_key(|(shard, domains)| {
            (
                std::cmp::Reverse(domains.iter().any(|(group, _)| group.repair_overlap)),
                std::cmp::Reverse(
                    l1_turn
                        && domains.iter().any(|(group, _)| {
                            group
                                .inputs
                                .iter()
                                .all(|segment| segment.descriptor().level == 1)
                        }),
                ),
                std::cmp::Reverse(domains.iter().map(|(_, count)| count).sum::<usize>()),
                *shard,
            )
        });
        eligible_shards.truncate(inner.options.flush_worker_count);
        let expired_archives = state
            .aggregate_archives
            .iter()
            .filter(|archive| {
                state
                    .aggregate_retention_cutoffs
                    .get(&archive.descriptor().policy_id)
                    .is_some_and(|cutoff| archive.descriptor().coverage_to_us <= *cutoff)
            })
            .cloned()
            .collect::<Vec<_>>();
        if eligible_shards.is_empty() && expired_archives.is_empty() {
            return Ok(CompactionStep::Idle);
        }
        let mut groups = Vec::new();
        let mut input_count = 0_usize;
        for (_shard, mut selected) in eligible_shards {
            selected.sort_by_key(|(group, count)| (std::cmp::Reverse(*count), group.domain));
            selected.truncate(MAX_COMPACTION_DOMAINS);
            for (group, _) in selected {
                if !groups.is_empty()
                    && input_count.saturating_add(group.inputs.len())
                        > MAX_COMPACTION_INPUT_SEGMENTS
                {
                    continue;
                }
                input_count = input_count.saturating_add(group.inputs.len());
                groups.push(group);
            }
        }
        (
            groups,
            state.catalog.clone(),
            state.maintenance_now_us,
            state.retention_cutoffs.clone(),
            state.aggregate_retention_cutoffs.clone(),
            state.aggregate_archives.clone(),
            expired_archives,
        )
    };
    let started = Instant::now();
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
    let work_memory = inner.work_memory_governor.reserve_background(
        WorkMemoryClass::Compaction,
        active_decode_bytes.min(input_bytes),
    )?;
    ensure_compaction_space(inner, input_bytes)?;
    let segment_read_metrics = Arc::new(SegmentReadMetrics::default());
    let sealed_work = class == CompactionClass::Historical
        || groups.iter().all(|group| {
            group.sealed_before_us.is_some_and(|before| {
                group
                    .inputs
                    .iter()
                    .all(|segment| is_sealed_descriptor(segment.descriptor(), before))
            })
        });
    let mut packed_requests = BTreeMap::<(u16, u32, bool), Vec<SegmentWriteRequest>>::new();
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
            let archives = build_retention_archives(
                inner,
                &group,
                policy,
                raw_cutoff,
                aggregate_cutoff,
                &existing_archives,
                Arc::clone(&segment_read_metrics),
                &mut unpublished,
            )?;
            new_archives.extend(archives);
        }
        for range in compaction_output_ranges(&group, inner.options.history_segment_target_bytes)? {
            let from_us =
                retention_cutoff.map_or(range.from_us, |cutoff| range.from_us.max(cutoff));
            if from_us >= range.to_us {
                continue;
            }
            let mut sources = Vec::<Box<dyn OperationCursor>>::with_capacity(group.inputs.len());
            for segment in &group.inputs {
                let segment_range = TimeRange::for_segment(segment)?;
                if !segment_range.overlaps(range) {
                    continue;
                }
                if let Some(source) = segment.compaction_time_cursor(
                    from_us,
                    range.to_us,
                    Arc::clone(&segment_read_metrics),
                )? && let Some(source) = non_empty_cursor(source)?
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
                    .entry((domain.shard_id, domain.policy_id, sealed))
                    .or_default()
                    .push(SegmentWriteRequest {
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
    let expected_output_count = packed_requests.values().map(Vec::len).sum::<usize>();
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
    // All decoded compaction inputs have been consumed by the pack workers. Release
    // their reservation before the publisher budgets the read-directory build.
    drop(work_memory);
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
    let (reply_tx, reply_rx) = bounded(1);
    inner
        .publish_tx
        .send(PublishCommand::Compaction {
            inputs,
            outputs,
            new_archives,
            expired_archives,
            unpublished,
            reply: reply_tx,
        })
        .map_err(|_| Error::WorkerStopped)?;
    reply_rx.recv().map_err(|_| Error::WorkerStopped)??;
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
    read_metrics: Arc<SegmentReadMetrics>,
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
        if let Some(source) = segment.compaction_time_cursor(
            coverage_from_us,
            coverage_to_us,
            Arc::clone(&read_metrics),
        )? && let Some(source) = non_empty_cursor(source)?
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
