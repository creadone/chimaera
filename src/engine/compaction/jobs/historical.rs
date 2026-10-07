//! Select descriptors before opening history files. No state survives a call.
use super::*;
use crate::segment::SegmentDescriptor;

pub(super) fn choose(
    inner: &Inner,
    state: &Snapshot,
    domain: Domain,
    summary: &DomainEntry,
    force: bool,
) -> Result<(Option<CompactionGroup>, usize)> {
    let Some(before) = sealed_before_for_policy(
        &inner.options,
        &state.catalog,
        &state.event_watermarks,
        domain.policy_id,
    ) else {
        return Ok((None, 0));
    };
    if summary.minimum_timestamp >= before {
        return Ok((None, 0));
    }
    let expired = state
        .retention_cutoffs
        .get(&domain.policy_id)
        .is_some_and(|cutoff| summary.minimum_timestamp < *cutoff);
    // The active retention pass builds archives from all levels before dropping
    // raw. Even initial sealing must not race ahead and discard its inputs.
    if expired
        && state
            .catalog
            .policy_by_id(domain.policy_id)
            .is_some_and(|policy| policy.aggregate_retention_us.is_some())
    {
        return Ok((None, 0));
    }
    let (group, l0_count) = {
        let _timer = diagnostics::stage(Stage::HistoricalSealing);
        sealing(inner, state, domain, summary, before, force)?
    };
    if group.is_some() {
        return Ok((group, l0_count));
    }
    // Optional L2 consolidation must not bypass archive creation by retention.
    let trigger = inner.options.history_rewrite_trigger.max(2);
    if expired
        || summary.counts[2] < trigger as u64
        || summary
            .minimum_l2_bytes
            .is_none_or(|size| size >= inner.options.history_segment_target_bytes)
        || (summary.counts[0] == 0
            && summary.counts[1] == 0
            && summary.history_target_bytes == inner.options.history_segment_target_bytes
            && summary.history_trigger == trigger
            && summary.history_window_us == inner.options.history_window.as_micros() as i64
            && !summary.history_merge)
    {
        return Ok((None, l0_count));
    }
    // Keep all levels as overlap barriers, including unsealed L0/L1. The
    // existing geometric selection also coalesces disjoint L2 components.
    let _timer = diagnostics::stage(Stage::HistoricalFallback);
    let mut cursor = crate::engine::files::Cursor::new(&state.files, domain);
    let descriptors = history::select(
        || {
            cursor.next().map(|entry| {
                entry.map(|entry| {
                    diagnostics::add(Counter::HistoricalFallbackDescriptors, 1);
                    entry.descriptor
                })
            })
        },
        before,
        inner.options.history_segment_target_bytes,
        trigger,
        inner.options.history_window.as_micros() as i64,
        &inner.work_memory_governor,
    )?;
    if descriptors.is_empty() {
        return Ok((None, l0_count));
    }
    let mut inputs = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors.iter() {
        record_open(CompactionClass::Historical, descriptor.level);
        inputs.push(state.files.resources.open(descriptor.clone(), None)?);
    }
    Ok((
        Some(CompactionGroup {
            domain,
            inputs,
            focus_ranges: Vec::new(),
            sealed_before_us: Some(before),
            coalesce_disjoint: true,
            repair_overlap: false,
        }),
        l0_count,
    ))
}

fn sealing(
    inner: &Inner,
    state: &Snapshot,
    domain: Domain,
    summary: &DomainEntry,
    before: i64,
    force: bool,
) -> Result<(Option<CompactionGroup>, usize)> {
    let mut memory = inner
        .work_memory_governor
        .reserve_background(WorkMemoryClass::Compaction, 0)?;
    let mut selected = BTreeMap::<u64, SegmentDescriptor>::new();
    let mut levels = [false; 2];
    for (level, needed) in levels.iter_mut().enumerate() {
        if summary.counts[level] > 0 {
            *needed = summary.debt_bounds[level]
                .ok_or_else(|| corrupt("missing compaction level bounds"))?
                .minimum_timestamp
                < before;
        }
    }
    state
        .files
        .domain_levels(domain, summary, levels, |entry| {
            if is_sealed_descriptor(&entry.descriptor, before) {
                overlap::charge(&mut memory)?;
                selected.insert(entry.descriptor.id, entry.descriptor);
            }
            Ok(())
        })?;
    let l0_count = selected.values().filter(|d| d.level == 0).count();
    let compact_l0 = force
        || l0_count >= inner.options.history_rewrite_trigger
        || domain.window_end_us <= before;
    selected.retain(|_, d| d.level == 1 || compact_l0);
    if selected.is_empty() {
        return Ok((None, l0_count));
    }
    overlap::extend(
        state,
        domain,
        CompactionClass::Historical,
        Some(before),
        &mut selected,
        &mut memory,
    )?;
    let mut descriptors = selected.into_values().collect::<Vec<_>>();
    descriptors.sort_unstable_by_key(|d| (d.min_timestamp_us, d.id));
    let focus_ranges = if descriptors.iter().any(|d| d.level > 0) {
        selection::merge_time_ranges(
            descriptors
                .iter()
                .filter(|d| d.level == 0)
                .map(TimeRange::for_descriptor)
                .collect::<Result<_>>()?,
        )
    } else {
        Vec::new()
    };
    let mut inputs = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        record_open(CompactionClass::Historical, descriptor.level);
        inputs.push(state.files.resources.open(descriptor, None)?);
    }
    Ok((
        Some(CompactionGroup {
            domain,
            inputs,
            focus_ranges,
            sealed_before_us: Some(before),
            coalesce_disjoint: false,
            repair_overlap: false,
        }),
        l0_count,
    ))
}
