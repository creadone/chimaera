//! Выбор активных и исторических входов, справедливость и долг L1.

use super::options::OpenOptions;
use super::stats::L1CompactionDebtStats;
use super::support::{duration_micros, read};
use super::{
    ACTIVE_COMPACTION_MAX_INPUT_BYTES, ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS,
    ACTIVE_L1_CONSOLIDATION_TRIGGER, ACTIVE_L1_DELTA_BASE_RATIO, CachedL1Debt, Inner,
    L1_OVERLAP_REPAIR_MAX_INPUT_BYTES, State, TimeRange,
};
use crate::catalog::{CatalogSnapshot, CompressionProfile};
use crate::error::{Result, corrupt};
use crate::layout::Domain;
use crate::segment::Segment;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn duration_micros_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

pub(super) fn sealed_before_for_policy(
    options: &OpenOptions,
    catalog: &CatalogSnapshot,
    event_watermarks: &BTreeMap<u32, i64>,
    policy_id: u32,
) -> Option<i64> {
    let configured_lag = options.history_seal_lag?;
    let policy_lateness = catalog
        .policy_by_id(policy_id)
        .and_then(|policy| policy.max_lateness_us)
        .unwrap_or(0);
    let lag_us = duration_micros_i64(configured_lag).max(policy_lateness);
    let watermark = *event_watermarks.get(&policy_id)?;
    let candidate = watermark.checked_sub(lag_us)?;
    let window_us = duration_micros_i64(options.history_window);
    let aligned = i128::from(candidate)
        .div_euclid(i128::from(window_us))
        .checked_mul(i128::from(window_us))?;
    i64::try_from(aligned).ok()
}

pub(super) fn is_sealed_descriptor(
    descriptor: &crate::segment::SegmentDescriptor,
    before_us: i64,
) -> bool {
    descriptor.max_timestamp_us < before_us
}

pub(super) struct CompactionGroup {
    pub(super) domain: Domain,
    pub(super) inputs: Vec<Arc<Segment>>,
    pub(super) focus_ranges: Vec<TimeRange>,
    pub(super) sealed_before_us: Option<i64>,
    // Bounded active runs must reduce logical fan-out even when successive
    // flushes have gaps in event time. Maintenance groups retain selective ranges.
    pub(super) coalesce_disjoint: bool,
    pub(super) repair_overlap: bool,
}

#[derive(Clone, Copy)]
pub(super) struct CompactionSelection {
    pub(super) compact_l0: bool,
    pub(super) class: CompactionClass,
    pub(super) sealed_before_us: Option<i64>,
    pub(super) retention_cutoff: Option<i64>,
    pub(super) rewrite_profile: bool,
    pub(super) target_profile: CompressionProfile,
    pub(super) include_overlapping_levels: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CompactionClass {
    Active,
    Historical,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct CompactionDebt {
    pub(super) active_bytes: u64,
    pub(super) historical_bytes: u64,
    pub(super) active_oldest_age_us: u64,
    pub(super) historical_oldest_age_us: u64,
}

pub(super) fn compaction_debt_for_state(options: &OpenOptions, state: &State) -> CompactionDebt {
    let mut debt = CompactionDebt::default();
    let mut sealed_before = BTreeMap::<u32, Option<i64>>::new();
    let now = Instant::now();
    for segment in &state.segments {
        let descriptor = segment.descriptor();
        let boundary = *sealed_before
            .entry(descriptor.policy_id)
            .or_insert_with(|| {
                sealed_before_for_policy(
                    options,
                    &state.catalog,
                    &state.event_watermarks,
                    descriptor.policy_id,
                )
            });
        let historical = match descriptor.level {
            0 | 1 => boundary.is_some_and(|before| is_sealed_descriptor(descriptor, before)),
            _ => false,
        };
        let active = descriptor.level == 0
            || (descriptor.level == 1
                && state
                    .retention_cutoffs
                    .get(&descriptor.policy_id)
                    .is_some_and(|cutoff| descriptor.min_timestamp_us < *cutoff));
        if !historical && !active {
            continue;
        }
        let age_us = state
            .segment_published_at
            .get(&descriptor.id)
            .map(|published| duration_micros(now.saturating_duration_since(*published)))
            .unwrap_or(0);
        if historical {
            debt.historical_bytes = debt.historical_bytes.saturating_add(descriptor.file_size);
            debt.historical_oldest_age_us = debt.historical_oldest_age_us.max(age_us);
        } else {
            debt.active_bytes = debt.active_bytes.saturating_add(descriptor.file_size);
            debt.active_oldest_age_us = debt.active_oldest_age_us.max(age_us);
        }
    }
    debt
}

pub(super) fn weighted_compaction_order(
    inner: &Inner,
    fallback: CompactionClass,
) -> Result<(CompactionClass, CompactionClass)> {
    let state = read(&inner.state)?;
    let debt = compaction_debt_for_state(&inner.options, &state);
    drop(state);
    Ok(weighted_compaction_order_for_debt(debt, fallback))
}

pub(super) fn weighted_compaction_order_for_debt(
    debt: CompactionDebt,
    fallback: CompactionClass,
) -> (CompactionClass, CompactionClass) {
    let age_weight_bytes = 1024 * 1024;
    let active_score = debt.active_bytes.saturating_add(
        debt.active_oldest_age_us
            .saturating_div(1_000_000)
            .saturating_mul(age_weight_bytes),
    );
    let historical_score = debt.historical_bytes.saturating_add(
        debt.historical_oldest_age_us
            .saturating_div(1_000_000)
            .saturating_mul(age_weight_bytes),
    );
    let first = match active_score.cmp(&historical_score) {
        std::cmp::Ordering::Greater => CompactionClass::Active,
        std::cmp::Ordering::Less => CompactionClass::Historical,
        std::cmp::Ordering::Equal => fallback,
    };
    let second = match first {
        CompactionClass::Active => CompactionClass::Historical,
        CompactionClass::Historical => CompactionClass::Active,
    };
    (first, second)
}

pub(super) fn merge_time_ranges(mut ranges: Vec<TimeRange>) -> Vec<TimeRange> {
    ranges.sort_by_key(|range| (range.from_us, range.to_us));
    let mut merged = Vec::<TimeRange>::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && range.from_us <= previous.to_us
        {
            previous.to_us = previous.to_us.max(range.to_us);
        } else {
            merged.push(range);
        }
    }
    merged
}

pub(super) fn select_compaction_group(
    domain: Domain,
    segments: &[Arc<Segment>],
    selection: CompactionSelection,
) -> Result<Option<CompactionGroup>> {
    let CompactionSelection {
        compact_l0,
        class,
        sealed_before_us,
        retention_cutoff,
        rewrite_profile,
        target_profile,
        include_overlapping_levels,
    } = selection;
    let mut selected = vec![false; segments.len()];
    let mut covered = Vec::new();
    let mut seed_candidates = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let descriptor = segment.descriptor();
        let historical_l0 = descriptor.level == 0
            && sealed_before_us.is_some_and(|before| is_sealed_descriptor(descriptor, before));
        let class_l0 = match class {
            CompactionClass::Active => descriptor.level == 0 && !historical_l0,
            CompactionClass::Historical => historical_l0,
        };
        let maintenance_seed = class == CompactionClass::Active
            && (retention_cutoff.is_some_and(|cutoff| descriptor.min_timestamp_us < cutoff)
                || (rewrite_profile
                    && descriptor.level == 1
                    && !segment.uses_profile(target_profile)));
        let seal_seed = class == CompactionClass::Historical
            && descriptor.level == 1
            && sealed_before_us.is_some_and(|before| is_sealed_descriptor(descriptor, before));
        let is_seed = (compact_l0 && class_l0) || maintenance_seed || seal_seed;
        if is_seed {
            seed_candidates.push(index);
        }
    }
    seed_candidates.sort_unstable_by_key(|index| segments[*index].descriptor().id);
    let bounded_active_l0 = class == CompactionClass::Active
        && compact_l0
        && !include_overlapping_levels
        && seed_candidates
            .iter()
            .all(|index| segments[*index].descriptor().level == 0);
    let mut selected_seed_bytes = 0_u64;
    let mut selected_seed_count = 0_usize;
    for index in seed_candidates {
        let bytes = segments[index].descriptor().file_size;
        if bounded_active_l0
            && selected_seed_count > 0
            && (selected_seed_count == ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
                || selected_seed_bytes.saturating_add(bytes) > ACTIVE_COMPACTION_MAX_INPUT_BYTES)
        {
            break;
        }
        selected[index] = true;
        selected_seed_bytes = selected_seed_bytes.saturating_add(bytes);
        selected_seed_count = selected_seed_count.saturating_add(1);
        covered.push(TimeRange::for_segment(&segments[index])?);
    }
    if covered.is_empty() {
        return Ok(None);
    }
    covered = merge_time_ranges(covered);
    if include_overlapping_levels {
        loop {
            let mut changed = false;
            for (index, segment) in segments.iter().enumerate() {
                if selected[index] {
                    continue;
                }
                let descriptor = segment.descriptor();
                if descriptor.level == 0 {
                    let historical = sealed_before_us
                        .is_some_and(|before| is_sealed_descriptor(descriptor, before));
                    let belongs_to_class = match class {
                        CompactionClass::Active => !historical,
                        CompactionClass::Historical => historical,
                    };
                    if !belongs_to_class {
                        continue;
                    }
                }
                let range = TimeRange::for_segment(segment)?;
                if covered.iter().any(|covered| covered.overlaps(range)) {
                    selected[index] = true;
                    covered.push(range);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
            covered = merge_time_ranges(covered);
        }
    }
    let inputs = segments
        .iter()
        .zip(&selected)
        .filter(|(_, selected)| **selected)
        .map(|(segment, _)| Arc::clone(segment))
        .collect::<Vec<_>>();
    let focus_ranges = if inputs.iter().any(|segment| segment.descriptor().level > 0) {
        merge_time_ranges(
            inputs
                .iter()
                .filter(|segment| segment.descriptor().level == 0)
                .map(|segment| TimeRange::for_segment(segment))
                .collect::<Result<Vec<_>>>()?,
        )
    } else {
        Vec::new()
    };
    let coalesce_disjoint = bounded_active_l0
        && can_coalesce_without_l1_overlap(
            &covered,
            segments
                .iter()
                .zip(&selected)
                .filter(|(_, selected)| !**selected)
                .map(|(segment, _)| segment.descriptor()),
        );
    Ok(Some(CompactionGroup {
        domain,
        inputs,
        focus_ranges,
        sealed_before_us,
        coalesce_disjoint,
        repair_overlap: false,
    }))
}

pub(super) fn can_coalesce_without_l1_overlap<'a>(
    ranges: &[TimeRange],
    remaining: impl Iterator<Item = &'a crate::segment::SegmentDescriptor>,
) -> bool {
    let (Some(first), Some(last)) = (ranges.first(), ranges.last()) else {
        return false;
    };
    !remaining
        .filter(|part| part.level == 1)
        .any(|part| part.min_timestamp_us < last.to_us && part.max_timestamp_us >= first.from_us)
}

pub(super) fn select_l1_overlap_repair(
    domain: Domain,
    segments: &[Arc<Segment>],
    sealed_before_us: Option<i64>,
) -> Option<CompactionGroup> {
    let active = segments
        .iter()
        .filter(|segment| {
            let part = segment.descriptor();
            part.level == 1
                && !sealed_before_us.is_some_and(|before| is_sealed_descriptor(part, before))
        })
        .collect::<Vec<_>>();
    let descriptors = active
        .iter()
        .map(|segment| segment.descriptor())
        .collect::<Vec<_>>();
    let selected = select_l1_repair_indices(&descriptors)?;
    Some(CompactionGroup {
        domain,
        inputs: selected
            .into_iter()
            .map(|i| Arc::clone(active[i]))
            .collect(),
        focus_ranges: Vec::new(),
        sealed_before_us,
        coalesce_disjoint: false,
        repair_overlap: true,
    })
}

pub(super) fn select_l1_repair_indices(
    active: &[&crate::segment::SegmentDescriptor],
) -> Option<Vec<usize>> {
    l1_components(active).into_iter().find(|component| {
        let bytes = component
            .iter()
            .fold(0_u64, |sum, i| sum.saturating_add(active[*i].file_size));
        // Smaller components use the normal consolidation policy. Never select
        // only a subset of an oversized connected component.
        component.len() >= 2
            && component.len() <= ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            && bytes > ACTIVE_COMPACTION_MAX_INPUT_BYTES
            && bytes <= L1_OVERLAP_REPAIR_MAX_INPUT_BYTES
    })
}

pub(super) fn select_active_l1_consolidation(
    domain: Domain,
    segments: &[Arc<Segment>],
    sealed_before_us: Option<i64>,
    force: bool,
) -> Result<Option<CompactionGroup>> {
    let active = segments
        .iter()
        .filter(|segment| {
            let descriptor = segment.descriptor();
            descriptor.level == 1
                && !sealed_before_us.is_some_and(|before| is_sealed_descriptor(descriptor, before))
        })
        .cloned()
        .collect::<Vec<_>>();
    if active.len() < 2 {
        return Ok(None);
    }
    let descriptors = active
        .iter()
        .map(|segment| segment.descriptor())
        .collect::<Vec<_>>();
    let triggered = l1_consolidation_triggered(&descriptors, force);
    if !triggered {
        return Ok(None);
    }

    let inputs = select_l1_input_indices(&descriptors)
        .into_iter()
        .map(|index| Arc::clone(&active[index]))
        .collect::<Vec<_>>();
    if inputs.len() < 2 {
        return Ok(None);
    }
    Ok(Some(CompactionGroup {
        domain,
        inputs,
        focus_ranges: Vec::new(),
        sealed_before_us,
        coalesce_disjoint: true,
        repair_overlap: false,
    }))
}

fn l1_consolidation_triggered(active: &[&crate::segment::SegmentDescriptor], force: bool) -> bool {
    if active.len() < 2 {
        return false;
    }
    let base = active.iter().map(|part| part.file_size).max().unwrap_or(0);
    let delta = active
        .iter()
        .fold(0_u64, |sum, part| sum.saturating_add(part.file_size))
        .saturating_sub(base);
    force
        || active.len() >= ACTIVE_L1_CONSOLIDATION_TRIGGER
        || delta.saturating_mul(ACTIVE_L1_DELTA_BASE_RATIO) >= base
}

pub(super) fn l1_domain_debt(
    domain: Domain,
    active: &[&crate::segment::SegmentDescriptor],
    previous: Option<&CachedL1Debt>,
    now: Instant,
) -> CachedL1Debt {
    let mut stats = L1CompactionDebtStats {
        shard_id: domain.shard_id,
        policy_id: domain.policy_id,
        window_start_us: domain.window_start_us,
        parts: active.len(),
        ..L1CompactionDebtStats::default()
    };
    let triggered = l1_consolidation_triggered(active, false);
    stats.below_consolidation_trigger = !triggered;
    let components = l1_components(active);
    let sizes = components
        .iter()
        .map(|component| {
            component
                .iter()
                .fold(0_u64, |sum, i| sum.saturating_add(active[*i].file_size))
        })
        .collect::<Vec<_>>();
    let mut eligible = vec![false; components.len()];
    for (index, component) in components.iter().enumerate() {
        let bytes = sizes[index];
        if component.len() >= 2 {
            stats.overlap_components += 1;
            stats.overlap_input_parts += component.len();
            stats.overlap_input_bytes = stats.overlap_input_bytes.saturating_add(bytes);
            stats.overlap_components_over_byte_limit +=
                usize::from(bytes > L1_OVERLAP_REPAIR_MAX_INPUT_BYTES);
            stats.overlap_components_over_fan_in_limit +=
                usize::from(component.len() > ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS);
            eligible[index] = component.len() <= ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
                && bytes <= L1_OVERLAP_REPAIR_MAX_INPUT_BYTES
                && (bytes > ACTIVE_COMPACTION_MAX_INPUT_BYTES || triggered);
        }
        if triggered
            && index > 0
            && component.len() + components[index - 1].len() <= ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            && bytes.saturating_add(sizes[index - 1]) <= ACTIVE_COMPACTION_MAX_INPUT_BYTES
        {
            eligible[index] = true;
            eligible[index - 1] = true;
        }
    }
    let mut oldest_eligible = None;
    let mut eligible_since = HashMap::new();
    for (component, eligible) in components.iter().zip(eligible) {
        if !eligible {
            continue;
        }
        for i in component {
            stats.eligible_input_parts += 1;
            stats.eligible_input_bytes = stats
                .eligible_input_bytes
                .saturating_add(active[*i].file_size);
            let id = active[*i].id;
            let at = previous
                .and_then(|debt| debt.eligible_since.get(&id))
                .copied()
                .unwrap_or(now);
            eligible_since.insert(id, at);
            oldest_eligible =
                Some(oldest_eligible.map_or(at, |previous: Instant| previous.min(at)));
        }
    }
    stats.no_adjacent_pack = triggered && stats.eligible_input_parts == 0;
    CachedL1Debt {
        stats,
        oldest_eligible,
        eligible_since,
    }
}

pub(super) fn refresh_l1_debt(options: &OpenOptions, state: &mut State, shards: &BTreeSet<u16>) {
    let now = Instant::now();
    let mut updated = BTreeSet::new();
    for shard_id in shards {
        let Some(shard) = state.segment_index.by_shard.get(shard_id) else {
            continue;
        };
        let mut domains = BTreeMap::<Domain, Vec<&crate::segment::SegmentDescriptor>>::new();
        let mut boundaries = BTreeMap::<u32, Option<i64>>::new();
        for segment in &shard.segments {
            let part = segment.descriptor();
            if part.level != 1 {
                continue;
            }
            let before = *boundaries.entry(part.policy_id).or_insert_with(|| {
                sealed_before_for_policy(
                    options,
                    &state.catalog,
                    &state.event_watermarks,
                    part.policy_id,
                )
            });
            if before.is_some_and(|before| is_sealed_descriptor(part, before)) {
                continue;
            }
            domains.entry(part.domain()).or_default().push(part);
        }
        for (domain, parts) in domains {
            let previous = state.l1_debt.remove(&domain);
            state.l1_debt.insert(
                domain,
                l1_domain_debt(domain, &parts, previous.as_ref(), now),
            );
            updated.insert(domain);
        }
    }
    state
        .l1_debt
        .retain(|domain, _| !shards.contains(&domain.shard_id) || updated.contains(domain));
}

/// Chronological connected components are indivisible: joining around an omitted
/// part would create a new L1 overlap and disable exact summary reads.
fn l1_components(active: &[&crate::segment::SegmentDescriptor]) -> Vec<Vec<usize>> {
    let mut ordered = (0..active.len()).collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|i| {
        let part = active[*i];
        (part.min_timestamp_us, part.max_timestamp_us, part.id)
    });
    let mut components = Vec::<Vec<usize>>::new();
    let mut max_timestamp = i64::MIN;
    for index in ordered {
        let part = active[index];
        if !components.is_empty() && part.min_timestamp_us <= max_timestamp {
            components.last_mut().expect("component exists").push(index);
            max_timestamp = max_timestamp.max(part.max_timestamp_us);
        } else {
            components.push(vec![index]);
            max_timestamp = part.max_timestamp_us;
        }
    }
    components
}

pub(super) fn select_l1_input_indices(active: &[&crate::segment::SegmentDescriptor]) -> Vec<usize> {
    let mut inputs = Vec::new();
    let mut input_bytes = 0_u64;
    for component in l1_components(active) {
        let bytes = component
            .iter()
            .fold(0_u64, |sum, i| sum.saturating_add(active[*i].file_size));
        let fits = component.len() <= ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            && bytes <= ACTIVE_COMPACTION_MAX_INPUT_BYTES;
        if !fits
            || inputs.len().saturating_add(component.len()) > ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            || input_bytes.saturating_add(bytes) > ACTIVE_COMPACTION_MAX_INPUT_BYTES
        {
            if inputs.len() >= 2 {
                return inputs;
            }
            inputs.clear();
            input_bytes = 0;
        }
        if fits {
            input_bytes = input_bytes.saturating_add(bytes);
            inputs.extend(component);
        }
    }
    if inputs.len() < 2 {
        inputs.clear();
    }
    inputs
}

/// Existing Mutable deltas may overlap L1. Publication may preserve that overlap,
/// but must never extend it into a gap not covered by any compaction input.
pub(super) fn validate_l1_publication<'a>(
    inputs: impl Iterator<Item = &'a crate::segment::SegmentDescriptor>,
    outputs: impl Iterator<Item = &'a crate::segment::SegmentDescriptor>,
    remaining: impl Iterator<Item = &'a crate::segment::SegmentDescriptor>,
) -> Result<()> {
    let mut covered = BTreeMap::<Domain, Vec<TimeRange>>::new();
    for part in inputs {
        covered
            .entry(part.domain())
            .or_default()
            .push(TimeRange::for_descriptor(part)?);
    }
    for ranges in covered.values_mut() {
        *ranges = merge_time_ranges(std::mem::take(ranges));
    }
    let mut remaining_by_domain = BTreeMap::<Domain, Vec<TimeRange>>::new();
    for part in remaining.filter(|part| part.level == 1 && covered.contains_key(&part.domain())) {
        remaining_by_domain
            .entry(part.domain())
            .or_default()
            .push(TimeRange::for_descriptor(part)?);
    }
    for part in outputs.filter(|part| part.level == 1) {
        let output = TimeRange::for_descriptor(part)?;
        if let Some(remaining) = remaining_by_domain.get(&part.domain()) {
            for old in remaining.iter().filter(|old| old.overlaps(output)) {
                let intersection = TimeRange {
                    from_us: old.from_us.max(output.from_us),
                    to_us: old.to_us.min(output.to_us),
                };
                if !covered.get(&part.domain()).is_some_and(|ranges| {
                    ranges.iter().any(|range| {
                        range.from_us <= intersection.from_us && range.to_us >= intersection.to_us
                    })
                }) {
                    return Err(corrupt(
                        "compaction output widens an L1 overlap across an input gap",
                    ));
                }
            }
        }
    }
    Ok(())
}
