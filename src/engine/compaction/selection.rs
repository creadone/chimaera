//! Selection of bounded compaction inputs.

use crate::catalog::{CatalogSnapshot, CompressionProfile};
use crate::engine::options::OpenOptions;
use crate::engine::stats::L1CompactionDebtStats;
use crate::engine::{
    ACTIVE_COMPACTION_MAX_INPUT_BYTES, ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS,
    ACTIVE_L1_CONSOLIDATION_TRIGGER, ACTIVE_L1_DELTA_BASE_RATIO, CachedL1Debt, Inner,
    L1_OVERLAP_REPAIR_MAX_INPUT_BYTES, TimeRange,
};
use crate::error::{Result, corrupt};
use crate::persistence::layout::Domain;
use crate::segment::Segment;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

fn duration_micros_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

pub(in crate::engine) fn sealed_before_for_policy(
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

pub(in crate::engine) fn is_sealed_descriptor(
    descriptor: &crate::segment::SegmentDescriptor,
    before_us: i64,
) -> bool {
    descriptor.max_timestamp_us < before_us
}

pub(in crate::engine) struct CompactionGroup {
    pub(in crate::engine) domain: Domain,
    pub(in crate::engine) inputs: Vec<Arc<Segment>>,
    pub(in crate::engine) focus_ranges: Vec<TimeRange>,
    pub(in crate::engine) sealed_before_us: Option<i64>,
    // Bounded active runs must reduce logical fan-out even when successive
    // flushes have gaps in event time. Maintenance groups retain selective ranges.
    pub(in crate::engine) coalesce_disjoint: bool,
    pub(in crate::engine) repair_overlap: bool,
}

#[derive(Clone, Copy)]
pub(in crate::engine) struct CompactionSelection {
    pub(in crate::engine) compact_l0: bool,
    pub(in crate::engine) class: CompactionClass,
    pub(in crate::engine) sealed_before_us: Option<i64>,
    pub(in crate::engine) retention_cutoff: Option<i64>,
    pub(in crate::engine) rewrite_profile: bool,
    pub(in crate::engine) target_profile: CompressionProfile,
    pub(in crate::engine) include_overlapping_levels: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::engine) enum CompactionClass {
    Active,
    Historical,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::engine) struct CompactionDebt {
    pub(in crate::engine) active_bytes: u64,
    pub(in crate::engine) historical_bytes: u64,
    pub(in crate::engine) active_oldest_age_us: u64,
    pub(in crate::engine) historical_oldest_age_us: u64,
}

impl CompactionDebt {
    fn add(&mut self, class: CompactionClass, bytes: u64, published_ms: u64, now: u64) {
        let (total, oldest) = match class {
            CompactionClass::Active => (&mut self.active_bytes, &mut self.active_oldest_age_us),
            CompactionClass::Historical => (
                &mut self.historical_bytes,
                &mut self.historical_oldest_age_us,
            ),
        };
        *total = total.saturating_add(bytes);
        *oldest = (*oldest).max(now.saturating_sub(published_ms).saturating_mul(1000));
    }
}

pub(in crate::engine) fn compaction_debt(
    options: &OpenOptions,
    files: &crate::engine::files::View,
    catalog: &CatalogSnapshot,
    event_watermarks: &BTreeMap<u32, i64>,
    retention_cutoffs: &BTreeMap<u32, i64>,
) -> Result<CompactionDebt> {
    use crate::persistence::catalog::records::{self, DomainEntry};
    let mut debt = CompactionDebt::default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| corrupt("clock before epoch"))?
        .as_millis() as u64;
    files.visit_kind(records::PENDING, |key, summary: DomainEntry| {
        let domain = records::domain_from_key(key)?;
        let boundary =
            sealed_before_for_policy(options, catalog, event_watermarks, domain.policy_id);
        let cutoff = retention_cutoffs.get(&domain.policy_id).copied();
        let mut scan = [false; 2];
        for (level, needs_scan) in scan.iter_mut().enumerate() {
            if summary.counts[level] == 0 {
                continue;
            }
            let bounds = summary.debt_bounds[level]
                .ok_or_else(|| corrupt("missing compaction debt bounds"))?;
            let historical = boundary.is_some_and(|before| bounds.maximum_timestamp < before);
            if !historical && boundary.is_some_and(|before| bounds.minimum_timestamp < before) {
                *needs_scan = true;
                continue;
            }
            let class = if historical {
                CompactionClass::Historical
            } else if level == 0 || cutoff.is_some_and(|cutoff| bounds.maximum_timestamp < cutoff) {
                CompactionClass::Active
            } else {
                *needs_scan = cutoff.is_some_and(|cutoff| bounds.minimum_timestamp < cutoff);
                continue;
            };
            let published = [summary.oldest_l0_ms, summary.oldest_l1_ms][level]
                .ok_or_else(|| corrupt("missing compaction debt age"))?;
            debt.add(class, summary.bytes[level], published, now);
        }
        if !scan.iter().any(|needed| *needed) {
            return Ok(());
        }
        // A bound crossing sealing/retention cannot classify individual files.
        // Fall back only for those levels; summarized levels are already counted.
        files.domain_levels(domain, &summary, scan, |entry| {
            let d = &entry.descriptor;
            let historical =
                d.level < 2 && boundary.is_some_and(|before| is_sealed_descriptor(d, before));
            let active = d.level == 0
                || (d.level == 1
                    && retention_cutoffs
                        .get(&d.policy_id)
                        .is_some_and(|cutoff| d.min_timestamp_us < *cutoff));
            if historical {
                debt.add(
                    CompactionClass::Historical,
                    d.file_size,
                    entry.published_ms,
                    now,
                );
            } else if active {
                debt.add(
                    CompactionClass::Active,
                    d.file_size,
                    entry.published_ms,
                    now,
                );
            }
            Ok(())
        })
    })?;
    Ok(debt)
}

pub(in crate::engine) fn weighted_compaction_order(
    inner: &Inner,
    fallback: CompactionClass,
) -> Result<(CompactionClass, CompactionClass)> {
    #[cfg(feature = "publication-diagnostics")]
    let _scope = crate::diagnostics::publication::enter(inner.publication_diagnostics.get());
    let _timer =
        crate::diagnostics::publication::stage(crate::diagnostics::publication::Stage::Priority);
    let (files, catalog, events, cutoffs) = {
        let state = inner.state.read()?;
        (
            state.files.clone(),
            state.catalog.clone(),
            state.event_watermarks.clone(),
            state.retention_cutoffs.clone(),
        )
    };
    let debt = compaction_debt(&inner.options, &files, &catalog, &events, &cutoffs)?;
    Ok(weighted_compaction_order_for_debt(debt, fallback))
}

pub(in crate::engine) fn weighted_compaction_order_for_debt(
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

pub(in crate::engine) fn merge_time_ranges(mut ranges: Vec<TimeRange>) -> Vec<TimeRange> {
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

pub(in crate::engine) fn select_compaction_group(
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

pub(in crate::engine) fn can_coalesce_without_l1_overlap<'a>(
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

pub(in crate::engine) fn select_l1_overlap_repair(
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

pub(in crate::engine) fn select_l1_repair_indices(
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

pub(in crate::engine) fn select_active_l1_consolidation(
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

pub(in crate::engine) fn l1_domain_debt(
    domain: Domain,
    active: &[&crate::segment::SegmentDescriptor],
    previous: Option<&CachedL1Debt>,
    now: u64,
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
            oldest_eligible = Some(oldest_eligible.map_or(at, |previous: u64| previous.min(at)));
        }
    }
    stats.no_adjacent_pack = triggered && stats.eligible_input_parts == 0;
    CachedL1Debt {
        stats,
        oldest_eligible,
        eligible_since,
    }
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

pub(in crate::engine) fn select_l1_input_indices(
    active: &[&crate::segment::SegmentDescriptor],
) -> Vec<usize> {
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

/// Independent series may add overlapping parts to an older window. Publication
/// but must never extend it into a gap not covered by any compaction input.
pub(in crate::engine) fn validate_l1_publication<'a>(
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
