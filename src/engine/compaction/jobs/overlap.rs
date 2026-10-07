//! Expand maintenance seeds through indexed overlap ranges of one leased root.
use super::*;
use crate::segment::{Segment, SegmentDescriptor};
use std::collections::btree_map::Entry;

pub(super) fn charge(memory: &mut WorkMemoryReservation) -> Result<()> {
    // Covers a descriptor, its frontier range and eventual opened handle. The
    // coordinator owns this temporary set; exhaustion returns ResourceLimit.
    if !memory.try_grow(2048)? {
        return Err(Error::ResourceLimit(
            "compaction selection exceeds work memory".into(),
        ));
    }
    Ok(())
}

pub(super) fn extend(
    state: &Snapshot,
    domain: Domain,
    class: CompactionClass,
    sealed_before: Option<i64>,
    selected: &mut BTreeMap<u64, SegmentDescriptor>,
    memory: &mut WorkMemoryReservation,
) -> Result<()> {
    let mut frontier = selection::merge_time_ranges(
        selected
            .values()
            .map(TimeRange::for_descriptor)
            .collect::<Result<_>>()?,
    );
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for range in frontier {
            state
                .files
                .domain_range(domain, range.from_us, range.to_us, |entry| {
                    let d = entry.descriptor;
                    if d.level == 0 {
                        let historical = sealed_before.is_some_and(|b| is_sealed_descriptor(&d, b));
                        if historical != (class == CompactionClass::Historical) {
                            return Ok(());
                        }
                    }
                    if let Entry::Vacant(slot) = selected.entry(d.id) {
                        charge(memory)?;
                        next.push(TimeRange::for_descriptor(&d)?);
                        slot.insert(d);
                    }
                    Ok(())
                })?;
        }
        frontier = selection::merge_time_ranges(next);
    }
    Ok(())
}

pub(super) fn retention_inputs(
    state: &Snapshot,
    domain: Domain,
    selection: CompactionSelection,
    parts: &mut Vec<Arc<Segment>>,
    memory: &mut WorkMemoryReservation,
) -> Result<()> {
    let cutoff = selection
        .retention_cutoff
        .expect("expired data has a cutoff");
    let mut selected = BTreeMap::new();
    for segment in parts.iter() {
        let d = segment.descriptor();
        let active_l0 = d.level == 0
            && !selection
                .sealed_before_us
                .is_some_and(|b| is_sealed_descriptor(d, b));
        if d.min_timestamp_us < cutoff
            || (selection.compact_l0 && active_l0)
            || (selection.rewrite_profile
                && d.level == 1
                && !segment.uses_profile(selection.target_profile))
        {
            selected.insert(d.id, d.clone());
        }
    }
    // L0/L1 were already loaded for pressure/profile decisions. Only expired
    // L2 and their transitive overlap component need opening now.
    state
        .files
        .domain_range(domain, domain.window_start_us, cutoff, |entry| {
            let d = entry.descriptor;
            if d.level == 2 {
                charge(memory)?;
                selected.insert(d.id, d);
            }
            Ok(())
        })?;
    extend(
        state,
        domain,
        CompactionClass::Active,
        selection.sealed_before_us,
        &mut selected,
        memory,
    )?;
    for descriptor in selected.into_values().filter(|d| d.level == 2) {
        record_open(CompactionClass::Active, 2);
        parts.push(state.files.resources.open(descriptor, None)?);
    }
    parts.sort_unstable_by_key(|s| (s.descriptor().min_timestamp_us, s.descriptor().id));
    Ok(())
}
