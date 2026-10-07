//! Select compaction jobs from an immutable view, outside the shared state lock.

use super::*;
use crate::catalog::CatalogSnapshot;
use crate::diagnostics::publication::{self as diagnostics, Counter, Stage};
use crate::engine::files::View;
use crate::memory::work::WorkMemoryReservation;
use crate::persistence::catalog::records::{self, DomainEntry};

mod historical;
mod overlap;

struct Snapshot {
    deletions: Arc<crate::deletion::Deletions>,
    files: View,
    catalog: CatalogSnapshot,
    maintenance_now_us: i64,
    event_watermarks: BTreeMap<u32, i64>,
    retention_cutoffs: BTreeMap<u32, i64>,
    aggregate_retention_cutoffs: BTreeMap<u32, i64>,
}

impl Snapshot {
    fn retention_selection_cutoff(&self, inner: &Inner, policy: u32, force: bool) -> Option<i64> {
        let cutoff = self.retention_cutoffs.get(&policy).copied()?;
        let interval = inner.options.retention_rewrite_interval.as_micros();
        if force
            || interval == 0
            || self
                .catalog
                .policy_by_id(policy)
                .is_some_and(|policy| policy.aggregate_retention_us.is_some())
        {
            return Some(cutoff);
        }
        // Options validation bounds the interval. Use i128 because flooring a
        // negative i64 timestamp can fall below i64::MIN. No timer/map survives
        // selection; an idle database becomes eligible at the next clock tick.
        let interval = interval as i128;
        let rounded = i128::from(cutoff).div_euclid(interval) * interval;
        Some(rounded.max(i128::from(i64::MIN)) as i64)
    }

    fn capture(inner: &Inner) -> Result<Self> {
        let observed_now_us = inner.options.clock.now_us();
        let waiting = diagnostics::stage(Stage::SelectionSnapshotWait);
        let mut state = inner.state.write()?;
        drop(waiting);
        let _held = diagnostics::stage(Stage::SelectionSnapshotHold);
        state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
        let now_us = state.maintenance_now_us;
        for policy in state.catalog.policies().to_vec() {
            advance_retention_cutoff(&mut state, &policy, now_us);
        }
        // Root leases keep retired input files alive until selection finishes.
        // There is one coordinator, hence at most one such view at a time.
        Ok(Self {
            deletions: Arc::clone(&state.deletions),
            files: state.files.clone(),
            catalog: state.catalog.clone(),
            maintenance_now_us: now_us,
            event_watermarks: state.event_watermarks.clone(),
            retention_cutoffs: state.retention_cutoffs.clone(),
            aggregate_retention_cutoffs: state.aggregate_retention_cutoffs.clone(),
        })
    }
}

pub(super) struct Plan {
    pub deletions: Arc<crate::deletion::Deletions>,
    pub file_view: View,
    pub memory: WorkMemoryReservation,
    pub archive_memory: WorkMemoryReservation,
    pub groups: Vec<CompactionGroup>,
    pub catalog: CatalogSnapshot,
    pub maintenance_now_us: i64,
    pub retention_cutoffs: BTreeMap<u32, i64>,
    pub aggregate_retention_cutoffs: BTreeMap<u32, i64>,
    pub existing_archives: Vec<Arc<AggregateArchive>>,
    pub expired_archives: Vec<Arc<AggregateArchive>>,
}

// Count opening attempts, including an attempt that returns an I/O error.
fn record_open(class: CompactionClass, level: u8) {
    let (opened, l2) = match class {
        CompactionClass::Active => (Counter::SelectionActiveOpened, Counter::SelectionActiveL2),
        CompactionClass::Historical => (
            Counter::SelectionHistoricalOpened,
            Counter::SelectionHistoricalL2,
        ),
    };
    diagnostics::add(opened, 1);
    if level == 2 {
        diagnostics::add(l2, 1);
    }
}

pub(super) fn select(inner: &Inner, force: bool, class: CompactionClass) -> Result<Option<Plan>> {
    #[cfg(feature = "publication-diagnostics")]
    let _scope = crate::diagnostics::publication::enter(inner.publication_diagnostics.get());
    let _timer =
        crate::diagnostics::publication::stage(crate::diagnostics::publication::Stage::Selection);

    // Preserve the existing fairness rule and all selection limits.
    let l1_turn = class == CompactionClass::Active
        && inner.metrics.active_compaction_jobs.load(Ordering::Relaxed) % 4 == 3;
    let state = Snapshot::capture(inner)?;
    choose(inner, state, force, class, l1_turn)
}

fn choose(
    inner: &Inner,
    state: Snapshot,
    force: bool,
    class: CompactionClass,
    l1_turn: bool,
) -> Result<Option<Plan>> {
    #[cfg(feature = "publication-diagnostics")]
    let started = Instant::now();
    let result = choose_from_snapshot(inner, state, force, class, l1_turn);
    #[cfg(feature = "publication-diagnostics")]
    {
        let stage = match (class, &result) {
            (CompactionClass::Active, Ok(Some(_))) => Stage::SelectionActiveReady,
            (CompactionClass::Active, Ok(None)) => Stage::SelectionActiveEmpty,
            (CompactionClass::Active, Err(_)) => Stage::SelectionActiveError,
            (CompactionClass::Historical, Ok(Some(_))) => Stage::SelectionHistoricalReady,
            (CompactionClass::Historical, Ok(None)) => Stage::SelectionHistoricalEmpty,
            (CompactionClass::Historical, Err(_)) => Stage::SelectionHistoricalError,
        };
        diagnostics::record_elapsed(stage, Some(started));
    }
    result
}

fn choose_from_snapshot(
    inner: &Inner,
    state: Snapshot,
    force: bool,
    class: CompactionClass,
    l1_turn: bool,
) -> Result<Option<Plan>> {
    let mut memory = inner
        .work_memory_governor
        .reserve_background(WorkMemoryClass::Compaction, 0)?;
    let mut eligible_by_shard = BTreeMap::<u16, Vec<(CompactionGroup, usize)>>::new();
    let mut consider = |domain: Domain, summary: DomainEntry| -> Result<()> {
        diagnostics::add(
            match class {
                CompactionClass::Active => Counter::SelectionActiveDomains,
                CompactionClass::Historical => Counter::SelectionHistoricalDomains,
            },
            1,
        );
        let (group, l0_count) = match class {
            CompactionClass::Active => {
                active_group(inner, &state, domain, &summary, force, l1_turn)?
            }
            CompactionClass::Historical => {
                historical::choose(inner, &state, domain, &summary, force)?
            }
        };
        if let Some(group) = group {
            eligible_by_shard
                .entry(domain.shard_id)
                .or_default()
                .push((group, l0_count));
        }
        // Candidate ownership is bounded by the existing domain/shard limits.
        for domains in eligible_by_shard.values_mut() {
            domains.sort_by_key(|(g, n)| {
                (
                    std::cmp::Reverse(g.repair_overlap),
                    std::cmp::Reverse(*n),
                    g.domain,
                )
            });
            domains.truncate(MAX_COMPACTION_DOMAINS);
        }
        while eligible_by_shard.len() > inner.options.flush_worker_count {
            let worst = eligible_by_shard
                .iter()
                .min_by_key(|(id, groups)| {
                    (
                        groups.iter().any(|(g, _)| g.repair_overlap),
                        l1_turn
                            && groups
                                .iter()
                                .any(|(g, _)| g.inputs.iter().all(|s| s.descriptor().level == 1)),
                        groups.iter().map(|(_, n)| *n).sum::<usize>(),
                        std::cmp::Reverse(**id),
                    )
                })
                .map(|(id, _)| *id)
                .expect("candidate map not empty");
            eligible_by_shard.remove(&worst);
        }
        let bytes = eligible_by_shard
            .values()
            .flat_map(|groups| groups.iter())
            .map(|(group, _)| {
                512 + group.inputs.capacity() as u64 * 64
                    + group.focus_ranges.capacity() as u64 * 32
            })
            .sum::<u64>();
        if !memory.try_grow(bytes.saturating_sub(memory.bytes()))? {
            return Err(Error::ResourceLimit(
                "compaction candidates exceed work memory".into(),
            ));
        }
        Ok(())
    };
    let kind = if force {
        records::DOMAIN
    } else {
        records::PENDING
    };
    state.files.visit_kind(kind, |key, summary: DomainEntry| {
        consider(records::domain_from_key(key)?, summary)
    })?;
    if !force && class == CompactionClass::Active {
        for &policy in state.retention_cutoffs.keys() {
            let cutoff = state
                .retention_selection_cutoff(inner, policy, force)
                .expect("iterated retention policy");
            for shard in 0..inner.options.virtual_shards {
                let scope = records::scope(records::DOMAIN, policy, shard);
                state
                    .files
                    .snapshot
                    .visit(scope, scope, i64::MIN, cutoff, None, |row| {
                        let summary: DomainEntry = records::decode(row)?;
                        // Domains in the pending index were considered above.
                        if !summary.pending() && summary.minimum_timestamp < cutoff {
                            consider(records::domain_from_key(row.key)?, summary)?;
                        }
                        Ok(())
                    })?;
            }
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
    let mut expired_archives = Vec::new();
    let mut archive_memory = inner
        .work_memory_governor
        .reserve_background(WorkMemoryClass::Compaction, 0)?;
    for (&policy, &cutoff) in &state.aggregate_retention_cutoffs {
        for shard in 0..inner.options.virtual_shards {
            state
                .files
                .archives(policy, shard, i64::MIN, cutoff, None, |d| {
                    if d.coverage_to_us <= cutoff
                        && expired_archives.len() < MAX_COMPACTION_INPUT_SEGMENTS
                    {
                        if !archive_memory.try_grow(4096)? {
                            return Err(Error::ResourceLimit("archive selection memory".into()));
                        }
                        expired_archives.push(state.files.resources.archive(d)?);
                    }
                    Ok(())
                })?;
        }
    }
    if eligible_shards.is_empty() && expired_archives.is_empty() {
        return Ok(None);
    }
    let mut groups = Vec::new();
    let mut input_count = 0_usize;
    for (_shard, mut selected) in eligible_shards {
        selected.sort_by_key(|(group, count)| (std::cmp::Reverse(*count), group.domain));
        selected.truncate(MAX_COMPACTION_DOMAINS);
        for (group, _) in selected {
            if !groups.is_empty()
                && input_count.saturating_add(group.inputs.len()) > MAX_COMPACTION_INPUT_SEGMENTS
            {
                continue;
            }
            input_count = input_count.saturating_add(group.inputs.len());
            groups.push(group);
        }
    }
    let mut existing_archives = Vec::new();
    for group in &groups {
        let d = group.domain;
        state.files.archives(
            d.policy_id,
            d.shard_id,
            d.window_start_us,
            d.window_end_us,
            None,
            |a| {
                if !archive_memory.try_grow(4096)? {
                    return Err(Error::ResourceLimit(
                        "archive coverage selection memory".into(),
                    ));
                }
                existing_archives.push(state.files.resources.archive(a)?);
                Ok(())
            },
        )?;
    }
    Ok(Some(Plan {
        deletions: state.deletions,
        file_view: state.files,
        memory,
        archive_memory,
        groups,
        catalog: state.catalog,
        maintenance_now_us: state.maintenance_now_us,
        retention_cutoffs: state.retention_cutoffs,
        aggregate_retention_cutoffs: state.aggregate_retention_cutoffs,
        existing_archives,
        expired_archives,
    }))
}

fn active_group(
    inner: &Inner,
    state: &Snapshot,
    domain: Domain,
    summary: &DomainEntry,
    force: bool,
    l1_turn: bool,
) -> Result<(Option<CompactionGroup>, usize)> {
    let class = CompactionClass::Active;
    let mut work = inner
        .work_memory_governor
        .reserve_background(WorkMemoryClass::Compaction, 0)?;
    let mut parts = Vec::new();
    let selection_cutoff = state.retention_selection_cutoff(inner, domain.policy_id, force);
    let expired_data = selection_cutoff.is_some_and(|cutoff| summary.minimum_timestamp < cutoff);
    // Archive retention still needs the whole domain to build exact archives.
    // Raw retention discovers only expired L2 and their indexed overlap closure.
    let archive_retention = state
        .catalog
        .policy_by_id(domain.policy_id)
        .is_some_and(|policy| policy.aggregate_retention_us.is_some());
    let include_l2 = expired_data && archive_retention;
    if !expired_data && summary.counts[0] == 0 && summary.counts[1] == 0 {
        return Ok((None, 0));
    }
    let mut open = |entry: records::SegmentEntry| -> Result<()> {
        if !work.try_grow(2048)? {
            return Err(Error::ResourceLimit(
                "compaction domain exceeds work memory".into(),
            ));
        }
        record_open(class, entry.descriptor.level);
        parts.push(state.files.resources.open(entry.descriptor, None)?);
        Ok(())
    };
    if include_l2 {
        state.files.domain(domain, &mut open)?;
    } else {
        state
            .files
            .domain_levels(domain, summary, [true, true], &mut open)?;
    }
    let domain = &domain;
    let sealed_before_us = sealed_before_for_policy(
        &inner.options,
        &state.catalog,
        &state.event_watermarks,
        domain.policy_id,
    );
    let l0_count = parts
        .iter()
        .filter(|segment| {
            let descriptor = segment.descriptor();
            if descriptor.level != 0 {
                return false;
            }
            let historical =
                sealed_before_us.is_some_and(|before| is_sealed_descriptor(descriptor, before));
            !historical
        })
        .count();
    let retention_needed = expired_data;
    let target_profile =
        compression_profile_for_domain(&state.catalog, *domain, state.maintenance_now_us, 1);
    let profile_rewrite_needed = l0_count == 0
        && parts.iter().any(|segment| {
            segment.descriptor().level == 1 && !segment.uses_profile(target_profile)
        });
    if profile_rewrite_needed && !expired_data && summary.counts[2] > 0 {
        // A changed compression profile is detected from opened L1 headers.
        // Its rewrite must include overlapping L2, just as before.
        state.files.domain(*domain, |entry| {
            if entry.descriptor.level != 2 {
                return Ok(());
            }
            if !work.try_grow(2048)? {
                return Err(Error::ResourceLimit(
                    "compaction domain exceeds work memory".into(),
                ));
            }
            record_open(class, entry.descriptor.level);
            parts.push(state.files.resources.open(entry.descriptor, None)?);
            Ok(())
        })?;
        // Preserve catalogue order for maintenance selection.
        parts.sort_unstable_by_key(|segment| {
            let d = segment.descriptor();
            (d.min_timestamp_us, d.id)
        });
    }
    let compact_l0 = l0_count > 0 && (force || l0_count >= inner.options.l0_compaction_trigger);
    let selection = CompactionSelection {
        compact_l0,
        class,
        sealed_before_us,
        retention_cutoff: selection_cutoff,
        rewrite_profile: profile_rewrite_needed,
        target_profile,
        include_overlapping_levels: retention_needed || profile_rewrite_needed,
    };
    if expired_data && !archive_retention {
        overlap::retention_inputs(state, *domain, selection, &mut parts, &mut work)?;
    }
    let segments = &parts;
    let group = if retention_needed && archive_retention {
        Some(CompactionGroup {
            domain: *domain,
            inputs: segments.clone(),
            focus_ranges: Vec::new(),
            sealed_before_us,
            coalesce_disjoint: false,
            repair_overlap: false,
        })
    } else if !retention_needed
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
    } else if retention_needed || profile_rewrite_needed || compact_l0 {
        select_compaction_group(*domain, segments, selection)?
    } else {
        select_active_l1_consolidation(*domain, segments, sealed_before_us, false)?
    };
    Ok((group, l0_count))
}
