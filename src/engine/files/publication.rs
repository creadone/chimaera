//! Incremental metadata proposal. All records commit through the same root.
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::View;
use crate::diagnostics::publication::{self as diagnostics, Counter, Stage};
use crate::error::{Error, Result, corrupt};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use crate::persistence::catalog::records::{
    self, DomainEntry, PackEntry, RetiredEntry, SegmentEntry, Totals,
};
use crate::persistence::catalog::{Edit, Key, Record};
use crate::persistence::layout::Domain;
use crate::segment::{Segment, SegmentDescriptor};
mod summary;

// Bound the private proposal independently of the catalogue size.
const MAX_FILE_RECORDS: usize = 65_536;

pub(in crate::engine) struct Proposal {
    pub edits: Vec<Edit>,
    pub totals: Totals,
    _memory: WorkMemoryReservation,
}

impl Proposal {
    pub fn prepare(
        &self,
        catalog: &crate::persistence::catalog::Catalog,
        base: &crate::persistence::catalog::Snapshot,
    ) -> Result<crate::persistence::catalog::Snapshot> {
        catalog.prepare(base, &self.edits)
    }
}

pub(in crate::engine) struct Builder<'a> {
    pub base: &'a View,
    changes: BTreeMap<Key, Option<Record>>,
    pub totals: Totals,
    memory: WorkMemoryReservation,
    work: Arc<WorkMemoryGovernor>,
    domains: BTreeSet<Domain>,
    removed: HashSet<u64>,
    added: Vec<SegmentEntry>,
    packs: BTreeMap<Key, Option<PackEntry>>,
    pub reclaim_removed_bytes: bool,
}

impl<'a> Builder<'a> {
    pub fn new(base: &'a View, work: &Arc<WorkMemoryGovernor>) -> Result<Self> {
        let mut totals = base.totals.clone();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| corrupt("clock before epoch"))?;
        totals.last_publication_ms = totals.last_publication_ms.max(
            u64::try_from(now.as_millis())
                .map_err(|_| corrupt("publication timestamp overflow"))?,
        );
        Ok(Self {
            base,
            changes: BTreeMap::new(),
            totals,
            work: Arc::clone(work),
            memory: work
                .try_reserve_background_with_headroom(WorkMemoryClass::Compaction, 4096, 0)?
                .ok_or_else(|| {
                    Error::ResourceLimit("catalog proposal work memory is busy".into())
                })?,
            domains: BTreeSet::new(),
            removed: HashSet::new(),
            added: Vec::new(),
            packs: BTreeMap::new(),
            reclaim_removed_bytes: false,
        })
    }
    fn charge(&mut self, bytes: u64) -> Result<()> {
        if !self.memory.try_grow(bytes)? {
            return Err(Error::ResourceLimit(
                "catalog proposal exceeds work memory".into(),
            ));
        }
        Ok(())
    }
    pub fn set(&mut self, key: Key, value: Option<Record>) -> Result<()> {
        if self.changes.len() >= MAX_FILE_RECORDS && !self.changes.contains_key(&key) {
            return Err(Error::ResourceLimit(
                "catalog file publication record limit".into(),
            ));
        }
        self.charge(
            value
                .as_ref()
                .map_or(512, |r| r.bytes.capacity() as u64 + 512),
        )?;
        self.changes.insert(key, value);
        Ok(())
    }
    pub fn put(&mut self, record: Record) -> Result<()> {
        self.set(record.key, Some(record))
    }
    pub fn get(&self, key: Key) -> Result<Option<Record>> {
        match self.changes.get(&key) {
            Some(value) => Ok(value.clone()),
            None => self.base.snapshot.get(key, None),
        }
    }
    pub fn replace_segments(
        &mut self,
        inputs: &[Arc<Segment>],
        outputs: &[Arc<Segment>],
    ) -> Result<()> {
        self.charge((inputs.len() + outputs.len()) as u64 * 4096)?;
        for input in inputs {
            let d = input.descriptor();
            let stored = self
                .base
                .lookup(d.id)?
                .ok_or_else(|| corrupt("compaction input is no longer published"))?;
            if stored.descriptor != *d || !self.removed.insert(d.id) {
                return Err(corrupt("compaction input identity or duplicate"));
            }
            self.domains.insert(d.domain());
            self.set(stored.key(), None)?;
            self.set(records::id_key(d.id), None)?;
            let level = usize::from(d.level);
            if level >= 3 {
                return Err(corrupt("catalog segment level"));
            }
            subtract(&mut self.totals.segments[level], 1)?;
            subtract(&mut self.totals.payload_bytes[level], d.file_size)?;
            self.adjust_pack(d, false)?;
        }
        let published_ms = self.totals.last_publication_ms;
        for output in outputs {
            let d = output.descriptor();
            if self.base.lookup(d.id)?.is_some()
                || self.added.iter().any(|e| e.descriptor.id == d.id)
            {
                return Err(corrupt("published segment id already exists"));
            }
            let level = usize::from(d.level);
            if level >= 3 {
                return Err(corrupt("catalog segment level"));
            }
            let entry = SegmentEntry {
                descriptor: d.clone(),
                published_ms,
                l1_eligible_since_ms: None,
                directory_id: None,
                catalog_indexed: false,
            };
            self.put(entry.record()?)?;
            self.put(records::encode(records::id_key(d.id), 1, &entry.key())?)?;
            self.domains.insert(d.domain());
            add(&mut self.totals.segments[level], 1)?;
            add(&mut self.totals.payload_bytes[level], d.file_size)?;
            self.adjust_pack(d, true)?;
            self.added.push(entry);
        }
        Ok(())
    }
    fn adjust_pack(&mut self, d: &SegmentDescriptor, inserting: bool) -> Result<()> {
        let key = records::pack_key(d);
        if !self.packs.contains_key(&key) {
            let existing = self
                .base
                .snapshot
                .get(key, None)?
                .as_ref()
                .map(records::decode)
                .transpose()?;
            self.packs.insert(key, existing);
        }
        let slot = self.packs.get_mut(&key).expect("inserted pack slot");
        if inserting && slot.is_none() {
            let size = std::fs::metadata(self.base.resources.root.join(&d.relative_path))?.len();
            *slot = Some(PackEntry {
                relative_path: d.relative_path.clone(),
                references: 0,
                file_size: size,
                live_bytes: 0,
                repack_since_ms: None,
                force_repack: false,
            });
        }
        let pack = slot
            .as_mut()
            .ok_or_else(|| corrupt("catalog lost physical pack"))?;
        if pack.relative_path != d.relative_path
            || d.file_offset
                .checked_add(d.file_size)
                .is_none_or(|end| end > pack.file_size)
        {
            return Err(corrupt("catalog pack identity or bounds"));
        }
        if inserting {
            add(&mut pack.references, 1)?;
            add(&mut pack.live_bytes, d.file_size)?;
            if pack.live_bytes > pack.file_size {
                return Err(corrupt("pack live bytes exceed file"));
            }
        } else {
            subtract(&mut pack.references, 1)?;
            subtract(&mut pack.live_bytes, d.file_size)?;
            pack.force_repack |= self.reclaim_removed_bytes;
        }
        if let Some(id) = d.pack_id {
            let key = records::member_key(id, d.id);
            self.set(
                key,
                if inserting {
                    Some(records::encode(key, 1, &records::segment_key(d))?)
                } else {
                    None
                },
            )?;
        }
        Ok(())
    }
    pub fn retire(&mut self, kind: u8, id: u64, path: String, size: u64) -> Result<()> {
        let generation = self
            .base
            .root()
            .generation
            .checked_add(1)
            .ok_or_else(|| corrupt("retirement generation overflow"))?;
        let start = i64::try_from(generation)
            .map_err(|_| corrupt("retirement generation exceeds time key"))?;
        let key = Key {
            series: Default::default(),
            scope: records::scope(records::RETIRED, u32::from(kind), 0),
            start,
            id,
        };
        if self.get(key)?.is_some() {
            return Err(corrupt("duplicate retired file"));
        }
        let retired_ms = self.totals.last_publication_ms;
        self.totals.oldest_retired_ms = Some(
            self.totals
                .oldest_retired_ms
                .map_or(retired_ms, |old| old.min(retired_ms)),
        );
        self.put(records::encode(
            key,
            start
                .checked_add(1)
                .ok_or_else(|| corrupt("retirement end overflow"))?,
            &RetiredEntry {
                relative_path: path,
                file_size: size,
                generation,
                retired_ms,
            },
        )?)?;
        add(&mut self.totals.retired_files, 1)?;
        add(&mut self.totals.retired_bytes, size)?;
        Ok(())
    }

    pub fn replace_archives(
        &mut self,
        inputs: &[Arc<crate::aggregate::archive::AggregateArchive>],
        outputs: &[Arc<crate::aggregate::archive::AggregateArchive>],
    ) -> Result<()> {
        for archive in inputs {
            let d = archive.descriptor();
            let key = records::archive_key(d);
            let stored = self
                .get(key)?
                .ok_or_else(|| corrupt("expired archive is no longer active"))?;
            let expected = records::encode(key, d.coverage_to_us, d)?;
            if stored.bytes != expected.bytes {
                return Err(corrupt("archive identity changed"));
            }
            self.set(key, None)?;
            self.set(records::object_id_key(records::ARCHIVE, d.id), None)?;
            subtract(&mut self.totals.archives, 1)?;
            subtract(&mut self.totals.archive_bytes, d.file_size)?;
            self.retire(records::ARCHIVE, d.id, d.relative_path.clone(), d.file_size)?;
        }
        for archive in outputs {
            let d = archive.descriptor();
            let key = records::archive_key(d);
            if self.get(key)?.is_some() {
                return Err(corrupt("duplicate archive"));
            }
            self.put(records::encode(key, d.coverage_to_us, d)?)?;
            self.put(records::encode(
                records::object_id_key(records::ARCHIVE, d.id),
                1,
                &key,
            )?)?;
            add(&mut self.totals.archives, 1)?;
            add(&mut self.totals.archive_bytes, d.file_size)?;
        }
        Ok(())
    }

    pub fn refresh_domain(&mut self, domain: Domain) -> Result<()> {
        self.charge(128)?;
        self.domains.insert(domain);
        Ok(())
    }
    pub fn finish(
        mut self,
        _l0_trigger: usize,
        history_trigger: usize,
        history_target_bytes: u64,
        history_window_us: i64,
    ) -> Result<Proposal> {
        for (key, pack) in std::mem::take(&mut self.packs) {
            let previous: Option<PackEntry> = self
                .base
                .snapshot
                .get(key, None)?
                .as_ref()
                .map(records::decode)
                .transpose()?;
            let mut pack = pack.ok_or_else(|| corrupt("empty touched pack"))?;
            if let Some(old) = previous.as_ref()
                && let Some(candidate) = old.candidate_key(key.id)?
            {
                self.set(candidate, None)?;
                subtract(&mut self.totals.repack_files, 1)?;
                subtract(&mut self.totals.repack_live_bytes, old.live_bytes)?;
                subtract(
                    &mut self.totals.repack_dead_bytes,
                    old.file_size - old.live_bytes,
                )?;
            }
            if key.scope >> 56 == u64::from(records::PACK) && pack.needs_repack() {
                pack.repack_since_ms
                    .get_or_insert(self.totals.last_publication_ms);
                let candidate = pack
                    .candidate_key(key.id)?
                    .ok_or_else(|| corrupt("repack age missing"))?;
                let end = candidate
                    .start
                    .checked_add(1)
                    .ok_or_else(|| corrupt("repack age overflow"))?;
                self.put(records::encode(candidate, end, &key)?)?;
                add(&mut self.totals.repack_files, 1)?;
                add(&mut self.totals.repack_live_bytes, pack.live_bytes)?;
                add(
                    &mut self.totals.repack_dead_bytes,
                    pack.file_size - pack.live_bytes,
                )?;
            } else {
                pack.repack_since_ms = None;
            }
            if pack.references == 0 {
                if let Some(previous) = previous {
                    self.set(key, None)?;
                    subtract(&mut self.totals.physical_files, 1)?;
                    subtract(&mut self.totals.physical_bytes, previous.file_size)?;
                    self.retire(
                        (key.scope >> 56) as u8,
                        key.id,
                        pack.relative_path,
                        pack.file_size,
                    )?;
                }
            } else {
                if previous.is_none() {
                    add(&mut self.totals.physical_files, 1)?;
                    add(&mut self.totals.physical_bytes, pack.file_size)?;
                }
                self.put(records::encode(key, 1, &pack)?)?;
            }
        }
        for domain in std::mem::take(&mut self.domains) {
            let summary_timer = diagnostics::stage(Stage::PublicationSummary);
            let summary = self.summarize_domain(
                domain,
                history_target_bytes,
                history_trigger,
                history_window_us,
            )?;
            drop(summary_timer);
            // Only the touched domain is materialized. Persist eligibility age
            // alongside its inputs so reopen and unrelated publications cannot reset it.
            let mut active = Vec::new();
            let base = self.base;
            let eligibility_timer = diagnostics::stage(Stage::PublicationEligibility);
            base.domain_levels(domain, &summary, [false, true], |entry| {
                diagnostics::add(Counter::PublicationEligibilityDescriptors, 1);
                if entry.descriptor.level == 1 && !self.removed.contains(&entry.descriptor.id) {
                    self.charge(4096)?;
                    active.push(entry);
                }
                Ok(())
            })?;
            drop(eligibility_timer);
            self.charge(
                self.added
                    .iter()
                    .filter(|e| e.descriptor.level == 1 && e.descriptor.domain() == domain)
                    .count() as u64
                    * 4096,
            )?;
            active.extend(
                self.added
                    .iter()
                    .filter(|e| e.descriptor.level == 1 && e.descriptor.domain() == domain)
                    .cloned(),
            );
            let previous = crate::engine::CachedL1Debt {
                stats: Default::default(),
                oldest_eligible: None,
                eligible_since: active
                    .iter()
                    .filter_map(|e| e.l1_eligible_since_ms.map(|at| (e.descriptor.id, at)))
                    .collect(),
            };
            let descriptors = active.iter().map(|e| &e.descriptor).collect::<Vec<_>>();
            let debt = crate::engine::compaction::selection::l1_domain_debt(
                domain,
                &descriptors,
                Some(&previous),
                self.totals.last_publication_ms,
            );
            for mut entry in active {
                let since = debt.eligible_since.get(&entry.descriptor.id).copied();
                if since != entry.l1_eligible_since_ms {
                    entry.l1_eligible_since_ms = since;
                    self.put(entry.record()?)?;
                }
            }
            let key = records::domain_key(records::DOMAIN, domain);
            if summary.count() == 0 {
                self.set(key, None)?;
            } else {
                self.put(records::encode(key, domain.window_end_us, &summary)?)?;
            }
            let key = records::domain_key(records::PENDING, domain);
            // Pure, already compacted L2 history is absent from the work index.
            if summary.pending() {
                self.put(records::encode(key, domain.window_end_us, &summary)?)?;
            } else {
                self.set(key, None)?;
            }
        }
        let edits = self
            .changes
            .into_iter()
            .map(|(key, value)| Edit { key, value })
            .collect();
        Ok(Proposal {
            edits,
            totals: self.totals,
            _memory: self.memory,
        })
    }
}

pub(super) fn accumulate(
    summary: &mut DomainEntry,
    entry: &SegmentEntry,
    _target: u64,
) -> Result<()> {
    let d = &entry.descriptor;
    let level = usize::from(d.level);
    if level >= 3 {
        return Err(corrupt("catalog domain level"));
    }
    add(&mut summary.counts[level], 1)?;
    add(&mut summary.bytes[level], d.file_size)?;
    summary.minimum_timestamp = summary.minimum_timestamp.min(d.min_timestamp_us);
    summary.maximum_timestamp = summary.maximum_timestamp.max(d.max_timestamp_us);
    summary.max_rewrites[level] = summary.max_rewrites[level].max(d.rewrite_count);
    if let Some(bounds) = summary.debt_bounds.get_mut(level) {
        let bounds = bounds.get_or_insert(records::DebtBounds {
            minimum_timestamp: d.min_timestamp_us,
            maximum_timestamp: d.max_timestamp_us,
        });
        bounds.minimum_timestamp = bounds.minimum_timestamp.min(d.min_timestamp_us);
        bounds.maximum_timestamp = bounds.maximum_timestamp.max(d.max_timestamp_us);
    }
    if level == 0 {
        summary.oldest_l0_ms = Some(
            summary
                .oldest_l0_ms
                .map_or(entry.published_ms, |old| old.min(entry.published_ms)),
        );
    }
    if level == 1 {
        summary.oldest_l1_ms = Some(
            summary
                .oldest_l1_ms
                .map_or(entry.published_ms, |old| old.min(entry.published_ms)),
        );
    }
    if level == 2 {
        summary.minimum_l2_bytes = Some(
            summary
                .minimum_l2_bytes
                .map_or(d.file_size, |old| old.min(d.file_size)),
        );
    }
    Ok(())
}
fn add(value: &mut u64, n: u64) -> Result<()> {
    *value = value
        .checked_add(n)
        .ok_or_else(|| corrupt("catalog total overflow"))?;
    Ok(())
}
fn subtract(value: &mut u64, n: u64) -> Result<()> {
    *value = value
        .checked_sub(n)
        .ok_or_else(|| corrupt("catalog total underflow"))?;
    Ok(())
}
