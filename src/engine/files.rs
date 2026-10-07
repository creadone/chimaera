//! Resolve only selected catalogue descriptors into bounded, evictable handles.
use crate::aggregate::archive::{AggregateArchive, AggregateArchiveDescriptor};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Result, corrupt};
use crate::memory::cache::{
    BlockCache, CacheOwner, Evictable, MemoryClass, MemoryGovernor, MemoryReservation,
};
use crate::persistence::catalog::records::{self, SegmentEntry};
use crate::persistence::catalog::{self, Key, Snapshot};
use crate::persistence::files::{FilePermitPool, OpenFileCache};
use crate::persistence::layout::Domain;
use crate::query::{QueryBudget, QueryReservation};
use crate::segment::{MetadataCache, Segment, SegmentDescriptor};
use crate::types::SeriesKey;
mod cursor;
pub(in crate::engine) use cursor::Cursor;
mod inventory;
mod members;
pub(super) mod publication;

struct CachedSegment {
    segment: Arc<Segment>,
    _memory: MemoryReservation,
}

/// One shared owner per Storage. The handle cache has at most 1024 entries and
/// also participates in the combined metadata budget. Segment collisions evict;
/// directory roots use a separate 128-entry LRU. Readers retain selected handles
/// under their snapshot and query budgets.
pub(crate) struct Resources {
    pub root: PathBuf,
    pub uuid: [u8; 16],
    pub block_cache: Arc<BlockCache>,
    pub metadata_cache: Arc<MetadataCache>,
    pub governor: Arc<MemoryGovernor>,
    pub file_pool: FilePermitPool,
    pub file_cache: Arc<OpenFileCache>,
    handles: Mutex<Vec<Option<CachedSegment>>>,
    _handles_memory: MemoryReservation,
    eviction_cursor: AtomicUsize,
}

impl Resources {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root: PathBuf,
        uuid: [u8; 16],
        block_cache: Arc<BlockCache>,
        metadata_cache: Arc<MetadataCache>,
        governor: Arc<MemoryGovernor>,
        file_pool: FilePermitPool,
        file_cache: Arc<OpenFileCache>,
        metadata_bytes: u64,
    ) -> Result<Arc<Self>> {
        let count = usize::try_from((metadata_bytes / 1024).min(1024))
            .map_err(|_| corrupt("segment cache size"))?;
        let memory = governor
            .try_reserve_token(
                MemoryClass::MetadataCatalog,
                (count * std::mem::size_of::<Option<CachedSegment>>()) as u64,
            )
            .ok_or_else(|| {
                crate::Error::ResourceLimit("segment cache slots exceed managed memory".into())
            })?;
        let resources = Arc::new(Self {
            root,
            uuid,
            block_cache,
            metadata_cache,
            governor,
            file_pool,
            file_cache,
            handles: Mutex::new((0..count).map(|_| None).collect()),
            _handles_memory: memory,
            eviction_cursor: AtomicUsize::new(0),
        });
        resources
            .governor
            .register_cache(CacheOwner::Handles, &resources)?;
        Ok(resources)
    }
    pub fn open(
        &self,
        descriptor: SegmentDescriptor,
        budget: Option<&Arc<QueryBudget>>,
    ) -> Result<Arc<Segment>> {
        use crate::diagnostics::publication::{self as cost, Counter};

        if let Some(budget) = budget {
            budget.check()?;
        }
        {
            let mut handles = self
                .handles
                .lock()
                .map_err(|_| corrupt("segment handles poisoned"))?;
            if !handles.is_empty()
                && let Some(entry) = &handles[descriptor.id as usize % handles.len()]
                && entry.segment.descriptor().id == descriptor.id
            {
                if entry.segment.descriptor() != &descriptor {
                    return Err(corrupt("segment id reused for different descriptor"));
                }
                if let Some(budget) = budget {
                    budget.record_segment_handle_cache_hit();
                } else {
                    cost::add(Counter::SegmentHandleCacheHits, 1);
                }
                return Ok(Arc::clone(&entry.segment));
            }
            let collision =
                !handles.is_empty() && handles[descriptor.id as usize % handles.len()].is_some();
            if let Some(budget) = budget {
                budget.record_segment_handle_cache_miss(collision);
            } else {
                cost::add(Counter::SegmentHandleCacheMisses, 1);
                cost::add(
                    Counter::SegmentHandleCacheCollisions,
                    usize::from(collision),
                );
            }
            if !handles.is_empty() {
                let slot = descriptor.id as usize % handles.len();
                handles[slot] = None;
            }
        }
        let segment = Segment::open_catalog(
            &self.root,
            self.uuid,
            descriptor,
            Arc::clone(&self.block_cache),
            Arc::clone(&self.metadata_cache),
            self.file_pool.clone(),
            Arc::clone(&self.file_cache),
            budget.map(AsRef::as_ref),
        )?;
        if let Some(budget) = budget {
            budget.record_segment_handle_open();
            budget.check()?;
        } else {
            cost::add(Counter::SegmentHandleOpens, 1);
        }
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| corrupt("segment handles poisoned"))?;
        if handles.is_empty() {
            return Ok(segment);
        }
        let slot = segment.descriptor().id as usize % handles.len();
        handles[slot] = None;
        // Segment itself already owns a MetadataCatalog reservation. This
        // conservative cache charge additionally limits retained handle count
        // through the same optional metadata allowance as pages and indexes.
        if let Some(memory) = self
            .governor
            .reserve_reclaiming(MemoryClass::MetadataHandle, 1024)
        {
            handles[slot] = Some(CachedSegment {
                segment: Arc::clone(&segment),
                _memory: memory,
            });
        }
        Ok(segment)
    }

    pub fn archive(&self, descriptor: AggregateArchiveDescriptor) -> Result<Arc<AggregateArchive>> {
        Ok(Arc::new(AggregateArchive::open(
            &self.root,
            self.uuid,
            descriptor,
            self.file_pool.clone(),
        )?))
    }
    pub fn invalidate(&self, ids: impl Iterator<Item = u64>) -> Result<()> {
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| corrupt("segment handles poisoned"))?;
        if !handles.is_empty() {
            for id in ids {
                let slot = id as usize % handles.len();
                if handles[slot]
                    .as_ref()
                    .is_some_and(|e| e.segment.descriptor().id == id)
                {
                    handles[slot] = None;
                }
            }
        }
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        self.handles
            .lock()
            .map_err(|_| corrupt("segment handles poisoned"))?
            .iter_mut()
            .for_each(|slot| *slot = None);
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct View {
    pub snapshot: Snapshot,
    pub resources: Arc<Resources>,
    pub totals: records::Totals,
}

pub(crate) struct Selection {
    pub segments: Vec<Arc<Segment>>,
    pub entries_examined: usize,
}

impl View {
    pub fn range(
        &self,
        policy: u32,
        shard: u16,
        from: i64,
        to: i64,
        series: &[SeriesKey],
        memory: &mut QueryReservation,
    ) -> Result<Selection> {
        let budget = Arc::clone(memory.budget());
        let mut selection = Selection {
            segments: Vec::new(),
            entries_examined: 0,
        };
        let scope = records::scope(records::SEGMENT, policy, shard);
        self.snapshot
            .visit(scope, scope, from, to, Some(&budget), |record| {
                selection.entries_examined += 1;
                let entry = records::segment(record)?;
                if entry.descriptor.max_timestamp_us < from
                    || entry.descriptor.min_timestamp_us >= to
                {
                    return Ok(());
                }
                let segment = self.resources.open(entry.descriptor, Some(&budget))?;
                if segment.may_contain_any_series(series) {
                    memory.push(&mut selection.segments, segment)?;
                }
                Ok(())
            })?;
        budget.record_catalog_entries(selection.entries_examined);
        Ok(selection)
    }
    pub fn visit_kind<T: serde::de::DeserializeOwned>(
        &self,
        kind: u8,
        mut visit: impl FnMut(Key, T) -> Result<()>,
    ) -> Result<()> {
        let (lo, hi) = records::kind_scopes(kind);
        self.snapshot.visit(lo, hi, i64::MIN, i64::MAX, None, |r| {
            visit(r.key, records::decode(r)?)
        })?;
        Ok(())
    }

    pub fn archives(
        &self,
        policy: u32,
        shard: u16,
        from: i64,
        to: i64,
        budget: Option<&Arc<QueryBudget>>,
        mut visit: impl FnMut(AggregateArchiveDescriptor) -> Result<()>,
    ) -> Result<()> {
        if self.totals.archives == 0 {
            return Ok(());
        }
        let scope = records::scope(records::ARCHIVE, policy, shard);
        self.snapshot.visit(scope, scope, from, to, budget, |r| {
            let d: AggregateArchiveDescriptor = records::decode(r)?;
            if records::archive_key(&d) != r.key || d.coverage_to_us != r.end {
                return Err(corrupt("archive catalogue key"));
            }
            visit(d)
        })?;
        Ok(())
    }
    pub fn descriptors(
        &self,
        minimum_scope: u64,
        maximum_scope: u64,
        from: i64,
        to: i64,
        mut visit: impl FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        self.snapshot
            .visit(minimum_scope, maximum_scope, from, to, None, |record| {
                let entry = records::segment(record)?;
                visit(entry)
            })?;
        Ok(())
    }
    pub fn domain(
        &self,
        domain: Domain,
        visit: impl FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        // Every nonempty segment interval is contained in its partition, so
        // visiting the entire domain still finds all of its levels and IDs.
        self.domain_range(domain, domain.window_start_us, domain.window_end_us, visit)
    }
    pub(in crate::engine) fn domain_range(
        &self,
        domain: Domain,
        from: i64,
        to: i64,
        mut visit: impl FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        let scope = records::scope(records::SEGMENT, domain.policy_id, domain.shard_id);
        self.descriptors(scope, scope, from, to, |entry| {
            if entry.descriptor.domain() == domain {
                visit(entry)?;
            }
            Ok(())
        })
    }
    /// Visit selected L0/L1 levels using bounds from this view's domain summary.
    /// Overlapping L2 may be decoded, but never returned. At most two ranges
    /// are needed; merging them preserves catalogue order and prevents duplicates.
    pub fn domain_levels(
        &self,
        domain: Domain,
        summary: &records::DomainEntry,
        levels: [bool; 2],
        mut visit: impl FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        let mut ranges = [None; 2];
        for (level, range) in ranges.iter_mut().enumerate() {
            if !levels[level] || summary.counts[level] == 0 {
                continue;
            }
            let bounds = summary.debt_bounds[level]
                .ok_or_else(|| corrupt("missing compaction level bounds"))?;
            if bounds.minimum_timestamp < domain.window_start_us
                || bounds.maximum_timestamp >= domain.window_end_us
                || bounds.minimum_timestamp > bounds.maximum_timestamp
            {
                return Err(corrupt("compaction level bounds outside domain"));
            }
            // maximum < window_end <= i64::MAX, hence the exclusive end fits.
            *range = Some((bounds.minimum_timestamp, bounds.maximum_timestamp + 1));
        }
        ranges.sort_unstable();
        if let [Some((from, to)), Some((next_from, next_to))] = ranges
            && next_from <= to
        {
            ranges = [Some((from, to.max(next_to))), None];
        }
        for (from, to) in ranges.into_iter().flatten() {
            let scope = records::scope(records::SEGMENT, domain.policy_id, domain.shard_id);
            let mask = u8::from(levels[0]) | (u8::from(levels[1]) << 1);
            self.snapshot
                .visit_levels(scope, from, to, mask, |record| {
                    let entry = records::segment(record)?;
                    if entry.descriptor.domain() != domain {
                        return Err(corrupt("catalog level key/domain bounds"));
                    }
                    if levels
                        .get(usize::from(entry.descriptor.level))
                        .copied()
                        .unwrap_or(false)
                    {
                        visit(entry)?;
                    }
                    Ok(())
                })?;
        }
        Ok(())
    }
    pub fn all(&self, visit: impl FnMut(SegmentEntry) -> Result<()>) -> Result<()> {
        let (minimum, maximum) = records::kind_scopes(records::SEGMENT);
        self.descriptors(minimum, maximum, i64::MIN, i64::MAX, visit)
    }
    pub fn domain_metadata(&self, domain: Domain) -> Result<Option<records::DomainEntry>> {
        self.snapshot
            .get(records::domain_key(records::DOMAIN, domain), None)?
            .as_ref()
            .map(records::decode)
            .transpose()
    }
    pub fn l0_count(&self, domain: Domain) -> Result<usize> {
        Ok(self
            .domain_metadata(domain)?
            .map_or(0, |entry| entry.counts[0] as usize))
    }
    pub fn lookup(&self, id: u64) -> Result<Option<SegmentEntry>> {
        self.snapshot.segment(id, None)
    }

    pub fn root(&self) -> catalog::Root {
        self.snapshot.root
    }
}

impl Evictable for Resources {
    fn evict(&self, bytes: u64) -> u64 {
        let Ok(mut handles) = self.handles.try_lock() else {
            return 0;
        };
        if handles.is_empty() {
            return 0;
        }
        let count = handles.len();
        let start = self.eviction_cursor.fetch_add(64, Ordering::Relaxed) % count;
        let mut freed = 0;
        for offset in 0..count.min(64) {
            let slot = &mut handles[(start + offset) % count];
            if slot
                .as_ref()
                .is_some_and(|entry| Arc::strong_count(&entry.segment) == 1)
            {
                *slot = None;
                freed += 1024;
                if freed >= bytes {
                    break;
                }
            }
        }
        freed
    }
}
