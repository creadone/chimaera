//! Storage's disk catalogue. Manifest owns the sole durable publication point.
//! Snapshot leases protect both metadata slots and not-yet-opened payload files.
mod arena;

mod page;
mod prepare;
mod private;
mod reader;
pub(crate) mod records;
mod summary;
mod tree;

mod writer;

use crate::error::{Error, Result, corrupt};
use crate::memory::cache::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor};
use crate::persistence::files::FilePermitPool;
use crate::persistence::wal::sync_directory;
use crate::query::QueryBudget;
use anyhow::ensure;
use arena::Marks;
pub(crate) use page::{Key, Link, Record};
use reader::Reader;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
pub(crate) use summary::Summary;
pub(crate) use tree::Edit;
use writer::Writer;
const MAX_LEASES: usize = 64;
fn scratch_bytes(height: usize, entries: u64) -> u64 {
    // One decoded leaf and its input page; ancestors retain only branch links.
    // A validated tree cannot put more than its total entry count in one leaf.
    let rows = entries.min(page::MAX_LEAF_RECORDS as u64) as usize;
    (page::PAGE * 2
        + rows * std::mem::size_of::<Record>()
        + height.saturating_sub(1) * (page::BRANCH_CAP * std::mem::size_of::<Link>() + 4096))
        as u64
}

fn work_busy(work: &WorkMemoryGovernor, operation: &str, bytes: u64) -> Error {
    let usage = work
        .stats()
        .map(|stats| (stats.used_bytes, stats.maximum_bytes));
    Error::ResourceLimit(format!(
        "catalog {operation} needs {bytes} work bytes; used/limit {usage:?}"
    ))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub generation: u64,
    pub link: Option<Link>,
    pub height: usize,
    pub entries: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct Counters {
    pub writes: u64,
    pub written_bytes: u64,
    pub syncs: u64,
    pub reused_pages: u64,
    pub reclaimed_pages: u64,
    pub pack_opens: u64,
    pub retirement_logs: u64,
}

struct Lease {
    generation: u64,
}

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub root: Root,
    reader: Arc<Reader>,
    _lease: Arc<Lease>,
    work: Arc<WorkMemoryGovernor>,
    scratch_bytes: u64,
}

pub(crate) struct Catalog {
    reader: Arc<Reader>,
    writer: Mutex<Writer>,
    leases: Mutex<Vec<Weak<Lease>>>,
    work: Arc<WorkMemoryGovernor>,
    _memory: MemoryReservation,
    recovery: Mutex<Option<Marks>>,
}

impl Catalog {
    pub(crate) fn open(
        database: &Path,
        uuid: [u8; 16],
        root: Root,
        pool: FilePermitPool,
        governor: Arc<MemoryGovernor>,
        work: Arc<WorkMemoryGovernor>,
        cache_bytes: u64,
    ) -> Result<Arc<Self>> {
        Self::open_inner(database, uuid, root, pool, governor, work, cache_bytes).map_err(external)
    }
    fn open_inner(
        database: &Path,
        uuid: [u8; 16],
        root: Root,
        pool: FilePermitPool,
        governor: Arc<MemoryGovernor>,
        work: Arc<WorkMemoryGovernor>,
        cache_bytes: u64,
    ) -> anyhow::Result<Arc<Self>> {
        ensure!(
            root.height <= page::MAX_DEPTH
                && root.link.is_some() == (root.height > 0)
                && root.link.is_some() == (root.entries > 0)
                && root
                    .link
                    .is_none_or(|l| l.page.generation <= root.generation),
            "catalog root bounds"
        );
        let memory = governor
            .try_reserve_token(MemoryClass::MetadataCatalog, 16 * 1024)
            .ok_or_else(|| Error::ResourceLimit("catalog owners exceed managed memory".into()))?;
        let _work = work
            .try_reserve_background_with_headroom(
                WorkMemoryClass::Recovery,
                // Recovery has not checked the persisted entry count yet.
                scratch_bytes(root.height, u64::MAX),
                0,
            )?
            .ok_or_else(|| work_busy(&work, "recovery", scratch_bytes(root.height, u64::MAX)))?;
        let path = database.join("manifest/catalog");
        if root.link.is_some() {
            ensure!(path.is_dir(), "missing committed catalog");
        }
        fs::create_dir_all(path.join("pages"))?;
        sync_directory(&path)?;
        sync_directory(&database.join("manifest"))?;
        let reader = Reader::new(path, uuid, pool, governor, cache_bytes)?;
        let mut writer = Writer::open(Arc::clone(&reader), root)?;
        let mut marks = Marks::new(&reader.path, writer.arena.slots)?;
        let mut previous = None;
        let count = if let Some(link) = root.link {
            tree::visit(
                &reader,
                link,
                &tree::Range {
                    series: None,
                    minimum_scope: 0,
                    maximum_scope: u64::MAX,
                    from: i64::MIN,
                    to: i64::MAX,
                    levels: 0,
                },
                0,
                None,
                &mut |r| {
                    ensure!(
                        previous.is_none_or(|key| key < r.key),
                        "catalog cross-page order"
                    );
                    previous = Some(r.key);
                    Ok(())
                },
                &mut |p| marks.insert(p.address),
            )?
        } else {
            0
        };
        ensure!(count == root.entries, "catalog record count");
        writer.needs_reopen = true;
        Ok(Arc::new(Self {
            reader,
            writer: Mutex::new(writer),
            leases: Mutex::new(Vec::with_capacity(MAX_LEASES)),
            work,
            _memory: memory,
            recovery: Mutex::new(Some(marks)),
        }))
    }
    /// Destructive allocator recovery starts only after payload references and WAL pass validation.
    pub(crate) fn finish_recovery(&self, database: &Path) -> Result<()> {
        let mut marks = self
            .recovery
            .lock()
            .map_err(|_| corrupt("catalog recovery poisoned"))?
            .take()
            .ok_or_else(|| corrupt("catalog recovery already finished"))?;
        File::open(database.join("manifest/MANIFEST.json"))?.sync_all()?;
        sync_directory(&database.join("manifest"))?;
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| corrupt("catalog writer poisoned"))?;
        writer.recover(&mut marks).map_err(external)?;
        writer.needs_reopen = false;
        self.clear_cache()
    }
    pub(crate) fn inspect(
        database: &Path,
        uuid: [u8; 16],
        root: Root,
        pool: FilePermitPool,
        governor: Arc<MemoryGovernor>,
        work: Arc<WorkMemoryGovernor>,
        cache_bytes: u64,
    ) -> Result<Snapshot> {
        let reader = Reader::new(
            database.join("manifest/catalog"),
            uuid,
            pool,
            governor,
            cache_bytes,
        )
        .map_err(external)?;
        Ok(Snapshot {
            root,
            reader,
            _lease: Arc::new(Lease {
                generation: root.generation,
            }),
            work,
            // Inspection reads an unvalidated root, just like recovery.
            scratch_bytes: scratch_bytes(root.height, u64::MAX),
        })
    }

    pub(crate) fn snapshot(&self, root: Root) -> Result<Snapshot> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| corrupt("catalog leases poisoned"))?;
        leases.retain(|lease| lease.strong_count() > 0);
        let lease = if let Some(lease) = leases
            .iter()
            .filter_map(Weak::upgrade)
            .find(|l| l.generation == root.generation)
        {
            lease
        } else {
            if leases.len() == MAX_LEASES {
                return Err(Error::ResourceLimit(
                    "catalog snapshot generations limit".into(),
                ));
            }
            let lease = Arc::new(Lease {
                generation: root.generation,
            });
            leases.push(Arc::downgrade(&lease));
            lease
        };
        Ok(Snapshot {
            root,
            reader: Arc::clone(&self.reader),
            _lease: lease,
            work: Arc::clone(&self.work),
            scratch_bytes: scratch_bytes(root.height, root.entries),
        })
    }

    /// Prepare durable pages without publishing a second ROOT. The caller must
    /// place `snapshot.root` in the manifest, then call `committed` after sync.
    pub(crate) fn prepare(&self, base: &Snapshot, edits: &[Edit]) -> Result<Snapshot> {
        self.prepare_inner(base, edits).map_err(external)
    }
    fn prepare_inner(&self, base: &Snapshot, edits: &[Edit]) -> anyhow::Result<Snapshot> {
        ensure!(
            edits.windows(2).all(|w| w[0].key < w[1].key),
            "catalog edit order"
        );
        self.prepare_stream_inner(base, |pending| {
            for edit in edits {
                pending.push(edit.clone())?;
            }
            Ok(())
        })
    }

    fn prepare_stream_inner(
        &self,
        base: &Snapshot,
        produce: impl FnOnce(&mut prepare::Preparation<'_>) -> Result<()>,
    ) -> anyhow::Result<Snapshot> {
        ensure!(
            Arc::ptr_eq(&base.reader, &self.reader),
            "foreign catalog root"
        );
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| corrupt("catalog writer poisoned"))?;
        ensure!(
            !writer.needs_reopen && writer.generation == base.root.generation,
            "catalog requires reopen or has a stale writer"
        );
        let next = {
            let mut pending = prepare::Preparation::new(&mut writer, &self.work, base.root)?;
            produce(&mut pending)?;
            pending.flush()?;
            pending.finish_pages()?;
            pending.root
        };
        if next == base.root {
            return Ok(base.clone());
        }
        let snapshot = self.snapshot(next)?;
        // A single durability barrier, after every private batch, before the
        // final root can be installed in the manifest.
        writer.sync_pages()?;
        writer.prepared = Some(next);
        Ok(snapshot)
    }

    pub(crate) fn committed(&self, root: Root, durable: bool) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| corrupt("catalog writer poisoned"))?;
        if (writer.needs_reopen && writer.prepared.is_none())
            || writer.prepared.is_some_and(|prepared| prepared != root)
            || (writer.prepared.is_none() && root.generation != writer.generation)
        {
            return Err(corrupt("catalog commit generation"));
        }
        writer.generation = root.generation;
        writer.prepared = None;
        writer.needs_reopen = !durable;
        Ok(())
    }

    pub(crate) fn oldest_generation(&self, current: u64) -> Result<u64> {
        let leases = self
            .leases
            .lock()
            .map_err(|_| corrupt("catalog leases poisoned"))?;
        Ok(leases
            .iter()
            .filter_map(Weak::upgrade)
            .map(|l| l.generation)
            .min()
            .unwrap_or(current))
    }
    pub(crate) fn collect(&self) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| corrupt("catalog writer poisoned"))?;
        if writer.needs_reopen {
            return Err(corrupt("catalog requires reopen before GC"));
        }
        let committed = writer.generation;
        let oldest = self.oldest_generation(committed)?;
        writer.needs_reopen = true;
        writer.collect(committed, oldest).map_err(external)?;
        writer.needs_reopen = false;
        Ok(())
    }
    pub(crate) fn clear_cache(&self) -> Result<()> {
        self.reader.clear().map_err(external)
    }
    pub(crate) fn requires_reopen(&self) -> bool {
        self.writer
            .lock()
            .map_or(true, |writer| writer.needs_reopen)
    }
    pub(crate) fn close(&self) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| corrupt("catalog writer poisoned"))?;
        writer.arena.close_handles();
        writer.needs_reopen = true;
        self.clear_cache()
    }
}

pub(crate) struct RecordPage {
    rows: Vec<Record>,
    _memory: crate::memory::work::WorkMemoryReservation,
}

impl std::ops::Deref for RecordPage {
    type Target = [Record];
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl Snapshot {
    pub(crate) fn work_governor(&self) -> &Arc<WorkMemoryGovernor> {
        &self.work
    }
    pub(crate) fn summarize(&self, lo: Key, hi: Key, excluded: &[Key]) -> Result<Summary> {
        if lo >= hi || excluded.windows(2).any(|w| w[0] >= w[1]) {
            return Err(corrupt("catalog summary range/key order"));
        }
        let _work = self
            .work
            .try_reserve_background_with_headroom(
                WorkMemoryClass::Compaction,
                self.scratch_bytes,
                0,
            )?
            .ok_or_else(|| work_busy(&self.work, "summary", self.scratch_bytes))?;
        self.root
            .link
            .map(|link| tree::summarize(&self.reader, link, lo, hi, excluded, 0))
            .transpose()
            .map(|v| v.unwrap_or_default())
            .map_err(external)
    }
    pub(crate) fn page(&self, kind: u8, after: Option<Key>, limit: usize) -> Result<RecordPage> {
        let (minimum_scope, maximum_scope) = records::kind_scopes(kind);
        self.page_inner(
            tree::Range {
                series: None,
                minimum_scope,
                maximum_scope,
                from: i64::MIN,
                to: i64::MAX,
                levels: 0,
            },
            after,
            limit,
        )
    }
    pub(crate) fn page_range(
        &self,
        scope: u64,
        from: i64,
        to: i64,
        after: Option<Key>,
        limit: usize,
    ) -> Result<RecordPage> {
        self.page_inner(
            tree::Range {
                series: None,
                minimum_scope: scope,
                maximum_scope: scope,
                from,
                to,
                levels: 0,
            },
            after,
            limit,
        )
    }
    fn page_inner(
        &self,
        range: tree::Range,
        after: Option<Key>,
        limit: usize,
    ) -> Result<RecordPage> {
        if limit == 0 || limit > 512 {
            return Err(corrupt("catalog page count limit"));
        }
        let mut _work = self
            .work
            .try_reserve_background_with_headroom(
                WorkMemoryClass::Compaction,
                self.scratch_bytes + limit as u64 * std::mem::size_of::<Record>() as u64,
                0,
            )?
            .ok_or_else(|| {
                work_busy(
                    &self.work,
                    "page",
                    self.scratch_bytes + limit as u64 * std::mem::size_of::<Record>() as u64,
                )
            })?;
        let mut rows = Vec::with_capacity(limit);
        if let Some(root) = self.root.link {
            tree::page(
                &self.reader,
                root,
                &range,
                after,
                limit,
                0,
                &mut rows,
                None,
                &mut |bytes| {
                    if !_work.try_grow(bytes)? {
                        return Err(
                            Error::ResourceLimit("catalog page output memory".into()).into()
                        );
                    }
                    Ok(())
                },
            )
            .map_err(external)?;
        }
        Ok(RecordPage {
            rows,
            _memory: _work,
        })
    }
    pub(crate) fn visit(
        &self,
        minimum_scope: u64,
        maximum_scope: u64,
        from: i64,
        to: i64,
        budget: Option<&Arc<QueryBudget>>,
        consumer: impl FnMut(&Record) -> Result<()>,
    ) -> Result<u64> {
        self.visit_inner(minimum_scope, maximum_scope, from, to, 0, budget, consumer)
    }
    pub(crate) fn visit_levels(
        &self,
        scope: u64,
        from: i64,
        to: i64,
        levels: u8,
        consumer: impl FnMut(&Record) -> Result<()>,
    ) -> Result<u64> {
        self.visit_inner(scope, scope, from, to, levels, None, consumer)
    }
    #[allow(clippy::too_many_arguments)]
    fn visit_inner(
        &self,
        minimum_scope: u64,
        maximum_scope: u64,
        from: i64,
        to: i64,
        levels: u8,
        budget: Option<&Arc<QueryBudget>>,
        mut consumer: impl FnMut(&Record) -> Result<()>,
    ) -> Result<u64> {
        if self.root.link.is_none() || minimum_scope > maximum_scope || from >= to {
            return Ok(0);
        }
        let _memory = budget
            .map(|b| b.reserve_owned(self.scratch_bytes))
            .transpose()?;
        let _background = if budget.is_none() {
            Some(
                self.work
                    .try_reserve_background_with_headroom(
                        WorkMemoryClass::Compaction,
                        self.scratch_bytes,
                        0,
                    )?
                    .ok_or_else(|| work_busy(&self.work, "visit", self.scratch_bytes))?,
            )
        } else {
            None
        };
        match self.root.link {
            None => Ok(0),
            Some(root) => tree::visit(
                &self.reader,
                root,
                &tree::Range {
                    series: None,
                    minimum_scope,
                    maximum_scope,
                    from,
                    to,
                    levels,
                },
                0,
                budget.map(AsRef::as_ref),
                &mut |r| consumer(r).map_err(Into::into),
                &mut |_| Ok(()),
            )
            .map_err(external),
        }
    }
    pub(crate) fn get(
        &self,
        key: Key,
        budget: Option<&Arc<QueryBudget>>,
    ) -> Result<Option<Record>> {
        if self.root.link.is_none() {
            return Ok(None);
        }
        let _memory = budget
            .map(|b| b.reserve_owned(self.scratch_bytes))
            .transpose()?;
        let _background = if budget.is_none() {
            Some(
                self.work
                    .try_reserve_background_with_headroom(
                        WorkMemoryClass::Compaction,
                        self.scratch_bytes,
                        0,
                    )?
                    .ok_or_else(|| work_busy(&self.work, "get", self.scratch_bytes))?,
            )
        } else {
            None
        };
        self.root
            .link
            .map(|root| tree::get(&self.reader, root, key, budget.map(AsRef::as_ref)))
            .transpose()
            .map(Option::flatten)
            .map_err(external)
    }
}

// The private page codec adds decoding context. Preserve domain and I/O errors
// (especially cancellation and admission) when crossing back into Storage.
fn external(error: anyhow::Error) -> Error {
    match error.downcast::<Error>() {
        Ok(error) => error,
        Err(error) => match error.downcast::<std::io::Error>() {
            Ok(error) => Error::Io(error),
            Err(error) => corrupt(format!("{error:#}")),
        },
    }
}
