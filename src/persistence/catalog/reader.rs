//! Read-only page access; writers never hold the cache lock during durable I/O.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use super::arena::PACK_SLOTS;
use super::page::{Link, Node, PAGE};
use crate::error::{Error, corrupt};
use crate::memory::cache::{CacheOwner, Evictable, MemoryClass, MemoryGovernor, MemoryReservation};
use crate::persistence::files::FilePermitPool;
use crate::query::QueryBudget;

pub(super) struct Page {
    link: Link,
    node: Node,
    bytes: u64,
    _memory: Option<(MemoryReservation, MemoryReservation)>,
}

impl std::ops::Deref for Page {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.node
    }
}

impl Page {
    pub(super) fn into_node(self: Arc<Self>) -> Node {
        match Arc::try_unwrap(self) {
            Ok(page) => page.node,
            Err(page) => page.node.clone(),
        }
    }
}

pub(super) struct Reader {
    pub path: PathBuf,
    pub uuid: [u8; 16],
    pub pool: FilePermitPool,
    pub governor: Arc<MemoryGovernor>,
    // A byte quota for this existing cache, including nodes pinned by readers.
    // The same pages also hold reservations in Storage's shared governor.
    cache_budget: Arc<MemoryGovernor>,
    sets: Vec<Mutex<[Option<Arc<Page>>; 4]>>,
    ways: usize,
    _slots_memory: MemoryReservation,
    eviction_cursor: AtomicUsize,
}

impl Reader {
    pub fn new(
        path: PathBuf,
        uuid: [u8; 16],
        pool: FilePermitPool,
        governor: Arc<MemoryGovernor>,
        cache_bytes: u64,
    ) -> Result<Arc<Self>> {
        Self::with_ways(path, uuid, pool, governor, cache_bytes, 4)
    }

    // One-way mode exists only for the diagnostic control. Production uses four
    // ways (fewer for a tiny cache), ordered most recently used first per set.
    pub(super) fn with_ways(
        path: PathBuf,
        uuid: [u8; 16],
        pool: FilePermitPool,
        governor: Arc<MemoryGovernor>,
        cache_bytes: u64,
        ways: usize,
    ) -> Result<Arc<Self>> {
        anyhow::ensure!((1..=4).contains(&ways), "invalid catalog cache ways");
        // This is a share of the existing combined metadata budget, not a new
        // independent allowance. Failed cache admission leaves the page uncached.
        let cache_bytes = cache_bytes.min(governor.maximum_bytes());
        let count = usize::try_from(cache_bytes / (PAGE + 128) as u64)?;
        let ways = ways.min(count.max(1));
        let sets = count / ways;
        let charge = (sets * std::mem::size_of::<Mutex<[Option<Arc<Page>>; 4]>>()
            + std::mem::size_of::<MemoryGovernor>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let memory = governor
            .try_reserve_token(MemoryClass::MetadataCatalog, charge)
            .ok_or_else(|| {
                Error::ResourceLimit("catalog cache slots exceed managed memory".into())
            })?;
        let reader = Arc::new(Self {
            path,
            uuid,
            pool,
            governor,
            cache_budget: Arc::new(MemoryGovernor::new(cache_bytes)),
            sets: (0..sets)
                .map(|_| Mutex::new(std::array::from_fn(|_| None)))
                .collect(),
            ways,
            _slots_memory: memory,
            eviction_cursor: AtomicUsize::new(0),
        });
        reader
            .governor
            .register_cache(CacheOwner::Catalog, &reader)?;
        Ok(reader)
    }

    pub fn read(&self, link: Link, budget: Option<&QueryBudget>) -> Result<Arc<Page>> {
        if let Some(budget) = budget {
            budget.check()?;
        }
        let slot = (!self.sets.is_empty()).then(|| {
            let hash = link.page.address.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ link.page.generation.wrapping_mul(0x517c_c1b7_2722_0a95);
            &self.sets[hash as usize % self.sets.len()]
        });
        if let Some(slot) = slot {
            let mut set = slot
                .lock()
                .map_err(|_| corrupt("catalog cache lock poisoned"))?;
            let pages = &mut set[..self.ways];
            let page = Self::cached(pages, link);

            drop(set);
            if let Some(page) = page {
                if let Some(budget) = budget {
                    budget.record_catalog_page(false);
                }
                // CRC/structure were checked when this immutable version loaded.
                // The caller must still supply its exact, validated parent link.
                anyhow::ensure!(page.link == link, "cached catalog page link mismatch");
                return Ok(page);
            }
        }
        anyhow::ensure!(link.page.address > 0, "zero catalog page address");
        let mut bytes = Box::new([0; PAGE]);
        let address = link.page.address - 1;
        let path = self
            .path
            .join("pages")
            .join(format!("{:08}.pack", address / PACK_SLOTS));
        let _permit = match budget {
            Some(b) => self.pool.acquire_for_query(b)?,
            None => self.pool.acquire()?,
        };
        File::open(path)?.read_exact_at(&mut bytes[..], address % PACK_SLOTS * PAGE as u64)?;
        if let Some(budget) = budget {
            budget.check()?;
            budget.record_metadata_bytes(PAGE as u64);
            budget.record_catalog_page(true);
            budget.record_file_open();
        }
        let node = self.decode(&bytes, link, budget)?;
        drop(bytes);
        let charge = (node.heap_bytes()
            + std::mem::size_of::<Page>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let mut memory = None;
        if let Some(slot) = slot {
            let mut target = slot
                .lock()
                .map_err(|_| corrupt("catalog cache lock poisoned"))?;
            let pages = &mut target[..self.ways];
            // Concurrent misses may finish the same read. Keep one cached copy.
            if let Some(page) = Self::cached(pages, link) {
                anyhow::ensure!(page.link == link, "cached catalog page link mismatch");
                return Ok(page);
            }
            // Prefer a free entry, then an unpinned LRU victim. If every entry
            // is pinned, its reservation survives removal until the reader exits.
            let victim = pages
                .iter()
                .position(Option::is_none)
                .or_else(|| {
                    pages
                        .iter()
                        .rposition(|p| p.as_ref().is_some_and(|p| Arc::strong_count(p) == 1))
                })
                .unwrap_or(self.ways - 1);
            pages[victim] = None;
            let class = MemoryClass::MetadataCatalogPage;
            let local = self
                .cache_budget
                .try_reserve_token(class, charge)
                .or_else(|| {
                    self.evict(charge);
                    self.cache_budget.try_reserve_token(class, charge)
                });
            memory = local.and_then(|local| {
                self.governor
                    .reserve_reclaiming(class, charge)
                    .map(|shared| (shared, local))
            });
            if memory.is_some() {
                let page = Arc::new(Page {
                    link,
                    node,
                    bytes: charge,
                    _memory: memory,
                });
                pages[..=victim].rotate_right(1);
                pages[0] = Some(Arc::clone(&page));
                return Ok(page);
            }
        }
        // Caller scratch reservations cover a transient, uncached node.
        Ok(Arc::new(Page {
            link,
            node,
            bytes: charge,
            _memory: memory,
        }))
    }

    fn cached(pages: &mut [Option<Arc<Page>>], link: Link) -> Option<Arc<Page>> {
        let index = pages
            .iter()
            .position(|p| p.as_ref().is_some_and(|p| p.link.page == link.page))?;
        pages[..=index].rotate_right(1);
        pages[0].clone()
    }

    fn decode(&self, bytes: &[u8; PAGE], link: Link, budget: Option<&QueryBudget>) -> Result<Node> {
        let node = Node::decode(bytes, link, self.uuid)?;
        if let Some(budget) = budget {
            budget.record_catalog_decode(match &node {
                Node::Leaf(rows) => rows.len(),
                Node::Branch(links) => links.len(),
            });
        }
        Ok(node)
    }

    pub fn clear(&self) -> Result<()> {
        for slot in &self.sets {
            *slot
                .lock()
                .map_err(|_| corrupt("catalog cache lock poisoned"))? =
                std::array::from_fn(|_| None);
        }
        Ok(())
    }
}

impl Evictable for Reader {
    fn evict(&self, bytes: u64) -> u64 {
        let count = self.sets.len();
        if count == 0 {
            return 0;
        }
        let scan = count.min(64 / self.ways);
        let start = self.eviction_cursor.fetch_add(scan, Ordering::Relaxed) % count;
        let mut freed = 0;
        for offset in 0..scan {
            if let Ok(mut set) = self.sets[(start + offset) % count].try_lock() {
                for slot in set[..self.ways].iter_mut().rev() {
                    if slot
                        .as_ref()
                        .is_some_and(|page| Arc::strong_count(page) == 1)
                    {
                        let page = slot.take().expect("idle cached page");
                        freed += page.bytes;
                        drop(page);
                        if freed >= bytes {
                            return freed;
                        }
                    }
                }
            }
        }
        freed
    }
}
