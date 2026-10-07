//! One series/segment cursor owns one leaf; advancing releases the previous leaf.
use super::*;
use crate::query::QueryReservation;

struct Leaf {
    bytes: Arc<Vec<u8>>,
    _lease: Option<Arc<DirectoryPage>>,
    _memory: QueryReservation,
}

pub(in crate::segment) struct BlockCursor {
    directory: Arc<OfflinePackDirectory>,
    pages: index::Cursor,
    segment_id: u64,
    series: SeriesKey,
    from_us: i64,
    to_us: i64,
    leaf: Option<Leaf>,
    next: usize,
    started: bool,
    budget: Arc<QueryBudget>,
}

impl BlockCursor {
    pub(in crate::segment) fn skip_before(&mut self, timestamp_us: i64) {
        self.from_us = self.from_us.max(timestamp_us);
        if self.from_us >= self.to_us {
            self.leaf = None;
        }
    }

    pub(in crate::segment) fn new(
        directory: Arc<OfflinePackDirectory>,
        segment_id: u64,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        budget: Arc<QueryBudget>,
    ) -> Result<Self> {
        Ok(Self {
            pages: index::Cursor::new(Arc::clone(&budget))?,
            directory,
            segment_id,
            series,
            from_us,
            to_us,
            leaf: None,
            next: 0,
            started: false,
            budget,
        })
    }

    pub(in crate::segment) fn next_block(&mut self) -> Result<Option<BlockIndex>> {
        if self.from_us >= self.to_us {
            return Ok(None);
        }
        if !self.started {
            self.budget.record_pack_directory_root_consulted();
            self.started = true;
        }
        loop {
            self.budget.check()?;
            if let Some(leaf) = &self.leaf {
                let entries = leaf.bytes.as_chunks::<LOCATOR_SIZE>().0;
                while let Some(entry) = entries.get(self.next) {
                    self.next += 1;
                    self.budget.check()?;
                    if decode_series(entry, 0) != self.series
                        || read_u64(entry, 16) != self.segment_id
                        || read_i64(entry, 44) < self.from_us
                        || read_i64(entry, 36) >= self.to_us
                    {
                        continue;
                    }
                    let locator = decode_locator(entry)?;
                    self.budget.record_pack_directory_locators_decoded(1);
                    self.budget.record_pack_directory_extents(1);
                    return Ok(Some(locator.block));
                }
            }
            self.leaf = None;
            let Some(page) = self.pages.next_page(
                &self.directory,
                &[(self.series, self.from_us)],
                self.to_us,
            )?
            else {
                return Ok(None);
            };
            let memory = self
                .budget
                .reserve_owned_class(QueryMemoryClass::Metadata, u64::from(page.length))?;
            let mut cached = self.directory.cached_page(page.offset)?;
            let bytes = if let Some(cached) = &cached {
                self.budget.record_pack_directory_page_cache_hit();
                Arc::clone(&cached.bytes)
            } else {
                let _load = crate::query::lock_for_query(&self.directory.page_load, &self.budget)?;
                cached = self.directory.cached_page(page.offset)?;
                if let Some(cached) = &cached {
                    self.budget.record_pack_directory_page_cache_hit();
                    Arc::clone(&cached.bytes)
                } else {
                    let file = self
                        .directory
                        .file_cache
                        .get(&self.directory.path, &self.budget)?;
                    let started = Instant::now();
                    let mut bytes = vec![0; page.length as usize];
                    file.read_exact_at(page.offset, &mut bytes, &self.budget)?;
                    self.budget.record_metadata_bytes(u64::from(page.length));
                    self.budget.record_pack_directory_pages_read(1);
                    self.directory.validate_leaf(page, &bytes, &self.budget)?;
                    self.budget.record_metadata(started.elapsed());
                    let bytes = Arc::new(bytes);
                    cached = self.directory.cache_page(page.offset, Arc::clone(&bytes))?;
                    cached
                        .as_ref()
                        .map_or(bytes, |page| Arc::clone(&page.bytes))
                }
            };
            self.leaf = Some(Leaf {
                bytes,
                _lease: cached,
                _memory: memory,
            });
            self.next = 0;
        }
    }
}

impl OfflinePackDirectory {
    fn validate_leaf(
        &self,
        page: PageDescriptor,
        bytes: &[u8],
        budget: &QueryBudget,
    ) -> Result<()> {
        if crc32c::crc32c(bytes) != page.crc32c
            || bytes.len() != page.entries as usize * LOCATOR_SIZE
        {
            return Err(corrupt(
                "offline pack directory leaf checksum or length is invalid",
            ));
        }
        let mut previous = None;
        for entry in bytes.as_chunks::<LOCATOR_SIZE>().0 {
            budget.check()?;
            let locator = decode_locator(entry)?;
            validate_leaf_fence(page, &locator)?;
            let order = (
                locator.series,
                locator.block.min_timestamp_us,
                locator.block.max_timestamp_us,
                locator.segment_id,
                locator.block.offset,
            );
            if previous.is_some_and(|v| v > order)
                || (!self.detached && !self.segments.contains_key(&locator.segment_id))
            {
                return Err(corrupt(
                    "offline pack directory leaf is unordered or refers to an unknown segment",
                ));
            }
            previous = Some(order);
        }
        budget.record_pack_directory_locators_decoded(u64::from(page.entries));
        Ok(())
    }
}
