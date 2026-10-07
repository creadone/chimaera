//! Bounded file-local block navigation.
use super::*;
use crate::query::QueryReservation;
use crate::segment::{BlockIndex, Segment};

enum Root {
    Pending,
    Resident {
        next: usize,
    },
    Disk {
        next: usize,
        end: usize,
        directory_offset: u64,
        root_length: usize,
        index_length: u64,
    },
}

pub(in crate::segment) struct BlockCursor {
    segment: Arc<Segment>,
    series: SeriesKey,
    from_us: i64,
    to_us: i64,
    root: Root,
    page: Option<Arc<SegmentMetadata>>,
    page_memory: Option<QueryReservation>,
    large: Option<super::large::LargePage>,
    next: usize,
    budget: Arc<QueryBudget>,
}

impl BlockCursor {
    pub(in crate::segment) fn skip_before(&mut self, timestamp_us: i64) {
        if timestamp_us > self.from_us
            && self
                .page
                .as_ref()
                .and_then(|p| p.index.last())
                .is_none_or(|b| (b.series, b.max_timestamp_us) < (self.series, timestamp_us))
        {
            self.root = Root::Pending;
            self.page = None;
            self.page_memory = None;
            self.large = None;
        }
        self.from_us = self.from_us.max(timestamp_us);
        if self.from_us >= self.to_us {
            self.page = None;
            self.page_memory = None;
            self.large = None;
        }
    }

    pub(in crate::segment) fn new(
        segment: Arc<Segment>,
        series: SeriesKey,
        from_us: i64,
        to_us: i64,
        budget: Arc<QueryBudget>,
    ) -> Self {
        Self {
            segment,
            series,
            from_us,
            to_us,
            root: Root::Pending,
            page: None,
            page_memory: None,
            large: None,
            next: 0,
            budget,
        }
    }

    fn resources(&self) -> QueryMetadataResources<'_> {
        QueryMetadataResources {
            path: &self.segment.path,
            file_pool: &self.segment.file_pool,
            open_file_cache: self.segment.open_file_cache.as_deref(),
            query_budget: &self.budget,
        }
    }

    fn open_root(&mut self) -> Result<()> {
        self.budget.record_index_root_consulted();
        if let Some(root) = &self.segment.index_root {
            let next = root
                .pages
                .partition_point(|p| p.last_key() < (self.series, self.from_us));
            self.root = Root::Resident { next };
            return Ok(());
        }
        let mut file = None;
        let file = opened_file(
            &mut file,
            self.resources(),
            self.segment.storage_uuid,
            &self.segment.descriptor,
        )?;
        let end = self.segment.descriptor.file_offset + self.segment.descriptor.file_size;
        let mut footer = [0; FOOTER_SIZE];
        file.read_exact_at(end - FOOTER_SIZE as u64, &mut footer)?;
        let footer = validate_footer(&footer, self.segment.descriptor.file_size)?;
        if footer.version != 3 || footer.index_offset != self.segment.index_offset {
            return Err(corrupt("segment index layout changed after open"));
        }
        let root_offset = self.segment.descriptor.file_offset + self.segment.index_offset;
        let mut header = [0; PAGED_INDEX_HEADER_SIZE];
        file.read_exact_at(root_offset, &mut header)?;
        let length = read_u32(&header, 20) as usize;
        if length > MAX_INDEX_BYTES || length as u64 > footer.index_length {
            return Err(corrupt("invalid paged segment index root length"));
        }
        let (count, _, _, directory_offset) = paged_index_root_layout(&header, length)?;
        {
            // The header and each descriptor have independent checksums. Seek
            // without reading or allocating a root proportional to file history.
            let directory_offset = root_offset + directory_offset as u64;
            let read = |file: &mut QueryFile<'_>, n| {
                disk_page(
                    file,
                    directory_offset,
                    n,
                    length,
                    footer.index_length,
                    &self.budget,
                )
            };
            let mut lo = 0;
            let mut hi = count;
            while lo < hi {
                self.budget.check()?;
                let mid = lo + (hi - lo) / 2;
                if read(file, mid)?.last_key() < (self.series, self.from_us) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let first = lo;
            hi = count;
            while lo < hi {
                self.budget.check()?;
                let mid = lo + (hi - lo) / 2;
                if read(file, mid)?.first_key() < (self.series, self.to_us) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            self.budget.record_index_root_fallback_read();
            self.budget.record_metadata_bytes(
                (HEADER_SIZE + FOOTER_SIZE + PAGED_INDEX_HEADER_SIZE) as u64,
            );
            self.root = Root::Disk {
                next: first,
                end: lo,
                directory_offset,
                root_length: length,
                index_length: footer.index_length,
            };
            Ok(())
        }
    }

    fn next_page(&mut self) -> Result<Option<IndexPageDescriptor>> {
        if matches!(self.root, Root::Pending) {
            self.open_root()?;
        }
        match &mut self.root {
            Root::Resident { next } => {
                let root = self
                    .segment
                    .index_root
                    .as_ref()
                    .ok_or_else(|| corrupt("resident index root disappeared"))?;
                while let Some(page) = root.pages.get(*next).copied() {
                    *next += 1;
                    if page.first_key() >= (self.series, self.to_us) {
                        return Ok(None);
                    }
                    if page.last_key() >= (self.series, self.from_us) {
                        return Ok(Some(page));
                    }
                }
                Ok(None)
            }
            Root::Disk {
                next,
                end,
                directory_offset,
                root_length,
                index_length,
            } => {
                if *next == *end {
                    return Ok(None);
                }
                let number = *next;
                let directory_offset = *directory_offset;
                let root_length = *root_length;
                let index_length = *index_length;
                *next += 1;
                let mut opened = None;
                let file = opened_file(
                    &mut opened,
                    self.resources(),
                    self.segment.storage_uuid,
                    &self.segment.descriptor,
                )?;
                Ok(Some(disk_page(
                    file,
                    directory_offset,
                    number,
                    root_length,
                    index_length,
                    &self.budget,
                )?))
            }
            Root::Pending => Err(corrupt("index root was not opened")),
        }
    }

    pub(in crate::segment) fn next_block(&mut self) -> Result<Option<BlockIndex>> {
        if self.from_us >= self.to_us {
            return Ok(None);
        }
        loop {
            self.budget.check()?;
            if let Some(page) = &self.page {
                while let Some(block) = page.index.get(self.next).copied() {
                    self.next += 1;
                    if block.series == self.series
                        && block.max_timestamp_us >= self.from_us
                        && block.min_timestamp_us < self.to_us
                    {
                        return Ok(Some(block));
                    }
                }
            }
            self.page = None;
            self.page_memory = None;
            if let Some(mut large) = self.large.take()
                && large.remaining() != 0
            {
                let memory = self.budget.reserve_owned_class(
                    QueryMemoryClass::Metadata,
                    SegmentMetadata::estimated_bytes(large.remaining(), large.remaining())?,
                )?;
                let mut opened = None;
                let file = opened_file(
                    &mut opened,
                    self.resources(),
                    self.segment.storage_uuid,
                    &self.segment.descriptor,
                )?;
                let started = std::time::Instant::now();
                self.page = Some(large.read_window(file, self.segment.data_end, &self.budget)?);
                self.budget.record_metadata(started.elapsed());
                self.page_memory = Some(memory);
                self.large = Some(large);
                self.next = 0;
                continue;
            }
            let Some(page) = self.next_page()? else {
                return Ok(None);
            };
            if page.last_key() < (self.series, self.from_us) {
                continue;
            }
            if page.block_count as usize > super::large::WINDOW_BLOCKS {
                let mut opened = None;
                let file = opened_file(
                    &mut opened,
                    self.resources(),
                    self.segment.storage_uuid,
                    &self.segment.descriptor,
                )?;
                let started = std::time::Instant::now();
                self.large = Some(super::large::LargePage::open(
                    file,
                    self.segment.descriptor.file_offset + self.segment.index_offset + page.offset,
                    page,
                    self.segment.data_end,
                    &self.budget,
                )?);
                self.budget.record_metadata(started.elapsed());
                continue;
            }
            let bytes = SegmentMetadata::estimated_bytes(
                page.block_count as usize,
                page.block_count as usize,
            )?;
            let memory = self
                .budget
                .reserve_owned_class(QueryMemoryClass::Metadata, bytes)?;
            let absolute =
                self.segment.descriptor.file_offset + self.segment.index_offset + page.offset;
            let key = MetadataCacheKey::IndexPage(self.segment.descriptor.id, absolute);
            let metadata =
                if let Some(cached) = self.segment.metadata_cache.get_blocks_entry(key)? {
                    cached
                } else {
                    let _input = self
                        .budget
                        .reserve_owned_class(QueryMemoryClass::Metadata, u64::from(page.length))?;
                    let started = std::time::Instant::now();
                    let mut opened = None;
                    let file = opened_file(
                        &mut opened,
                        self.resources(),
                        self.segment.storage_uuid,
                        &self.segment.descriptor,
                    )?;
                    let decoded = read_page(file, absolute, page, self.segment.data_end)?;
                    let (metadata, _) = self
                        .segment
                        .metadata_cache
                        .insert_blocks_entry(key, decoded)?;
                    self.budget.record_metadata(started.elapsed());
                    self.budget
                        .record_metadata_bytes(u64::from(page.length) + HEADER_SIZE as u64);
                    self.budget.record_index_pages_read(1);
                    metadata
                };
            self.page = Some(metadata);
            self.page_memory = Some(memory);
            self.next = 0;
        }
    }
}

fn disk_page(
    file: &mut QueryFile<'_>,
    directory_offset: u64,
    number: usize,
    root_length: usize,
    index_length: u64,
    budget: &QueryBudget,
) -> Result<IndexPageDescriptor> {
    let mut bytes = [0; INDEX_PAGE_DIRECTORY_SIZE];
    file.read_exact_at(
        directory_offset + number as u64 * INDEX_PAGE_DIRECTORY_SIZE as u64,
        &mut bytes,
    )?;
    budget.record_metadata_bytes(INDEX_PAGE_DIRECTORY_SIZE as u64);
    let page = decode_index_page_descriptor(&bytes)?;
    validate_index_page_descriptor(page, None, page.offset)?;
    if page.offset < root_length as u64
        || page
            .offset
            .checked_add(u64::from(page.length))
            .is_none_or(|end| end > index_length)
    {
        return Err(corrupt("segment index page outside section"));
    }
    Ok(page)
}
