//! Bounded interval-tree pages for the immutable directory. Old flat roots
//! remain readable; new roots contain at most FANOUT entries.
use super::*;
use crate::query::QueryReservation;

pub(super) const ENTRY_SIZE: usize = 72;
pub(super) const FANOUT: usize = 64;
const MAX_LEVELS: u32 = 8;

pub(super) fn root_entries(mut leaves: usize) -> usize {
    while leaves > FANOUT {
        leaves = leaves.div_ceil(FANOUT);
    }
    leaves
}

pub(super) fn encode(entries: &[PageDescriptor]) -> Vec<u8> {
    let mut bytes = vec![0; entries.len() * ENTRY_SIZE];
    for (page, output) in entries.iter().zip(bytes.as_chunks_mut::<ENTRY_SIZE>().0) {
        encode_series(output, 0, page.first_series);
        encode_series(output, 16, page.last_series);
        put_u64(output, 32, page.offset);
        put_u32(output, 40, page.length);
        put_u32(output, 44, page.entries);
        put_u32(output, 48, page.crc32c);
        put_u32(output, 52, page.level);
        put_i64(output, 56, page.min_timestamp_us);
        put_i64(output, 64, page.max_timestamp_us);
    }
    bytes
}

pub(super) fn decode(bytes: &[u8], start: u64, end: u64) -> Result<Vec<PageDescriptor>> {
    if !bytes.len().is_multiple_of(ENTRY_SIZE) || bytes.len() / ENTRY_SIZE > FANOUT {
        return Err(corrupt("directory interval index shape is invalid"));
    }
    let mut pages = Vec::with_capacity(bytes.len() / ENTRY_SIZE);
    for input in bytes.as_chunks::<ENTRY_SIZE>().0 {
        let page = PageDescriptor {
            first_series: decode_series(input, 0),
            last_series: decode_series(input, 16),
            offset: read_u64(input, 32),
            length: read_u32(input, 40),
            entries: read_u32(input, 44),
            crc32c: read_u32(input, 48),
            level: read_u32(input, 52),
            min_timestamp_us: read_i64(input, 56),
            max_timestamp_us: read_i64(input, 64),
        };
        let entry_size = if page.level == 0 {
            LOCATOR_SIZE
        } else {
            ENTRY_SIZE
        };
        if page.first_series > page.last_series
            || page.min_timestamp_us > page.max_timestamp_us
            || page.offset < start
            || page
                .offset
                .checked_add(u64::from(page.length))
                .is_none_or(|v| v > end)
            || page.entries == 0
            || page.length as usize > MAX_PAGE_BYTES
            || u64::from(page.length) != u64::from(page.entries) * entry_size as u64
            || page.level >= MAX_LEVELS
            || (page.level > 0 && page.entries as usize > FANOUT)
        {
            return Err(corrupt("directory interval index entry is invalid"));
        }
        if let Some(previous) = pages.last() {
            let previous: &PageDescriptor = previous;
            if previous.last_series > page.first_series
                || previous.level != page.level
                || previous.offset.checked_add(u64::from(previous.length)) != Some(page.offset)
            {
                return Err(corrupt("directory interval index entries are not ordered"));
            }
        }
        pages.push(page);
    }
    Ok(pages)
}

pub(super) struct BuiltIndex {
    pub(super) root: Vec<u8>,
    pub(super) branches: Vec<Vec<u8>>,
    pub(super) end: u64,
}

pub(super) fn build(mut pages: Vec<PageDescriptor>, mut offset: u64) -> Result<BuiltIndex> {
    let mut branches = Vec::new();
    while pages.len() > FANOUT {
        let mut parents = Vec::with_capacity(pages.len().div_ceil(FANOUT));
        for children in pages.chunks(FANOUT) {
            let bytes = encode(children);
            let first = children
                .first()
                .ok_or_else(|| corrupt("empty directory index node"))?;
            let last = children
                .last()
                .ok_or_else(|| corrupt("empty directory index node"))?;
            let level = first.level + 1;
            if level >= MAX_LEVELS {
                return Err(invalid("directory interval index is too deep"));
            }
            parents.push(PageDescriptor {
                first_series: first.first_series,
                last_series: last.last_series,
                min_timestamp_us: children
                    .iter()
                    .map(|v| v.min_timestamp_us)
                    .min()
                    .unwrap_or(0),
                max_timestamp_us: children
                    .iter()
                    .map(|v| v.max_timestamp_us)
                    .max()
                    .unwrap_or(0),
                offset,
                length: bytes.len() as u32,
                entries: children.len() as u32,
                crc32c: crc32c::crc32c(&bytes),
                level,
            });
            offset = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| invalid("directory interval index length overflows"))?;
            branches.push(bytes);
        }
        pages = parents;
    }
    Ok(BuiltIndex {
        root: encode(&pages),
        branches,
        end: offset,
    })
}

fn validate_children(parent: PageDescriptor, children: &[PageDescriptor]) -> Result<()> {
    if children.len() != parent.entries as usize
        || children.is_empty()
        || children.iter().any(|child| {
            child.level + 1 != parent.level
                || child.first_series < parent.first_series
                || child.last_series > parent.last_series
                || child.min_timestamp_us < parent.min_timestamp_us
                || child.max_timestamp_us > parent.max_timestamp_us
                || child
                    .offset
                    .checked_add(u64::from(child.length))
                    .is_none_or(|end| end > parent.offset)
        })
    {
        return Err(corrupt(
            "directory interval index disagrees with its children",
        ));
    }
    Ok(())
}

pub(super) struct Charge<'a> {
    budget: &'a QueryBudget,
    bytes: u64,
}

impl<'a> Charge<'a> {
    pub(super) fn new(budget: &'a QueryBudget, bytes: u64) -> Result<Self> {
        budget.reserve_memory_class(QueryMemoryClass::Metadata, bytes)?;
        Ok(Self { budget, bytes })
    }
}

impl Drop for Charge<'_> {
    fn drop(&mut self) {
        self.budget
            .release_memory_class(QueryMemoryClass::Metadata, self.bytes);
    }
}

struct Frame {
    pages: Vec<PageDescriptor>,
    next: usize,
    _memory: QueryReservation,
}

/// Traverses only overlapping subtrees; working memory depends on tree height.
pub(super) struct Cursor {
    budget: Arc<QueryBudget>,
    root_next: usize,
    frames: Vec<Frame>,
    _stack_memory: QueryReservation,
}

impl Cursor {
    pub(super) fn new(budget: Arc<QueryBudget>) -> Result<Self> {
        let memory = budget.reserve_owned_class(
            QueryMemoryClass::Metadata,
            u64::from(MAX_LEVELS) * std::mem::size_of::<Frame>() as u64,
        )?;
        Ok(Self {
            budget,
            root_next: 0,
            frames: Vec::with_capacity(MAX_LEVELS as usize),
            _stack_memory: memory,
        })
    }

    pub(super) fn next_page(
        &mut self,
        directory: &OfflinePackDirectory,
        ranges: &[(SeriesKey, i64)],
        to_us: i64,
    ) -> Result<Option<PageDescriptor>> {
        loop {
            self.budget.check()?;
            let page = if let Some(frame) = self.frames.last_mut() {
                if let Some(page) = frame.pages.get(frame.next).copied() {
                    frame.next += 1;
                    page
                } else {
                    self.frames.pop();
                    continue;
                }
            } else if let Some(page) = directory.pages.get(self.root_next).copied() {
                self.root_next += 1;
                page
            } else {
                return Ok(None);
            };
            self.budget.record_pack_directory_fences_examined(1);
            if !ranges.iter().any(|(series, from)| {
                page.first_series <= *series
                    && *series <= page.last_series
                    && page.min_timestamp_us < to_us
                    && page.max_timestamp_us >= *from
            }) {
                continue;
            }
            if page.level == 0 {
                return Ok(Some(page));
            }
            let frame = self.read_branch(directory, page)?;
            self.frames.push(frame);
        }
    }

    fn read_branch(&self, directory: &OfflinePackDirectory, page: PageDescriptor) -> Result<Frame> {
        let memory = self.budget.reserve_owned_class(
            QueryMemoryClass::Metadata,
            u64::from(page.entries) * std::mem::size_of::<PageDescriptor>() as u64,
        )?;
        let cached = directory.cached_page(page.offset)?;
        let pages = if let Some(cached) = &cached {
            self.budget.record_pack_directory_page_cache_hit();
            decode(&cached.bytes, directory.pages_start, directory.pages_end)?
        } else {
            let _load = crate::query::lock_for_query(&directory.page_load, &self.budget)?;
            if let Some(cached) = directory.cached_page(page.offset)? {
                self.budget.record_pack_directory_page_cache_hit();
                decode(&cached.bytes, directory.pages_start, directory.pages_end)?
            } else {
                let _input = Charge::new(&self.budget, u64::from(page.length))?;
                let file = directory.file_cache.get(&directory.path, &self.budget)?;
                let started = Instant::now();
                let mut bytes = vec![0; page.length as usize];
                file.read_exact_at(page.offset, &mut bytes, &self.budget)?;
                self.budget.record_metadata_bytes(u64::from(page.length));
                self.budget.record_pack_directory_pages_read(1);
                if crc32c::crc32c(&bytes) != page.crc32c {
                    return Err(corrupt("directory interval index checksum mismatch"));
                }
                let pages = decode(&bytes, directory.pages_start, directory.pages_end)?;
                validate_children(page, &pages)?;
                self.budget.record_metadata(started.elapsed());
                directory.cache_page(page.offset, Arc::new(bytes))?;
                pages
            }
        };
        validate_children(page, &pages)?;
        Ok(Frame {
            pages,
            next: 0,
            _memory: memory,
        })
    }
}

/// Full recovery/verify traversal. Per-level offsets prove complete, nonoverlapping
/// coverage without retaining a list of every page in memory.
pub(super) fn visit_leaves(
    file: &mut File,
    roots: &[PageDescriptor],
    start: u64,
    end: u64,
    mut visit: impl FnMut(&mut File, PageDescriptor) -> Result<()>,
) -> Result<(u64, u64, u64)> {
    struct Coverage {
        first: [Option<u64>; MAX_LEVELS as usize],
        end: [u64; MAX_LEVELS as usize],
        leaves: u64,
        branches: u64,
        branch_bytes: u64,
    }
    fn walk(
        file: &mut File,
        pages: &[PageDescriptor],
        start: u64,
        end: u64,
        coverage: &mut Coverage,
        visit: &mut impl FnMut(&mut File, PageDescriptor) -> Result<()>,
    ) -> Result<()> {
        for &page in pages {
            let level = page.level as usize;
            if coverage.first[level].is_some() {
                if coverage.end[level] != page.offset {
                    return Err(corrupt("directory pages overlap or leave a gap"));
                }
            } else {
                coverage.first[level] = Some(page.offset);
            }
            coverage.end[level] = page
                .offset
                .checked_add(u64::from(page.length))
                .ok_or_else(|| corrupt("directory page offset overflows"))?;
            if page.level == 0 {
                coverage.leaves += 1;
                visit(file, page)?;
            } else {
                let mut bytes = vec![0; page.length as usize];
                file.seek(SeekFrom::Start(page.offset))?;
                file.read_exact(&mut bytes)?;
                if crc32c::crc32c(&bytes) != page.crc32c {
                    return Err(corrupt("directory interval index checksum mismatch"));
                }
                let children = decode(&bytes, start, end)?;
                validate_children(page, &children)?;
                coverage.branches += 1;
                coverage.branch_bytes += u64::from(page.length);
                walk(file, &children, start, end, coverage, visit)?;
            }
        }
        Ok(())
    }
    let mut coverage = Coverage {
        first: [None; MAX_LEVELS as usize],
        end: [0; MAX_LEVELS as usize],
        leaves: 0,
        branches: 0,
        branch_bytes: 0,
    };
    walk(file, roots, start, end, &mut coverage, &mut visit)?;
    let mut expected = start;
    for (first, end) in coverage.first.into_iter().zip(coverage.end) {
        if let Some(first) = first {
            if first != expected {
                return Err(corrupt("directory index coverage is invalid"));
            }
            expected = end;
        }
    }
    if expected != end {
        return Err(corrupt("directory index does not cover the artifact"));
    }
    Ok((coverage.leaves, coverage.branches, coverage.branch_bytes))
}
