//! Older index pages can contain an entire long series. Validate their CRC in
//! bounded windows, then stream the entries without admitting a whole-page cache.
use super::*;
use crate::segment::index::{decode_index_entry, validate_index_entries};
use crate::segment::{BlockIndex, INDEX_ENTRY_SIZE, INDEX_PAGE_HEADER_SIZE, INDEX_PAGE_MAGIC};

pub(super) const WINDOW_BLOCKS: usize = 128;

pub(super) struct LargePage {
    offset: u64,
    next: usize,
    count: usize,
}

impl LargePage {
    pub(super) fn open(
        file: &mut QueryFile<'_>,
        offset: u64,
        page: IndexPageDescriptor,
        data_end: u64,
        budget: &Arc<QueryBudget>,
    ) -> Result<Self> {
        let _memory = budget.reserve_owned_class(
            QueryMemoryClass::Metadata,
            (WINDOW_BLOCKS * (INDEX_ENTRY_SIZE + std::mem::size_of::<BlockIndex>())) as u64,
        )?;
        let count = page.block_count as usize;
        let mut header = [0; INDEX_PAGE_HEADER_SIZE];
        file.read_exact_at(offset, &mut header)?;
        if &header[..8] != INDEX_PAGE_MAGIC
            || read_u16(&header, 8) != 2
            || read_u16(&header, 10) as usize != INDEX_ENTRY_SIZE
            || read_u32(&header, 12) != page.block_count
            || INDEX_PAGE_HEADER_SIZE as u64 + count as u64 * INDEX_ENTRY_SIZE as u64
                != u64::from(page.length)
        {
            return Err(corrupt("invalid oversized segment index page header"));
        }
        let mut crc = crc32c::crc32c(&header);
        let mut bytes = vec![0; WINDOW_BLOCKS * INDEX_ENTRY_SIZE];
        let mut previous: Option<BlockIndex> = None;
        for start in (0..count).step_by(WINDOW_BLOCKS) {
            budget.check()?;
            let entries = WINDOW_BLOCKS.min(count - start);
            let bytes = &mut bytes[..entries * INDEX_ENTRY_SIZE];
            file.read_exact_at(
                offset + INDEX_PAGE_HEADER_SIZE as u64 + (start * INDEX_ENTRY_SIZE) as u64,
                bytes,
            )?;
            crc = crc32c::crc32c_append(crc, bytes);
            let decoded = decode_window(bytes, data_end)?;
            let first = decoded
                .first()
                .ok_or_else(|| corrupt("empty index window"))?;
            if (start == 0 && first.series != page.first_series)
                || previous.is_some_and(|p| {
                    p.series > first.series || p.offset + u64::from(p.size) > first.offset
                })
            {
                return Err(corrupt("segment index windows are not ordered"));
            }
            previous = decoded.last().copied();
        }
        if crc != page.crc32c || previous.map(|p| p.series) != Some(page.last_series) {
            return Err(corrupt(
                "oversized segment index page checksum or bounds mismatch",
            ));
        }
        budget.record_metadata_bytes(u64::from(page.length));
        budget.record_index_pages_read(1);
        Ok(Self {
            offset: offset + INDEX_PAGE_HEADER_SIZE as u64,
            next: 0,
            count,
        })
    }

    pub(super) fn remaining(&self) -> usize {
        WINDOW_BLOCKS.min(self.count - self.next)
    }

    pub(super) fn read_window(
        &mut self,
        file: &mut QueryFile<'_>,
        data_end: u64,
        budget: &Arc<QueryBudget>,
    ) -> Result<Arc<SegmentMetadata>> {
        budget.check()?;
        let count = self.remaining();
        let _input = budget.reserve_owned_class(
            QueryMemoryClass::Metadata,
            (count * INDEX_ENTRY_SIZE) as u64,
        )?;
        let mut bytes = vec![0; count * INDEX_ENTRY_SIZE];
        file.read_exact_at(
            self.offset + (self.next * INDEX_ENTRY_SIZE) as u64,
            &mut bytes,
        )?;
        let decoded = decode_window(&bytes, data_end)?;
        self.next += count;
        budget.record_metadata_bytes(bytes.len() as u64);
        Ok(Arc::new(SegmentMetadata::new(decoded)))
    }
}

fn decode_window(bytes: &[u8], data_end: u64) -> Result<Vec<BlockIndex>> {
    let entries = bytes
        .as_chunks::<INDEX_ENTRY_SIZE>()
        .0
        .iter()
        .map(|entry| decode_index_entry(entry))
        .collect::<Result<Vec<_>>>()?;
    validate_index_entries(entries, data_end)
}
