//! Формат индекса: кодирование, декодирование и проверка границ блоков.

use super::{
    BLOCK_HEADER_SIZE, BLOCK_MAGIC, BlockIndex, CountSumSummary, FOOTER_MAGIC, FOOTER_SIZE,
    FooterMetadata, HEADER_SIZE, INDEX_ENTRY_SIZE, INDEX_FLAG_COUNT_SUM_UNAVAILABLE,
    INDEX_HEADER_SIZE, INDEX_MAGIC, INDEX_PAGE_DIRECTORY_SIZE, INDEX_PAGE_HEADER_SIZE,
    INDEX_PAGE_MAGIC, IndexPageDescriptor, LEGACY_INDEX_ENTRY_SIZE, MAX_INDEX_BYTES,
    MAX_STORED_BLOCK_OVERHEAD, MembershipFilter, PAGED_INDEX_HEADER_SIZE, PAGED_INDEX_MAGIC,
    PagedIndexRoot, SEGMENT_MAGIC, SegmentDescriptor, TARGET_INDEX_PAGE_BLOCKS,
    read_count_sum_summary,
};
use crate::aggregate;
use crate::error::{Result, corrupt, invalid};
use crate::types::SeriesKey;
use crate::wal::{put_i64, put_u16, put_u32, put_u64, read_i64, read_u16, read_u32, read_u64};
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;

pub(super) fn validate_descriptor(descriptor: &SegmentDescriptor) -> Result<()> {
    if descriptor.id == 0
        || descriptor.level > 2
        || descriptor.policy_id == 0
        || descriptor.window_start_us >= descriptor.window_end_us
        || descriptor.min_timestamp_us > descriptor.max_timestamp_us
        || descriptor.min_timestamp_us < descriptor.window_start_us
        || descriptor.max_timestamp_us >= descriptor.window_end_us
        || descriptor.min_commit > descriptor.max_commit
        || descriptor.record_count == 0
        || descriptor.pack_id == Some(0)
        || (descriptor.pack_id.is_none() && descriptor.file_offset != 0)
        || descriptor
            .file_offset
            .checked_add(descriptor.file_size)
            .is_none()
    {
        return Err(corrupt("invalid segment descriptor"));
    }
    Ok(())
}

pub(super) fn validate_header(
    header: &[u8; HEADER_SIZE],
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
    path: &Path,
) -> Result<()> {
    if &header[..8] != SEGMENT_MAGIC
        || read_u16(header, 8) != 3
        || read_u16(header, 10) as usize != HEADER_SIZE
        || header[12..28] != storage_uuid
        || read_u64(header, 28) != descriptor.id
        || read_u16(header, 36) != descriptor.shard_id
        || read_u32(header, 38) != descriptor.policy_id
        || header[42] != descriptor.level
        || header[43] != 0
        || read_i64(header, 44) != descriptor.window_start_us
        || read_i64(header, 52) != descriptor.window_end_us
        || read_i64(header, 60) != descriptor.min_timestamp_us
        || read_i64(header, 68) != descriptor.max_timestamp_us
        || read_u64(header, 76) != descriptor.min_commit
        || read_u64(header, 84) != descriptor.max_commit
        || read_u64(header, 92) != 0
        || read_u64(header, 100) != 0
        || read_u32(header, 108) != crc32c::crc32c(&header[..108])
    {
        return Err(corrupt(format!(
            "invalid Chimaera v1 segment header: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(super) fn validate_footer(
    footer: &[u8; FOOTER_SIZE],
    file_length: u64,
) -> Result<FooterMetadata> {
    let version = read_u16(footer, 8);
    let index_offset = read_u64(footer, 12);
    let index_length = read_u64(footer, 20);
    let aggregate_length = read_u32(footer, 48);
    let aggregate_crc = read_u32(footer, 52);
    if &footer[..8] != FOOTER_MAGIC
        || !matches!(version, 1..=3)
        || read_u16(footer, 10) as usize != FOOTER_SIZE
        || read_u64(footer, 36) != file_length
        || read_u32(footer, 56) != 0
        || read_u32(footer, 60) != crc32c::crc32c(&footer[..60])
        || (version == 1 && (aggregate_length != 0 || aggregate_crc != 0))
        || (version == 2 && aggregate_length < aggregate::MIN_SECTION_SIZE as u32)
        || (version == 3
            && aggregate_length != 0
            && aggregate_length < aggregate::MIN_SECTION_SIZE as u32)
        || aggregate_length as usize > aggregate::MAX_BYTES
        || index_length > MAX_INDEX_BYTES as u64
        || index_offset < HEADER_SIZE as u64 + u64::from(aggregate_length)
        || index_offset
            .checked_add(index_length)
            .and_then(|value| value.checked_add(FOOTER_SIZE as u64))
            != Some(file_length)
    {
        return Err(corrupt(format!(
            "invalid Chimaera v1 segment footer: version={version} index={index_offset}+{index_length} aggregate={aggregate_length} declared={} actual={file_length}",
            read_u64(footer, 36)
        )));
    }
    Ok(FooterMetadata {
        version,
        index_offset,
        index_length,
        index_crc: read_u32(footer, 44),
        aggregate_length,
        aggregate_crc,
    })
}

pub(super) fn paged_index_root_bytes(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < PAGED_INDEX_HEADER_SIZE || !bytes.starts_with(PAGED_INDEX_MAGIC) {
        return Err(corrupt("paged segment index root is absent"));
    }
    let root_length = read_u32(bytes, 20) as usize;
    bytes
        .get(..root_length)
        .ok_or_else(|| corrupt("paged segment index root is truncated"))
}

pub(super) fn decode_index(bytes: &[u8], data_end: u64) -> Result<Vec<BlockIndex>> {
    if bytes.starts_with(PAGED_INDEX_MAGIC) {
        return decode_paged_index(bytes, data_end);
    }
    decode_legacy_index(bytes, data_end)
}

pub(super) fn decode_legacy_index(bytes: &[u8], data_end: u64) -> Result<Vec<BlockIndex>> {
    if bytes.len() < INDEX_HEADER_SIZE
        || &bytes[..8] != INDEX_MAGIC
        || read_u16(bytes, 8) != 1
        || read_u16(bytes, 10) != 0
    {
        return Err(corrupt("invalid Chimaera v1 segment index"));
    }
    let count = read_u32(bytes, 12) as usize;
    let expected = INDEX_HEADER_SIZE
        .checked_add(
            count
                .checked_mul(LEGACY_INDEX_ENTRY_SIZE)
                .ok_or_else(|| corrupt("segment index size overflow"))?,
        )
        .ok_or_else(|| corrupt("segment index size overflow"))?;
    if bytes.len() != expected {
        return Err(corrupt("segment index length does not match entry count"));
    }
    let mut result = Vec::with_capacity(count);
    let mut previous_end = HEADER_SIZE as u64;
    let mut previous_series = None;
    for position in 0..count {
        let offset = INDEX_HEADER_SIZE + position * LEGACY_INDEX_ENTRY_SIZE;
        let entry = &bytes[offset..offset + LEGACY_INDEX_ENTRY_SIZE];
        let current = BlockIndex {
            series: SeriesKey::new(read_u32(entry, 0), read_u32(entry, 4), read_u64(entry, 8)),
            offset: read_u64(entry, 16),
            size: read_u32(entry, 24),
            min_timestamp_us: read_i64(entry, 28),
            max_timestamp_us: read_i64(entry, 36),
            count_sum: None,
        };
        let end = current.offset.saturating_add(u64::from(current.size));
        if current.offset < previous_end
            || current.size < BLOCK_HEADER_SIZE as u32
            || current.size as usize
                > BLOCK_HEADER_SIZE + crate::v1::MAX_BLOCK_BYTES + MAX_STORED_BLOCK_OVERHEAD
            || end > data_end
            || current.min_timestamp_us > current.max_timestamp_us
            || previous_series.is_some_and(|series| series > current.series)
        {
            return Err(corrupt("invalid Chimaera v1 segment index entry"));
        }
        previous_end = end;
        previous_series = Some(current.series);
        result.push(current);
    }
    Ok(result)
}

pub(super) fn decode_paged_index(bytes: &[u8], data_end: u64) -> Result<Vec<BlockIndex>> {
    let (root, root_length) = decode_paged_index_root(bytes)?;
    if root.block_count == 0 || root_length > bytes.len() {
        return Err(corrupt("invalid paged segment index root"));
    }
    let mut result = Vec::with_capacity(root.block_count as usize);
    let mut previous_page_end = root_length as u64;
    for page in &root.pages {
        if page.offset < previous_page_end {
            return Err(corrupt("paged segment index pages overlap"));
        }
        let start = usize::try_from(page.offset)
            .map_err(|_| corrupt("paged segment index offset exceeds usize"))?;
        let end = start
            .checked_add(page.length as usize)
            .ok_or_else(|| corrupt("paged segment index page length overflow"))?;
        let page_bytes = bytes
            .get(start..end)
            .ok_or_else(|| corrupt("paged segment index page is outside its section"))?;
        if crc32c::crc32c(page_bytes) != page.crc32c {
            return Err(corrupt("paged segment index page CRC32C mismatch"));
        }
        let decoded = decode_index_page(page_bytes, data_end)?;
        if decoded.len() != page.block_count as usize
            || decoded.first().map(|entry| entry.series) != Some(page.first_series)
            || decoded.last().map(|entry| entry.series) != Some(page.last_series)
            || decoded
                .iter()
                .any(|entry| !root.membership.contains(entry.series))
        {
            return Err(corrupt("paged segment index page disagrees with its root"));
        }
        result.extend(decoded);
        previous_page_end = end as u64;
    }
    if result.len() != root.block_count as usize || previous_page_end != bytes.len() as u64 {
        return Err(corrupt(
            "paged segment index length or block count mismatch",
        ));
    }
    validate_index_entries(result, data_end)
}

pub(super) fn paged_index_root_layout(
    header: &[u8],
    available_length: usize,
) -> Result<(usize, u32, usize, usize)> {
    if header.len() < PAGED_INDEX_HEADER_SIZE
        || &header[..8] != PAGED_INDEX_MAGIC
        || read_u16(header, 8) != 3
        || read_u16(header, 10) as usize != INDEX_PAGE_DIRECTORY_SIZE
    {
        return Err(corrupt("invalid paged segment index header"));
    }
    let page_count = read_u32(header, 12) as usize;
    let block_count = read_u32(header, 16);
    let root_length = read_u32(header, 20) as usize;
    let membership_word_count = read_u32(header, 24) as usize;
    let membership_bytes = membership_word_count
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| corrupt("paged segment membership filter size overflow"))?;
    let directory_offset = PAGED_INDEX_HEADER_SIZE
        .checked_add(membership_bytes)
        .ok_or_else(|| corrupt("paged segment index root size overflow"))?;
    let expected_root_length = directory_offset
        .checked_add(
            page_count
                .checked_mul(INDEX_PAGE_DIRECTORY_SIZE)
                .ok_or_else(|| corrupt("paged segment index root size overflow"))?,
        )
        .ok_or_else(|| corrupt("paged segment index root size overflow"))?;
    if page_count == 0
        || block_count == 0
        || membership_word_count == 0
        || !membership_word_count.is_power_of_two()
        || root_length != expected_root_length
        || root_length > available_length
        || root_length > MAX_INDEX_BYTES
    {
        return Err(corrupt("paged segment index root length mismatch"));
    }
    Ok((page_count, block_count, root_length, directory_offset))
}

pub(super) fn decode_index_page_descriptor(entry: &[u8]) -> Result<IndexPageDescriptor> {
    if entry.len() != INDEX_PAGE_DIRECTORY_SIZE || read_u32(entry, 52) != 0 {
        return Err(corrupt("invalid paged segment index directory"));
    }
    Ok(IndexPageDescriptor {
        first_series: SeriesKey::new(read_u32(entry, 0), read_u32(entry, 4), read_u64(entry, 8)),
        last_series: SeriesKey::new(
            read_u32(entry, 16),
            read_u32(entry, 20),
            read_u64(entry, 24),
        ),
        offset: read_u64(entry, 32),
        length: read_u32(entry, 40),
        block_count: read_u32(entry, 44),
        crc32c: read_u32(entry, 48),
    })
}

pub(super) fn validate_index_page_descriptor(
    descriptor: IndexPageDescriptor,
    previous_last: Option<SeriesKey>,
    expected_offset: u64,
) -> Result<()> {
    if descriptor.first_series > descriptor.last_series
        || previous_last.is_some_and(|previous| previous >= descriptor.first_series)
        || descriptor.offset != expected_offset
        || descriptor.length < INDEX_PAGE_HEADER_SIZE as u32 + INDEX_ENTRY_SIZE as u32
        || descriptor.block_count == 0
    {
        return Err(corrupt("invalid paged segment index directory"));
    }
    Ok(())
}

pub(super) fn decode_paged_index_root(bytes: &[u8]) -> Result<(PagedIndexRoot, usize)> {
    let (page_count, block_count, root_length, directory_offset) =
        paged_index_root_layout(bytes, bytes.len())?;
    let membership_slice = &bytes[PAGED_INDEX_HEADER_SIZE..directory_offset];
    if crc32c::crc32c(membership_slice) != read_u32(bytes, 28) {
        return Err(corrupt("paged segment membership filter CRC32C mismatch"));
    }
    let membership = MembershipFilter::from_words(
        membership_slice
            .as_chunks::<8>()
            .0
            .iter()
            .map(|word| read_u64(word, 0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    )?;
    let mut pages = Vec::with_capacity(page_count);
    let mut previous_last = None;
    let mut expected_offset = root_length as u64;
    let mut counted_blocks = 0_u64;
    for position in 0..page_count {
        let offset = directory_offset + position * INDEX_PAGE_DIRECTORY_SIZE;
        let entry = &bytes[offset..offset + INDEX_PAGE_DIRECTORY_SIZE];
        let descriptor = decode_index_page_descriptor(entry)?;
        validate_index_page_descriptor(descriptor, previous_last, expected_offset)?;
        if !membership.contains(descriptor.first_series)
            || !membership.contains(descriptor.last_series)
        {
            return Err(corrupt("invalid paged segment index directory"));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(descriptor.length))
            .ok_or_else(|| corrupt("paged segment index page end overflow"))?;
        counted_blocks = counted_blocks.saturating_add(u64::from(descriptor.block_count));
        previous_last = Some(descriptor.last_series);
        pages.push(descriptor);
    }
    if counted_blocks != u64::from(block_count) {
        return Err(corrupt("paged segment index root block count mismatch"));
    }
    Ok((
        PagedIndexRoot {
            pages: pages.into_boxed_slice(),
            block_count,
            membership,
        },
        root_length,
    ))
}

pub(super) fn decode_index_page(bytes: &[u8], data_end: u64) -> Result<Vec<BlockIndex>> {
    if bytes.len() < INDEX_PAGE_HEADER_SIZE
        || &bytes[..8] != INDEX_PAGE_MAGIC
        || read_u16(bytes, 8) != 2
        || read_u16(bytes, 10) as usize != INDEX_ENTRY_SIZE
    {
        return Err(corrupt("invalid segment index page header"));
    }
    let count = read_u32(bytes, 12) as usize;
    let expected = INDEX_PAGE_HEADER_SIZE
        .checked_add(
            count
                .checked_mul(INDEX_ENTRY_SIZE)
                .ok_or_else(|| corrupt("segment index page size overflow"))?,
        )
        .ok_or_else(|| corrupt("segment index page size overflow"))?;
    if count == 0 || bytes.len() != expected {
        return Err(corrupt("segment index page length mismatch"));
    }
    let mut result = Vec::with_capacity(count);
    for position in 0..count {
        let offset = INDEX_PAGE_HEADER_SIZE + position * INDEX_ENTRY_SIZE;
        result.push(decode_index_entry(
            &bytes[offset..offset + INDEX_ENTRY_SIZE],
        )?);
    }
    validate_index_entries(result, data_end)
}

pub(super) fn decode_index_entry(bytes: &[u8]) -> Result<BlockIndex> {
    if bytes.len() != INDEX_ENTRY_SIZE {
        return Err(corrupt("invalid Chimaera segment index entry"));
    }
    let flags = read_u32(bytes, 44);
    let count_sum = match flags {
        0 => Some(CountSumSummary {
            count: read_u64(bytes, 48),
            raw_sum: f64::from_bits(read_u64(bytes, 56)),
            compensation: f64::from_bits(read_u64(bytes, 64)),
        }),
        INDEX_FLAG_COUNT_SUM_UNAVAILABLE
            if read_u64(bytes, 48) == 0 && read_u64(bytes, 56) == 0 && read_u64(bytes, 64) == 0 =>
        {
            None
        }
        _ => return Err(corrupt("invalid Chimaera segment index entry flags")),
    };
    Ok(BlockIndex {
        series: SeriesKey::new(read_u32(bytes, 0), read_u32(bytes, 4), read_u64(bytes, 8)),
        offset: read_u64(bytes, 16),
        size: read_u32(bytes, 24),
        min_timestamp_us: read_i64(bytes, 28),
        max_timestamp_us: read_i64(bytes, 36),
        count_sum,
    })
}

pub(super) fn validate_index_entries(
    index: Vec<BlockIndex>,
    data_end: u64,
) -> Result<Vec<BlockIndex>> {
    let mut previous_end = HEADER_SIZE as u64;
    let mut previous_series = None;
    for current in &index {
        let end = current.offset.saturating_add(u64::from(current.size));
        if current.offset < previous_end
            || current.size < BLOCK_HEADER_SIZE as u32
            || current.size as usize
                > BLOCK_HEADER_SIZE + crate::v1::MAX_BLOCK_BYTES + MAX_STORED_BLOCK_OVERHEAD
            || end > data_end
            || current.min_timestamp_us > current.max_timestamp_us
            || current
                .count_sum
                .is_some_and(|summary| read_count_sum_summary(summary).is_err())
            || previous_series.is_some_and(|series| series > current.series)
        {
            return Err(corrupt("invalid Chimaera segment index entry"));
        }
        previous_end = end;
        previous_series = Some(current.series);
    }
    Ok(index)
}

pub(super) fn validate_block_headers(
    file: &mut File,
    descriptor: &SegmentDescriptor,
    index: &[BlockIndex],
) -> Result<Option<[u8; 4]>> {
    let mut records = 0_u64;
    let mut min_commit = u64::MAX;
    let mut max_commit = 0_u64;
    let mut storage_signature = None;
    let mut mixed_storage_profiles = false;
    for entry in index {
        file.seek(SeekFrom::Start(descriptor.file_offset + entry.offset))?;
        let mut header = [0_u8; BLOCK_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let record_count = read_u32(&header, 56);
        let put_count = read_u32(&header, 60);
        let delete_count = read_u32(&header, 64);
        let block_min_commit = read_u64(&header, 40);
        let block_max_commit = read_u64(&header, 48);
        if &header[..4] != BLOCK_MAGIC
            || read_u16(&header, 4) as usize != BLOCK_HEADER_SIZE
            || header[6] != 1
            || header[7] != 0
            || SeriesKey::new(
                read_u32(&header, 8),
                read_u32(&header, 12),
                read_u64(&header, 16),
            ) != entry.series
            || read_i64(&header, 24) != entry.min_timestamp_us
            || read_i64(&header, 32) != entry.max_timestamp_us
            || block_min_commit > block_max_commit
            || record_count == 0
            || put_count.saturating_add(delete_count) != record_count
            || read_u32(&header, 76).saturating_add(BLOCK_HEADER_SIZE as u32) != entry.size
            || read_u32(&header, 84) != crc32c::crc32c(&header[..84])
        {
            return Err(corrupt("segment block header does not match index"));
        }
        let current_signature = <[u8; 4]>::try_from(&header[68..72])
            .map_err(|_| corrupt("segment block profile is truncated"))?;
        match storage_signature {
            None => storage_signature = Some(current_signature),
            Some(signature) if signature != current_signature => mixed_storage_profiles = true,
            Some(_) => {}
        }
        records = records
            .checked_add(u64::from(record_count))
            .ok_or_else(|| corrupt("segment record count overflow"))?;
        min_commit = min_commit.min(block_min_commit);
        max_commit = max_commit.max(block_max_commit);
    }
    if records != descriptor.record_count
        || min_commit != descriptor.min_commit
        || max_commit != descriptor.max_commit
    {
        return Err(corrupt("segment block summaries do not match descriptor"));
    }
    Ok((!mixed_storage_profiles)
        .then_some(storage_signature)
        .flatten())
}

pub(super) fn encode_index(index: &[BlockIndex]) -> Result<Vec<u8>> {
    if index.is_empty() {
        return Err(invalid("cannot encode an empty segment index"));
    }
    let ranges = index_page_ranges(index);
    let membership = MembershipFilter::from_index(index);
    let membership_bytes = usize::try_from(membership.bytes())
        .map_err(|_| invalid("segment membership filter exceeds usize"))?;
    let directory_offset = PAGED_INDEX_HEADER_SIZE
        .checked_add(membership_bytes)
        .ok_or_else(|| invalid("segment index root size overflow"))?;
    let root_length = directory_offset
        .checked_add(
            ranges
                .len()
                .checked_mul(INDEX_PAGE_DIRECTORY_SIZE)
                .ok_or_else(|| invalid("segment index root size overflow"))?,
        )
        .ok_or_else(|| invalid("segment index root size overflow"))?;
    let page_bytes = ranges.iter().try_fold(0_usize, |total, range| {
        total
            .checked_add(INDEX_PAGE_HEADER_SIZE)
            .and_then(|value| {
                value.checked_add((range.end - range.start).checked_mul(INDEX_ENTRY_SIZE)?)
            })
            .ok_or_else(|| invalid("segment index size overflow"))
    })?;
    let length = root_length
        .checked_add(page_bytes)
        .ok_or_else(|| invalid("segment index size overflow"))?;
    if length > MAX_INDEX_BYTES {
        return Err(invalid("segment index exceeds hard limit"));
    }
    let mut bytes = vec![0_u8; length];
    bytes[..8].copy_from_slice(PAGED_INDEX_MAGIC);
    put_u16(&mut bytes, 8, 3);
    put_u16(&mut bytes, 10, INDEX_PAGE_DIRECTORY_SIZE as u16);
    put_u32(
        &mut bytes,
        12,
        u32::try_from(ranges.len()).map_err(|_| invalid("too many segment index pages"))?,
    );
    put_u32(
        &mut bytes,
        16,
        u32::try_from(index.len()).map_err(|_| invalid("too many segment blocks"))?,
    );
    put_u32(
        &mut bytes,
        20,
        u32::try_from(root_length).map_err(|_| invalid("segment index root exceeds u32"))?,
    );
    put_u32(
        &mut bytes,
        24,
        u32::try_from(membership.words.len())
            .map_err(|_| invalid("segment membership filter exceeds u32 words"))?,
    );
    for (position, word) in membership.words.iter().enumerate() {
        put_u64(
            &mut bytes,
            PAGED_INDEX_HEADER_SIZE + position * std::mem::size_of::<u64>(),
            *word,
        );
    }
    let membership_crc = crc32c::crc32c(&bytes[PAGED_INDEX_HEADER_SIZE..directory_offset]);
    put_u32(&mut bytes, 28, membership_crc);
    let mut page_offset = root_length;
    for (page_number, range) in ranges.iter().enumerate() {
        let page_length = INDEX_PAGE_HEADER_SIZE + (range.end - range.start) * INDEX_ENTRY_SIZE;
        let page_crc = {
            let page = &mut bytes[page_offset..page_offset + page_length];
            page[..8].copy_from_slice(INDEX_PAGE_MAGIC);
            put_u16(page, 8, 2);
            put_u16(page, 10, INDEX_ENTRY_SIZE as u16);
            put_u32(
                page,
                12,
                u32::try_from(range.end - range.start)
                    .map_err(|_| invalid("too many blocks in an index page"))?,
            );
            for (position, entry) in index[range.clone()].iter().enumerate() {
                let offset = INDEX_PAGE_HEADER_SIZE + position * INDEX_ENTRY_SIZE;
                encode_index_entry(&mut page[offset..offset + INDEX_ENTRY_SIZE], *entry);
            }
            crc32c::crc32c(page)
        };
        let page_directory_offset = directory_offset + page_number * INDEX_PAGE_DIRECTORY_SIZE;
        let directory =
            &mut bytes[page_directory_offset..page_directory_offset + INDEX_PAGE_DIRECTORY_SIZE];
        let first = index[range.start].series;
        let last = index[range.end - 1].series;
        put_u32(directory, 0, first.space_id);
        put_u32(directory, 4, first.bucket_id);
        put_u64(directory, 8, first.key);
        put_u32(directory, 16, last.space_id);
        put_u32(directory, 20, last.bucket_id);
        put_u64(directory, 24, last.key);
        put_u64(
            directory,
            32,
            u64::try_from(page_offset).map_err(|_| invalid("index page offset exceeds u64"))?,
        );
        put_u32(
            directory,
            40,
            u32::try_from(page_length).map_err(|_| invalid("index page exceeds u32"))?,
        );
        put_u32(
            directory,
            44,
            u32::try_from(range.end - range.start)
                .map_err(|_| invalid("too many blocks in an index page"))?,
        );
        put_u32(directory, 48, page_crc);
        page_offset += page_length;
    }
    Ok(bytes)
}

pub(super) fn index_page_ranges(index: &[BlockIndex]) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    let mut position = 0_usize;
    while position < index.len() {
        let series = index[position].series;
        let series_start = position;
        while position < index.len() && index[position].series == series {
            position += 1;
        }
        if series_start > start && position - start > TARGET_INDEX_PAGE_BLOCKS {
            ranges.push(start..series_start);
            start = series_start;
        }
    }
    if start < index.len() {
        ranges.push(start..index.len());
    }
    ranges
}

pub(super) fn encode_index_entry(bytes: &mut [u8], entry: BlockIndex) {
    put_u32(bytes, 0, entry.series.space_id);
    put_u32(bytes, 4, entry.series.bucket_id);
    put_u64(bytes, 8, entry.series.key);
    put_u64(bytes, 16, entry.offset);
    put_u32(bytes, 24, entry.size);
    put_i64(bytes, 28, entry.min_timestamp_us);
    put_i64(bytes, 36, entry.max_timestamp_us);
    if let Some(summary) = entry.count_sum {
        put_u32(bytes, 44, 0);
        put_u64(bytes, 48, summary.count);
        put_u64(bytes, 56, summary.raw_sum.to_bits());
        put_u64(bytes, 64, summary.compensation.to_bits());
    } else {
        put_u32(bytes, 44, INDEX_FLAG_COUNT_SUM_UNAVAILABLE);
        put_u64(bytes, 48, 0);
        put_u64(bytes, 56, 0);
        put_u64(bytes, 64, 0);
    }
}

pub(super) fn series_count(index: &[BlockIndex]) -> usize {
    index
        .iter()
        .enumerate()
        .filter(|(position, entry)| *position == 0 || index[*position - 1].series != entry.series)
        .count()
}

pub(super) fn usize_u32(value: usize, name: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| invalid(format!("{name} exceeds u32")))
}
