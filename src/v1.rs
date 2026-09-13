//! Потоковый read-only декодер логического сегмента Chimaera v1.
//!
//! Поддержаны отдельные `.tss` и логические области `.tsp`, все фактически
//! используемые кодеки v1 (`RawInt64`, `DeltaVarint`, `DeltaOfDelta`,
//! `RawFloat64`, `GorillaXor`, `None`, `Gzip`, `Zstd`) и put/delete. [`StorageImage`]
//! открывает checkpoint-образ Chimaera через `IDENTITY`, `CURRENT` и журнал
//! manifest без изменения исходных файлов.

mod storage;
mod zstd_block;

pub use storage::{Identity, SegmentDescriptor, StorageImage, WalReference};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use flate2::bufread::GzDecoder;

use crate::error::{Result, corrupt, invalid};
use crate::types::{Point, SeriesKey};
use crate::wal::{read_i64, read_u16, read_u32, read_u64};

const SEGMENT_MAGIC: &[u8; 8] = b"TSSEG001";
const INDEX_MAGIC: &[u8; 8] = b"TSIDX001";
const PAGED_INDEX_MAGIC: &[u8; 8] = b"TSIDX002";
const INDEX_PAGE_MAGIC: &[u8; 8] = b"TSIPG001";
const FOOTER_MAGIC: &[u8; 8] = b"TSFTR001";
const BLOCK_MAGIC: &[u8; 4] = b"TSBL";
const HEADER_SIZE: usize = 112;
const BLOCK_HEADER_SIZE: usize = 88;
const INDEX_HEADER_SIZE: usize = 16;
const LEGACY_INDEX_ENTRY_SIZE: usize = 44;
const INDEX_ENTRY_SIZE: usize = 72;
const INDEX_FLAG_COUNT_SUM_UNAVAILABLE: u32 = 1;
const PAGED_INDEX_HEADER_SIZE: usize = 32;
const INDEX_PAGE_DIRECTORY_SIZE: usize = 56;
const INDEX_PAGE_HEADER_SIZE: usize = 16;
const FOOTER_SIZE: usize = 64;
pub(crate) const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;
const MAX_STORED_OVERHEAD: usize = 64 * 1024;
const MAX_INDEX_BYTES: usize = 256 * 1024 * 1024;

/// Вид физической операции v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Put,
    Delete,
}

/// Физическая версия точки из сегмента v1.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Operation {
    pub series: SeriesKey,
    pub timestamp_us: i64,
    pub commit_id: u64,
    pub ordinal: u32,
    pub kind: OperationKind,
    pub value: Option<f64>,
}

/// Проверенные границы одного логического сегмента.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentSummary {
    pub storage_uuid: [u8; 16],
    pub segment_id: u64,
    pub shard_id: u16,
    pub policy_id: u32,
    pub level: u8,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub min_timestamp_us: i64,
    pub max_timestamp_us: i64,
    pub min_commit_id: u64,
    pub max_commit_id: u64,
    pub block_count: u32,
    pub series_count: u32,
    pub operation_count: u64,
}

#[derive(Clone, Copy)]
struct SegmentHeader {
    storage_uuid: [u8; 16],
    segment_id: u64,
    shard_id: u16,
    policy_id: u32,
    level: u8,
    window_start: i64,
    window_end: i64,
    min_timestamp: i64,
    max_timestamp: i64,
    min_commit: u64,
    max_commit: u64,
}

#[derive(Clone, Copy)]
struct IndexEntry {
    series: SeriesKey,
    offset: u64,
    size: u32,
    min_timestamp: i64,
    max_timestamp: i64,
    count_sum: Option<(u64, f64, f64)>,
}

/// Потоково проверяет и декодирует одну логическую область v1.
///
/// `file_offset` и `logical_length` позволяют читать область из `.tsp`. В памяти
/// одновременно находятся индекс и один распакованный блок.
///
/// # Errors
///
/// Возвращает ошибку при нарушении границ, CRC, порядка, кодека или числового
/// инварианта формата v1.
pub fn visit_segment(
    path: impl AsRef<Path>,
    file_offset: u64,
    logical_length: u64,
    mut visitor: impl FnMut(Operation) -> Result<()>,
) -> Result<SegmentSummary> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    let physical_length = file.metadata()?.len();
    let logical_end = file_offset
        .checked_add(logical_length)
        .ok_or_else(|| corrupt("v1 logical segment length overflow"))?;
    if logical_length < (HEADER_SIZE + FOOTER_SIZE) as u64 || logical_end > physical_length {
        return Err(corrupt("v1 logical segment is outside the physical file"));
    }
    file.seek(SeekFrom::Start(file_offset))?;
    let mut header_bytes = [0_u8; HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = decode_segment_header(&header_bytes)?;

    file.seek(SeekFrom::Start(logical_end - FOOTER_SIZE as u64))?;
    let mut footer = [0_u8; FOOTER_SIZE];
    file.read_exact(&mut footer)?;
    if &footer[..8] != FOOTER_MAGIC
        || !matches!(read_u16(&footer, 8), 1..=3)
        || read_u16(&footer, 10) as usize != FOOTER_SIZE
        || read_u64(&footer, 36) != logical_length
        || read_u32(&footer, 56) != 0
        || read_u32(&footer, 60) != crc32c::crc32c(&footer[..60])
    {
        return Err(corrupt("invalid Chimaera v1 footer"));
    }
    let footer_version = read_u16(&footer, 8);
    let index_offset = read_u64(&footer, 12);
    let index_length = read_u64(&footer, 20);
    let series_count = read_u32(&footer, 28);
    let block_count = read_u32(&footer, 32);
    let aggregate_length = read_u32(&footer, 48) as u64;
    let aggregate_crc = read_u32(&footer, 52);
    if (footer_version == 1 && (aggregate_length != 0 || aggregate_crc != 0))
        || (footer_version == 2 && aggregate_length < 24)
        || (footer_version == 3 && aggregate_length != 0 && aggregate_length < 24)
        || aggregate_length > crate::aggregate::MAX_BYTES as u64
        || index_length < INDEX_HEADER_SIZE as u64
        || index_length > MAX_INDEX_BYTES as u64
        || index_offset < HEADER_SIZE as u64 + aggregate_length
        || index_offset
            .checked_add(index_length)
            .and_then(|value| value.checked_add(FOOTER_SIZE as u64))
            != Some(logical_length)
    {
        return Err(corrupt("invalid Chimaera v1 footer bounds"));
    }
    if aggregate_length > 0 {
        let aggregate_offset = index_offset - aggregate_length;
        file.seek(SeekFrom::Start(file_offset + aggregate_offset))?;
        let mut aggregate = vec![0_u8; usize_from_u64(aggregate_length, "aggregate length")?];
        file.read_exact(&mut aggregate)?;
        let checksum_bytes = if footer_version == 3 {
            crate::aggregate::checksum_bytes(&aggregate)?
        } else {
            aggregate.as_slice()
        };
        if crc32c::crc32c(checksum_bytes) != aggregate_crc
            || !(aggregate.starts_with(b"TSAGG002") || aggregate.starts_with(b"TSAGG003"))
        {
            return Err(corrupt("invalid Chimaera v1 aggregate section"));
        }
    }
    file.seek(SeekFrom::Start(file_offset + index_offset))?;
    let mut index_bytes = vec![0_u8; usize_from_u64(index_length, "index length")?];
    file.read_exact(&mut index_bytes)?;
    let checksum_bytes = if footer_version == 3 {
        paged_index_root_bytes(&index_bytes)?
    } else {
        index_bytes.as_slice()
    };
    if crc32c::crc32c(checksum_bytes) != read_u32(&footer, 44) {
        return Err(corrupt("Chimaera v1 index CRC32C mismatch"));
    }
    let index = decode_index(
        &index_bytes,
        logical_length,
        index_offset - aggregate_length,
    )?;
    if index.len() != block_count as usize {
        return Err(corrupt("Chimaera v1 footer block count mismatch"));
    }
    let actual_series_count = index
        .iter()
        .enumerate()
        .filter(|(position, entry)| *position == 0 || index[*position - 1].series != entry.series)
        .count();
    if actual_series_count != series_count as usize {
        return Err(corrupt("Chimaera v1 footer series count mismatch"));
    }

    let mut prior: Option<(SeriesKey, i64, u64, u32)> = None;
    let mut operation_count = 0_u64;
    let mut observed_min_timestamp = i64::MAX;
    let mut observed_max_timestamp = i64::MIN;
    let mut observed_min_commit = u64::MAX;
    let mut observed_max_commit = 0_u64;
    for descriptor in &index {
        file.seek(SeekFrom::Start(file_offset + descriptor.offset))?;
        let mut block = vec![0_u8; descriptor.size as usize];
        file.read_exact(&mut block)?;
        let operations = decode_block(&block)?;
        if operations.is_empty()
            || operations[0].series != descriptor.series
            || operations[0].timestamp_us != descriptor.min_timestamp
            || operations
                .last()
                .is_none_or(|last| last.timestamp_us != descriptor.max_timestamp)
        {
            return Err(corrupt("Chimaera v1 block does not match its index"));
        }
        if let Some((expected_count, expected_raw_sum, expected_compensation)) =
            descriptor.count_sum
        {
            let mut state =
                crate::types::AggregateState::for_aggregates(crate::types::AggregateSet::COUNT_SUM);
            for operation in operations
                .iter()
                .filter(|operation| operation.kind == OperationKind::Put)
            {
                state.add(
                    operation.timestamp_us,
                    operation
                        .value
                        .ok_or_else(|| corrupt("put operation is missing a value"))?,
                )?;
            }
            let actual = state.count_sum_parts();
            if actual != (expected_count, expected_raw_sum, expected_compensation) {
                return Err(corrupt(
                    "Chimaera block count/sum summary does not match its payload",
                ));
            }
        }
        for operation in operations {
            let order = (
                operation.series,
                operation.timestamp_us,
                operation.commit_id,
                operation.ordinal,
            );
            if prior.is_some_and(|previous| previous > order) {
                return Err(corrupt("Chimaera v1 operations are not globally sorted"));
            }
            prior = Some(order);
            observed_min_timestamp = observed_min_timestamp.min(operation.timestamp_us);
            observed_max_timestamp = observed_max_timestamp.max(operation.timestamp_us);
            observed_min_commit = observed_min_commit.min(operation.commit_id);
            observed_max_commit = observed_max_commit.max(operation.commit_id);
            operation_count = operation_count
                .checked_add(1)
                .ok_or_else(|| corrupt("Chimaera v1 operation count overflow"))?;
            visitor(operation)?;
        }
    }
    if operation_count == 0
        || observed_min_timestamp != header.min_timestamp
        || observed_max_timestamp != header.max_timestamp
        || observed_min_commit != header.min_commit
        || observed_max_commit != header.max_commit
    {
        return Err(corrupt(
            "Chimaera v1 segment summary does not match its blocks",
        ));
    }
    Ok(SegmentSummary {
        storage_uuid: header.storage_uuid,
        segment_id: header.segment_id,
        shard_id: header.shard_id,
        policy_id: header.policy_id,
        level: header.level,
        window_start_us: header.window_start,
        window_end_us: header.window_end,
        min_timestamp_us: header.min_timestamp,
        max_timestamp_us: header.max_timestamp,
        min_commit_id: header.min_commit,
        max_commit_id: header.max_commit,
        block_count,
        series_count,
        operation_count,
    })
}

/// Читает отдельный `.tss` и возвращает физические версии.
///
/// # Errors
///
/// Возвращает ошибку декодирования или I/O. Для больших файлов предпочтительнее
/// [`visit_segment`], который не материализует весь результат.
pub fn read_operations(path: impl AsRef<Path>) -> Result<Vec<Operation>> {
    let path = path.as_ref();
    let length = path.metadata()?.len();
    let mut operations = Vec::new();
    visit_segment(path, 0, length, |operation| {
        operations.push(operation);
        Ok(())
    })?;
    Ok(operations)
}

/// Читает отдельный `.tss` как логические точки на максимальном commit ID.
///
/// # Errors
///
/// Возвращает ошибку декодирования или I/O.
pub fn read_segment(path: impl AsRef<Path>) -> Result<Vec<Point>> {
    let operations = read_operations(path)?;
    let mut visible = BTreeMap::<(SeriesKey, i64), Operation>::new();
    for operation in operations {
        let key = (operation.series, operation.timestamp_us);
        if visible.get(&key).is_none_or(|current| {
            (operation.commit_id, operation.ordinal) > (current.commit_id, current.ordinal)
        }) {
            visible.insert(key, operation);
        }
    }
    visible
        .into_values()
        .filter(|operation| operation.kind == OperationKind::Put)
        .map(|operation| {
            Point::new(
                operation.series,
                operation.timestamp_us,
                operation
                    .value
                    .ok_or_else(|| corrupt("put operation is missing a value"))?,
            )
        })
        .collect()
}

fn decode_segment_header(bytes: &[u8; HEADER_SIZE]) -> Result<SegmentHeader> {
    let version = read_u16(bytes, 8);
    if &bytes[..8] != SEGMENT_MAGIC
        || !matches!(version, 1..=3)
        || read_u16(bytes, 10) as usize != HEADER_SIZE
        || bytes[43] != 0
        || (version == 1 && (read_u32(bytes, 100) != 0 || read_u32(bytes, 104) != 0))
        || (version == 3 && (read_u64(bytes, 92) != 0 || read_u64(bytes, 100) != 0))
        || read_u32(bytes, 108) != crc32c::crc32c(&bytes[..108])
    {
        return Err(corrupt("invalid Chimaera v1 segment header"));
    }
    let header = SegmentHeader {
        storage_uuid: bytes[12..28]
            .try_into()
            .expect("fixed-size segment UUID slice"),
        segment_id: read_u64(bytes, 28),
        shard_id: read_u16(bytes, 36),
        policy_id: read_u32(bytes, 38),
        level: bytes[42],
        window_start: read_i64(bytes, 44),
        window_end: read_i64(bytes, 52),
        min_timestamp: read_i64(bytes, 60),
        max_timestamp: read_i64(bytes, 68),
        min_commit: read_u64(bytes, 76),
        max_commit: read_u64(bytes, 84),
    };
    if header.level > 2
        || header.window_start >= header.window_end
        || header.min_timestamp > header.max_timestamp
        || header.min_timestamp < header.window_start
        || header.max_timestamp >= header.window_end
        || header.min_commit > header.max_commit
    {
        return Err(corrupt("invalid Chimaera v1 segment header bounds"));
    }
    Ok(header)
}

fn decode_index(bytes: &[u8], logical_length: u64, data_end: u64) -> Result<Vec<IndexEntry>> {
    if bytes.starts_with(PAGED_INDEX_MAGIC) {
        return decode_paged_index(bytes, logical_length, data_end);
    }
    decode_legacy_index(bytes, logical_length, data_end)
}

fn decode_legacy_index(
    bytes: &[u8],
    logical_length: u64,
    data_end: u64,
) -> Result<Vec<IndexEntry>> {
    if bytes.len() < INDEX_HEADER_SIZE
        || &bytes[..8] != INDEX_MAGIC
        || read_u16(bytes, 8) != 1
        || read_u16(bytes, 10) != 0
    {
        return Err(corrupt("invalid Chimaera v1 index header"));
    }
    let count = read_u32(bytes, 12) as usize;
    let expected = INDEX_HEADER_SIZE
        .checked_add(
            count
                .checked_mul(LEGACY_INDEX_ENTRY_SIZE)
                .ok_or_else(|| corrupt("Chimaera v1 index size overflow"))?,
        )
        .ok_or_else(|| corrupt("Chimaera v1 index size overflow"))?;
    if expected != bytes.len() {
        return Err(corrupt("Chimaera v1 index descriptor count mismatch"));
    }
    let mut result = Vec::with_capacity(count);
    let mut previous_end = HEADER_SIZE as u64;
    let mut previous_series: Option<SeriesKey> = None;
    for position in 0..count {
        let offset = INDEX_HEADER_SIZE + position * LEGACY_INDEX_ENTRY_SIZE;
        let entry = &bytes[offset..offset + LEGACY_INDEX_ENTRY_SIZE];
        let descriptor = IndexEntry {
            series: SeriesKey::new(read_u32(entry, 0), read_u32(entry, 4), read_u64(entry, 8)),
            offset: read_u64(entry, 16),
            size: read_u32(entry, 24),
            min_timestamp: read_i64(entry, 28),
            max_timestamp: read_i64(entry, 36),
            count_sum: None,
        };
        let end = descriptor.offset.saturating_add(descriptor.size as u64);
        if descriptor.offset < previous_end
            || descriptor.offset < HEADER_SIZE as u64
            || descriptor.size < BLOCK_HEADER_SIZE as u32
            || descriptor.size as usize > BLOCK_HEADER_SIZE + MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD
            || end > data_end
            || end > logical_length
            || descriptor.min_timestamp > descriptor.max_timestamp
            || previous_series.is_some_and(|series| series > descriptor.series)
        {
            return Err(corrupt("invalid Chimaera v1 index descriptor"));
        }
        previous_end = end;
        previous_series = Some(descriptor.series);
        result.push(descriptor);
    }
    Ok(result)
}

fn paged_index_root_bytes(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < PAGED_INDEX_HEADER_SIZE || !bytes.starts_with(PAGED_INDEX_MAGIC) {
        return Err(corrupt("Chimaera paged index root is absent"));
    }
    let root_length = read_u32(bytes, 20) as usize;
    bytes
        .get(..root_length)
        .ok_or_else(|| corrupt("Chimaera paged index root is truncated"))
}

fn decode_paged_index(bytes: &[u8], logical_length: u64, data_end: u64) -> Result<Vec<IndexEntry>> {
    let root = paged_index_root_bytes(bytes)?;
    let version = read_u16(root, 8);
    if !matches!(version, 2 | 3) || read_u16(root, 10) as usize != INDEX_PAGE_DIRECTORY_SIZE {
        return Err(corrupt("invalid Chimaera paged index header"));
    }
    let page_count = read_u32(root, 12) as usize;
    let block_count = read_u32(root, 16) as usize;
    let membership_word_count = if version == 3 {
        read_u32(root, 24) as usize
    } else {
        if read_u32(root, 24) != 0 || read_u32(root, 28) != 0 {
            return Err(corrupt("invalid Chimaera paged index header"));
        }
        0
    };
    let membership_bytes = membership_word_count
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| corrupt("Chimaera membership filter size overflow"))?;
    let directory_offset = PAGED_INDEX_HEADER_SIZE
        .checked_add(membership_bytes)
        .ok_or_else(|| corrupt("Chimaera paged index root size overflow"))?;
    let expected_root = directory_offset
        .checked_add(
            page_count
                .checked_mul(INDEX_PAGE_DIRECTORY_SIZE)
                .ok_or_else(|| corrupt("Chimaera paged index root size overflow"))?,
        )
        .ok_or_else(|| corrupt("Chimaera paged index root size overflow"))?;
    if page_count == 0
        || block_count == 0
        || (version == 3
            && (membership_word_count == 0 || !membership_word_count.is_power_of_two()))
        || root.len() != expected_root
    {
        return Err(corrupt("Chimaera paged index root length mismatch"));
    }
    let membership = if version == 3 {
        let encoded = &root[PAGED_INDEX_HEADER_SIZE..directory_offset];
        if crc32c::crc32c(encoded) != read_u32(root, 28) {
            return Err(corrupt("Chimaera membership filter checksum mismatch"));
        }
        Some(
            encoded
                .as_chunks::<8>()
                .0
                .iter()
                .map(|word| read_u64(word, 0))
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let mut result = Vec::with_capacity(block_count);
    let mut previous_last = None;
    let mut expected_offset = root.len() as u64;
    for position in 0..page_count {
        let offset = directory_offset + position * INDEX_PAGE_DIRECTORY_SIZE;
        let directory = &root[offset..offset + INDEX_PAGE_DIRECTORY_SIZE];
        let first = SeriesKey::new(
            read_u32(directory, 0),
            read_u32(directory, 4),
            read_u64(directory, 8),
        );
        let last = SeriesKey::new(
            read_u32(directory, 16),
            read_u32(directory, 20),
            read_u64(directory, 24),
        );
        let page_offset = read_u64(directory, 32);
        let page_length = read_u32(directory, 40) as usize;
        let page_count = read_u32(directory, 44) as usize;
        if read_u32(directory, 52) != 0
            || first > last
            || previous_last.is_some_and(|previous| previous >= first)
            || page_offset != expected_offset
            || page_count == 0
        {
            return Err(corrupt("invalid Chimaera paged index directory"));
        }
        let start = usize_from_u64(page_offset, "paged index page offset")?;
        let end = start
            .checked_add(page_length)
            .ok_or_else(|| corrupt("Chimaera paged index page overflow"))?;
        let page = bytes
            .get(start..end)
            .ok_or_else(|| corrupt("Chimaera paged index page is outside its section"))?;
        if crc32c::crc32c(page) != read_u32(directory, 48) {
            return Err(corrupt("Chimaera paged index page checksum mismatch"));
        }
        let decoded = decode_index_page(page)?;
        if decoded.len() != page_count
            || decoded.first().map(|entry| entry.series) != Some(first)
            || decoded.last().map(|entry| entry.series) != Some(last)
        {
            return Err(corrupt("Chimaera paged index page disagrees with its root"));
        }
        result.extend(decoded);
        previous_last = Some(last);
        expected_offset = end as u64;
    }
    if result.len() != block_count || expected_offset != bytes.len() as u64 {
        return Err(corrupt("Chimaera paged index block count mismatch"));
    }
    if membership.as_ref().is_some_and(|membership| {
        result
            .iter()
            .any(|entry| !membership_contains(membership, entry.series))
    }) {
        return Err(corrupt(
            "Chimaera membership filter excludes indexed series",
        ));
    }
    validate_index_entries(result, logical_length, data_end)
}

fn membership_contains(words: &[u64], series: SeriesKey) -> bool {
    let bit_mask = words.len() * u64::BITS as usize - 1;
    membership_bits(series, bit_mask)
        .into_iter()
        .all(|bit| words[bit / u64::BITS as usize] & (1_u64 << (bit % u64::BITS as usize)) != 0)
}

fn membership_bits(series: SeriesKey, bit_mask: usize) -> [usize; 3] {
    let catalog = (u64::from(series.space_id) << 32) | u64::from(series.bucket_id);
    let first = mix64(catalog ^ series.key);
    let second = mix64(series.key.rotate_left(29) ^ catalog).max(1);
    [
        first as usize & bit_mask,
        first.wrapping_add(second) as usize & bit_mask,
        first.wrapping_add(second.wrapping_mul(2)) as usize & bit_mask,
    ]
}

fn mix64(input: u64) -> u64 {
    let mut value = input.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn decode_index_page(bytes: &[u8]) -> Result<Vec<IndexEntry>> {
    if bytes.len() < INDEX_PAGE_HEADER_SIZE
        || &bytes[..8] != INDEX_PAGE_MAGIC
        || read_u16(bytes, 8) != 2
        || read_u16(bytes, 10) as usize != INDEX_ENTRY_SIZE
    {
        return Err(corrupt("invalid Chimaera index page header"));
    }
    let count = read_u32(bytes, 12) as usize;
    let expected = INDEX_PAGE_HEADER_SIZE
        .checked_add(
            count
                .checked_mul(INDEX_ENTRY_SIZE)
                .ok_or_else(|| corrupt("Chimaera index page size overflow"))?,
        )
        .ok_or_else(|| corrupt("Chimaera index page size overflow"))?;
    if count == 0 || bytes.len() != expected {
        return Err(corrupt("Chimaera index page length mismatch"));
    }
    let mut result = Vec::with_capacity(count);
    for position in 0..count {
        let offset = INDEX_PAGE_HEADER_SIZE + position * INDEX_ENTRY_SIZE;
        let entry = &bytes[offset..offset + INDEX_ENTRY_SIZE];
        let flags = read_u32(entry, 44);
        let count_sum = match flags {
            0 => {
                let count = read_u64(entry, 48);
                let raw_sum = f64::from_bits(read_u64(entry, 56));
                let compensation = f64::from_bits(read_u64(entry, 64));
                crate::types::AggregateState::from_count_sum_parts(count, raw_sum, compensation)?;
                Some((count, raw_sum, compensation))
            }
            INDEX_FLAG_COUNT_SUM_UNAVAILABLE
                if read_u64(entry, 48) == 0
                    && read_u64(entry, 56) == 0
                    && read_u64(entry, 64) == 0 =>
            {
                None
            }
            _ => return Err(corrupt("invalid Chimaera index page entry flags")),
        };
        result.push(IndexEntry {
            series: SeriesKey::new(read_u32(entry, 0), read_u32(entry, 4), read_u64(entry, 8)),
            offset: read_u64(entry, 16),
            size: read_u32(entry, 24),
            min_timestamp: read_i64(entry, 28),
            max_timestamp: read_i64(entry, 36),
            count_sum,
        });
    }
    Ok(result)
}

fn validate_index_entries(
    index: Vec<IndexEntry>,
    logical_length: u64,
    data_end: u64,
) -> Result<Vec<IndexEntry>> {
    let mut previous_end = HEADER_SIZE as u64;
    let mut previous_series = None;
    for descriptor in &index {
        let end = descriptor.offset.saturating_add(descriptor.size as u64);
        if descriptor.offset < previous_end
            || descriptor.offset < HEADER_SIZE as u64
            || descriptor.size < BLOCK_HEADER_SIZE as u32
            || descriptor.size as usize > BLOCK_HEADER_SIZE + MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD
            || end > data_end
            || end > logical_length
            || descriptor.min_timestamp > descriptor.max_timestamp
            || previous_series.is_some_and(|series| series > descriptor.series)
        {
            return Err(corrupt("invalid Chimaera paged index descriptor"));
        }
        previous_end = end;
        previous_series = Some(descriptor.series);
    }
    Ok(index)
}

pub(crate) fn decode_block(bytes: &[u8]) -> Result<Vec<Operation>> {
    if bytes.len() < BLOCK_HEADER_SIZE {
        return Err(corrupt("Chimaera v1 block is shorter than its header"));
    }
    let header = &bytes[..BLOCK_HEADER_SIZE];
    if &header[..4] != BLOCK_MAGIC
        || read_u16(header, 4) as usize != BLOCK_HEADER_SIZE
        || header[6] != 1
        || header[7] != 0
        || read_u32(header, 84) != crc32c::crc32c(&header[..84])
    {
        return Err(corrupt("invalid Chimaera v1 block header"));
    }
    let series = SeriesKey::new(
        read_u32(header, 8),
        read_u32(header, 12),
        read_u64(header, 16),
    );
    let min_timestamp = read_i64(header, 24);
    let max_timestamp = read_i64(header, 32);
    let min_commit = read_u64(header, 40);
    let max_commit = read_u64(header, 48);
    let operation_count = read_u32(header, 56) as usize;
    let put_count = read_u32(header, 60) as usize;
    let delete_count = read_u32(header, 64) as usize;
    let timestamp_codec = header[68];
    let value_codec = header[69];
    let block_codec = header[70];
    let level_marker = header[71];
    let uncompressed_size = read_u32(header, 72) as usize;
    let compressed_size = read_u32(header, 76) as usize;
    if operation_count == 0
        || put_count.checked_add(delete_count) != Some(operation_count)
        || min_timestamp > max_timestamp
        || min_commit > max_commit
        || !matches!(timestamp_codec, 0..=2)
        || !matches!(value_codec, 0..=1)
        || !matches!(block_codec, 0..=2)
        || (block_codec == 0 && level_marker != 0)
        || (block_codec == 1 && !(1..=10).contains(&level_marker))
        || (block_codec == 2 && level_marker != 4)
        || uncompressed_size == 0
        || uncompressed_size > MAX_BLOCK_BYTES
        || compressed_size == 0
        || compressed_size > MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD
        || bytes.len() != BLOCK_HEADER_SIZE + compressed_size
    {
        return Err(corrupt("invalid Chimaera v1 block metadata"));
    }
    let stored = &bytes[BLOCK_HEADER_SIZE..];
    if crc32c::crc32c(stored) != read_u32(header, 80) {
        return Err(corrupt("Chimaera v1 block payload CRC32C mismatch"));
    }
    let payload = match block_codec {
        0 => {
            if compressed_size != uncompressed_size {
                return Err(corrupt("raw Chimaera v1 block sizes differ"));
            }
            stored.to_vec()
        }
        1 => decode_gzip(stored, uncompressed_size)?,
        2 => zstd_block::decode(stored, uncompressed_size)?,
        _ => return Err(corrupt("unsupported Chimaera v1 block codec")),
    };
    if payload.len() < 16 {
        return Err(corrupt("Chimaera v1 block stream lengths are truncated"));
    }
    let timestamp_size = read_u32(&payload, 0) as usize;
    let version_size = read_u32(&payload, 4) as usize;
    let operation_size = read_u32(&payload, 8) as usize;
    let value_size = read_u32(&payload, 12) as usize;
    let expected = 16_usize
        .checked_add(timestamp_size)
        .and_then(|value| value.checked_add(version_size))
        .and_then(|value| value.checked_add(operation_size))
        .and_then(|value| value.checked_add(value_size));
    if expected != Some(payload.len())
        || version_size != operation_count.saturating_mul(12)
        || operation_size != operation_count
    {
        return Err(corrupt("Chimaera v1 block stream lengths do not match"));
    }
    let timestamp_end = 16 + timestamp_size;
    let version_end = timestamp_end + version_size;
    let operation_end = version_end + operation_size;
    let timestamps = decode_timestamps(
        &payload[16..timestamp_end],
        timestamp_codec,
        operation_count,
    )?;
    let values = decode_values(&payload[operation_end..], value_codec, put_count)?;
    let mut operations = Vec::with_capacity(operation_count);
    let mut value_index = 0;
    let mut observed_min_commit = u64::MAX;
    let mut observed_max_commit = 0_u64;
    for index in 0..operation_count {
        let version_offset = timestamp_end + index * 12;
        let commit_id = read_u64(&payload, version_offset);
        let ordinal = read_u32(&payload, version_offset + 8);
        let (kind, value) = match payload[version_end + index] {
            1 => {
                let value = values
                    .get(value_index)
                    .copied()
                    .ok_or_else(|| corrupt("Chimaera v1 block has too few values"))?;
                value_index += 1;
                (OperationKind::Put, Some(value))
            }
            2 => (OperationKind::Delete, None),
            _ => return Err(corrupt("unknown operation in Chimaera v1 block")),
        };
        observed_min_commit = observed_min_commit.min(commit_id);
        observed_max_commit = observed_max_commit.max(commit_id);
        operations.push(Operation {
            series,
            timestamp_us: timestamps[index],
            commit_id,
            ordinal,
            kind,
            value,
        });
    }
    if value_index != values.len()
        || timestamps.first().copied() != Some(min_timestamp)
        || timestamps.last().copied() != Some(max_timestamp)
        || observed_min_commit != min_commit
        || observed_max_commit != max_commit
        || operations.windows(2).any(|pair| {
            (pair[0].timestamp_us, pair[0].commit_id, pair[0].ordinal)
                > (pair[1].timestamp_us, pair[1].commit_id, pair[1].ordinal)
        })
    {
        return Err(corrupt("Chimaera v1 block columns violate header bounds"));
    }
    Ok(operations)
}

fn decode_gzip(stored: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let cursor = Cursor::new(stored);
    let decoder = GzDecoder::new(cursor);
    let limit = u64::try_from(expected_size)
        .map_err(|_| corrupt("Chimaera v1 gzip expected size exceeds u64"))?
        .saturating_add(1);
    let mut limited = decoder.take(limit);
    let mut decoded = Vec::with_capacity(expected_size);
    limited.read_to_end(&mut decoded)?;
    let decoder = limited.into_inner();
    let consumed = decoder.into_inner().position();
    if decoded.len() != expected_size || consumed != stored.len() as u64 {
        return Err(corrupt(
            "Chimaera v1 gzip block size or member count mismatch",
        ));
    }
    Ok(decoded)
}

fn decode_timestamps(bytes: &[u8], codec: u8, count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        return if bytes.is_empty() {
            Ok(Vec::new())
        } else {
            Err(corrupt(
                "non-empty Chimaera v1 timestamp stream for zero count",
            ))
        };
    }
    if codec == 0 {
        if bytes.len() != count.saturating_mul(8) {
            return Err(corrupt("raw Chimaera v1 timestamp length mismatch"));
        }
        let timestamps = (0..count)
            .map(|index| read_i64(bytes, index * 8))
            .collect::<Vec<_>>();
        if timestamps.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(corrupt("Chimaera v1 timestamps are not monotonic"));
        }
        return Ok(timestamps);
    }
    if !matches!(codec, 1 | 2) || bytes.len() < 8 {
        return Err(corrupt("invalid Chimaera v1 timestamp codec or stream"));
    }
    let mut timestamps = Vec::with_capacity(count);
    let mut current = read_i64(bytes, 0);
    timestamps.push(current);
    let mut previous_delta = 0_i64;
    let mut position = 8;
    for offset in 0..count - 1 {
        let (encoded, next_position) = decode_varint(bytes, position)?;
        position = next_position;
        let delta_component = zigzag_decode(encoded);
        let delta = if codec == 2 && offset > 0 {
            previous_delta
                .checked_add(delta_component)
                .ok_or_else(|| corrupt("Chimaera v1 delta-of-delta overflow"))?
        } else {
            delta_component
        };
        let next = current
            .checked_add(delta)
            .ok_or_else(|| corrupt("Chimaera v1 timestamp delta overflow"))?;
        if next < current {
            return Err(corrupt("Chimaera v1 timestamps are not monotonic"));
        }
        timestamps.push(next);
        current = next;
        previous_delta = delta;
    }
    if position != bytes.len() {
        return Err(corrupt("trailing bytes in Chimaera v1 timestamp stream"));
    }
    Ok(timestamps)
}

fn decode_varint(bytes: &[u8], offset: usize) -> Result<(u64, usize)> {
    if offset >= bytes.len() {
        return Err(corrupt("truncated Chimaera v1 varint"));
    }
    let mut result = 0_u64;
    for index in 0..10 {
        let position = offset + index;
        let byte = *bytes
            .get(position)
            .ok_or_else(|| corrupt("truncated Chimaera v1 varint"))?;
        if index == 9 && byte & 0xfe != 0 {
            return Err(corrupt("Chimaera v1 varint overflow"));
        }
        result |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if index > 0 && byte & 0x7f == 0 {
                return Err(corrupt("non-canonical Chimaera v1 varint"));
            }
            return Ok((result, position + 1));
        }
    }
    Err(corrupt("unterminated Chimaera v1 varint"))
}

fn zigzag_decode(value: u64) -> i64 {
    let magnitude = (value >> 1) as i64;
    if value & 1 == 0 {
        magnitude
    } else {
        -magnitude - 1
    }
}

pub(crate) fn decode_values(bytes: &[u8], codec: u8, count: usize) -> Result<Vec<f64>> {
    let values = match codec {
        0 => {
            if bytes.len() != count.saturating_mul(8) {
                return Err(corrupt("raw Chimaera v1 value length mismatch"));
            }
            (0..count)
                .map(|index| f64::from_bits(read_u64(bytes, index * 8)))
                .collect::<Vec<_>>()
        }
        1 => decode_gorilla(bytes, count)?,
        _ => return Err(corrupt("unsupported Chimaera v1 value codec")),
    };
    if values.iter().any(|value| !value.is_finite()) {
        return Err(corrupt("Chimaera v1 value stream contains NaN or Infinity"));
    }
    Ok(values)
}

fn decode_gorilla(bytes: &[u8], count: usize) -> Result<Vec<f64>> {
    if count == 0 {
        return if bytes.is_empty() {
            Ok(Vec::new())
        } else {
            Err(corrupt(
                "non-empty Chimaera v1 Gorilla stream for zero count",
            ))
        };
    }
    if bytes.len() < 8 {
        return Err(corrupt("Chimaera v1 Gorilla stream misses its first value"));
    }
    let mut previous = read_u64(bytes, 0);
    let mut values = Vec::with_capacity(count);
    values.push(f64::from_bits(previous));
    let mut bits = BitReader::new(&bytes[8..]);
    let mut window: Option<(usize, usize)> = None;
    for _ in 1..count {
        let mut xor = 0_u64;
        if bits.read_bit()? {
            if bits.read_bit()? {
                let leading = bits.read_bits(6)? as usize;
                let significant = bits.read_bits(6)? as usize + 1;
                if leading + significant > 64 {
                    return Err(corrupt("invalid Chimaera v1 Gorilla window"));
                }
                let trailing = 64 - leading - significant;
                xor = bits.read_bits(significant)? << trailing;
                window = Some((leading, trailing));
            } else {
                let (leading, trailing) =
                    window.ok_or_else(|| corrupt("Chimaera v1 Gorilla reuses a missing window"))?;
                xor = bits.read_bits(64 - leading - trailing)? << trailing;
            }
        }
        previous ^= xor;
        values.push(f64::from_bits(previous));
    }
    bits.expect_zero_padding()?;
    Ok(values)
}

struct BitReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> BitReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_bit(&mut self) -> Result<bool> {
        if self.position >= self.bytes.len().saturating_mul(8) {
            return Err(corrupt("truncated Chimaera v1 bit stream"));
        }
        let byte = self.bytes[self.position / 8];
        let shift = 7 - self.position % 8;
        self.position += 1;
        Ok((byte >> shift) & 1 == 1)
    }

    fn read_bits(&mut self, width: usize) -> Result<u64> {
        if width > 64 {
            return Err(corrupt("invalid Chimaera v1 bit field width"));
        }
        let mut result = 0_u64;
        for _ in 0..width {
            result = (result << 1) | u64::from(self.read_bit()?);
        }
        Ok(result)
    }

    fn expect_zero_padding(&mut self) -> Result<()> {
        let remaining = self.bytes.len().saturating_mul(8) - self.position;
        if remaining >= 8 {
            return Err(corrupt("trailing bytes in Chimaera v1 bit stream"));
        }
        for _ in 0..remaining {
            if self.read_bit()? {
                return Err(corrupt("non-zero Chimaera v1 bit padding"));
            }
        }
        Ok(())
    }
}

fn usize_from_u64(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| invalid(format!("{name} exceeds usize")))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tempfile::TempDir;

    use super::*;
    use crate::error::Error;

    const ONE_PUT: &str = "545353454730303101007000000000000000000000000000000000000100000000000000000001000000000000000000000000000a000000000000000100000000000000010000000000000001000000000000000100000000000000000000000000000000000000000000001b4efff75453424c58000100010000000100000001000000000000000100000000000000010000000000000001000000000000000100000000000000010000000100000000000000000000002d0000002d00000088164e9907a6cb96080000000c0000000100000008000000010000000000000001000000000000000000000001000000000000f83f545349445830303101000000010000000100000001000000010000000000000070000000000000008500000001000000000000000100000000000000545346545230303101004000f5000000000000003c0000000000000001000000010000007101000000000000a2a241260000000000000000000000007d2397e9";
    const GZIP_BLOCK: &str = "5453424c5800010001000000010000000100000000000000e8030000000000001a0400000000000001000000000000000600000000000000060000000600000000000000020101047b0000004a0000000485bcb7ef74c5041f8b08000000000000ffe3656060f0006236201600e217cc40020844400423840d064c486ca81ab0080b92382b121b64200c30820184f7c13e8df12cd301f3ff1f00095e95b77b000000";

    #[test]
    fn v1_fixture_decodes() -> Result<()> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("segment.tss");
        fs::write(&path, decode_hex(ONE_PUT)?)?;
        let operations = read_operations(&path)?;
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].series, SeriesKey::new(1, 1, 1));
        assert_eq!(operations[0].timestamp_us, 1);
        assert_eq!(operations[0].commit_id, 1);
        assert_eq!(operations[0].value, Some(1.5));
        Ok(())
    }

    #[test]
    fn v1_delta_gorilla_and_gzip_fixtures_decode() -> Result<()> {
        let expected_timestamps = [1_000, 1_010, 1_025, 1_040, 1_040];
        assert_eq!(
            decode_timestamps(&decode_hex("e803000000000000141e1e00")?, 1, 5)?,
            expected_timestamps
        );
        assert_eq!(
            decode_timestamps(&decode_hex("e803000000000000140a001d")?, 2, 5)?,
            expected_timestamps
        );
        assert_eq!(
            decode_values(&decode_hex("000000000000f03f6601cd02c037fff0")?, 1, 6)?,
            [1.0, 1.0, 1.5, 1.75, 1.75, -2.0]
        );
        let operations = decode_block(&decode_hex(GZIP_BLOCK)?)?;
        assert_eq!(operations.len(), 6);
        assert_eq!(operations[0].timestamp_us, 1_000);
        assert_eq!(operations[5].timestamp_us, 1_050);
        assert_eq!(operations[5].value, Some(-2.0));
        Ok(())
    }

    #[test]
    fn gzip_decode_stops_after_declared_size() -> Result<()> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&vec![0_u8; 1024 * 1024])?;
        let stored = encoder.finish()?;
        assert!(matches!(
            decode_gzip(&stored, 16),
            Err(Error::Corruption(_))
        ));
        Ok(())
    }

    fn decode_hex(value: &str) -> Result<Vec<u8>> {
        if !value.len().is_multiple_of(2) {
            return Err(invalid("hex fixture length is odd"));
        }
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        if !remainder.is_empty() {
            return Err(invalid("hex fixture has a partial byte"));
        }
        pairs
            .iter()
            .map(|pair| {
                let text = std::str::from_utf8(pair).map_err(|_| invalid("invalid hex fixture"))?;
                u8::from_str_radix(text, 16).map_err(|_| invalid("invalid hex fixture"))
            })
            .collect()
    }
}
