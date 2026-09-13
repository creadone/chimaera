//! Чтение корней индексов, сводок и проверка области сегмента в pack-файле.

mod index_pages;
pub(super) use index_pages::read_paged_metadata_for_query;

use super::index::{
    decode_index, decode_index_page, decode_index_page_descriptor, decode_paged_index_root,
    paged_index_root_bytes, paged_index_root_layout, validate_block_headers, validate_footer,
    validate_header, validate_index_page_descriptor,
};
use super::{
    BLOCK_HEADER_SIZE, BLOCK_MAGIC, FOOTER_SIZE, HEADER_SIZE, INDEX_PAGE_DIRECTORY_SIZE,
    INDEX_ROOT_STREAM_BUFFER_SIZE, IndexPageDescriptor, MAX_INDEX_BYTES, MAX_STORED_BLOCK_OVERHEAD,
    OpenedSegmentIndex, PAGED_INDEX_HEADER_SIZE, PAGED_INDEX_MAGIC, PagedIndexRoot,
    SegmentDescriptor, SegmentMetadata,
};
use crate::aggregate;
use crate::cache::{MemoryClass, MemoryGovernor};
use crate::error::{Result, corrupt};
use crate::file_pool::{CachedFile, FilePermit, FilePermitPool, OpenFileCache};
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::{AggregateResolution, SeriesKey};
use crate::wal::{read_i64, read_u16, read_u32, read_u64};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

pub(super) struct LoadedSegmentMetadata {
    pub(super) metadata: Arc<SegmentMetadata>,
    pub(super) aggregate: Arc<aggregate::Section>,
    pub(super) bytes_read: u64,
    pub(super) metadata_reserved: u64,
    pub(super) aggregate_reserved: u64,
}

pub(super) struct LoadedPagedMetadata {
    pub(super) metadata: Arc<SegmentMetadata>,
    pub(super) bytes_read: u64,
    pub(super) metadata_reserved: u64,
}

#[derive(Clone, Copy)]
pub(super) struct QueryMetadataResources<'a> {
    pub(super) path: &'a Path,
    pub(super) file_pool: &'a FilePermitPool,
    pub(super) open_file_cache: Option<&'a OpenFileCache>,
    pub(super) query_budget: &'a QueryBudget,
}

pub(super) enum QueryFile<'a> {
    Cached(Arc<CachedFile>, &'a QueryBudget),
    Owned {
        file: File,
        length: u64,
        _permit: FilePermit,
    },
}

impl<'a> QueryFile<'a> {
    pub(super) fn open(
        path: &Path,
        file_pool: &FilePermitPool,
        open_file_cache: Option<&OpenFileCache>,
        query_budget: &'a QueryBudget,
    ) -> Result<Self> {
        if let Some(open_file_cache) = open_file_cache {
            return open_file_cache
                .get(path, query_budget)
                .map(|file| Self::Cached(file, query_budget));
        }
        let permit = file_pool.acquire_for_query(query_budget)?;
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        query_budget.record_file_open();
        Ok(Self::Owned {
            file,
            length,
            _permit: permit,
        })
    }

    pub(super) fn length(&self) -> u64 {
        match self {
            Self::Cached(file, _) => file.length(),
            Self::Owned { length, .. } => *length,
        }
    }

    pub(super) fn read_exact_at(&mut self, offset: u64, bytes: &mut [u8]) -> Result<()> {
        match self {
            Self::Cached(file, budget) => file.read_exact_at(offset, bytes, budget),
            Self::Owned { file, .. } => {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(bytes)?;
                Ok(())
            }
        }
    }
}

pub(super) fn validate_query_segment_region(
    file: &mut QueryFile<'_>,
    path: &Path,
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
) -> Result<u64> {
    let logical_end = descriptor
        .file_offset
        .checked_add(descriptor.file_size)
        .ok_or_else(|| corrupt("segment physical bounds overflow"))?;
    let invalid_physical_bounds = if descriptor.pack_id.is_some() {
        logical_end > file.length()
    } else {
        descriptor.file_offset != 0 || file.length() != descriptor.file_size
    };
    if invalid_physical_bounds || descriptor.file_size < (HEADER_SIZE + FOOTER_SIZE) as u64 {
        return Err(corrupt(format!(
            "invalid segment length: {}",
            path.display()
        )));
    }
    let mut header = [0_u8; HEADER_SIZE];
    file.read_exact_at(descriptor.file_offset, &mut header)?;
    validate_header(&header, storage_uuid, descriptor, path)?;
    Ok(logical_end)
}

pub(super) fn select_index_pages(
    pages: &[IndexPageDescriptor],
    requested: &[SeriesKey],
) -> Vec<IndexPageDescriptor> {
    pages
        .iter()
        .copied()
        .filter(|page| index_page_intersects_requested(*page, requested))
        .collect()
}

pub(super) fn index_page_intersects_requested(
    page: IndexPageDescriptor,
    requested: &[SeriesKey],
) -> bool {
    requested
        .binary_search_by(|series| {
            if *series < page.first_series {
                Ordering::Less
            } else if *series > page.last_series {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
        .is_ok()
}

pub(super) fn stream_index_root_selection(
    file: &mut QueryFile<'_>,
    root_offset: u64,
    header: &[u8; PAGED_INDEX_HEADER_SIZE],
    root_length: usize,
    expected_root_crc: u32,
    requested: &[SeriesKey],
    query_budget: &QueryBudget,
) -> Result<Vec<IndexPageDescriptor>> {
    let (page_count, block_count, decoded_root_length, directory_offset) =
        paged_index_root_layout(header, root_length)?;
    if decoded_root_length != root_length {
        return Err(corrupt(
            "paged segment index root length changed while streaming",
        ));
    }
    let scratch_length =
        INDEX_ROOT_STREAM_BUFFER_SIZE.min(directory_offset - PAGED_INDEX_HEADER_SIZE);
    let scratch_reserved = u64::try_from(scratch_length).unwrap_or(u64::MAX);
    query_budget.reserve_memory_class(QueryMemoryClass::Metadata, scratch_reserved)?;
    let result = (|| {
        let mut scratch = vec![0_u8; scratch_length];
        let mut root_crc = crc32c::crc32c(header);
        let mut membership_crc = 0_u32;
        let mut position = PAGED_INDEX_HEADER_SIZE;
        while position < directory_offset {
            query_budget.check()?;
            let length = scratch.len().min(directory_offset - position);
            let bytes = &mut scratch[..length];
            file.read_exact_at(root_offset + position as u64, bytes)?;
            root_crc = crc32c::crc32c_append(root_crc, bytes);
            membership_crc = crc32c::crc32c_append(membership_crc, bytes);
            position += length;
        }
        if membership_crc != read_u32(header, 28) {
            return Err(corrupt("paged segment membership filter CRC32C mismatch"));
        }

        let mut selected = Vec::with_capacity(requested.len().min(page_count));
        let mut previous_last = None;
        let mut expected_offset = root_length as u64;
        let mut counted_blocks = 0_u64;
        let mut entry = [0_u8; INDEX_PAGE_DIRECTORY_SIZE];
        for page_number in 0..page_count {
            query_budget.check()?;
            let position = directory_offset + page_number * INDEX_PAGE_DIRECTORY_SIZE;
            file.read_exact_at(root_offset + position as u64, &mut entry)?;
            root_crc = crc32c::crc32c_append(root_crc, &entry);
            let descriptor = decode_index_page_descriptor(&entry)?;
            validate_index_page_descriptor(descriptor, previous_last, expected_offset)?;
            expected_offset = expected_offset
                .checked_add(u64::from(descriptor.length))
                .ok_or_else(|| corrupt("paged segment index page end overflow"))?;
            counted_blocks = counted_blocks.saturating_add(u64::from(descriptor.block_count));
            previous_last = Some(descriptor.last_series);
            if index_page_intersects_requested(descriptor, requested) {
                selected.push(descriptor);
            }
        }
        if counted_blocks != u64::from(block_count) {
            return Err(corrupt("paged segment index root block count mismatch"));
        }
        if root_crc != expected_root_crc {
            return Err(corrupt("paged segment index root CRC32C mismatch"));
        }
        Ok(selected)
    })();
    query_budget.release_memory_class(QueryMemoryClass::Metadata, scratch_reserved);
    result
}

pub(super) fn read_segment_metadata(
    path: &Path,
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
    file_pool: &FilePermitPool,
    query_budget: Option<&QueryBudget>,
    validate_all_block_headers: bool,
) -> Result<LoadedSegmentMetadata> {
    let _file_permit = match query_budget {
        Some(query_budget) => file_pool.acquire_for_query(query_budget)?,
        None => file_pool.acquire()?,
    };
    let (mut file, logical_end) = open_segment_region(path, storage_uuid, descriptor)?;
    file.seek(SeekFrom::Start(logical_end - FOOTER_SIZE as u64))?;
    let mut footer_bytes = [0_u8; FOOTER_SIZE];
    file.read_exact(&mut footer_bytes)?;
    let footer = validate_footer(&footer_bytes, descriptor.file_size)?;
    if footer.index_length > MAX_INDEX_BYTES as u64 {
        return Err(corrupt("segment index exceeds hard limit"));
    }
    let aggregate_offset = footer
        .index_offset
        .checked_sub(u64::from(footer.aggregate_length))
        .ok_or_else(|| corrupt("segment aggregate offset underflow"))?;
    let aggregate = if footer.aggregate_length == 0 {
        None
    } else {
        if let Some(query_budget) = query_budget {
            query_budget.reserve_memory_class(
                QueryMemoryClass::Metadata,
                u64::from(footer.aggregate_length),
            )?;
        }
        file.seek(SeekFrom::Start(descriptor.file_offset + aggregate_offset))?;
        let mut bytes = vec![0_u8; footer.aggregate_length as usize];
        file.read_exact(&mut bytes)?;
        if crc32c::crc32c(aggregate::checksum_bytes(&bytes)?) != footer.aggregate_crc {
            return Err(corrupt("segment aggregate CRC32C mismatch"));
        }
        aggregate::validate(&bytes, descriptor.domain())?;
        Some(bytes)
    };
    file.seek(SeekFrom::Start(
        descriptor.file_offset + footer.index_offset,
    ))?;
    if let Some(query_budget) = query_budget {
        query_budget.reserve_memory_class(QueryMemoryClass::Metadata, footer.index_length)?;
    }
    let mut index_bytes = vec![
        0_u8;
        usize::try_from(footer.index_length)
            .map_err(|_| corrupt("segment index length exceeds usize"))?
    ];
    file.read_exact(&mut index_bytes)?;
    let index_checksum_bytes = if footer.version == 3 {
        paged_index_root_bytes(&index_bytes)?
    } else {
        index_bytes.as_slice()
    };
    if crc32c::crc32c(index_checksum_bytes) != footer.index_crc {
        return Err(corrupt("segment index CRC32C mismatch"));
    }
    let block_count = read_u32(&footer_bytes, 32) as usize;
    let expected_series_count = read_u32(&footer_bytes, 28) as usize;
    let metadata_reserved = SegmentMetadata::estimated_bytes(block_count, expected_series_count)?;
    if let Some(query_budget) = query_budget {
        query_budget.reserve_memory_class(QueryMemoryClass::Metadata, metadata_reserved)?;
    }
    let index = decode_index(&index_bytes, aggregate_offset)?;
    if let Some(query_budget) = query_budget {
        query_budget.release_memory_class(QueryMemoryClass::Metadata, footer.index_length);
    }
    if index.len() != block_count {
        return Err(corrupt("segment block count does not match footer"));
    }
    let series_count = index
        .iter()
        .enumerate()
        .filter(|(position, entry)| *position == 0 || index[*position - 1].series != entry.series)
        .count();
    if series_count != expected_series_count {
        return Err(corrupt("segment series count does not match footer"));
    }
    if validate_all_block_headers {
        validate_block_headers(&mut file, descriptor, &index)?;
    }
    let metadata_bytes = (HEADER_SIZE as u64)
        .saturating_add(FOOTER_SIZE as u64)
        .saturating_add(footer.index_length)
        .saturating_add(u64::from(footer.aggregate_length))
        .saturating_add(if validate_all_block_headers {
            u64::try_from(index.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(BLOCK_HEADER_SIZE as u64)
        } else {
            0
        });
    Ok(LoadedSegmentMetadata {
        metadata: Arc::new(SegmentMetadata::new(index)),
        aggregate: Arc::new(aggregate::Section::new(aggregate)),
        bytes_read: metadata_bytes,
        metadata_reserved: if query_budget.is_some() {
            metadata_reserved
        } else {
            0
        },
        aggregate_reserved: if query_budget.is_some() {
            u64::from(footer.aggregate_length)
        } else {
            0
        },
    })
}

pub(super) fn read_segment_aggregates(
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
    series: &[SeriesKey],
    resolution: AggregateResolution,
    resources: QueryMetadataResources<'_>,
) -> Result<(Arc<aggregate::Section>, u64, u64, bool)> {
    let QueryMetadataResources {
        path,
        file_pool,
        open_file_cache,
        query_budget,
    } = resources;
    let mut file = QueryFile::open(path, file_pool, open_file_cache, query_budget)?;
    let logical_end = validate_query_segment_region(&mut file, path, storage_uuid, descriptor)?;
    let mut footer_bytes = [0_u8; FOOTER_SIZE];
    file.read_exact_at(logical_end - FOOTER_SIZE as u64, &mut footer_bytes)?;
    let footer = validate_footer(&footer_bytes, descriptor.file_size)?;
    let aggregate_offset = footer
        .index_offset
        .checked_sub(u64::from(footer.aggregate_length))
        .ok_or_else(|| corrupt("segment aggregate offset underflow"))?;
    if footer.version == 3 && footer.aggregate_length != 0 {
        let aggregate_start = descriptor.file_offset + aggregate_offset;
        let mut header = [0_u8; 32];
        file.read_exact_at(aggregate_start, &mut header)?;
        let section_length = footer.aggregate_length as usize;
        let root_length = aggregate::root_length_from_header(&header, section_length)?;
        let root_reserved = u64::try_from(root_length)
            .unwrap_or(u64::MAX)
            .saturating_mul(2);
        query_budget.reserve_memory_class(QueryMemoryClass::Metadata, root_reserved)?;
        let result = (|| {
            let mut root = vec![0_u8; root_length];
            root[..header.len()].copy_from_slice(&header);
            file.read_exact_at(
                aggregate_start + header.len() as u64,
                &mut root[header.len()..],
            )?;
            if crc32c::crc32c(&root) != footer.aggregate_crc {
                return Err(corrupt("segment aggregate root CRC32C mismatch"));
            }
            let pages = aggregate::locate_pages(&root, section_length, series, resolution)?;
            if pages.is_empty() {
                return Ok((
                    Arc::new(aggregate::Section::new(None)),
                    (HEADER_SIZE + FOOTER_SIZE + root_length) as u64,
                    0,
                    false,
                ));
            }
            let page_bytes = pages.iter().fold(0_u64, |total, (_, page)| {
                total.saturating_add(u64::from(page.length))
            });
            let section_bytes = (32_u64)
                .saturating_add(
                    u64::try_from(pages.len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(40),
                )
                .saturating_add(page_bytes);
            query_budget.reserve_memory_class(QueryMemoryClass::Metadata, page_bytes)?;
            if let Err(error) =
                query_budget.reserve_memory_class(QueryMemoryClass::Metadata, section_bytes)
            {
                query_budget.release_memory_class(QueryMemoryClass::Metadata, page_bytes);
                return Err(error);
            }
            let page_result = (|| {
                let mut records_by_series = Vec::with_capacity(pages.len());
                for (series, page) in pages {
                    query_budget.check()?;
                    let mut records = vec![0_u8; page.length as usize];
                    file.read_exact_at(aggregate_start + page.offset, &mut records)?;
                    if crc32c::crc32c(&records) != page.crc32c {
                        return Err(corrupt("segment aggregate page CRC32C mismatch"));
                    }
                    records_by_series.push((series, records));
                }
                let section = aggregate::section_from_pages(&records_by_series, resolution)?;
                aggregate::validate(&section, descriptor.domain())?;
                Ok(Arc::new(aggregate::Section::new(Some(section))))
            })();
            query_budget.release_memory_class(QueryMemoryClass::Metadata, page_bytes);
            let section = match page_result {
                Ok(section) => section,
                Err(error) => {
                    query_budget.release_memory_class(QueryMemoryClass::Metadata, section_bytes);
                    return Err(error);
                }
            };
            Ok((
                section,
                (HEADER_SIZE + FOOTER_SIZE + root_length) as u64 + page_bytes,
                section_bytes,
                false,
            ))
        })();
        query_budget.release_memory_class(QueryMemoryClass::Metadata, root_reserved);
        return result;
    }
    let aggregate = if footer.aggregate_length == 0 {
        None
    } else {
        query_budget.reserve_memory_class(
            QueryMemoryClass::Metadata,
            u64::from(footer.aggregate_length),
        )?;
        let mut bytes = vec![0_u8; footer.aggregate_length as usize];
        file.read_exact_at(descriptor.file_offset + aggregate_offset, &mut bytes)?;
        if crc32c::crc32c(aggregate::checksum_bytes(&bytes)?) != footer.aggregate_crc {
            return Err(corrupt("segment aggregate CRC32C mismatch"));
        }
        aggregate::validate(&bytes, descriptor.domain())?;
        Some(bytes)
    };
    let bytes_read = (HEADER_SIZE as u64)
        .saturating_add(FOOTER_SIZE as u64)
        .saturating_add(u64::from(footer.aggregate_length));
    Ok((
        Arc::new(aggregate::Section::new(aggregate)),
        bytes_read,
        u64::from(footer.aggregate_length),
        true,
    ))
}

pub(super) fn read_segment_signature(
    path: &Path,
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
    file_pool: &FilePermitPool,
    governor: &Arc<MemoryGovernor>,
    load_resident_index: bool,
) -> Result<OpenedSegmentIndex> {
    let _file_permit = file_pool.acquire()?;
    let (mut file, logical_end) = open_segment_region(path, storage_uuid, descriptor)?;
    if descriptor.file_size < (HEADER_SIZE + BLOCK_HEADER_SIZE + FOOTER_SIZE) as u64 {
        return Err(corrupt("segment is too short for its first block"));
    }
    file.seek(SeekFrom::Start(descriptor.file_offset + HEADER_SIZE as u64))?;
    let mut header = [0_u8; BLOCK_HEADER_SIZE];
    file.read_exact(&mut header)?;
    let stored_length = u64::from(read_u32(&header, 76));
    let block_end = descriptor
        .file_offset
        .checked_add(HEADER_SIZE as u64)
        .and_then(|offset| offset.checked_add(BLOCK_HEADER_SIZE as u64))
        .and_then(|offset| offset.checked_add(stored_length))
        .ok_or_else(|| corrupt("first segment block bounds overflow"))?;
    if &header[..4] != BLOCK_MAGIC
        || read_u16(&header, 4) as usize != BLOCK_HEADER_SIZE
        || header[6] != 1
        || header[7] != 0
        || read_u32(&header, 56) == 0
        || read_u32(&header, 60).saturating_add(read_u32(&header, 64)) != read_u32(&header, 56)
        || read_i64(&header, 24) > read_i64(&header, 32)
        || read_i64(&header, 24) < descriptor.min_timestamp_us
        || read_i64(&header, 32) > descriptor.max_timestamp_us
        || read_u64(&header, 40) > read_u64(&header, 48)
        || read_u64(&header, 40) < descriptor.min_commit
        || read_u64(&header, 48) > descriptor.max_commit
        || stored_length > (4 * 1024 * 1024 + MAX_STORED_BLOCK_OVERHEAD) as u64
        || block_end > logical_end - FOOTER_SIZE as u64
        || read_u32(&header, 84) != crc32c::crc32c(&header[..84])
    {
        return Err(corrupt("invalid first segment block header"));
    }

    file.seek(SeekFrom::Start(logical_end - FOOTER_SIZE as u64))?;
    let mut footer_bytes = [0_u8; FOOTER_SIZE];
    file.read_exact(&mut footer_bytes)?;
    let footer = validate_footer(&footer_bytes, descriptor.file_size)?;
    if footer.version != 3 {
        return Err(corrupt(
            "series-indexed segment requires a paged index footer",
        ));
    }
    let data_end = footer
        .index_offset
        .checked_sub(u64::from(footer.aggregate_length))
        .ok_or_else(|| corrupt("segment aggregate offset underflow"))?;
    let root_offset = descriptor
        .file_offset
        .checked_add(footer.index_offset)
        .ok_or_else(|| corrupt("segment index root offset overflows"))?;
    let mut root_header = [0_u8; PAGED_INDEX_HEADER_SIZE];
    file.seek(SeekFrom::Start(root_offset))?;
    file.read_exact(&mut root_header)?;
    if &root_header[..8] != PAGED_INDEX_MAGIC {
        return Err(corrupt("segment footer expects a paged index"));
    }
    let root_length = read_u32(&root_header, 20) as usize;
    if root_length < PAGED_INDEX_HEADER_SIZE
        || root_length as u64 > footer.index_length
        || root_length > MAX_INDEX_BYTES
    {
        return Err(corrupt(
            "paged segment index root length exceeds its section",
        ));
    }
    let storage_signature = Some(
        <[u8; 4]>::try_from(&header[68..72])
            .map_err(|_| corrupt("segment block profile is truncated"))?,
    );
    if !load_resident_index {
        return Ok(OpenedSegmentIndex {
            storage_signature,
            root: None,
            index_offset: footer.index_offset,
            data_end,
            reservation: None,
        });
    }
    let Some(root_load_reservation) =
        governor.try_reserve_token(MemoryClass::MetadataCache, root_length as u64)
    else {
        return Ok(OpenedSegmentIndex {
            storage_signature,
            root: None,
            index_offset: footer.index_offset,
            data_end,
            reservation: None,
        });
    };
    let mut root_bytes = vec![0_u8; root_length];
    root_bytes[..PAGED_INDEX_HEADER_SIZE].copy_from_slice(&root_header);
    file.read_exact(&mut root_bytes[PAGED_INDEX_HEADER_SIZE..])?;
    if crc32c::crc32c(&root_bytes) != footer.index_crc {
        return Err(corrupt("paged segment index root CRC32C mismatch"));
    }
    let (index_root, decoded_root_length) = decode_paged_index_root(&root_bytes)?;
    if decoded_root_length != root_length {
        return Err(corrupt(
            "paged segment index root length changed while decoding",
        ));
    }
    let index_root_reservation =
        governor.try_reserve_token(MemoryClass::MetadataCache, index_root.bytes());
    drop(root_bytes);
    drop(root_load_reservation);
    let Some(index_root_reservation) = index_root_reservation else {
        return Ok(OpenedSegmentIndex {
            storage_signature,
            root: None,
            index_offset: footer.index_offset,
            data_end,
            reservation: None,
        });
    };
    Ok(OpenedSegmentIndex {
        storage_signature,
        root: Some(index_root),
        index_offset: footer.index_offset,
        data_end,
        reservation: Some(index_root_reservation),
    })
}

pub(super) fn open_segment_region(
    path: &Path,
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
) -> Result<(File, u64)> {
    let mut file = File::open(path)?;
    let physical_length = file.metadata()?.len();
    let logical_end = descriptor
        .file_offset
        .checked_add(descriptor.file_size)
        .ok_or_else(|| corrupt("segment physical bounds overflow"))?;
    let invalid_physical_bounds = if descriptor.pack_id.is_some() {
        logical_end > physical_length
    } else {
        descriptor.file_offset != 0 || physical_length != descriptor.file_size
    };
    if invalid_physical_bounds || descriptor.file_size < (HEADER_SIZE + FOOTER_SIZE) as u64 {
        return Err(corrupt(format!(
            "invalid segment length: {}",
            path.display()
        )));
    }
    let mut header = [0_u8; HEADER_SIZE];
    file.seek(SeekFrom::Start(descriptor.file_offset))?;
    file.read_exact(&mut header)?;
    validate_header(&header, storage_uuid, descriptor, path)?;
    Ok((file, logical_end))
}
