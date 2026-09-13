//! Query-independent index pages in the existing bounded metadata cache.

use super::*;
use crate::segment::metadata_cache::{MetadataCache, MetadataCacheKey};

#[cfg(test)]
mod tests;

// Opening and header validation happen only on an actual cache miss. Neither
// the cache mutex nor a metadata load mutex is held while reading the file.
fn opened_file<'a, 'b>(
    opened: &'b mut Option<QueryFile<'a>>,
    resources: QueryMetadataResources<'a>,
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
) -> Result<&'b mut QueryFile<'a>> {
    if opened.is_none() {
        let mut file = QueryFile::open(
            resources.path,
            resources.file_pool,
            resources.open_file_cache,
            resources.query_budget,
        )?;
        validate_query_segment_region(&mut file, resources.path, storage_uuid, descriptor)?;
        *opened = Some(file);
    }
    opened
        .as_mut()
        .ok_or_else(|| corrupt("index page file was not opened"))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::segment) fn read_paged_metadata_for_query(
    storage_uuid: [u8; 16],
    descriptor: &SegmentDescriptor,
    resident_index_root: Option<&PagedIndexRoot>,
    index_offset: u64,
    data_end: u64,
    requested_series: &[SeriesKey],
    cache: &MetadataCache,
    resources: QueryMetadataResources<'_>,
) -> Result<Option<LoadedPagedMetadata>> {
    let query_budget = resources.query_budget;
    query_budget.check()?;
    let root_offset = descriptor
        .file_offset
        .checked_add(index_offset)
        .ok_or_else(|| corrupt("segment index root offset overflows"))?;
    let mut requested = requested_series.to_vec();
    requested.sort_unstable();
    requested.dedup();
    let mut opened = None;
    let mut bytes_read = 0_u64;
    let selected = if let Some(index_root) = resident_index_root {
        select_index_pages(&index_root.pages, &requested)
    } else {
        let file = opened_file(&mut opened, resources, storage_uuid, descriptor)?;
        let logical_end = descriptor
            .file_offset
            .checked_add(descriptor.file_size)
            .ok_or_else(|| corrupt("segment physical bounds overflow"))?;
        let mut footer_bytes = [0_u8; FOOTER_SIZE];
        file.read_exact_at(logical_end - FOOTER_SIZE as u64, &mut footer_bytes)?;
        let footer = validate_footer(&footer_bytes, descriptor.file_size)?;
        if footer.version != 3
            || footer.index_offset != index_offset
            || footer
                .index_offset
                .checked_sub(u64::from(footer.aggregate_length))
                != Some(data_end)
        {
            return Err(corrupt("segment index layout changed after open"));
        }
        let mut root_header = [0_u8; PAGED_INDEX_HEADER_SIZE];
        file.read_exact_at(root_offset, &mut root_header)?;
        let root_length = read_u32(&root_header, 20) as usize;
        if &root_header[..8] != PAGED_INDEX_MAGIC
            || root_length < PAGED_INDEX_HEADER_SIZE
            || root_length as u64 > footer.index_length
            || root_length > MAX_INDEX_BYTES
        {
            return Err(corrupt("invalid paged segment index root length"));
        }
        let selected = stream_index_root_selection(
            file,
            root_offset,
            &root_header,
            root_length,
            footer.index_crc,
            &requested,
            query_budget,
        )?;
        query_budget.record_index_root_read();
        bytes_read += FOOTER_SIZE as u64 + root_length as u64;
        selected
    };
    let selected_block_count = selected.iter().try_fold(0_usize, |total, page| {
        total
            .checked_add(page.block_count as usize)
            .ok_or_else(|| corrupt("selected metadata block count overflow"))
    })?;
    let metadata_reserved =
        SegmentMetadata::estimated_bytes(selected_block_count, requested.len())?;
    query_budget.reserve_memory_class(QueryMemoryClass::Metadata, metadata_reserved)?;
    let result = (|| {
        let mut index = Vec::with_capacity(selected_block_count);
        for page in selected {
            query_budget.check()?;
            let absolute_offset = root_offset
                .checked_add(page.offset)
                .ok_or_else(|| corrupt("index page offset overflows"))?;
            let key = MetadataCacheKey::IndexPage(descriptor.id, absolute_offset);
            if let Some(cached) = cache.get_blocks_entry(key)? {
                index.extend(
                    cached
                        .index
                        .iter()
                        .copied()
                        .filter(|entry| requested.binary_search(&entry.series).is_ok()),
                );
                continue;
            }
            // The selected output and this temporary decoded page coexist.
            // Charge both before allocation, even if cache admission later fails.
            let temporary_bytes =
                u64::from(page.length).saturating_add(SegmentMetadata::estimated_bytes(
                    page.block_count as usize,
                    page.block_count as usize,
                )?);
            query_budget.reserve_memory_class(QueryMemoryClass::Metadata, temporary_bytes)?;
            let page_result = (|| {
                let file = opened_file(&mut opened, resources, storage_uuid, descriptor)?;
                let mut bytes = vec![0_u8; page.length as usize];
                file.read_exact_at(absolute_offset, &mut bytes)?;
                if crc32c::crc32c(&bytes) != page.crc32c {
                    return Err(corrupt("paged segment index page CRC32C mismatch"));
                }
                let decoded = decode_index_page(&bytes, data_end)?;
                if decoded.len() != page.block_count as usize
                    || decoded.first().map(|entry| entry.series) != Some(page.first_series)
                    || decoded.last().map(|entry| entry.series) != Some(page.last_series)
                {
                    return Err(corrupt("paged segment index page disagrees with its root"));
                }
                let (cached, _) =
                    cache.insert_blocks_entry(key, Arc::new(SegmentMetadata::new(decoded)))?;
                index.extend(
                    cached
                        .index
                        .iter()
                        .copied()
                        .filter(|entry| requested.binary_search(&entry.series).is_ok()),
                );
                Ok(())
            })();
            query_budget.release_memory_class(QueryMemoryClass::Metadata, temporary_bytes);
            page_result?;
            query_budget.record_index_pages_read(1);
            bytes_read = bytes_read.saturating_add(u64::from(page.length));
        }
        if opened.is_some() {
            bytes_read = bytes_read.saturating_add(HEADER_SIZE as u64);
        }
        Ok(LoadedPagedMetadata {
            metadata: Arc::new(SegmentMetadata::new(index)),
            bytes_read,
            metadata_reserved,
        })
    })();
    if result.is_err() {
        query_budget.release_memory_class(QueryMemoryClass::Metadata, metadata_reserved);
    }
    result.map(Some)
}
