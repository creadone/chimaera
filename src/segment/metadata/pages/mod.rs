//! Query-independent index pages in the existing bounded metadata cache.

use super::*;
use crate::segment::metadata::cache::MetadataCacheKey;
mod cursor;
mod large;

pub(in crate::segment::metadata) mod watermarks;
pub(in crate::segment) use cursor::BlockCursor;

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

// The caller charges encoded input and decoded output before entering here.
fn read_page(
    file: &mut QueryFile<'_>,
    offset: u64,
    page: IndexPageDescriptor,
    data_end: u64,
) -> Result<Arc<SegmentMetadata>> {
    let mut bytes = vec![0; page.length as usize];
    file.read_exact_at(offset, &mut bytes)?;
    if crc32c::crc32c(&bytes) != page.crc32c {
        return Err(corrupt("paged segment index page CRC32C mismatch"));
    }
    let index = decode_index_page(&bytes, data_end)?;
    if !page.matches_blocks(&index) {
        return Err(corrupt("paged segment index page disagrees with its root"));
    }
    Ok(Arc::new(SegmentMetadata::new(index)))
}
