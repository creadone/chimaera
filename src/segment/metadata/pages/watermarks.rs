//! Recover exact series tails from existing CRC-protected index fences.
//! Single-series pages need no leaf reads. Mixed pages are decoded one at a time;
//! payload/header consistency is checked by reads and full verification.
use super::*;
use crate::diagnostics::publication::{Counter, add};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor};
use crate::segment::Segment;

fn reserve(
    work: &Arc<WorkMemoryGovernor>,
    bytes: u64,
) -> Result<crate::memory::work::WorkMemoryReservation> {
    work.try_reserve_background_with_headroom(WorkMemoryClass::Recovery, bytes, 0)?
        .ok_or_else(|| {
            crate::Error::ResourceLimit(format!("watermark index needs {bytes} work bytes"))
        })
}

pub(in crate::segment) fn visit(
    segment: &Segment,
    work: &Arc<WorkMemoryGovernor>,
    mut visit: impl FnMut(SeriesKey, i64) -> Result<()>,
) -> Result<()> {
    let descriptor = &segment.descriptor;
    let permit = segment.file_pool.acquire()?;
    let (file, end) = open_segment_region(&segment.path, segment.storage_uuid, descriptor)?;
    let length = file.metadata()?.len();
    let mut file = QueryFile::Owned {
        file,
        length,
        _permit: permit,
    };
    let mut footer_bytes = [0; FOOTER_SIZE];
    file.read_exact_at(end - FOOTER_SIZE as u64, &mut footer_bytes)?;
    let footer = validate_footer(&footer_bytes, descriptor.file_size)?;
    if footer.version != 3
        || footer.index_offset != segment.index_offset
        || footer
            .index_offset
            .checked_sub(u64::from(footer.aggregate_length))
            != Some(segment.data_end)
    {
        return Err(corrupt("watermark index layout changed after open"));
    }
    let root_offset = descriptor.file_offset + footer.index_offset;
    let expected_series = u64::from(read_u32(&footer_bytes, 28));
    let expected_blocks = u64::from(read_u32(&footer_bytes, 32));
    let mut count = 0_u64;
    let mut current = None::<(SeriesKey, i64)>;
    let mut record = |series: SeriesKey, time: i64| -> Result<()> {
        if time < descriptor.min_timestamp_us || time > descriptor.max_timestamp_us {
            return Err(corrupt("watermark time is outside segment bounds"));
        }
        match current {
            Some((prior, last)) if prior == series => {
                if time <= last {
                    return Err(corrupt("watermark index times are not increasing"));
                }
            }
            Some((prior, last)) => {
                if prior > series {
                    return Err(corrupt("watermark index series are not ordered"));
                }
                visit(prior, last)?;
                count += 1;
            }
            None => {}
        }
        current = Some((series, time));
        Ok(())
    };
    let mut blocks = 0_u64;
    let mut last_page_end = 0_u64;
    let mut bytes_read = (HEADER_SIZE + FOOTER_SIZE) as u64;
    let mut page = |file: &mut QueryFile<'_>, _: usize, page: IndexPageDescriptor| {
        last_page_end = page
            .offset
            .checked_add(u64::from(page.length))
            .ok_or_else(|| corrupt("watermark index page end overflows"))?;
        if last_page_end > footer.index_length {
            return Err(corrupt("watermark page is outside the index"));
        }
        blocks += u64::from(page.block_count);
        if page.first_series == page.last_series {
            if page.first_timestamp < descriptor.min_timestamp_us {
                return Err(corrupt("watermark page is outside segment time bounds"));
            }
            return record(page.last_series, page.last_timestamp);
        }
        let bytes = u64::from(page.length)
            .checked_add(SegmentMetadata::estimated_bytes(
                page.block_count as usize,
                page.block_count as usize,
            )?)
            .ok_or_else(|| corrupt("watermark page memory overflows"))?;
        // Owned by this page read; no cache admission or retention after the callback.
        let _memory = reserve(work, bytes)?;
        let decoded = read_page(file, root_offset + page.offset, page, segment.data_end)?;
        bytes_read += u64::from(page.length);
        for block in &decoded.index {
            record(block.series, block.max_timestamp_us)?;
        }
        Ok(())
    };
    if let Some(root) = &segment.index_root {
        for (number, descriptor) in root.pages.iter().copied().enumerate() {
            page(&mut file, number, descriptor)?;
        }
    } else {
        let _scratch = reserve(work, INDEX_ROOT_STREAM_BUFFER_SIZE as u64)?;
        let mut header = [0; PAGED_INDEX_HEADER_SIZE];
        file.read_exact_at(root_offset, &mut header)?;
        let root_length = read_u32(&header, 20) as usize;
        if root_length as u64 > footer.index_length || root_length > MAX_INDEX_BYTES {
            return Err(corrupt("watermark index root exceeds its section"));
        }
        visit_index_root(
            &mut file,
            root_offset,
            &header,
            root_length,
            footer.index_crc,
            None,
            &mut page,
        )?;
        bytes_read += root_length as u64;
    }
    if blocks != expected_blocks || last_page_end != footer.index_length {
        return Err(corrupt("watermark index block count or length mismatch"));
    }
    if let Some((series, time)) = current {
        visit(series, time)?;
        count += 1;
    }
    if count != expected_series {
        return Err(corrupt("watermark index series count mismatch"));
    }
    add(
        Counter::SegmentMetadataReadBytes,
        usize::try_from(bytes_read).unwrap_or(usize::MAX),
    );
    Ok(())
}
