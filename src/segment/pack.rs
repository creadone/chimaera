//! Запись сегментов и pack-файлов, сборка блоков и публикация результата.

use super::codecs::{encode_gorilla_values, encode_raw_values, encode_timestamps};
use super::cursor::OperationCursor;
use super::index::{
    decode_paged_index_root, encode_index, paged_index_root_bytes, series_count, usize_u32,
};
use super::merge::VisibleCursor;
use super::metadata_cache::MetadataCache;
use super::{
    BLOCK_HEADER_SIZE, BLOCK_MAGIC, BlockIndex, CountSumSummary, EncodedRegion, FOOTER_MAGIC,
    FOOTER_SIZE, HEADER_SIZE, MAX_STORED_BLOCK_OVERHEAD, SEGMENT_MAGIC, Segment, SegmentDescriptor,
    TARGET_BLOCK_POINTS,
};
use crate::admission::WorkMemoryGovernor;
use crate::aggregate;
use crate::cache::BlockCache;
use crate::catalog::{BlockCodec, CompressionProfile, ValueCodec};
use crate::error::{Result, invalid};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::layout::Domain;
use crate::types::{AggregateSet, AggregateState, VersionedPoint};
use crate::wal::{UnpublishedFileGuard, put_i64, put_u16, put_u32, put_u64, sync_directory};
use flate2::{Compression, GzBuilder};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek as _, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub(crate) struct SegmentWriteResult {
    pub segment: Arc<Segment>,
    pub bytes_written: u64,
}

pub(crate) struct SegmentWriteRequest {
    pub segment_id: u64,
    pub level: u8,
    pub domain: Domain,
    pub profile: CompressionProfile,
    pub rewrite_count: u32,
    pub sources: Vec<Box<dyn OperationCursor>>,
}

pub(crate) struct SegmentPackResources<'a> {
    pub block_cache: &'a Arc<BlockCache>,
    pub metadata_cache: &'a Arc<MetadataCache>,
    pub file_pool: &'a FilePermitPool,
    pub open_file_cache: Option<&'a Arc<OpenFileCache>>,
    pub work_memory: Option<&'a Arc<WorkMemoryGovernor>>,
}

#[cfg(test)]
pub(crate) fn write_segment(
    root: &Path,
    storage_uuid: [u8; 16],
    segment_id: u64,
    level: u8,
    domain: Domain,
    sources: Vec<Box<dyn OperationCursor>>,
    block_cache: &Arc<BlockCache>,
) -> Result<SegmentWriteResult> {
    if sources.is_empty() || segment_id == 0 || level > 2 {
        return Err(invalid("invalid segment write request"));
    }
    let temporary_directory = root.join("tmp");
    let segment_directory = root
        .join("segments")
        .join(format!("S{:04}", domain.shard_id));
    fs::create_dir_all(&temporary_directory)?;
    fs::create_dir_all(&segment_directory)?;
    let temporary_path = temporary_directory.join(format!("SEG-{segment_id:020}.tmp"));
    let relative_path = format!(
        "segments/S{:04}/SEG-{segment_id:020}-L{level}.tss",
        domain.shard_id
    );
    let final_path = root.join(&relative_path);
    let mut unpublished = UnpublishedFileGuard::new(final_path.clone());
    let mut temporary = UnpublishedFileGuard::new(temporary_path.clone());
    let file_pool = FilePermitPool::new(8)?;
    let result = (|| -> Result<EncodedRegion> {
        let _file_permit = file_pool.acquire()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temporary_path)?;
        temporary.mark_present();
        let mut file = BufWriter::with_capacity(1024 * 1024, file);
        let result = write_segment_region(
            &mut file,
            0,
            storage_uuid,
            SegmentWriteRequest {
                segment_id,
                level,
                domain,
                profile: if level == 0 {
                    CompressionProfile::hot_fast()
                } else {
                    CompressionProfile::columnar()
                },
                rewrite_count: 0,
                sources,
            },
            None,
        )?;
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(result)
    })();
    let mut encoded = result?;
    fs::rename(&temporary_path, &final_path)?;
    temporary.commit();
    unpublished.mark_present();
    sync_directory(&temporary_directory)?;
    sync_directory(&segment_directory)?;
    encoded.descriptor.relative_path = relative_path;
    let bytes_written = encoded.descriptor.file_size;
    let metadata_cache = Arc::new(MetadataCache::new(u64::MAX));
    let segment = Segment::from_written(
        root,
        encoded,
        Arc::clone(block_cache),
        metadata_cache,
        file_pool,
        None,
        storage_uuid,
    )?;
    unpublished.commit();
    Ok(SegmentWriteResult {
        segment,
        bytes_written,
    })
}

#[cfg(test)]
pub(crate) fn write_segment_pack(
    root: &Path,
    storage_uuid: [u8; 16],
    pack_id: u64,
    shard_id: u16,
    requests: Vec<SegmentWriteRequest>,
    resources: SegmentPackResources<'_>,
) -> Result<Vec<SegmentWriteResult>> {
    let (results, unpublished) =
        prepare_segment_pack(root, storage_uuid, pack_id, shard_id, requests, resources)?;
    sync_segment_pack_group(root)?;
    unpublished.commit();
    Ok(results)
}

/// Prepared files remain owned by the returned guard until the coordinator
/// adopts them. A successful return does not yet permit manifest publication.
pub(crate) fn prepare_segment_pack(
    root: &Path,
    storage_uuid: [u8; 16],
    pack_id: u64,
    shard_id: u16,
    requests: Vec<SegmentWriteRequest>,
    resources: SegmentPackResources<'_>,
) -> Result<(Vec<SegmentWriteResult>, UnpublishedFileGuard)> {
    if pack_id == 0
        || requests.is_empty()
        || requests.iter().any(|request| {
            request.sources.is_empty()
                || request.segment_id == 0
                || request.level > 2
                || request.domain.shard_id != shard_id
        })
    {
        return Err(invalid("invalid segment pack request"));
    }
    let temporary_directory = root.join("tmp");
    let segments_directory = root.join("segments");
    let pack_directory = segments_directory.join("packs");
    fs::create_dir_all(&temporary_directory)?;
    let pack_directory_existed = pack_directory.exists();
    fs::create_dir_all(&pack_directory)?;
    if !pack_directory_existed {
        sync_directory(&segments_directory)?;
    }
    let temporary_path = temporary_directory.join(format!("PACK-{pack_id:020}.tmp"));
    let relative_path = format!("segments/packs/PACK-S{shard_id:04}-{pack_id:020}.tsp");
    let final_path = root.join(&relative_path);
    let mut unpublished = UnpublishedFileGuard::new(final_path.clone());
    let mut temporary = UnpublishedFileGuard::new(temporary_path.clone());
    let result = (|| -> Result<Vec<EncodedRegion>> {
        let _file_permit = resources.file_pool.acquire()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temporary_path)?;
        temporary.mark_present();
        let mut file = BufWriter::with_capacity(1024 * 1024, file);
        let mut regions = Vec::with_capacity(requests.len());
        for request in requests {
            let base_offset = file.stream_position()?;
            let mut encoded = write_segment_region(
                &mut file,
                base_offset,
                storage_uuid,
                request,
                resources.work_memory,
            )?;
            encoded.descriptor.relative_path.clone_from(&relative_path);
            encoded.descriptor.pack_id = Some(pack_id);
            encoded.descriptor.file_offset = base_offset;
            regions.push(encoded);
        }
        file.flush()?;
        crate::failpoint::io_error("pack_file_sync")?;
        file.get_ref().sync_all()?;
        crate::failpoint::hit("segment_after_sync");
        Ok(regions)
    })();
    let regions = result?;
    crate::failpoint::io_error("pack_rename")?;
    fs::rename(&temporary_path, &final_path)?;
    temporary.commit();
    unpublished.mark_present();
    crate::failpoint::hit("segment_after_rename");
    let mut results = Vec::with_capacity(regions.len());
    for encoded in regions {
        let bytes_written = encoded.descriptor.file_size;
        results.push(SegmentWriteResult {
            segment: Segment::from_written(
                root,
                encoded,
                Arc::clone(resources.block_cache),
                Arc::clone(resources.metadata_cache),
                resources.file_pool.clone(),
                resources.open_file_cache.cloned(),
                storage_uuid,
            )?,
            bytes_written,
        });
    }
    Ok((results, unpublished))
}

/// Complete the namespace durability barrier after every rename in this group.
/// Callers retain ownership of all prepared files until manifest commit.
pub(crate) fn sync_segment_pack_group(root: &Path) -> Result<()> {
    crate::failpoint::hit("pack_group_before_barrier");
    crate::failpoint::io_error("pack_group_tmp_sync")?;
    sync_directory(&root.join("tmp"))?;
    crate::failpoint::hit("pack_group_after_tmp_sync");
    crate::failpoint::io_error("pack_group_packs_sync")?;
    sync_directory(&root.join("segments/packs"))?;
    // Also covers creation of packs/ by a concurrent group whose own attempt
    // failed. The parent barrier cannot depend on another group's success.
    crate::failpoint::io_error("pack_group_parent_sync")?;
    sync_directory(&root.join("segments"))?;
    crate::failpoint::hit("pack_group_after_barrier");
    Ok(())
}

pub(super) struct SegmentRegionBuilder {
    pub(super) base_offset: u64,
    pub(super) segment_id: u64,
    pub(super) level: u8,
    pub(super) domain: Domain,
    pub(super) profile: CompressionProfile,
    pub(super) rewrite_count: u32,
    pub(super) logical_offset: u64,
    pub(super) block: Vec<VersionedPoint>,
    pub(super) index: Vec<BlockIndex>,
    pub(super) minimum_timestamp: i64,
    pub(super) maximum_timestamp: i64,
    pub(super) minimum_commit: u64,
    pub(super) maximum_commit: u64,
    pub(super) record_count: u64,
    pub(super) aggregate_builder: Option<aggregate::Builder>,
}

impl SegmentRegionBuilder {
    pub(super) fn new(
        file: &mut BufWriter<File>,
        base_offset: u64,
        request: &SegmentWriteRequest,
        work_memory: Option<&Arc<WorkMemoryGovernor>>,
    ) -> Result<Self> {
        let SegmentWriteRequest {
            segment_id,
            level,
            domain,
            profile,
            rewrite_count,
            ..
        } = request;
        file.write_all(&[0_u8; HEADER_SIZE])?;
        Ok(Self {
            base_offset,
            segment_id: *segment_id,
            level: *level,
            domain: *domain,
            profile: *profile,
            rewrite_count: *rewrite_count,
            logical_offset: HEADER_SIZE as u64,
            block: Vec::with_capacity(TARGET_BLOCK_POINTS),
            index: Vec::new(),
            minimum_timestamp: i64::MAX,
            maximum_timestamp: i64::MIN,
            minimum_commit: u64::MAX,
            maximum_commit: 0,
            record_count: 0,
            aggregate_builder: (*level > 0).then(|| match work_memory {
                Some(governor) => {
                    aggregate::Builder::with_work_memory(*domain, Vec::new(), Arc::clone(governor))
                }
                None => aggregate::Builder::new(*domain),
            }),
        })
    }

    pub(super) fn push(&mut self, file: &mut BufWriter<File>, point: VersionedPoint) -> Result<()> {
        if point.key.timestamp_us < self.domain.window_start_us
            || point.key.timestamp_us >= self.domain.window_end_us
        {
            return Err(invalid("segment operation is outside its partition window"));
        }
        if !self.block.is_empty()
            && (self.block[0].key.series != point.key.series
                || self.block.len() >= TARGET_BLOCK_POINTS)
        {
            self.flush_block(file)?;
        }
        self.minimum_timestamp = self.minimum_timestamp.min(point.key.timestamp_us);
        self.maximum_timestamp = self.maximum_timestamp.max(point.key.timestamp_us);
        self.minimum_commit = self.minimum_commit.min(point.commit_id);
        self.maximum_commit = self.maximum_commit.max(point.commit_id);
        self.record_count = self
            .record_count
            .checked_add(1)
            .ok_or_else(|| invalid("segment record count overflow"))?;
        if let Some(builder) = self.aggregate_builder.as_mut() {
            builder.add(point)?;
        }
        self.block.push(point);
        Ok(())
    }

    pub(super) fn flush_block(&mut self, file: &mut BufWriter<File>) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        self.logical_offset = self
            .logical_offset
            .checked_add(write_block(
                file,
                &self.block,
                self.profile,
                self.logical_offset,
                &mut self.index,
            )?)
            .ok_or_else(|| invalid("segment length overflow"))?;
        self.block.clear();
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        file: &mut BufWriter<File>,
        storage_uuid: [u8; 16],
    ) -> Result<EncodedRegion> {
        self.flush_block(file)?;
        if self.record_count == 0 {
            return Err(invalid("cannot write an empty segment"));
        }
        let data_end = self.logical_offset;
        let aggregate_bytes = match self.aggregate_builder.take() {
            Some(builder) => builder.finish()?,
            None => None,
        };
        if let Some(bytes) = aggregate_bytes.as_ref() {
            file.write_all(bytes)?;
            self.logical_offset = self
                .logical_offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| invalid("segment length overflow"))?;
        }
        let index_offset = self.logical_offset;
        let index_bytes = encode_index(&self.index)?;
        let (index_root, root_length) = decode_paged_index_root(&index_bytes)?;
        if root_length != paged_index_root_bytes(&index_bytes)?.len() {
            return Err(invalid("encoded segment index root length disagrees"));
        }
        file.write_all(&index_bytes)?;
        self.logical_offset = self
            .logical_offset
            .checked_add(index_bytes.len() as u64)
            .ok_or_else(|| invalid("segment length overflow"))?;
        let file_length = self
            .logical_offset
            .checked_add(FOOTER_SIZE as u64)
            .ok_or_else(|| invalid("segment length overflow"))?;
        let mut footer = [0_u8; FOOTER_SIZE];
        footer[..8].copy_from_slice(FOOTER_MAGIC);
        put_u16(&mut footer, 8, 3);
        put_u16(&mut footer, 10, FOOTER_SIZE as u16);
        put_u64(&mut footer, 12, index_offset);
        put_u64(&mut footer, 20, index_bytes.len() as u64);
        put_u32(
            &mut footer,
            28,
            u32::try_from(series_count(&self.index))
                .map_err(|_| invalid("too many segment series"))?,
        );
        put_u32(
            &mut footer,
            32,
            u32::try_from(self.index.len()).map_err(|_| invalid("too many segment blocks"))?,
        );
        put_u64(&mut footer, 36, file_length);
        put_u32(
            &mut footer,
            44,
            crc32c::crc32c(paged_index_root_bytes(&index_bytes)?),
        );
        if let Some(bytes) = aggregate_bytes.as_ref() {
            put_u32(
                &mut footer,
                48,
                u32::try_from(bytes.len()).map_err(|_| invalid("aggregate exceeds u32"))?,
            );
            put_u32(
                &mut footer,
                52,
                crc32c::crc32c(aggregate::checksum_bytes(bytes)?),
            );
        }
        let footer_crc = crc32c::crc32c(&footer[..60]);
        put_u32(&mut footer, 60, footer_crc);
        file.write_all(&footer)?;

        let mut header = [0_u8; HEADER_SIZE];
        header[..8].copy_from_slice(SEGMENT_MAGIC);
        put_u16(&mut header, 8, 3);
        put_u16(&mut header, 10, HEADER_SIZE as u16);
        header[12..28].copy_from_slice(&storage_uuid);
        put_u64(&mut header, 28, self.segment_id);
        put_u16(&mut header, 36, self.domain.shard_id);
        put_u32(&mut header, 38, self.domain.policy_id);
        header[42] = self.level;
        put_i64(&mut header, 44, self.domain.window_start_us);
        put_i64(&mut header, 52, self.domain.window_end_us);
        put_i64(&mut header, 60, self.minimum_timestamp);
        put_i64(&mut header, 68, self.maximum_timestamp);
        put_u64(&mut header, 76, self.minimum_commit);
        put_u64(&mut header, 84, self.maximum_commit);
        let header_crc = crc32c::crc32c(&header[..108]);
        put_u32(&mut header, 108, header_crc);
        file.seek(SeekFrom::Start(self.base_offset))?;
        file.write_all(&header)?;
        file.seek(SeekFrom::Start(
            self.base_offset
                .checked_add(file_length)
                .ok_or_else(|| invalid("segment physical offset overflow"))?,
        ))?;

        let descriptor = SegmentDescriptor {
            id: self.segment_id,
            level: self.level,
            shard_id: self.domain.shard_id,
            policy_id: self.domain.policy_id,
            window_start_us: self.domain.window_start_us,
            window_end_us: self.domain.window_end_us,
            min_timestamp_us: self.minimum_timestamp,
            max_timestamp_us: self.maximum_timestamp,
            relative_path: format!(
                "segments/S{:04}/SEG-{:020}-L{}.tss",
                self.domain.shard_id, self.segment_id, self.level
            ),
            pack_id: None,
            file_offset: 0,
            min_commit: self.minimum_commit,
            max_commit: self.maximum_commit,
            record_count: self.record_count,
            file_size: file_length,
            rewrite_count: self.rewrite_count,
        };
        Ok(EncodedRegion {
            descriptor,
            index: self.index,
            index_root,
            index_offset,
            data_end,
            aggregate: aggregate_bytes,
            storage_signature: self.profile.storage_signature(),
        })
    }
}

pub(super) fn write_segment_region(
    file: &mut BufWriter<File>,
    base_offset: u64,
    storage_uuid: [u8; 16],
    request: SegmentWriteRequest,
    work_memory: Option<&Arc<WorkMemoryGovernor>>,
) -> Result<EncodedRegion> {
    let mut builder = SegmentRegionBuilder::new(file, base_offset, &request, work_memory)?;
    let sources = request.sources;
    let mut merged = VisibleCursor::new(sources, u64::MAX)?;
    while let Some(point) = merged.next_latest()? {
        builder.push(file, point)?;
    }
    builder.finish(file, storage_uuid)
}

pub(super) fn write_block(
    file: &mut BufWriter<File>,
    operations: &[VersionedPoint],
    profile: CompressionProfile,
    offset: u64,
    index: &mut Vec<BlockIndex>,
) -> Result<u64> {
    let first = operations
        .first()
        .copied()
        .ok_or_else(|| invalid("cannot encode an empty block"))?;
    let last = operations
        .last()
        .copied()
        .ok_or_else(|| invalid("cannot encode an empty block"))?;
    if operations
        .iter()
        .any(|point| point.key.series != first.key.series)
    {
        return Err(invalid("a segment block must contain one series"));
    }
    let mut count_sum = Some(AggregateState::for_aggregates(AggregateSet::COUNT_SUM));
    for operation in operations.iter().filter(|operation| !operation.is_delete) {
        let value = operation.put_value()?;
        if let Some(summary) = count_sum.as_mut()
            && summary.add(operation.key.timestamp_us, value).is_err()
        {
            // A block summary is an optional read optimization. Finite stored
            // values may still have a sum outside f64; keep the payload and make
            // aggregate planning fall back to decoding it.
            count_sum = None;
        }
    }
    let timestamps = encode_timestamps(operations, profile.timestamp_codec)?;
    let mut versions = Vec::with_capacity(operations.len() * 12);
    let kinds = operations
        .iter()
        .map(|operation| if operation.is_delete { 2 } else { 1 })
        .collect::<Vec<_>>();
    for point in operations {
        let start = versions.len();
        versions.resize(start + 12, 0);
        put_u64(&mut versions, start, point.commit_id);
        put_u32(&mut versions, start + 8, point.ordinal);
    }
    let values = match profile.value_codec {
        ValueCodec::RawFloat64 => encode_raw_values(operations),
        ValueCodec::GorillaXor => encode_gorilla_values(operations),
    };
    let payload_length = 16_usize
        .checked_add(timestamps.len())
        .and_then(|size| size.checked_add(versions.len()))
        .and_then(|size| size.checked_add(kinds.len()))
        .and_then(|size| size.checked_add(values.len()))
        .ok_or_else(|| invalid("segment block length overflow"))?;
    if payload_length > 4 * 1024 * 1024 {
        return Err(invalid("segment block exceeds 4 MiB"));
    }
    let mut payload = vec![0_u8; 16];
    put_u32(
        &mut payload,
        0,
        usize_u32(timestamps.len(), "timestamp stream")?,
    );
    put_u32(
        &mut payload,
        4,
        usize_u32(versions.len(), "version stream")?,
    );
    put_u32(&mut payload, 8, usize_u32(kinds.len(), "operation stream")?);
    put_u32(&mut payload, 12, usize_u32(values.len(), "value stream")?);
    payload.extend_from_slice(&timestamps);
    payload.extend_from_slice(&versions);
    payload.extend_from_slice(&kinds);
    payload.extend_from_slice(&values);
    let uncompressed_size = payload.len();
    let (stored, block_codec_id, block_level_marker) = match profile.block_codec {
        BlockCodec::None => (payload, 0, 0),
        BlockCodec::Gzip => {
            let level = profile
                .effective_block_level()
                .ok_or_else(|| invalid("gzip profile has no effective level"))?;
            let mut encoder = GzBuilder::new()
                .mtime(0)
                .write(Vec::new(), Compression::new(u32::from(level)));
            encoder.write_all(&payload)?;
            let stored = encoder.finish()?;
            (stored, 1, level.saturating_add(1))
        }
        BlockCodec::Zstd => {
            if profile.effective_block_level() != Some(3) {
                return Err(invalid("zstd block level must be 3"));
            }
            // One independent frame with a known content size; no dictionary or
            // codec threads. Input is bounded above before creating the context.
            (zstd::bulk::compress(&payload, 3)?, 2, 4)
        }
    };
    if stored.len() > 4 * 1024 * 1024 + MAX_STORED_BLOCK_OVERHEAD {
        return Err(invalid("stored segment block exceeds format limit"));
    }

    let mut header = [0_u8; BLOCK_HEADER_SIZE];
    header[..4].copy_from_slice(BLOCK_MAGIC);
    put_u16(&mut header, 4, BLOCK_HEADER_SIZE as u16);
    header[6] = 1;
    put_u32(&mut header, 8, first.key.series.space_id);
    put_u32(&mut header, 12, first.key.series.bucket_id);
    put_u64(&mut header, 16, first.key.series.key);
    put_i64(&mut header, 24, first.key.timestamp_us);
    put_i64(&mut header, 32, last.key.timestamp_us);
    let (minimum_commit, maximum_commit) = operations.iter().skip(1).fold(
        (first.commit_id, first.commit_id),
        |(minimum, maximum), point| (minimum.min(point.commit_id), maximum.max(point.commit_id)),
    );
    put_u64(&mut header, 40, minimum_commit);
    put_u64(&mut header, 48, maximum_commit);
    put_u32(
        &mut header,
        56,
        usize_u32(operations.len(), "block points")?,
    );
    let put_count = operations
        .iter()
        .filter(|operation| !operation.is_delete)
        .count();
    put_u32(&mut header, 60, usize_u32(put_count, "block puts")?);
    put_u32(
        &mut header,
        64,
        usize_u32(operations.len() - put_count, "block deletes")?,
    );
    header[68] = profile.timestamp_codec_id();
    header[69] = profile.value_codec_id();
    header[70] = block_codec_id;
    header[71] = block_level_marker;
    put_u32(
        &mut header,
        72,
        usize_u32(uncompressed_size, "block payload")?,
    );
    put_u32(
        &mut header,
        76,
        usize_u32(stored.len(), "stored block payload")?,
    );
    put_u32(&mut header, 80, crc32c::crc32c(&stored));
    let header_crc = crc32c::crc32c(&header[..84]);
    put_u32(&mut header, 84, header_crc);
    file.write_all(&header)?;
    file.write_all(&stored)?;
    let size = BLOCK_HEADER_SIZE
        .checked_add(stored.len())
        .ok_or_else(|| invalid("block size overflow"))?;
    index.push(BlockIndex {
        series: first.key.series,
        offset,
        size: usize_u32(size, "encoded block")?,
        min_timestamp_us: first.key.timestamp_us,
        max_timestamp_us: last.key.timestamp_us,
        count_sum: count_sum.map(|summary| {
            let (count, raw_sum, compensation) = summary.count_sum_parts();
            CountSumSummary {
                count,
                raw_sum,
                compensation,
            }
        }),
    });
    Ok(size as u64)
}
