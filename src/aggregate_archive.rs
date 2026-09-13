use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::aggregate::{self, AggregateRecord};
use crate::error::{Result, corrupt, invalid};
use crate::file_pool::FilePermitPool;
use crate::layout::Domain;
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::{AggregateResolution, SeriesKey};
use crate::wal::{
    UnpublishedFileGuard, put_i64, put_u16, put_u32, put_u64, read_i64, read_u16, read_u32,
    read_u64, sync_directory,
};

const MAGIC: &[u8; 8] = b"TSARC001";
const VERSION: u16 = 2;
const HEADER_SIZE: usize = 96;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AggregateArchiveDescriptor {
    pub id: u64,
    pub shard_id: u16,
    pub policy_id: u32,
    pub window_start_us: i64,
    pub window_end_us: i64,
    pub coverage_from_us: i64,
    pub coverage_to_us: i64,
    pub relative_path: String,
    pub file_size: u64,
    pub section_crc32c: u32,
    pub resolutions: Vec<AggregateResolution>,
    #[serde(default)]
    pub minimum_series: Option<SeriesKey>,
    #[serde(default)]
    pub maximum_series: Option<SeriesKey>,
    #[serde(default)]
    pub chunk_index: u32,
    #[serde(default = "one_chunk")]
    pub chunk_count: u32,
}

const fn one_chunk() -> u32 {
    1
}

impl AggregateArchiveDescriptor {
    pub(crate) const fn domain(&self) -> Domain {
        Domain {
            shard_id: self.shard_id,
            policy_id: self.policy_id,
            window_start_us: self.coverage_from_us,
            window_end_us: self.coverage_to_us,
        }
    }

    pub(crate) fn validate(&self, virtual_shards: u16, next_id: u64) -> Result<()> {
        if self.id == 0
            || self.id >= next_id
            || self.shard_id >= virtual_shards
            || self.policy_id == 0
            || self.window_start_us >= self.window_end_us
            || self.coverage_from_us < self.window_start_us
            || self.coverage_to_us > self.window_end_us
            || self.coverage_from_us >= self.coverage_to_us
            || self.file_size <= HEADER_SIZE as u64
            || !self.relative_path.starts_with("aggregates/")
            || self.relative_path.contains("..")
            || self.resolutions.is_empty()
            || self.resolutions.windows(2).any(|pair| pair[0] >= pair[1])
            || self.minimum_series.is_some() != self.maximum_series.is_some()
            || self
                .minimum_series
                .zip(self.maximum_series)
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            || self.chunk_count == 0
            || self.chunk_index >= self.chunk_count
        {
            return Err(corrupt(
                "manifest contains invalid aggregate archive metadata",
            ));
        }
        for resolution in &self.resolutions {
            let duration = resolution.duration_us();
            if self.coverage_from_us.rem_euclid(duration) != 0
                || self.coverage_to_us.rem_euclid(duration) != 0
            {
                return Err(corrupt("aggregate archive coverage is not aligned"));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AggregateArchive {
    root: PathBuf,
    storage_uuid: [u8; 16],
    descriptor: AggregateArchiveDescriptor,
    file_pool: FilePermitPool,
}

pub(crate) struct AggregateArchiveWrite {
    pub id: u64,
    pub domain: Domain,
    pub coverage_from_us: i64,
    pub coverage_to_us: i64,
    pub resolutions: Vec<AggregateResolution>,
    pub section: Vec<u8>,
    pub minimum_series: SeriesKey,
    pub maximum_series: SeriesKey,
    pub chunk_index: u32,
    pub chunk_count: u32,
}

impl AggregateArchive {
    pub(crate) fn open(
        root: &Path,
        storage_uuid: [u8; 16],
        descriptor: AggregateArchiveDescriptor,
        file_pool: FilePermitPool,
    ) -> Result<Self> {
        let archive = Self {
            root: root.to_path_buf(),
            storage_uuid,
            descriptor,
            file_pool,
        };
        let _permit = archive.file_pool.acquire()?;
        let mut file = File::open(archive.path())?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        validate_header(&header, storage_uuid, &archive.descriptor)?;
        if file.metadata()?.len() != archive.descriptor.file_size {
            return Err(corrupt("aggregate archive length does not match manifest"));
        }
        Ok(archive)
    }

    pub(crate) fn write(
        root: &Path,
        storage_uuid: [u8; 16],
        mut request: AggregateArchiveWrite,
        file_pool: FilePermitPool,
    ) -> Result<Arc<Self>> {
        if request.section.is_empty() {
            return Err(invalid("aggregate archive section must not be empty"));
        }
        request.resolutions.sort_unstable();
        request.resolutions.dedup();
        let section_crc32c = crc32c::crc32c(aggregate::checksum_bytes(&request.section)?);
        let file_size = (HEADER_SIZE as u64)
            .checked_add(
                u64::try_from(request.section.len())
                    .map_err(|_| invalid("archive is too large"))?,
            )
            .ok_or_else(|| invalid("archive length overflows u64"))?;
        let relative_path = format!("aggregates/AGG-{:020}.tsa", request.id);
        let descriptor = AggregateArchiveDescriptor {
            id: request.id,
            shard_id: request.domain.shard_id,
            policy_id: request.domain.policy_id,
            window_start_us: request.domain.window_start_us,
            window_end_us: request.domain.window_end_us,
            coverage_from_us: request.coverage_from_us,
            coverage_to_us: request.coverage_to_us,
            relative_path,
            file_size,
            section_crc32c,
            resolutions: request.resolutions,
            minimum_series: Some(request.minimum_series),
            maximum_series: Some(request.maximum_series),
            chunk_index: request.chunk_index,
            chunk_count: request.chunk_count,
        };
        let final_path = root.join(&descriptor.relative_path);
        let mut unpublished = UnpublishedFileGuard::new(final_path.clone());
        let temporary = root.join(format!("tmp/AGG-{:020}.tsa.tmp", request.id));
        let mut unpublished_temporary = UnpublishedFileGuard::new(temporary.clone());
        let directory = final_path
            .parent()
            .ok_or_else(|| invalid("aggregate archive has no parent directory"))?;
        fs::create_dir_all(directory)?;
        let header = encode_header(storage_uuid, &descriptor)?;
        {
            let _permit = file_pool.acquire()?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            unpublished_temporary.mark_present();
            file.write_all(&header)?;
            file.write_all(&request.section)?;
            file.sync_all()?;
        }
        fs::rename(&temporary, &final_path)?;
        unpublished_temporary.commit();
        unpublished.mark_present();
        sync_directory(directory)?;
        sync_directory(root)?;
        let archive = Self::open(root, storage_uuid, descriptor, file_pool).map(Arc::new)?;
        unpublished.commit();
        Ok(archive)
    }

    pub(crate) const fn descriptor(&self) -> &AggregateArchiveDescriptor {
        &self.descriptor
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.root.join(&self.descriptor.relative_path)
    }

    pub(crate) fn cursor(
        &self,
        series: SeriesKey,
        resolution: AggregateResolution,
        from_us: i64,
        to_us: i64,
        budget: Arc<QueryBudget>,
    ) -> Result<Option<ArchiveCursor>> {
        if !self.descriptor.resolutions.contains(&resolution)
            || self.descriptor.coverage_from_us >= to_us
            || self.descriptor.coverage_to_us <= from_us
            || self
                .descriptor
                .minimum_series
                .is_some_and(|minimum| series < minimum)
            || self
                .descriptor
                .maximum_series
                .is_some_and(|maximum| series > maximum)
        {
            return Ok(None);
        }
        let section_bytes = self.descriptor.file_size.saturating_sub(HEADER_SIZE as u64);
        let load_started = Instant::now();
        let result = (|| {
            let _permit = self.file_pool.acquire_for_query(&budget)?;
            let mut file = File::open(self.path())?;
            let mut header = [0_u8; HEADER_SIZE];
            file.read_exact(&mut header)?;
            validate_header(&header, self.storage_uuid, &self.descriptor)?;
            if read_u16(&header, 8) == VERSION {
                let section_length = usize::try_from(section_bytes)
                    .map_err(|_| corrupt("aggregate archive exceeds usize"))?;
                let mut aggregate_header = [0_u8; 32];
                file.read_exact(&mut aggregate_header)?;
                let root_length =
                    aggregate::root_length_from_header(&aggregate_header, section_length)?;
                let root_reserved = u64::try_from(root_length)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2);
                budget.reserve_memory_class(QueryMemoryClass::Metadata, root_reserved)?;
                let partial = (|| {
                    let mut root = vec![0_u8; root_length];
                    root[..aggregate_header.len()].copy_from_slice(&aggregate_header);
                    file.read_exact(&mut root[aggregate_header.len()..])?;
                    if crc32c::crc32c(&root) != self.descriptor.section_crc32c {
                        return Err(corrupt("aggregate archive root checksum mismatch"));
                    }
                    let Some(page) =
                        aggregate::locate_page(&root, section_length, series, resolution)?
                    else {
                        return Ok(None);
                    };
                    let page_bytes = u64::from(page.length);
                    let selected_bytes = 32_u64.saturating_add(40).saturating_add(page_bytes);
                    budget.reserve_memory_class(QueryMemoryClass::Metadata, page_bytes)?;
                    budget.reserve_memory_class(QueryMemoryClass::Metadata, selected_bytes)?;
                    let page_result = (|| {
                        file.seek(std::io::SeekFrom::Start(HEADER_SIZE as u64 + page.offset))?;
                        let mut records = vec![0_u8; page.length as usize];
                        file.read_exact(&mut records)?;
                        if crc32c::crc32c(&records) != page.crc32c {
                            return Err(corrupt("aggregate archive page checksum mismatch"));
                        }
                        let selected = aggregate::section_from_page(series, resolution, &records)?;
                        aggregate::validate(&selected, self.descriptor.domain())?;
                        let section = Arc::new(aggregate::Section::new(Some(selected)));
                        let cursor = aggregate::Cursor::for_series(
                            section,
                            series,
                            resolution,
                            from_us.max(self.descriptor.coverage_from_us),
                            to_us.min(self.descriptor.coverage_to_us),
                        )?;
                        Ok(cursor.map(|cursor| (cursor, selected_bytes)))
                    })();
                    budget.release_memory_class(QueryMemoryClass::Metadata, page_bytes);
                    match page_result {
                        Ok(Some(value)) => Ok(Some(value)),
                        Ok(None) => {
                            budget.release_memory_class(QueryMemoryClass::Metadata, selected_bytes);
                            Ok(None)
                        }
                        Err(error) => {
                            budget.release_memory_class(QueryMemoryClass::Metadata, selected_bytes);
                            Err(error)
                        }
                    }
                })();
                budget.release_memory_class(QueryMemoryClass::Metadata, root_reserved);
                budget.record_file_open();
                budget.record_metadata_bytes(
                    (HEADER_SIZE + root_length) as u64
                        + partial
                            .as_ref()
                            .ok()
                            .and_then(|value| value.as_ref().map(|(_, bytes)| *bytes - 72))
                            .unwrap_or(0),
                );
                budget.record_metadata(load_started.elapsed());
                return partial;
            }
            budget.reserve_memory_class(QueryMemoryClass::Metadata, section_bytes)?;
            let full = (|| {
                let mut bytes = vec![
                    0_u8;
                    usize::try_from(section_bytes)
                        .map_err(|_| corrupt("aggregate archive exceeds usize"))?
                ];
                file.read_exact(&mut bytes)?;
                if crc32c::crc32c(&bytes) != self.descriptor.section_crc32c {
                    return Err(corrupt("aggregate archive checksum mismatch"));
                }
                aggregate::validate(&bytes, self.descriptor.domain())?;
                let section = Arc::new(aggregate::Section::new(Some(bytes)));
                aggregate::Cursor::for_series(
                    section,
                    series,
                    resolution,
                    from_us.max(self.descriptor.coverage_from_us),
                    to_us.min(self.descriptor.coverage_to_us),
                )
            })();
            budget.record_file_open();
            budget.record_metadata_bytes(HEADER_SIZE as u64 + section_bytes);
            budget.record_metadata(load_started.elapsed());
            match full {
                Ok(Some(cursor)) => Ok(Some((cursor, section_bytes))),
                Ok(None) => {
                    budget.release_memory_class(QueryMemoryClass::Metadata, section_bytes);
                    Ok(None)
                }
                Err(error) => {
                    budget.release_memory_class(QueryMemoryClass::Metadata, section_bytes);
                    Err(error)
                }
            }
        })();
        match result {
            Ok(Some((cursor, reserved_bytes))) => Ok(Some(ArchiveCursor {
                cursor,
                budget,
                reserved_bytes,
            })),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn verify(&self) -> Result<()> {
        let _permit = self.file_pool.acquire()?;
        let mut file = File::open(self.path())?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        validate_header(&header, self.storage_uuid, &self.descriptor)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if crc32c::crc32c(aggregate::checksum_bytes(&bytes)?) != self.descriptor.section_crc32c {
            return Err(corrupt("aggregate archive checksum mismatch"));
        }
        aggregate::validate(&bytes, self.descriptor.domain())?;
        Ok(())
    }
}

pub(crate) struct ArchiveCursor {
    cursor: aggregate::Cursor,
    budget: Arc<QueryBudget>,
    reserved_bytes: u64,
}

impl ArchiveCursor {
    pub(crate) fn next_record(&mut self) -> Result<Option<AggregateRecord>> {
        self.cursor.next_record()
    }
}

impl Drop for ArchiveCursor {
    fn drop(&mut self) {
        self.budget
            .release_memory_class(QueryMemoryClass::Metadata, self.reserved_bytes);
    }
}

fn encode_header(
    storage_uuid: [u8; 16],
    descriptor: &AggregateArchiveDescriptor,
) -> Result<[u8; HEADER_SIZE]> {
    let section_length = descriptor
        .file_size
        .checked_sub(HEADER_SIZE as u64)
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| invalid("aggregate archive section exceeds u32"))?;
    let mut bytes = [0_u8; HEADER_SIZE];
    bytes[..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, HEADER_SIZE as u16);
    bytes[12..28].copy_from_slice(&storage_uuid);
    put_u64(&mut bytes, 28, descriptor.id);
    put_u16(&mut bytes, 36, descriptor.shard_id);
    put_u16(&mut bytes, 38, 1);
    put_u32(&mut bytes, 40, descriptor.policy_id);
    put_i64(&mut bytes, 44, descriptor.window_start_us);
    put_i64(&mut bytes, 52, descriptor.window_end_us);
    put_i64(&mut bytes, 60, descriptor.coverage_from_us);
    put_i64(&mut bytes, 68, descriptor.coverage_to_us);
    put_u32(&mut bytes, 76, section_length);
    put_u32(&mut bytes, 80, descriptor.section_crc32c);
    let crc = crc32c::crc32c(&bytes[..84]);
    put_u32(&mut bytes, 84, crc);
    put_u32(&mut bytes, 88, descriptor.chunk_index);
    put_u32(&mut bytes, 92, descriptor.chunk_count);
    Ok(bytes)
}

fn validate_header(
    bytes: &[u8; HEADER_SIZE],
    storage_uuid: [u8; 16],
    descriptor: &AggregateArchiveDescriptor,
) -> Result<()> {
    let section_length = descriptor.file_size.saturating_sub(HEADER_SIZE as u64);
    if &bytes[..8] != MAGIC
        || !matches!(read_u16(bytes, 8), 1 | VERSION)
        || read_u16(bytes, 10) as usize != HEADER_SIZE
        || bytes[12..28] != storage_uuid
        || read_u64(bytes, 28) != descriptor.id
        || read_u16(bytes, 36) != descriptor.shard_id
        || read_u16(bytes, 38) != 1
        || read_u32(bytes, 40) != descriptor.policy_id
        || read_i64(bytes, 44) != descriptor.window_start_us
        || read_i64(bytes, 52) != descriptor.window_end_us
        || read_i64(bytes, 60) != descriptor.coverage_from_us
        || read_i64(bytes, 68) != descriptor.coverage_to_us
        || u64::from(read_u32(bytes, 76)) != section_length
        || read_u32(bytes, 80) != descriptor.section_crc32c
        || read_u32(bytes, 84) != crc32c::crc32c(&bytes[..84])
        || (read_u16(bytes, 8) == 1 && bytes[88..].iter().any(|byte| *byte != 0))
        || (read_u16(bytes, 8) == VERSION
            && (read_u32(bytes, 88) != descriptor.chunk_index
                || read_u32(bytes, 92) != descriptor.chunk_count))
    {
        return Err(corrupt("invalid aggregate archive header"));
    }
    Ok(())
}
