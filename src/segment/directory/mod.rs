use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::Serialize;
use uuid::Uuid;

use super::{
    BlockIndex, CountSumSummary, Segment, put_i64, put_u32, put_u64, read_i64, read_u32, read_u64,
    sync_directory,
};
use crate::diagnostics::publication::{self as cost, Counter as CostCounter, Stage as CostStage};
use crate::error::{Result, corrupt, invalid};
use crate::memory::cache::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor};
use crate::persistence::files::{FilePermitPool, OpenFileCache};
use crate::persistence::manifest::{Manifest, PublishedReadDirectoryDescriptor};
use crate::persistence::wal::UnpublishedFileGuard;
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::SeriesKey;

mod build;

mod cursor;
mod index;

pub(in crate::segment) use cursor::BlockCursor;
mod pages;
pub(crate) use pages::DirectoryPagePool;

const MAGIC: &[u8; 8] = b"TSPDR001";
const LEGACY_FORMAT: u32 = 1;
const PUBLISHED_FORMAT: u32 = 2;
const TIMED_FORMAT: u32 = 3;
const TIMED_PUBLISHED_FORMAT: u32 = 4;
const HEADER_SIZE: usize = 96;
const ROOT_ENTRY_SIZE: usize = 56;
const LOCATOR_SIZE: usize = 88;
const MAX_PAGE_BYTES: usize = 64 * 1024;
// Sparse histories may contain one locator for each of hundreds of thousands of
// series.  Keeping every series on a separate page makes the resident root grow
// by 56 bytes per series and per epoch.  A small fanout keeps point lookups
// selective while bounding the root for wide, sparse generations.
const MAX_SERIES_PER_PAGE: usize = 32;
const MAX_ROOT_BYTES: usize = 1024 * 1024;
const MAX_PUBLISHED_ROOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_EMBEDDED_DESCRIPTOR_BYTES: usize = 64 * 1024 * 1024;
const MAX_BUILD_BYTES: usize = 512 * 1024 * 1024;
const SUMMARY_PRESENT: u32 = 1;
const PAGE_REFERENCED: u64 = 1 << 63;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OfflinePackDirectoryInfo {
    pub format: &'static str,
    pub checkpoint_commit_id: u64,
    pub logical_segments: usize,
    pub physical_files: usize,
    pub block_locators: u64,
    pub pages: usize,
    pub root_bytes: u64,
    pub artifact_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct PageDescriptor {
    first_series: SeriesKey,
    last_series: SeriesKey,
    offset: u64,
    length: u32,
    entries: u32,
    crc32c: u32,
    level: u32,
    min_timestamp_us: i64,
    max_timestamp_us: i64,
}

#[derive(Clone, Copy, Debug)]
struct Locator {
    series: SeriesKey,
    segment_id: u64,
    block: BlockIndex,
}

pub(crate) struct OfflinePackDirectory {
    path: PathBuf,
    published_scope: Option<(u32, i64, i64)>,
    shard_scope: Option<u16>,
    pages: Vec<PageDescriptor>,
    pages_start: u64,
    pages_end: u64,

    segments: HashMap<u64, Arc<Segment>>,
    detached: bool,
    file_cache: Arc<OpenFileCache>,

    memory_governor: Arc<MemoryGovernor>,
    page_cache_max_bytes: u64,
    page_pool: Arc<DirectoryPagePool>,
    page_cache: Mutex<DirectoryPageCache>,
    // One bounded cold-page loader per immutable directory. Waiters use the
    // existing query deadline/cancellation-aware mutex acquisition.
    page_load: Mutex<()>,
    _root_memory: MemoryReservation,
}

struct CachedDirectoryPage {
    page: Arc<DirectoryPage>,
    last_used: u64,
}

struct DirectoryPage {
    bytes: Arc<Vec<u8>>,
    _memory: MemoryReservation,
}

#[derive(Default)]
struct DirectoryPageCache {
    entries: HashMap<u64, CachedDirectoryPage>,
    bytes: u64,
    clock: u64,
}

impl DirectoryPageCache {
    fn remove(&mut self, offset: u64, pool: &DirectoryPagePool) -> Option<CachedDirectoryPage> {
        let entry = self.entries.remove(&offset)?;
        let bytes = entry.page.bytes.len() as u64;
        self.bytes = self.bytes.saturating_sub(bytes);
        pool.remove_cached_bytes(bytes);
        Some(entry)
    }
}

impl OfflinePackDirectory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        path: &Path,
        manifest: &Manifest,
        segments: &[Arc<Segment>],
        memory_governor: &Arc<MemoryGovernor>,
        page_pool: &Arc<DirectoryPagePool>,
        file_pool: &FilePermitPool,
        file_cache: Arc<OpenFileCache>,
        page_cache_max_bytes: u64,
    ) -> Result<Arc<Self>> {
        Self::open_internal(
            path,
            manifest.storage_uuid,
            manifest.checkpoint_commit,
            u64::try_from(segments.len()).unwrap_or(u64::MAX),
            manifest_crc32c(manifest)?,
            segments,
            memory_governor,
            page_pool,
            file_pool,
            file_cache,
            page_cache_max_bytes,
            MAX_ROOT_BYTES,
            None,
            false,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_internal(
        path: &Path,
        storage_uuid: [u8; 16],
        identity_commit: u64,
        segment_count: u64,
        segments_crc32c: u32,
        segments: &[Arc<Segment>],
        memory_governor: &Arc<MemoryGovernor>,
        page_pool: &Arc<DirectoryPagePool>,
        file_pool: &FilePermitPool,
        file_cache: Arc<OpenFileCache>,
        page_cache_max_bytes: u64,
        maximum_root_bytes: usize,
        published_scope: Option<(u32, i64, i64)>,
        detached: bool,
        shard: Option<u16>,
        budget: Option<&crate::query::QueryBudget>,
    ) -> Result<Arc<Self>> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid(
                "offline pack directory must be a regular non-symlink file",
            ));
        }
        let _permit = match budget {
            Some(b) => file_pool.acquire_for_query(b)?,
            None => file_pool.acquire()?,
        };
        let mut file = File::open(path)?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        let decoded = decode_header(&header)?;
        if decoded.storage_uuid != storage_uuid
            || decoded.checkpoint_commit != identity_commit
            || decoded.segment_count != segment_count
            || decoded.manifest_crc32c != segments_crc32c
        {
            return Err(invalid(
                "offline pack directory does not match the current manifest",
            ));
        }
        if decoded.total_length != metadata.len() {
            return Err(corrupt("offline pack directory length is invalid"));
        }
        let root_length = usize::try_from(decoded.root_length)
            .map_err(|_| corrupt("offline pack directory root length overflows"))?;
        decoded.validate_root_shape(maximum_root_bytes)?;
        let entry_size = if decoded.timed() {
            index::ENTRY_SIZE
        } else {
            ROOT_ENTRY_SIZE
        };
        let decoded_root_bytes =
            (root_length / entry_size).saturating_mul(std::mem::size_of::<PageDescriptor>());
        let root_memory = page_pool
            .reserve_memory(
                memory_governor,
                MemoryClass::MetadataDirectoryRoot,
                u64::try_from(
                    HEADER_SIZE
                        .saturating_add(root_length)
                        .saturating_add(decoded_root_bytes)
                        // Segment map includes hash buckets, control bytes and spare capacity.
                        .saturating_add(segments.len().saturating_mul(64))
                        .saturating_add(std::mem::size_of::<Self>())
                        .saturating_add(path.as_os_str().len())
                        .saturating_add(128),
                )
                .unwrap_or(u64::MAX),
            )?
            .ok_or_else(|| {
                crate::error::Error::ResourceLimit(
                    "offline pack directory root exceeds managed memory".into(),
                )
            })?;
        let mut root = vec![0_u8; root_length];
        file.read_exact(&mut root)?;
        if crc32c::crc32c(&root) != decoded.root_crc32c {
            return Err(corrupt("offline pack directory root checksum mismatch"));
        }
        let pages_end = decoded.raw_pages_end()?;
        let pages = decoded.decode_pages(&root)?;
        if !detached && decoded.descriptor_length != 0 {
            let embedded = read_descriptor_section(&mut file, &decoded)?;
            if descriptor_crc32c(embedded)? != segments_crc32c {
                return Err(corrupt(
                    "published read directory descriptor checksum mismatch",
                ));
            }
        }
        let by_id = segments
            .iter()
            .map(|segment| (segment.descriptor.id, Arc::clone(segment)))
            .collect::<HashMap<_, _>>();
        if by_id.len() != segments.len() {
            return Err(corrupt("manifest contains duplicate segment IDs"));
        }
        let directory = Arc::new(Self {
            path: path.to_path_buf(),
            published_scope,
            shard_scope: shard.or_else(|| common_shard(segments)),
            pages,
            pages_start: HEADER_SIZE as u64 + u64::from(decoded.root_length),
            pages_end,

            segments: by_id,
            detached,
            file_cache,

            memory_governor: Arc::clone(memory_governor),
            page_cache_max_bytes,
            page_pool: Arc::clone(page_pool),
            page_cache: Mutex::new(DirectoryPageCache::default()),
            page_load: Mutex::new(()),
            _root_memory: root_memory,
        });
        page_pool.register(&directory)?;
        Ok(directory)
    }

    pub(crate) fn covers_segment(&self, descriptor: &super::SegmentDescriptor) -> bool {
        // A query may select hundreds of shard/epoch directories. Avoid hashing
        // every segment ID against every unrelated directory during planning.
        self.shard_scope
            .is_none_or(|shard| shard == descriptor.shard_id)
            && self.published_scope.is_none_or(|(policy, from, to)| {
                policy == descriptor.policy_id
                    && descriptor.min_timestamp_us < to
                    && descriptor.max_timestamp_us >= from
            })
            && self.segments.contains_key(&descriptor.id)
    }

    fn published_header(
        root: &Path,
        descriptor: &PublishedReadDirectoryDescriptor,
        storage_uuid: [u8; 16],
    ) -> Result<(File, DecodedHeader)> {
        let path = root.join(&descriptor.relative_path);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != descriptor.file_size
        {
            return Err(corrupt("published read directory file is invalid"));
        }
        let mut file = File::open(&path)?;
        let mut header = [0_u8; HEADER_SIZE];
        file.read_exact(&mut header)?;
        let decoded = decode_header(&header)?;
        if decoded.storage_uuid != storage_uuid
            || decoded.checkpoint_commit != descriptor.generation_id
            || decoded.segment_count != descriptor.segment_count
            || decoded.manifest_crc32c != descriptor.segments_crc32c
            || decoded.total_length != descriptor.file_size
        {
            return Err(corrupt("published read directory header is stale"));
        }

        decoded.validate_root_shape(MAX_PUBLISHED_ROOT_BYTES)?;
        Ok((file, decoded))
    }

    fn visit_published(
        root: &Path,
        descriptor: &PublishedReadDirectoryDescriptor,
        storage_uuid: [u8; 16],
        segments: &[super::SegmentDescriptor],
        mut visit: impl FnMut(Locator) -> Result<()>,
    ) -> Result<()> {
        if descriptor.segment_count != u64::try_from(segments.len()).unwrap_or(u64::MAX)
            || descriptor.segments_crc32c != descriptor_crc32c(segments.to_vec())?
        {
            return Err(corrupt(
                "published read directory descriptor disagrees with its segments",
            ));
        }
        let (mut file, decoded) = Self::published_header(root, descriptor, storage_uuid)?;
        let root_length = usize::try_from(decoded.root_length)
            .map_err(|_| corrupt("published read directory root length overflows"))?;
        let mut root_bytes = vec![0_u8; root_length];
        file.read_exact(&mut root_bytes)?;
        if crc32c::crc32c(&root_bytes) != decoded.root_crc32c {
            return Err(corrupt("published read directory root checksum mismatch"));
        }
        let pages_end = decoded.raw_pages_end()?;
        let pages = decoded.decode_pages(&root_bytes)?;
        let by_id = segments
            .iter()
            .map(|segment| (segment.id, segment))
            .collect::<HashMap<_, _>>();
        let mut seen_segments = HashSet::new();
        let mut previous = None;
        let mut count = 0_u64;
        let (leaves, _, _) = index::visit_leaves(
            &mut file,
            &pages,
            HEADER_SIZE as u64 + u64::from(decoded.root_length),
            pages_end,
            |file, page| {
                file.seek(SeekFrom::Start(page.offset))?;
                let mut bytes = vec![
                    0_u8;
                    usize::try_from(page.length).map_err(|_| {
                        corrupt("published read directory page length overflows")
                    })?
                ];
                file.read_exact(&mut bytes)?;
                if crc32c::crc32c(&bytes) != page.crc32c {
                    return Err(corrupt("published read directory page checksum mismatch"));
                }
                let (entries, remainder) = bytes.as_chunks::<LOCATOR_SIZE>();
                if !remainder.is_empty() || entries.len() != page.entries as usize {
                    return Err(corrupt("published read directory page shape is invalid"));
                }
                for entry in entries {
                    let locator = decode_locator(entry)?;
                    validate_leaf_fence(page, &locator)?;
                    let segment = by_id.get(&locator.segment_id).ok_or_else(|| {
                        corrupt("published read directory refers to an unknown segment")
                    })?;
                    let block_end = locator
                        .block
                        .offset
                        .checked_add(u64::from(locator.block.size))
                        .ok_or_else(|| corrupt("published read directory block range overflows"))?;
                    if locator.block.min_timestamp_us < segment.min_timestamp_us
                        || locator.block.max_timestamp_us > segment.max_timestamp_us
                        || locator.block.size == 0
                        || locator.block.offset < super::HEADER_SIZE as u64
                        || block_end > segment.file_size.saturating_sub(super::FOOTER_SIZE as u64)
                    {
                        return Err(corrupt(
                            "published read directory block is outside its segment",
                        ));
                    }
                    let order = (
                        locator.series,
                        locator.block.min_timestamp_us,
                        locator.block.max_timestamp_us,
                        locator.segment_id,
                        locator.block.offset,
                    );
                    if previous.is_some_and(|previous| previous >= order) {
                        return Err(corrupt("published read directory locators are not ordered"));
                    }
                    previous = Some(order);
                    seen_segments.insert(locator.segment_id);
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| corrupt("locator count overflows"))?;
                    visit(locator)?;
                }
                Ok(())
            },
        )?;
        if leaves != u64::from(decoded.page_count) || count != decoded.locator_count {
            return Err(corrupt("directory leaf count is invalid"));
        }
        if seen_segments.len() != by_id.len() {
            return Err(corrupt(
                "published read directory does not cover every declared segment",
            ));
        }
        if decoded.descriptor_length != 0 {
            let embedded = read_descriptor_section(&mut file, &decoded)?;
            if descriptor_crc32c(embedded)? != descriptor.segments_crc32c {
                return Err(corrupt(
                    "published read directory descriptor checksum mismatch",
                ));
            }
        }
        cost::add(
            CostCounter::DirectoryReadBytes,
            usize::try_from(descriptor.file_size).unwrap_or(usize::MAX),
        );
        Ok(())
    }

    fn cached_page(&self, offset: u64) -> Result<Option<Arc<DirectoryPage>>> {
        let mut cache = self
            .page_cache
            .lock()
            .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
        cache.clock = cache.clock.saturating_add(1).min(PAGE_REFERENCED - 1);
        let clock = cache.clock;
        Ok(cache.entries.get_mut(&offset).map(|entry| {
            entry.last_used = clock | PAGE_REFERENCED;
            Arc::clone(&entry.page)
        }))
    }

    // Admission follows validation of CRC, ordering and every locator or child
    // descriptor. Cached bytes are immutable.
    fn cache_page(&self, offset: u64, bytes: Arc<Vec<u8>>) -> Result<Option<Arc<DirectoryPage>>> {
        let page_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if page_bytes == 0 || page_bytes > self.page_cache_max_bytes {
            return Ok(None);
        }
        let mut cache = self
            .page_cache
            .lock()
            .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
        if let Some(entry) = cache.entries.get(&offset) {
            return Ok(Some(Arc::clone(&entry.page)));
        }
        while cache.bytes.saturating_add(page_bytes) > self.page_cache_max_bytes {
            let Some(evict) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used & !PAGE_REFERENCED)
                .map(|(offset, _)| *offset)
            else {
                break;
            };
            cache.remove(evict, &self.page_pool);
        }
        let memory = self
            .memory_governor
            .try_reserve_token(MemoryClass::MetadataDirectoryPage, page_bytes);
        let memory = if let Some(memory) = memory {
            memory
        } else {
            drop(cache);
            let Some(memory) = self.page_pool.reserve_memory(
                &self.memory_governor,
                MemoryClass::MetadataDirectoryPage,
                page_bytes,
            )?
            else {
                return Ok(None);
            };
            cache = self
                .page_cache
                .lock()
                .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
            if let Some(entry) = cache.entries.get(&offset) {
                return Ok(Some(Arc::clone(&entry.page)));
            }
            // Another loader may have filled this directory while its lock was
            // released. Optional admission must still respect the local cap.
            if cache.bytes.saturating_add(page_bytes) > self.page_cache_max_bytes {
                return Ok(None);
            }
            memory
        };
        cache.clock = cache.clock.saturating_add(1).min(PAGE_REFERENCED - 1);
        let last_used = cache.clock;
        let page = Arc::new(DirectoryPage {
            bytes,
            _memory: memory,
        });
        cache.entries.insert(
            offset,
            CachedDirectoryPage {
                page: Arc::clone(&page),
                last_used: last_used | PAGE_REFERENCED,
            },
        );
        cache.bytes = cache.bytes.saturating_add(page_bytes);
        self.page_pool.add_cached_bytes(page_bytes);
        Ok(Some(page))
    }
}

struct DecodedHeader {
    format: u32,
    storage_uuid: [u8; 16],
    checkpoint_commit: u64,
    segment_count: u64,
    locator_count: u64,
    page_count: u32,
    root_length: u32,
    root_crc32c: u32,
    manifest_crc32c: u32,
    total_length: u64,
    descriptor_length: u32,
}

impl DecodedHeader {
    fn timed(&self) -> bool {
        matches!(self.format, TIMED_FORMAT | TIMED_PUBLISHED_FORMAT)
    }

    fn validate_root_shape(&self, maximum: usize) -> Result<()> {
        let bytes = self.root_length as usize;
        let size = if self.timed() {
            index::ENTRY_SIZE
        } else {
            ROOT_ENTRY_SIZE
        };
        if bytes > maximum
            || !bytes.is_multiple_of(size)
            || (self.timed() && bytes / size > index::FANOUT)
            || (!self.timed() && bytes / size != self.page_count as usize)
            || (bytes == 0) != (self.page_count == 0)
        {
            return Err(corrupt("offline pack directory root shape is invalid"));
        }
        Ok(())
    }

    fn decode_pages(&self, bytes: &[u8]) -> Result<Vec<PageDescriptor>> {
        if self.timed() {
            index::decode(
                bytes,
                HEADER_SIZE as u64 + u64::from(self.root_length),
                self.raw_pages_end()?,
            )
        } else {
            decode_root(bytes, self.raw_pages_end()?)
        }
    }

    fn raw_pages_end(&self) -> Result<u64> {
        let section_end = self
            .total_length
            .checked_sub(u64::from(self.descriptor_length))
            .ok_or_else(|| corrupt("directory descriptor section underflows"))?;
        Ok(section_end)
    }
}

fn manifest_crc32c(manifest: &Manifest) -> Result<u32> {
    Ok(crc32c::crc32c(&serde_json::to_vec(&(
        manifest.checkpoint_commit,
        &manifest.segments,
    ))?))
}

fn descriptor_crc32c(mut descriptors: Vec<super::SegmentDescriptor>) -> Result<u32> {
    descriptors.sort_unstable_by_key(|descriptor| descriptor.id);
    Ok(crc32c::crc32c(&serde_json::to_vec(&descriptors)?))
}

#[allow(clippy::too_many_arguments)]
fn encode_header(
    storage_uuid: [u8; 16],
    identity_commit: u64,
    segment_count: usize,
    locator_count: usize,
    page_count: usize,
    root_length: usize,
    root_crc32c: u32,
    manifest_crc32c: u32,
    total_length: u64,
    descriptor_length: usize,
) -> Result<[u8; HEADER_SIZE]> {
    let mut header = [0_u8; HEADER_SIZE];
    header[..8].copy_from_slice(MAGIC);
    put_u32(
        &mut header,
        8,
        if descriptor_length == 0 {
            TIMED_FORMAT
        } else {
            TIMED_PUBLISHED_FORMAT
        },
    );
    put_u32(&mut header, 12, HEADER_SIZE as u32);
    header[16..32].copy_from_slice(&storage_uuid);
    put_u64(&mut header, 32, identity_commit);
    put_u64(
        &mut header,
        40,
        u64::try_from(segment_count)
            .map_err(|_| invalid("offline pack directory segment count overflows"))?,
    );
    put_u64(
        &mut header,
        48,
        u64::try_from(locator_count)
            .map_err(|_| invalid("offline pack directory locator count overflows"))?,
    );
    put_u32(
        &mut header,
        56,
        u32::try_from(page_count)
            .map_err(|_| invalid("offline pack directory page count overflows"))?,
    );
    put_u32(
        &mut header,
        60,
        u32::try_from(root_length)
            .map_err(|_| invalid("offline pack directory root length overflows"))?,
    );
    put_u32(&mut header, 64, root_crc32c);
    put_u32(&mut header, 68, manifest_crc32c);
    put_u64(&mut header, 72, total_length);
    put_u32(&mut header, 80, LOCATOR_SIZE as u32);
    put_u32(&mut header, 84, MAX_PAGE_BYTES as u32);
    put_u32(
        &mut header,
        88,
        u32::try_from(descriptor_length)
            .map_err(|_| invalid("published descriptor section exceeds u32"))?,
    );
    let header_crc32c = crc32c::crc32c(&header[..92]);
    put_u32(&mut header, 92, header_crc32c);
    Ok(header)
}

fn decode_header(header: &[u8; HEADER_SIZE]) -> Result<DecodedHeader> {
    let format = read_u32(header, 8);
    let descriptor_length = read_u32(header, 88);
    if &header[..8] != MAGIC
        || !matches!(
            format,
            LEGACY_FORMAT | PUBLISHED_FORMAT | TIMED_FORMAT | TIMED_PUBLISHED_FORMAT
        )
        || read_u32(header, 12) != HEADER_SIZE as u32
        || read_u32(header, 80) != LOCATOR_SIZE as u32
        || read_u32(header, 84) != MAX_PAGE_BYTES as u32
        || (matches!(format, LEGACY_FORMAT | TIMED_FORMAT) && descriptor_length != 0)
        || (matches!(format, PUBLISHED_FORMAT | TIMED_PUBLISHED_FORMAT) && descriptor_length == 0)
        || read_u32(header, 92) != crc32c::crc32c(&header[..92])
    {
        return Err(corrupt("offline pack directory header is invalid"));
    }
    let mut storage_uuid = [0_u8; 16];
    storage_uuid.copy_from_slice(&header[16..32]);
    Ok(DecodedHeader {
        format,
        storage_uuid,
        checkpoint_commit: read_u64(header, 32),
        segment_count: read_u64(header, 40),
        locator_count: read_u64(header, 48),
        page_count: read_u32(header, 56),
        root_length: read_u32(header, 60),
        root_crc32c: read_u32(header, 64),
        manifest_crc32c: read_u32(header, 68),
        total_length: read_u64(header, 72),
        descriptor_length,
    })
}

fn read_descriptor_section(
    file: &mut File,
    header: &DecodedHeader,
) -> Result<Vec<super::SegmentDescriptor>> {
    let length = usize::try_from(header.descriptor_length)
        .map_err(|_| corrupt("published descriptor section length overflows"))?;
    if !matches!(header.format, PUBLISHED_FORMAT | TIMED_PUBLISHED_FORMAT)
        || length == 0
        || length > MAX_EMBEDDED_DESCRIPTOR_BYTES
    {
        return Err(corrupt(
            "published read directory descriptor section is invalid",
        ));
    }
    let offset = header
        .total_length
        .checked_sub(u64::from(header.descriptor_length))
        .ok_or_else(|| corrupt("published descriptor section offset underflows"))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0_u8; length];
    file.read_exact(&mut bytes)?;
    if crc32c::crc32c(&bytes) != header.manifest_crc32c {
        return Err(corrupt(
            "published read directory descriptor checksum mismatch",
        ));
    }
    let descriptors = serde_json::from_slice::<Vec<super::SegmentDescriptor>>(&bytes)?;
    if descriptors.len() != usize::try_from(header.segment_count).unwrap_or(usize::MAX) {
        return Err(corrupt(
            "published read directory descriptor count is invalid",
        ));
    }
    Ok(descriptors)
}

fn common_shard(segments: &[Arc<Segment>]) -> Option<u16> {
    let shard = segments.first()?.descriptor.shard_id;
    segments
        .iter()
        .all(|segment| segment.descriptor.shard_id == shard)
        .then_some(shard)
}

fn decode_root(root: &[u8], total_length: u64) -> Result<Vec<PageDescriptor>> {
    let offset = u64::try_from(HEADER_SIZE.saturating_add(root.len()))
        .map_err(|_| corrupt("offline pack directory root offset overflows"))?;
    decode_root_at(root, offset, total_length)
}

fn decode_root_at(
    root: &[u8],
    mut expected_offset: u64,
    total_length: u64,
) -> Result<Vec<PageDescriptor>> {
    let mut pages = Vec::with_capacity(root.len() / ROOT_ENTRY_SIZE);
    let mut previous_last = None;
    let (root_entries, remainder) = root.as_chunks::<ROOT_ENTRY_SIZE>();
    if !remainder.is_empty() {
        return Err(corrupt("offline pack directory root is misaligned"));
    }
    for input in root_entries {
        let page = PageDescriptor {
            first_series: decode_series(input, 0),
            last_series: decode_series(input, 16),
            offset: read_u64(input, 32),
            length: read_u32(input, 40),
            entries: read_u32(input, 44),
            crc32c: read_u32(input, 48),
            level: 0,
            min_timestamp_us: i64::MIN,
            max_timestamp_us: i64::MAX,
        };
        if page.first_series > page.last_series
            || page.offset != expected_offset
            || page.length == 0
            || usize::try_from(page.length).unwrap_or(usize::MAX) > MAX_PAGE_BYTES
            || u64::from(page.length) != u64::from(page.entries).saturating_mul(LOCATOR_SIZE as u64)
            || previous_last.is_some_and(|last| last > page.first_series)
        {
            return Err(corrupt("offline pack directory page descriptor is invalid"));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(page.length))
            .ok_or_else(|| corrupt("offline pack directory page offset overflows"))?;
        previous_last = Some(page.last_series);
        pages.push(page);
    }
    if expected_offset != total_length {
        return Err(corrupt(
            "offline pack directory pages do not cover the artifact",
        ));
    }
    Ok(pages)
}

fn encode_page(locators: &[Locator], output: &mut Vec<u8>) {
    output.clear();
    output.resize(locators.len().saturating_mul(LOCATOR_SIZE), 0);
    let (entries, remainder) = output.as_chunks_mut::<LOCATOR_SIZE>();
    debug_assert!(remainder.is_empty());
    for (locator, entry) in locators.iter().zip(entries) {
        encode_locator(*locator, entry);
    }
}

fn validate_leaf_fence(page: PageDescriptor, locator: &Locator) -> Result<()> {
    if locator.series < page.first_series
        || locator.series > page.last_series
        || locator.block.min_timestamp_us < page.min_timestamp_us
        || locator.block.max_timestamp_us > page.max_timestamp_us
    {
        return Err(corrupt("directory locator is outside its page fence"));
    }
    Ok(())
}

fn encode_locator(locator: Locator, output: &mut [u8]) {
    encode_series(output, 0, locator.series);
    put_u64(output, 16, locator.segment_id);
    put_u64(output, 24, locator.block.offset);
    put_u32(output, 32, locator.block.size);
    put_i64(output, 36, locator.block.min_timestamp_us);
    put_i64(output, 44, locator.block.max_timestamp_us);
    if let Some(summary) = locator.block.count_sum {
        put_u64(output, 52, summary.count);
        put_u64(output, 60, summary.raw_sum.to_bits());
        put_u64(output, 68, summary.compensation.to_bits());
        put_u32(output, 76, SUMMARY_PRESENT);
    }
}

fn decode_locator(input: &[u8]) -> Result<Locator> {
    if input.len() != LOCATOR_SIZE || input[80..].iter().any(|byte| *byte != 0) {
        return Err(corrupt("offline pack directory locator is invalid"));
    }
    let flags = read_u32(input, 76);
    if flags & !SUMMARY_PRESENT != 0 {
        return Err(corrupt("offline pack directory locator flags are invalid"));
    }
    let count_sum = (flags & SUMMARY_PRESENT != 0).then(|| CountSumSummary {
        count: read_u64(input, 52),
        raw_sum: f64::from_bits(read_u64(input, 60)),
        compensation: f64::from_bits(read_u64(input, 68)),
    });
    Ok(Locator {
        series: decode_series(input, 0),
        segment_id: read_u64(input, 16),
        block: BlockIndex {
            series: decode_series(input, 0),
            offset: read_u64(input, 24),
            size: read_u32(input, 32),
            min_timestamp_us: read_i64(input, 36),
            max_timestamp_us: read_i64(input, 44),
            count_sum,
        },
    })
}

fn encode_series(output: &mut [u8], offset: usize, series: SeriesKey) {
    put_u32(output, offset, series.space_id);
    put_u32(output, offset + 4, series.bucket_id);
    put_u64(output, offset + 8, series.key);
}

fn decode_series(input: &[u8], offset: usize) -> SeriesKey {
    SeriesKey::new(
        read_u32(input, offset),
        read_u32(input, offset + 4),
        read_u64(input, offset + 8),
    )
}

fn directory_page_ranges(locators: &[Locator]) -> Result<Vec<Range<usize>>> {
    let entries_per_page = MAX_PAGE_BYTES / LOCATOR_SIZE;
    let mut page_ranges =
        Vec::<Range<usize>>::with_capacity(locators.len().div_ceil(MAX_SERIES_PER_PAGE));
    let mut page_start = 0_usize;
    while page_start < locators.len() {
        let page_limit = page_start
            .saturating_add(entries_per_page)
            .min(locators.len());
        let mut page_end = page_start;
        let mut series_count = 0_usize;
        while page_end < page_limit && series_count < MAX_SERIES_PER_PAGE {
            let series = locators[page_end].series;
            let series_end =
                page_end + locators[page_end..].partition_point(|locator| locator.series == series);
            if series_end > page_limit {
                if page_end == page_start {
                    page_end = page_limit;
                }
                break;
            }
            page_end = series_end;
            series_count = series_count.saturating_add(1);
        }
        if page_end == page_start {
            return Err(corrupt("offline pack directory page did not advance"));
        }
        page_ranges.push(page_start..page_end);
        page_start = page_end;
    }
    Ok(page_ranges)
}
