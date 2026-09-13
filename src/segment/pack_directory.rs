use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::Serialize;
use uuid::Uuid;

use super::{
    BlockIndex, CountSumSummary, QueryPlanReservation, QuerySegmentRead, Segment, put_i64, put_u32,
    put_u64, read_i64, read_u32, read_u64, sync_directory,
};
use crate::admission::{WorkMemoryClass, WorkMemoryGovernor};
use crate::cache::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::error::{Result, corrupt, invalid};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::manifest::{Manifest, PublishedReadDirectoryDescriptor, PublishedReadPayloadFile};
use crate::query::{QueryBudget, QueryMemoryClass};
use crate::types::SeriesKey;
use crate::wal::UnpublishedFileGuard;

mod page_pool;
pub(crate) use page_pool::DirectoryPagePool;

const MAGIC: &[u8; 8] = b"TSPDR001";
const LEGACY_FORMAT: u32 = 1;
const PUBLISHED_FORMAT: u32 = 2;
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
}

#[derive(Clone, Copy, Debug)]
struct Locator {
    series: SeriesKey,
    segment_id: u64,
    block: BlockIndex,
}

pub(crate) struct OfflinePackDirectory {
    path: PathBuf,
    identity_commit: u64,
    artifact_bytes: u64,
    embedded_descriptors: bool,
    published_scope: Option<(u32, i64, i64)>,
    shard_scope: Option<u16>,
    pages: Vec<PageDescriptor>,
    segments: HashMap<u64, Arc<Segment>>,
    covered_segment_ids: HashSet<u64>,
    file_cache: Arc<OpenFileCache>,
    file_pool: FilePermitPool,
    memory_governor: Arc<MemoryGovernor>,
    page_cache_max_bytes: u64,
    page_pool: Arc<DirectoryPagePool>,
    page_cache: Mutex<DirectoryPageCache>,
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

impl OfflinePackDirectory {
    pub(crate) fn build(
        output: &Path,
        manifest: &Manifest,
        segments: &[Arc<Segment>],
    ) -> Result<OfflinePackDirectoryInfo> {
        Self::build_internal(
            output,
            manifest.storage_uuid,
            manifest.checkpoint_commit,
            manifest_crc32c(manifest)?,
            segments,
            MAX_ROOT_BYTES,
            None,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_published(
        root: &Path,
        storage_uuid: [u8; 16],
        generation_id: u64,
        policy_id: u32,
        window_start_us: i64,
        window_end_us: i64,
        segments: &[Arc<Segment>],
        work_memory_governor: &Arc<WorkMemoryGovernor>,
    ) -> Result<PublishedReadDirectoryDescriptor> {
        if generation_id == 0
            || policy_id == 0
            || window_start_us >= window_end_us
            || segments.is_empty()
            || segments.iter().any(|segment| {
                let descriptor = segment.descriptor();
                descriptor.level != 2
                    || descriptor.policy_id != policy_id
                    || descriptor.window_start_us < window_start_us
                    || descriptor.window_end_us > window_end_us
            })
        {
            return Err(invalid("invalid published read directory generation"));
        }
        let directory = root.join("segments/directories");
        let existed = directory.exists();
        fs::create_dir_all(&directory)?;
        if !existed {
            sync_directory(&root.join("segments"))?;
        }
        let relative_path = format!("segments/directories/READ-DIRECTORY-{generation_id:020}.tspd");
        let output = root.join(&relative_path);
        let segments_crc32c = segments_crc32c(segments)?;
        let info = Self::build_internal(
            &output,
            storage_uuid,
            generation_id,
            segments_crc32c,
            segments,
            MAX_PUBLISHED_ROOT_BYTES,
            Some(work_memory_governor),
            true,
        )?;
        Ok(PublishedReadDirectoryDescriptor {
            generation_id,
            shard_id: common_shard(segments),
            policy_id,
            window_start_us,
            window_end_us,
            maximum_segment_id: segments
                .iter()
                .map(|segment| segment.descriptor.id)
                .max()
                .ok_or_else(|| invalid("published read directory has no segments"))?,
            segment_count: u64::try_from(segments.len()).unwrap_or(u64::MAX),
            segments_crc32c,
            relative_path,
            file_size: info.artifact_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_internal(
        output: &Path,
        storage_uuid: [u8; 16],
        identity_commit: u64,
        segments_crc32c: u32,
        segments: &[Arc<Segment>],
        maximum_root_bytes: usize,
        work_memory_governor: Option<&Arc<WorkMemoryGovernor>>,
        embed_descriptors: bool,
    ) -> Result<OfflinePackDirectoryInfo> {
        if output.exists() {
            return Err(invalid("offline pack directory output already exists"));
        }
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir() {
            return Err(invalid("offline pack directory parent does not exist"));
        }

        let locator_count = segments.iter().try_fold(0_usize, |count, segment| {
            count
                .checked_add(segment.metadata()?.index.len())
                .ok_or_else(|| invalid("offline pack directory locator count overflows"))
        })?;
        let descriptor_estimate = if embed_descriptors {
            segments.iter().try_fold(0_usize, |bytes, segment| {
                bytes
                    .checked_add(segment.descriptor.relative_path.len().saturating_add(512))
                    .ok_or_else(|| invalid("published descriptor memory overflows"))
            })?
        } else {
            0
        };
        let build_bytes = locator_count
            .checked_mul(
                std::mem::size_of::<Locator>()
                    .saturating_add(std::mem::size_of::<Range<usize>>())
                    .saturating_add(std::mem::size_of::<PageDescriptor>())
                    .saturating_add(ROOT_ENTRY_SIZE),
            )
            .and_then(|bytes| bytes.checked_add(MAX_PAGE_BYTES))
            .and_then(|bytes| bytes.checked_add(descriptor_estimate))
            .ok_or_else(|| invalid("offline pack directory build memory overflows"))?;
        if build_bytes > MAX_BUILD_BYTES {
            return Err(crate::error::Error::ResourceLimit(
                "offline pack directory build exceeds 512 MiB".into(),
            ));
        }
        let _work_memory = work_memory_governor
            .map(|governor| {
                // This optional index is built by the sole publisher. Waiting
                // for another job's scratch could prevent that job's output
                // from being published. Keep explicit manifest descriptors
                // when the budget is busy and retry on a later publication.
                governor
                    .try_reserve_background_with_headroom(
                        WorkMemoryClass::Compaction,
                        u64::try_from(build_bytes).unwrap_or(u64::MAX),
                        0,
                    )?
                    .ok_or_else(|| {
                        crate::error::Error::ResourceLimit(
                            "published read directory work-memory budget is busy or too small"
                                .into(),
                        )
                    })
            })
            .transpose()?;

        let descriptor_bytes = if embed_descriptors {
            let mut descriptors = segments
                .iter()
                .map(|segment| segment.descriptor())
                .collect::<Vec<_>>();
            descriptors.sort_unstable_by_key(|descriptor| descriptor.id);
            let bytes = serde_json::to_vec(&descriptors)?;
            if bytes.len() > descriptor_estimate || crc32c::crc32c(&bytes) != segments_crc32c {
                return Err(corrupt(
                    "published read directory descriptor section is invalid",
                ));
            }
            bytes
        } else {
            Vec::new()
        };

        let mut locators = Vec::with_capacity(locator_count);
        for segment in segments {
            let metadata = segment.metadata()?;
            locators.extend(metadata.index.iter().copied().map(|block| Locator {
                series: block.series,
                segment_id: segment.descriptor.id,
                block,
            }));
        }
        locators.sort_unstable_by_key(|locator| {
            (
                locator.series,
                locator.block.min_timestamp_us,
                locator.block.max_timestamp_us,
                locator.segment_id,
                locator.block.offset,
            )
        });

        let page_ranges = directory_page_ranges(&locators)?;
        let root_bytes = page_ranges
            .len()
            .checked_mul(ROOT_ENTRY_SIZE)
            .ok_or_else(|| invalid("offline pack directory root length overflows"))?;
        if root_bytes > maximum_root_bytes {
            return Err(crate::error::Error::ResourceLimit(
                "offline pack directory root exceeds 1 MiB".into(),
            ));
        }
        let mut next_offset = u64::try_from(HEADER_SIZE.saturating_add(root_bytes))
            .map_err(|_| invalid("offline pack directory offset overflows"))?;
        let mut pages = Vec::with_capacity(page_ranges.len());
        let mut encoded_page = Vec::with_capacity(MAX_PAGE_BYTES);
        for range in &page_ranges {
            let chunk = &locators[range.clone()];
            encode_page(chunk, &mut encoded_page);
            let length = u32::try_from(encoded_page.len())
                .map_err(|_| invalid("offline pack directory page is too large"))?;
            pages.push(PageDescriptor {
                first_series: chunk
                    .first()
                    .ok_or_else(|| corrupt("offline pack directory page is empty"))?
                    .series,
                last_series: chunk
                    .last()
                    .ok_or_else(|| corrupt("offline pack directory page is empty"))?
                    .series,
                offset: next_offset,
                length,
                entries: u32::try_from(chunk.len())
                    .map_err(|_| invalid("offline pack directory entry count overflows"))?,
                crc32c: crc32c::crc32c(&encoded_page),
            });
            next_offset = next_offset
                .checked_add(u64::from(length))
                .ok_or_else(|| invalid("offline pack directory length overflows"))?;
        }
        let root = encode_root(&pages);
        let total_length = next_offset
            .checked_add(u64::try_from(descriptor_bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| invalid("offline pack directory length overflows"))?;
        let header = encode_header(
            storage_uuid,
            identity_commit,
            segments.len(),
            locators.len(),
            pages.len(),
            root.len(),
            crc32c::crc32c(&root),
            segments_crc32c,
            total_length,
            descriptor_bytes.len(),
        )?;

        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            output
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("pack-directory"),
            Uuid::new_v4()
        ));
        let mut unpublished = UnpublishedFileGuard::new(output.to_path_buf());
        let mut unpublished_temporary = UnpublishedFileGuard::new(temporary.clone());
        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            unpublished_temporary.mark_present();
            file.write_all(&header)?;
            file.write_all(&root)?;
            for range in &page_ranges {
                let chunk = &locators[range.clone()];
                encode_page(chunk, &mut encoded_page);
                file.write_all(&encoded_page)?;
            }
            file.write_all(&descriptor_bytes)?;
            file.sync_all()?;
            crate::failpoint::hit("read_directory_after_sync");
            fs::rename(&temporary, output)?;
            unpublished_temporary.commit();
            unpublished.mark_present();
            crate::failpoint::hit("read_directory_after_rename");
            sync_directory(parent)?;
            Ok(())
        })();
        write_result?;
        unpublished.commit();

        let physical_files = segments
            .iter()
            .map(|segment| segment.descriptor.relative_path.as_str())
            .collect::<HashSet<_>>()
            .len();
        Ok(OfflinePackDirectoryInfo {
            format: "TSPDR001",
            checkpoint_commit_id: identity_commit,
            logical_segments: segments.len(),
            physical_files,
            block_locators: u64::try_from(locators.len()).unwrap_or(u64::MAX),
            pages: pages.len(),
            root_bytes: u64::try_from(root.len()).unwrap_or(u64::MAX),
            artifact_bytes: total_length,
        })
    }

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
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_published(
        root: &Path,
        descriptor: &PublishedReadDirectoryDescriptor,
        storage_uuid: [u8; 16],
        segments: &[Arc<Segment>],
        memory_governor: &Arc<MemoryGovernor>,
        page_pool: &Arc<DirectoryPagePool>,
        file_pool: &FilePermitPool,
        file_cache: Arc<OpenFileCache>,
        page_cache_max_bytes: u64,
    ) -> Result<Arc<Self>> {
        if descriptor
            .shard_id
            .is_some_and(|shard| common_shard(segments) != Some(shard))
            || descriptor.segment_count != u64::try_from(segments.len()).unwrap_or(u64::MAX)
            || descriptor.segments_crc32c != segments_crc32c(segments)?
            || descriptor.maximum_segment_id
                != segments
                    .iter()
                    .map(|segment| segment.descriptor.id)
                    .max()
                    .unwrap_or_default()
        {
            return Err(corrupt(
                "published read directory descriptor disagrees with its segments",
            ));
        }
        let path = root.join(&descriptor.relative_path);
        if fs::metadata(&path)?.len() != descriptor.file_size {
            return Err(corrupt("published read directory length is invalid"));
        }
        Self::open_internal(
            &path,
            storage_uuid,
            descriptor.generation_id,
            descriptor.segment_count,
            descriptor.segments_crc32c,
            segments,
            memory_governor,
            page_pool,
            file_pool,
            file_cache,
            page_cache_max_bytes,
            MAX_PUBLISHED_ROOT_BYTES,
            Some((
                descriptor.policy_id,
                descriptor.window_start_us,
                descriptor.window_end_us,
            )),
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
    ) -> Result<Arc<Self>> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid(
                "offline pack directory must be a regular non-symlink file",
            ));
        }
        let _permit = file_pool.acquire()?;
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
        if root_length > maximum_root_bytes
            || root_length % ROOT_ENTRY_SIZE != 0
            || root_length / ROOT_ENTRY_SIZE
                != usize::try_from(decoded.page_count)
                    .map_err(|_| corrupt("offline pack directory page count overflows"))?
        {
            return Err(corrupt("offline pack directory root shape is invalid"));
        }
        let mut root = vec![0_u8; root_length];
        file.read_exact(&mut root)?;
        if crc32c::crc32c(&root) != decoded.root_crc32c {
            return Err(corrupt("offline pack directory root checksum mismatch"));
        }
        let pages_end = decoded
            .total_length
            .checked_sub(u64::from(decoded.descriptor_length))
            .ok_or_else(|| corrupt("offline pack directory descriptor section underflows"))?;
        let pages = decode_root(&root, pages_end)?;
        if decoded.descriptor_length != 0 {
            let embedded = read_descriptor_section(&mut file, &decoded)?;
            if descriptor_crc32c(embedded)? != segments_crc32c {
                return Err(corrupt(
                    "published read directory descriptor checksum mismatch",
                ));
            }
        }
        let root_memory = memory_governor
            .try_reserve_token(
                MemoryClass::MetadataCache,
                u64::try_from(HEADER_SIZE.saturating_add(root_length).saturating_add(128))
                    .unwrap_or(u64::MAX),
            )
            .ok_or_else(|| {
                crate::error::Error::ResourceLimit(
                    "offline pack directory root exceeds managed memory".into(),
                )
            })?;
        let by_id = segments
            .iter()
            .map(|segment| (segment.descriptor.id, Arc::clone(segment)))
            .collect::<HashMap<_, _>>();
        if by_id.len() != segments.len() {
            return Err(corrupt("manifest contains duplicate segment IDs"));
        }
        let directory = Arc::new(Self {
            path: path.to_path_buf(),
            identity_commit,
            artifact_bytes: metadata.len(),
            embedded_descriptors: decoded.descriptor_length != 0,
            published_scope,
            shard_scope: common_shard(segments),
            pages,
            covered_segment_ids: by_id.keys().copied().collect(),
            segments: by_id,
            file_cache,
            file_pool: file_pool.clone(),
            memory_governor: Arc::clone(memory_governor),
            page_cache_max_bytes,
            page_pool: Arc::clone(page_pool),
            page_cache: Mutex::new(DirectoryPageCache::default()),
            _root_memory: root_memory,
        });
        page_pool.register(&directory)?;
        Ok(directory)
    }

    pub(crate) fn shard_scope(&self) -> Option<u16> {
        self.shard_scope
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
            && self.covered_segment_ids.contains(&descriptor.id)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn generation_id(&self) -> u64 {
        self.identity_commit
    }

    pub(crate) fn artifact_bytes(&self) -> u64 {
        self.artifact_bytes
    }

    pub(crate) fn covered_segment_count(&self) -> usize {
        self.covered_segment_ids.len()
    }

    pub(crate) fn root_pages(&self) -> usize {
        self.pages.len()
    }

    pub(crate) fn has_embedded_descriptors(&self) -> bool {
        self.embedded_descriptors
    }

    pub(crate) fn clear_page_cache(&self) -> Result<()> {
        let entries = {
            let mut cache = self
                .page_cache
                .lock()
                .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
            cache.bytes = 0;
            std::mem::take(&mut cache.entries)
        };
        drop(entries);
        Ok(())
    }

    pub(crate) fn published_scope(&self) -> Option<(u32, i64, i64)> {
        self.published_scope
    }

    pub(crate) fn visit_series_max_timestamps(
        &self,
        mut visit: impl FnMut(SeriesKey, i64) -> Result<()>,
    ) -> Result<(u64, u64)> {
        let _permit = self.file_pool.acquire()?;
        let mut file = File::open(&self.path)?;
        let mut current_series = None;
        let mut maximum_timestamp_us = i64::MIN;
        let mut pages_read = 0_u64;
        let mut bytes_read = 0_u64;
        for page in &self.pages {
            file.seek(SeekFrom::Start(page.offset))?;
            let mut bytes = vec![
                0_u8;
                usize::try_from(page.length).map_err(|_| {
                    corrupt("published read directory page length does not fit memory")
                })?
            ];
            file.read_exact(&mut bytes)?;
            pages_read = pages_read.saturating_add(1);
            bytes_read = bytes_read.saturating_add(u64::from(page.length));
            if crc32c::crc32c(&bytes) != page.crc32c {
                return Err(corrupt("published read directory page checksum mismatch"));
            }
            let (entries, remainder) = bytes.as_chunks::<LOCATOR_SIZE>();
            if !remainder.is_empty() {
                return Err(corrupt("published read directory page is misaligned"));
            }
            for entry in entries {
                let locator = decode_locator(entry)?;
                if current_series.is_some_and(|series| series != locator.series) {
                    visit(
                        current_series.ok_or_else(|| {
                            corrupt("published read directory series disappeared")
                        })?,
                        maximum_timestamp_us,
                    )?;
                    maximum_timestamp_us = i64::MIN;
                }
                current_series = Some(locator.series);
                maximum_timestamp_us = maximum_timestamp_us.max(locator.block.max_timestamp_us);
            }
        }
        if let Some(series) = current_series {
            visit(series, maximum_timestamp_us)?;
        }
        Ok((pages_read, bytes_read))
    }

    pub(crate) fn verify_published(
        root: &Path,
        descriptor: &PublishedReadDirectoryDescriptor,
        storage_uuid: [u8; 16],
        segments: &[super::SegmentDescriptor],
    ) -> Result<()> {
        if descriptor.segment_count != u64::try_from(segments.len()).unwrap_or(u64::MAX)
            || descriptor.segments_crc32c != descriptor_crc32c(segments.to_vec())?
        {
            return Err(corrupt(
                "published read directory descriptor disagrees with its segments",
            ));
        }
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
        let root_length = usize::try_from(decoded.root_length)
            .map_err(|_| corrupt("published read directory root length overflows"))?;
        if root_length > MAX_PUBLISHED_ROOT_BYTES || root_length % ROOT_ENTRY_SIZE != 0 {
            return Err(corrupt("published read directory root shape is invalid"));
        }
        let mut root_bytes = vec![0_u8; root_length];
        file.read_exact(&mut root_bytes)?;
        if crc32c::crc32c(&root_bytes) != decoded.root_crc32c {
            return Err(corrupt("published read directory root checksum mismatch"));
        }
        let pages_end = descriptor
            .file_size
            .checked_sub(u64::from(decoded.descriptor_length))
            .ok_or_else(|| corrupt("published descriptor section underflows"))?;
        let pages = decode_root(&root_bytes, pages_end)?;
        if pages.len() != usize::try_from(decoded.page_count).unwrap_or(usize::MAX) {
            return Err(corrupt("published read directory page count is invalid"));
        }
        let by_id = segments
            .iter()
            .map(|segment| (segment.id, segment))
            .collect::<HashMap<_, _>>();
        let mut seen_segments = HashSet::new();
        let mut previous = None;
        for page in pages {
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
                let segment = by_id.get(&locator.segment_id).ok_or_else(|| {
                    corrupt("published read directory refers to an unknown segment")
                })?;
                let block_end = locator
                    .block
                    .offset
                    .checked_add(u64::from(locator.block.size))
                    .ok_or_else(|| corrupt("published read directory block range overflows"))?;
                if locator.block.size == 0
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
                if previous.is_some_and(|previous| previous > order) {
                    return Err(corrupt("published read directory locators are not ordered"));
                }
                previous = Some(order);
                seen_segments.insert(locator.segment_id);
            }
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
        Ok(())
    }

    pub(crate) fn published_segment_descriptors(
        root: &Path,
        descriptor: &PublishedReadDirectoryDescriptor,
        payload_files: &[PublishedReadPayloadFile],
        storage_uuid: [u8; 16],
    ) -> Result<Option<Vec<super::SegmentDescriptor>>> {
        let path = root.join(&descriptor.relative_path);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != descriptor.file_size
        {
            return Err(corrupt("published read directory file is invalid"));
        }
        let mut file = File::open(path)?;
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
        if decoded.format == LEGACY_FORMAT {
            return Ok(None);
        }
        let embedded = read_descriptor_section(&mut file, &decoded)?;
        validate_embedded_descriptors(descriptor, payload_files, &embedded)?;
        Ok(Some(embedded))
    }

    pub(crate) fn plan_query_series(
        &self,
        series_ranges: &[(SeriesKey, i64)],
        to_us: i64,
        query_budget: &QueryBudget,
        plan_reservation: &mut QueryPlanReservation,
    ) -> Result<Vec<(SeriesKey, QuerySegmentRead)>> {
        query_budget.record_pack_directory_root_consulted();
        if series_ranges.is_empty() {
            return Ok(Vec::new());
        }

        let requested = series_ranges.iter().copied().collect::<HashMap<_, _>>();
        let selected_pages = self.selected_pages(series_ranges);
        if selected_pages.is_empty() {
            return Ok(Vec::new());
        }

        // Page residency is independent of the descriptor cache. A warm plan
        // needs neither a file permit nor an open/close cycle on a cache miss.
        let mut opened = None;
        let mut planned = BTreeMap::<SeriesKey, BTreeMap<u64, Vec<BlockIndex>>>::new();
        let mut returned = 0_u64;
        for page in selected_pages {
            query_budget.check()?;
            let page_bytes = u64::from(page.length);
            if let Some(cached) = self.cached_page(page.offset)? {
                query_budget.record_pack_directory_page_cache_hit();
                self.decode_page(
                    page,
                    &cached.bytes,
                    &requested,
                    to_us,
                    query_budget,
                    plan_reservation,
                    &mut planned,
                    &mut returned,
                )?;
                continue;
            }
            let opened = match &opened {
                Some(file) => file,
                None => opened.insert(self.file_cache.get(&self.path, query_budget)?),
            };
            query_budget.reserve_memory_class(QueryMemoryClass::Metadata, page_bytes)?;
            let started = Instant::now();
            let mut bytes = vec![
                0_u8;
                usize::try_from(page.length).map_err(|_| {
                    corrupt("offline pack directory page length does not fit memory")
                })?
            ];
            let read_result = opened.read_exact_at(page.offset, &mut bytes, query_budget);
            query_budget.record_metadata(started.elapsed());
            if let Err(error) = read_result {
                query_budget.release_memory_class(QueryMemoryClass::Metadata, page_bytes);
                return Err(error);
            }
            query_budget.record_metadata_bytes(page_bytes);
            query_budget.record_pack_directory_pages_read(1);
            let bytes = Arc::new(bytes);
            let decode_result = self.decode_page(
                page,
                &bytes,
                &requested,
                to_us,
                query_budget,
                plan_reservation,
                &mut planned,
                &mut returned,
            );
            if decode_result.is_ok() {
                self.cache_page(page.offset, Arc::clone(&bytes))?;
            }
            query_budget.release_memory_class(QueryMemoryClass::Metadata, page_bytes);
            decode_result?;
        }
        query_budget.record_pack_directory_extents(returned);

        let mut result = Vec::new();
        for (series, by_segment) in planned {
            for (segment_id, mut blocks) in by_segment {
                blocks.sort_unstable_by_key(|block| block.offset);
                blocks.dedup_by_key(|block| block.offset);
                let segment = self
                    .segments
                    .get(&segment_id)
                    .cloned()
                    .ok_or_else(|| corrupt("offline pack directory segment disappeared"))?;
                result.push((series, QuerySegmentRead { segment, blocks }));
            }
        }
        Ok(result)
    }

    fn selected_pages(&self, series_ranges: &[(SeriesKey, i64)]) -> Vec<PageDescriptor> {
        let mut selected_pages = Vec::new();
        for (series, _) in series_ranges {
            let mut page_index = self
                .pages
                .partition_point(|page| page.last_series < *series);
            while let Some(page) = self.pages.get(page_index)
                && page.first_series <= *series
            {
                if *series <= page.last_series {
                    selected_pages.push(*page);
                }
                page_index = page_index.saturating_add(1);
            }
        }
        selected_pages.sort_unstable_by_key(|page| page.offset);
        selected_pages.dedup_by_key(|page| page.offset);
        selected_pages
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_page(
        &self,
        page: PageDescriptor,
        bytes: &[u8],
        requested: &HashMap<SeriesKey, i64>,
        to_us: i64,
        query_budget: &QueryBudget,
        plan_reservation: &mut QueryPlanReservation,
        planned: &mut BTreeMap<SeriesKey, BTreeMap<u64, Vec<BlockIndex>>>,
        returned: &mut u64,
    ) -> Result<()> {
        if crc32c::crc32c(bytes) != page.crc32c {
            return Err(corrupt("offline pack directory page checksum mismatch"));
        }
        let expected = usize::try_from(page.entries)
            .ok()
            .and_then(|entries| entries.checked_mul(LOCATOR_SIZE))
            .ok_or_else(|| corrupt("offline pack directory page length overflows"))?;
        if bytes.len() != expected {
            return Err(corrupt("offline pack directory page length is invalid"));
        }
        let (entries, remainder) = bytes.as_chunks::<LOCATOR_SIZE>();
        if !remainder.is_empty() {
            return Err(corrupt("offline pack directory page is misaligned"));
        }
        let mut selected = 0_u64;
        for entry in entries {
            query_budget.check()?;
            let locator = decode_locator(entry)?;
            let Some(from_us) = requested.get(&locator.series).copied() else {
                continue;
            };
            if locator.block.max_timestamp_us < from_us || locator.block.min_timestamp_us >= to_us {
                continue;
            }
            if !self.segments.contains_key(&locator.segment_id) {
                return Err(corrupt(
                    "offline pack directory refers to an unknown segment",
                ));
            }
            selected = selected.saturating_add(1);
        }

        // Reserve before allocating any selected block. The first pass validates
        // the immutable page and counts exactly the same set as materialization.
        // One governor reservation replaces a shared mutex acquisition per block.
        if selected != 0 {
            plan_reservation.reserve_blocks(selected)?;
        }
        for entry in entries {
            query_budget.check()?;
            let locator = decode_locator(entry)?;
            let Some(from_us) = requested.get(&locator.series).copied() else {
                continue;
            };
            if locator.block.max_timestamp_us < from_us || locator.block.min_timestamp_us >= to_us {
                continue;
            }
            if !self.segments.contains_key(&locator.segment_id) {
                return Err(corrupt(
                    "offline pack directory refers to an unknown segment",
                ));
            }
            planned
                .entry(locator.series)
                .or_default()
                .entry(locator.segment_id)
                .or_default()
                .push(locator.block);
            *returned = returned.saturating_add(1);
        }
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

    fn cache_page(&self, offset: u64, bytes: Arc<Vec<u8>>) -> Result<()> {
        let page_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if page_bytes == 0 || page_bytes > self.page_cache_max_bytes {
            return Ok(());
        }
        let mut cache = self
            .page_cache
            .lock()
            .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
        if cache.entries.contains_key(&offset) {
            return Ok(());
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
            if let Some(entry) = cache.entries.remove(&evict) {
                cache.bytes = cache
                    .bytes
                    .saturating_sub(u64::try_from(entry.page.bytes.len()).unwrap_or(u64::MAX));
            }
        }
        let memory = self
            .memory_governor
            .try_reserve_token(MemoryClass::MetadataCache, page_bytes);
        let memory = if let Some(memory) = memory {
            memory
        } else {
            drop(cache);
            let Some(memory) = self
                .page_pool
                .reserve_page(&self.memory_governor, page_bytes)?
            else {
                return Ok(());
            };
            cache = self
                .page_cache
                .lock()
                .map_err(|_| corrupt("offline pack directory page cache mutex is poisoned"))?;
            if cache.entries.contains_key(&offset) {
                return Ok(());
            }
            // Another loader may have filled this directory while its lock was
            // released. Optional admission must still respect the local cap.
            if cache.bytes.saturating_add(page_bytes) > self.page_cache_max_bytes {
                return Ok(());
            }
            memory
        };
        cache.clock = cache.clock.saturating_add(1).min(PAGE_REFERENCED - 1);
        let last_used = cache.clock;
        cache.entries.insert(
            offset,
            CachedDirectoryPage {
                page: Arc::new(DirectoryPage {
                    bytes,
                    _memory: memory,
                }),
                last_used: last_used | PAGE_REFERENCED,
            },
        );
        cache.bytes = cache.bytes.saturating_add(page_bytes);
        Ok(())
    }
}

struct DecodedHeader {
    format: u32,
    storage_uuid: [u8; 16],
    checkpoint_commit: u64,
    segment_count: u64,
    page_count: u32,
    root_length: u32,
    root_crc32c: u32,
    manifest_crc32c: u32,
    total_length: u64,
    descriptor_length: u32,
}

fn manifest_crc32c(manifest: &Manifest) -> Result<u32> {
    Ok(crc32c::crc32c(&serde_json::to_vec(&(
        manifest.checkpoint_commit,
        &manifest.segments,
    ))?))
}

pub(crate) fn segments_crc32c(segments: &[Arc<Segment>]) -> Result<u32> {
    let descriptors = segments
        .iter()
        .map(|segment| segment.descriptor.clone())
        .collect::<Vec<_>>();
    descriptor_crc32c(descriptors)
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
            LEGACY_FORMAT
        } else {
            PUBLISHED_FORMAT
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
        || !matches!(format, LEGACY_FORMAT | PUBLISHED_FORMAT)
        || read_u32(header, 12) != HEADER_SIZE as u32
        || read_u32(header, 80) != LOCATOR_SIZE as u32
        || read_u32(header, 84) != MAX_PAGE_BYTES as u32
        || (format == LEGACY_FORMAT && descriptor_length != 0)
        || (format == PUBLISHED_FORMAT && descriptor_length == 0)
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
    if header.format != PUBLISHED_FORMAT || length == 0 || length > MAX_EMBEDDED_DESCRIPTOR_BYTES {
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

fn validate_embedded_descriptors(
    directory: &PublishedReadDirectoryDescriptor,
    payload_files: &[PublishedReadPayloadFile],
    descriptors: &[super::SegmentDescriptor],
) -> Result<()> {
    let payload_sizes = payload_files
        .iter()
        .map(|payload| (payload.relative_path.as_str(), payload.file_size))
        .collect::<HashMap<_, _>>();
    let referenced_paths = descriptors
        .iter()
        .map(|descriptor| descriptor.relative_path.as_str())
        .collect::<HashSet<_>>();
    if descriptors.is_empty()
        || payload_sizes.len() != payload_files.len()
        || referenced_paths
            .iter()
            .any(|path| !payload_sizes.contains_key(path))
        || descriptors.windows(2).any(|pair| pair[0].id >= pair[1].id)
        || descriptors.last().map(|descriptor| descriptor.id) != Some(directory.maximum_segment_id)
        || descriptors.iter().any(|descriptor| {
            descriptor.id == directory.generation_id
                || descriptor.level != 2
                || directory
                    .shard_id
                    .is_some_and(|shard| descriptor.shard_id != shard)
                || descriptor.policy_id != directory.policy_id
                || descriptor.window_start_us < directory.window_start_us
                || descriptor.window_end_us > directory.window_end_us
                || descriptor
                    .file_offset
                    .checked_add(descriptor.file_size)
                    .is_none_or(|end| {
                        payload_sizes
                            .get(descriptor.relative_path.as_str())
                            .is_none_or(|size| end > *size)
                    })
        })
        || descriptor_crc32c(descriptors.to_vec())? != directory.segments_crc32c
    {
        return Err(corrupt(
            "published read directory contains invalid segment descriptors",
        ));
    }
    Ok(())
}

fn encode_root(pages: &[PageDescriptor]) -> Vec<u8> {
    let mut root = vec![0_u8; pages.len().saturating_mul(ROOT_ENTRY_SIZE)];
    let (root_entries, remainder) = root.as_chunks_mut::<ROOT_ENTRY_SIZE>();
    debug_assert!(remainder.is_empty());
    for (page, output) in pages.iter().zip(root_entries) {
        encode_series(output, 0, page.first_series);
        encode_series(output, 16, page.last_series);
        put_u64(output, 32, page.offset);
        put_u32(output, 40, page.length);
        put_u32(output, 44, page.entries);
        put_u32(output, 48, page.crc32c);
    }
    root
}

fn decode_root(root: &[u8], total_length: u64) -> Result<Vec<PageDescriptor>> {
    let mut pages = Vec::with_capacity(root.len() / ROOT_ENTRY_SIZE);
    let mut expected_offset = u64::try_from(HEADER_SIZE.saturating_add(root.len()))
        .map_err(|_| corrupt("offline pack directory root offset overflows"))?;
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

#[cfg(test)]
#[path = "pack_directory/budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn locator(series_key: u64, offset: u64) -> Locator {
        let series = SeriesKey::new(1, 1, series_key);
        Locator {
            series,
            segment_id: 1,
            block: BlockIndex {
                series,
                offset,
                size: 1,
                min_timestamp_us: 0,
                max_timestamp_us: 0,
                count_sum: None,
            },
        }
    }

    #[test]
    fn directory_pages_split_after_thirty_two_sparse_series() -> Result<()> {
        let locators = (0..33_u64)
            .map(|series| locator(series, series))
            .collect::<Vec<_>>();
        assert_eq!(directory_page_ranges(&locators)?, vec![0..32, 32..33]);
        Ok(())
    }

    #[test]
    fn directory_pages_split_one_long_series_at_the_byte_limit() -> Result<()> {
        let entries_per_page = MAX_PAGE_BYTES / LOCATOR_SIZE;
        let locators = (0..=entries_per_page)
            .map(|offset| locator(7, offset as u64))
            .collect::<Vec<_>>();
        assert_eq!(
            directory_page_ranges(&locators)?,
            vec![0..entries_per_page, entries_per_page..entries_per_page + 1]
        );
        Ok(())
    }

    #[test]
    fn cached_page_keeps_its_memory_reserved_while_a_reader_holds_it() -> Result<()> {
        let governor = Arc::new(MemoryGovernor::new(128));
        let memory = governor
            .try_reserve_token(MemoryClass::MetadataCache, 64)
            .ok_or_else(|| invalid("test page reservation failed"))?;
        let page = Arc::new(DirectoryPage {
            bytes: Arc::new(vec![0_u8; 64]),
            _memory: memory,
        });
        let reader = Arc::clone(&page);
        let mut cache = DirectoryPageCache::default();
        cache
            .entries
            .insert(0, CachedDirectoryPage { page, last_used: 1 });
        cache.bytes = 64;

        cache.entries.clear();
        cache.bytes = 0;
        assert_eq!(governor.stats().1, 64);
        drop(reader);
        assert_eq!(governor.stats().1, 0);
        Ok(())
    }
}
