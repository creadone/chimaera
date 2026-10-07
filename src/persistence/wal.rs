mod head;
pub(crate) use head::{initialize_empty, validate_empty};

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, corrupt, invalid};
use crate::types::{PointKey, SeriesKey, VersionedPoint};

const MAGIC: &[u8; 8] = b"TSWAL001";
const VERSION: u16 = 2;
const FILE_HEADER_SIZE: usize = 64;
const FRAME_HEADER_SIZE: usize = 32;
const POINT_SIZE: usize = 32;
const MAX_FRAME_PAYLOAD: usize = 32 * 1024 * 1024;

pub(crate) struct WalWriter {
    file: File,
    path: PathBuf,
    failed: Option<String>,
}

impl WalWriter {
    pub(crate) fn point_frame_bytes(count: usize) -> Result<u64> {
        frame_bytes(count, POINT_SIZE)
    }

    pub(crate) fn deletion_frame_bytes(count: usize) -> Result<u64> {
        frame_bytes(count, 16)
    }

    pub fn create(
        directory: &Path,
        storage_uuid: [u8; 16],
        wal_id: u64,
        first_commit: u64,
    ) -> Result<Self> {
        if wal_id == 0 || first_commit == 0 {
            return Err(invalid("WAL ID and first commit must be positive"));
        }
        let path = wal_path(directory, wal_id);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let mut header = [0_u8; FILE_HEADER_SIZE];
        header[..8].copy_from_slice(MAGIC);
        put_u16(&mut header, 8, VERSION);
        put_u16(&mut header, 10, FILE_HEADER_SIZE as u16);
        header[12..28].copy_from_slice(&storage_uuid);
        put_u64(&mut header, 28, wal_id);
        put_u64(&mut header, 36, first_commit);
        put_i64(&mut header, 44, now_us());
        let checksum = crc32c::crc32c(&header[..60]);
        put_u32(&mut header, 60, checksum);
        file.write_all(&header)?;
        file.sync_all()?;
        sync_directory(directory)?;

        head::Head {
            id: wal_id,
            first_commit,
        }
        .persist(directory, storage_uuid)?;
        Ok(Self {
            file,
            path,
            failed: None,
        })
    }

    pub fn append(&mut self, commit_id: u64, points: &[VersionedPoint]) -> Result<u64> {
        self.append_frame(commit_id, points.len(), POINT_SIZE, 0, |payload| {
            for (row, point) in payload
                .as_chunks_mut::<POINT_SIZE>()
                .0
                .iter_mut()
                .zip(points)
            {
                put_u32(row, 0, point.key.series.space_id);
                put_u32(row, 4, point.key.series.bucket_id);
                put_u64(row, 8, point.key.series.key);
                put_i64(row, 16, point.key.timestamp_us);
                put_u64(row, 24, point.value_bits);
            }
        })
    }

    pub fn append_deletions(&mut self, commit_id: u64, keys: &[SeriesKey]) -> Result<u64> {
        self.append_frame(commit_id, keys.len(), 16, 1, |payload| {
            for (row, key) in payload.as_chunks_mut::<16>().0.iter_mut().zip(keys) {
                put_u32(row, 0, key.space_id);
                put_u32(row, 4, key.bucket_id);
                put_u64(row, 8, key.key);
            }
        })
    }

    fn append_frame(
        &mut self,
        commit_id: u64,
        count: usize,
        width: usize,
        kind: u16,
        encode: impl FnOnce(&mut [u8]),
    ) -> Result<u64> {
        self.ensure_healthy()?;
        let frame_len = frame_bytes(count, width)? as usize;
        let payload_len = frame_len - FRAME_HEADER_SIZE;
        let mut frame = vec![0_u8; frame_len];
        put_u32(
            &mut frame,
            0,
            u32::try_from(frame_len).map_err(|_| invalid("WAL frame too large"))?,
        );
        put_u16(&mut frame, 4, VERSION);
        put_u16(&mut frame, 6, kind);
        put_u64(&mut frame, 8, commit_id);
        put_u32(
            &mut frame,
            16,
            u32::try_from(count).map_err(|_| invalid("too many WAL operations"))?,
        );
        put_u32(
            &mut frame,
            20,
            u32::try_from(payload_len).map_err(|_| invalid("WAL payload too large"))?,
        );
        encode(&mut frame[FRAME_HEADER_SIZE..]);
        let payload_checksum = crc32c::crc32c(&frame[FRAME_HEADER_SIZE..]);
        put_u32(&mut frame, 24, payload_checksum);
        let header_checksum = crc32c::crc32c(&frame[..28]);
        put_u32(&mut frame, 28, header_checksum);

        if let Err(error) = self.file.write_all(&frame) {
            return Err(self.fail("append", error));
        }

        Ok(frame.len() as u64)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.ensure_healthy()?;

        if let Err(error) = self.file.sync_data() {
            return Err(self.fail("sync", error));
        }

        Ok(())
    }

    pub fn sync_all(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        if let Err(error) = self.file.sync_all() {
            return Err(self.fail("sync", error));
        }
        Ok(())
    }

    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ensure_healthy(&self) -> Result<()> {
        match self.failed.as_deref() {
            Some(message) => Err(corrupt(format!("WAL writer is unavailable: {message}"))),
            None => Ok(()),
        }
    }

    fn fail(&mut self, operation: &str, error: std::io::Error) -> crate::error::Error {
        self.failed.get_or_insert_with(|| {
            format!("{operation} failed for {}: {error}", self.path.display())
        });
        crate::error::Error::Io(error)
    }
}

fn frame_bytes(count: usize, width: usize) -> Result<u64> {
    let payload = count
        .checked_mul(width)
        .ok_or_else(|| invalid("WAL frame size overflow"))?;
    if payload > MAX_FRAME_PAYLOAD || count > u32::MAX as usize {
        return Err(invalid("WAL frame exceeds format limits"));
    }
    Ok((FRAME_HEADER_SIZE + payload) as u64)
}

#[derive(Debug)]
pub(crate) struct RecoveredFrame {
    pub commit_id: u64,
    pub points: Vec<VersionedPoint>,
    pub deletions: Vec<SeriesKey>,
}

pub(crate) struct WalInspection {
    pub maximum_wal_id: u64,
    pub replay_frames: u64,
    pub replay_operations: u64,
    pub truncated_tail_bytes: u64,
    truncated_tail_path: Option<PathBuf>,
    pub unpublished_paths: Vec<PathBuf>,
    pub obsolete_empty_paths: Vec<PathBuf>,
}

/// Validate WAL files incrementally and visit frames beyond the checkpoint.
///
/// `reserve_frame` runs before allocating payload and decoded points. Its return
/// value lives until `visit_frame` completes, allowing the caller to bind
/// the peak memory of one frame to an RAII reservation.
pub(crate) fn inspect_directory_with<G>(
    directory: &Path,
    storage_uuid: [u8; 16],
    replay_after_commit: u64,
    mut reserve_frame: impl FnMut(u64) -> Result<G>,
    mut visit_frame: impl FnMut(RecoveredFrame) -> Result<()>,
) -> Result<WalInspection> {
    let head = head::Head::load(directory, storage_uuid)?;
    let mut registered = Vec::new();
    let mut unpublished_paths = Vec::new();
    for path in paths(directory)? {
        let id = path_id(&path)?;
        if id > head.id {
            // Creation was interrupted before HEAD publication. No write could
            // have been acknowledged in this file, even if its header is torn.
            unpublished_paths.push(path);
        } else {
            registered.push(path);
        }
    }
    if head.id != 0 && registered.last() != Some(&wal_path(directory, head.id)) {
        return Err(corrupt("required current WAL file is missing"));
    }
    let mut previous_commit = replay_after_commit;
    let mut replay_frames = 0_u64;
    let mut replay_operations = 0_u64;
    let path_count = registered.len();
    let mut truncated_tail_bytes = 0_u64;
    let mut truncated_tail_path = None;
    let mut obsolete_empty_paths = Vec::new();
    for (path_index, path) in registered.into_iter().enumerate() {
        let preceding_commit = previous_commit;
        let mut has_frames = false;
        let (wal_id, first_commit, truncated) =
            recover_file_with(&path, storage_uuid, &mut reserve_frame, &mut |frame| {
                has_frames = true;
                if frame.commit_id <= replay_after_commit {
                    return Ok(());
                }
                let expected_commit = previous_commit
                    .checked_add(1)
                    .ok_or_else(|| corrupt("WAL commit ID is exhausted"))?;
                if frame.commit_id != expected_commit {
                    return Err(corrupt(format!(
                        "WAL commit IDs do not continuously follow the materialized commit at {}",
                        path.display()
                    )));
                }
                previous_commit = frame.commit_id;
                replay_frames = replay_frames.saturating_add(1);
                replay_operations = replay_operations
                    .saturating_add((frame.points.len() + frame.deletions.len()) as u64);
                visit_frame(frame)
            })?;
        if wal_id != path_id(&path)?
            || first_commit == 0
            || first_commit > preceding_commit.saturating_add(1)
            || (wal_id == head.id && first_commit != head.first_commit)
        {
            return Err(corrupt("WAL header does not follow the recovered prefix"));
        }
        if truncated != 0 {
            if path_index + 1 != path_count || truncated_tail_bytes != 0 {
                return Err(corrupt("only the final WAL may contain a truncated tail"));
            }
            truncated_tail_bytes = truncated;
            truncated_tail_path = Some(path.clone());
        }
        // Only a fully checked header with no frames is disposable. Keep the
        // HEAD file even when empty: it is the authority for a required WAL
        // prefix, and must survive a crash before the next HEAD is published.
        if !has_frames && truncated == 0 && wal_id < head.id {
            obsolete_empty_paths.push(path);
        }
    }
    if head.first_commit > previous_commit.saturating_add(1) {
        return Err(corrupt("WAL prefix required by its head is missing"));
    }
    Ok(WalInspection {
        maximum_wal_id: head.id,
        replay_frames,
        replay_operations,
        truncated_tail_bytes,
        truncated_tail_path,
        unpublished_paths,
        obsolete_empty_paths,
    })
}

pub(crate) fn repair_directory(directory: &Path, inspection: &WalInspection) -> Result<()> {
    if let Some(path) = inspection.truncated_tail_path.as_ref() {
        let file = OpenOptions::new().write(true).open(path)?;
        let repaired_length = file
            .metadata()?
            .len()
            .checked_sub(inspection.truncated_tail_bytes)
            .ok_or_else(|| corrupt("truncated WAL tail exceeds its file"))?;
        file.set_len(repaired_length)?;
        file.sync_all()?;
        sync_directory(directory)?;
    }
    for path in &inspection.unpublished_paths {
        fs::remove_file(path)?;
    }
    for path in &inspection.obsolete_empty_paths {
        fs::remove_file(path)?;
    }
    if !inspection.unpublished_paths.is_empty() || !inspection.obsolete_empty_paths.is_empty() {
        sync_directory(directory)?;
    }
    Ok(())
}

fn recover_file_with<G>(
    path: &Path,
    storage_uuid: [u8; 16],
    reserve_frame: &mut impl FnMut(u64) -> Result<G>,
    visit_frame: &mut impl FnMut(RecoveredFrame) -> Result<()>,
) -> Result<(u64, u64, u64)> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let mut header = [0_u8; FILE_HEADER_SIZE];
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC
        || read_u16(&header, 8) != VERSION
        || read_u16(&header, 10) as usize != FILE_HEADER_SIZE
        || header[12..28] != storage_uuid
        || read_u32(&header, 52) != 0
        || read_u32(&header, 56) != 0
        || read_u32(&header, 60) != crc32c::crc32c(&header[..60])
    {
        return Err(corrupt(format!("invalid WAL header: {}", path.display())));
    }
    let wal_id = read_u64(&header, 28);
    let mut truncated_tail_bytes = 0_u64;
    loop {
        let frame_offset = file.stream_position()?;
        let remaining = file_size.saturating_sub(frame_offset);
        if remaining == 0 {
            break;
        }
        if remaining < FRAME_HEADER_SIZE as u64 {
            truncated_tail_bytes = remaining;
            break;
        }
        let mut frame_header = [0_u8; FRAME_HEADER_SIZE];
        file.read_exact(&mut frame_header)?;
        let frame_size = read_u32(&frame_header, 0) as usize;
        let operation_count = read_u32(&frame_header, 16) as usize;
        let payload_size = read_u32(&frame_header, 20) as usize;
        let kind = read_u16(&frame_header, 6);
        let width = if kind == 1 { 16 } else { POINT_SIZE };
        let valid_lengths = frame_size == FRAME_HEADER_SIZE + payload_size
            && operation_count.checked_mul(width) == Some(payload_size)
            && payload_size <= MAX_FRAME_PAYLOAD;
        if read_u16(&frame_header, 4) != VERSION
            || kind > 1
            || (kind == 1 && operation_count > crate::deletion::MAX_KEYS)
            || !valid_lengths
            || read_u32(&frame_header, 28) != crc32c::crc32c(&frame_header[..28])
        {
            return Err(corrupt(format!(
                "invalid WAL frame header: {}",
                path.display()
            )));
        }
        if file_size.saturating_sub(file.stream_position()?) < payload_size as u64 {
            truncated_tail_bytes = file_size.saturating_sub(frame_offset);
            break;
        }
        let decoded_bytes = u64::try_from(operation_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(std::mem::size_of::<VersionedPoint>() as u64);
        let frame_memory_bytes = u64::try_from(payload_size)
            .unwrap_or(u64::MAX)
            .saturating_add(decoded_bytes);
        let _reservation = reserve_frame(frame_memory_bytes)?;
        let mut payload = vec![0_u8; payload_size];
        file.read_exact(&mut payload)?;
        if read_u32(&frame_header, 24) != crc32c::crc32c(&payload) {
            return Err(corrupt(format!(
                "invalid WAL frame checksum: {}",
                path.display()
            )));
        }
        let commit_id = read_u64(&frame_header, 8);
        let mut points = Vec::with_capacity(if kind == 0 { operation_count } else { 0 });
        let mut deletions = Vec::with_capacity(if kind == 1 { operation_count } else { 0 });
        let mut offset = 0_usize;
        if payload.len()
            != operation_count
                .checked_mul(width)
                .ok_or_else(|| corrupt("WAL point count overflow"))?
        {
            return Err(corrupt("WAL point count does not match payload length"));
        }
        for _ in 0..operation_count {
            if kind == 1 {
                let key = SeriesKey::new(
                    read_u32(&payload, offset),
                    read_u32(&payload, offset + 4),
                    read_u64(&payload, offset + 8),
                );
                if deletions.last().is_some_and(|previous| *previous >= key) {
                    return Err(corrupt("WAL deletion keys are not sorted and unique"));
                }
                deletions.push(key);
                offset += 16;
                continue;
            }

            let value_bits = read_u64(&payload, offset + 24);
            if !f64::from_bits(value_bits).is_finite() {
                return Err(corrupt(format!("non-finite WAL value: {}", path.display())));
            }
            points.push(VersionedPoint {
                key: PointKey {
                    series: SeriesKey::new(
                        read_u32(&payload, offset),
                        read_u32(&payload, offset + 4),
                        read_u64(&payload, offset + 8),
                    ),
                    timestamp_us: read_i64(&payload, offset + 16),
                },
                commit_id,
                value_bits,
            });
            offset += POINT_SIZE;
        }
        if offset != payload.len() {
            return Err(corrupt(format!(
                "WAL payload has trailing bytes: {}",
                path.display()
            )));
        }
        visit_frame(RecoveredFrame {
            commit_id,
            points,
            deletions,
        })?;
    }
    Ok((wal_id, read_u64(&header, 36), truncated_tail_bytes))
}

/// Shared enumeration for recovery, administration and accounting. An I/O
/// failure must never silently remove a required file from the returned set.
pub(crate) fn paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "tsw") {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

fn path_id(path: &Path) -> Result<u64> {
    let id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("WAL-"))
        .and_then(|id| id.parse::<u64>().ok())
        .filter(|id| *id > 0)
        .ok_or_else(|| corrupt("invalid WAL file name"))?;
    if path.file_name() != wal_path(Path::new(""), id).file_name() {
        return Err(corrupt("noncanonical WAL file name"));
    }
    Ok(id)
}

pub(crate) fn wal_path(directory: &Path, wal_id: u64) -> PathBuf {
    directory.join(format!("WAL-{wal_id:020}.tsw"))
}

pub(crate) fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

pub(crate) struct UnpublishedFileGuard {
    path: PathBuf,
    renamed: bool,
}

impl UnpublishedFileGuard {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            renamed: false,
        }
    }

    pub(crate) fn mark_present(&mut self) {
        self.renamed = true;
    }

    pub(crate) fn commit(mut self) {
        self.renamed = false;
    }
}

impl Drop for UnpublishedFileGuard {
    fn drop(&mut self) {
        if !self.renamed {
            return;
        }
        if fs::remove_file(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
        {
            let _ = sync_directory(parent);
        }
    }
}

fn now_us() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

pub(crate) fn put_u16(target: &mut [u8], offset: usize, value: u16) {
    target[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(target: &mut [u8], offset: usize, value: u32) {
    target[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(target: &mut [u8], offset: usize, value: u64) {
    target[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_i64(target: &mut [u8], offset: usize, value: i64) {
    target[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_u16(source: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([source[offset], source[offset + 1]])
}

pub(crate) fn read_u32(source: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        source[offset],
        source[offset + 1],
        source[offset + 2],
        source[offset + 3],
    ])
}

pub(crate) fn read_u64(source: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        source[offset],
        source[offset + 1],
        source[offset + 2],
        source[offset + 3],
        source[offset + 4],
        source[offset + 5],
        source[offset + 6],
        source[offset + 7],
    ])
}

pub(crate) fn read_i64(source: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes([
        source[offset],
        source[offset + 1],
        source[offset + 2],
        source[offset + 3],
        source[offset + 4],
        source[offset + 5],
        source[offset + 6],
        source[offset + 7],
    ])
}
