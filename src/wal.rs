use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, corrupt, invalid};
use crate::types::{Durability, PointKey, SeriesKey, VersionedPoint};

const MAGIC: &[u8; 8] = b"TSWAL001";
const VERSION: u16 = 1;
const FILE_HEADER_SIZE: usize = 64;
const FRAME_HEADER_SIZE: usize = 32;
const OPERATION_SIZE: usize = 29;
const PUT_SIZE: usize = 37;
const MAX_FRAME_PAYLOAD: usize = 32 * 1024 * 1024;

pub(crate) struct WalWriter {
    file: File,
    path: PathBuf,
    failed: Option<String>,
}

impl WalWriter {
    pub fn create(
        directory: &Path,
        storage_uuid: [u8; 16],
        wal_id: u64,
        first_commit: u64,
    ) -> Result<Self> {
        fs::create_dir_all(directory)?;
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
        Ok(Self {
            file,
            path,
            failed: None,
        })
    }

    pub fn append(
        &mut self,
        commit_id: u64,
        points: &[VersionedPoint],
        durability: Durability,
    ) -> Result<u64> {
        self.ensure_healthy()?;
        let payload_len = points.iter().try_fold(0_usize, |size, point| {
            size.checked_add(if point.is_delete {
                OPERATION_SIZE
            } else {
                PUT_SIZE
            })
            .ok_or_else(|| invalid("WAL frame size overflow"))
        })?;
        if payload_len > MAX_FRAME_PAYLOAD || points.len() > u32::MAX as usize {
            return Err(invalid("WAL frame exceeds v1 limits"));
        }
        let frame_len = FRAME_HEADER_SIZE + payload_len;
        let mut frame = vec![0_u8; frame_len];
        put_u32(
            &mut frame,
            0,
            u32::try_from(frame_len).map_err(|_| invalid("WAL frame too large"))?,
        );
        put_u16(&mut frame, 4, VERSION);
        put_u16(&mut frame, 6, durability.wal_flag());
        put_u64(&mut frame, 8, commit_id);
        put_u32(
            &mut frame,
            16,
            u32::try_from(points.len()).map_err(|_| invalid("too many WAL operations"))?,
        );
        put_u32(
            &mut frame,
            20,
            u32::try_from(payload_len).map_err(|_| invalid("WAL payload too large"))?,
        );
        let mut offset = FRAME_HEADER_SIZE;
        for point in points {
            frame[offset] = if point.is_delete { 2 } else { 1 };
            put_u32(&mut frame, offset + 1, point.ordinal);
            put_u32(&mut frame, offset + 5, point.key.series.space_id);
            put_u32(&mut frame, offset + 9, point.key.series.bucket_id);
            put_u64(&mut frame, offset + 13, point.key.series.key);
            put_i64(&mut frame, offset + 21, point.key.timestamp_us);
            if !point.is_delete {
                put_u64(&mut frame, offset + 29, point.value_bits);
                offset += PUT_SIZE;
            } else {
                offset += OPERATION_SIZE;
            }
        }
        let payload_checksum = crc32c::crc32c(&frame[FRAME_HEADER_SIZE..]);
        put_u32(&mut frame, 24, payload_checksum);
        let header_checksum = crc32c::crc32c(&frame[..28]);
        put_u32(&mut frame, 28, header_checksum);
        if let Some(bytes) = crate::failpoint::wal_partial_write_bytes(commit_id, frame.len()) {
            if let Err(error) = self.file.write_all(&frame[..bytes]) {
                return Err(self.fail("append", error));
            }
            return Err(self.fail(
                "append",
                std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "injected partial WAL write",
                ),
            ));
        }
        if let Err(error) = self.file.write_all(&frame) {
            return Err(self.fail("append", error));
        }
        crate::failpoint::hit("wal_after_append");
        Ok(frame.len() as u64)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        if crate::failpoint::wal_sync_error() {
            return Err(self.fail(
                "sync",
                std::io::Error::new(std::io::ErrorKind::StorageFull, "injected WAL sync failure"),
            ));
        }
        if let Err(error) = self.file.sync_data() {
            return Err(self.fail("sync", error));
        }
        crate::failpoint::hit("wal_after_sync");
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

#[derive(Debug)]
pub(crate) struct RecoveredFrame {
    pub commit_id: u64,
    pub points: Vec<VersionedPoint>,
}

pub(crate) struct WalInspection {
    pub maximum_wal_id: u64,
    pub replay_frames: u64,
    pub replay_operations: u64,
    pub truncated_tail_bytes: u64,
    truncated_tail_path: Option<PathBuf>,
}

#[cfg(test)]
pub(crate) fn recover_directory(
    directory: &Path,
    storage_uuid: [u8; 16],
    replay_after_commit: u64,
) -> Result<(Vec<RecoveredFrame>, u64)> {
    let mut frames = Vec::new();
    let inspection = inspect_directory_with(
        directory,
        storage_uuid,
        replay_after_commit,
        |_| Ok(()),
        |frame| {
            frames.push(frame);
            Ok(())
        },
    )?;
    repair_truncated_tail(directory, &inspection)?;
    Ok((frames, inspection.maximum_wal_id))
}

/// Потоково проверяет все WAL-файлы и передаёт непокрытые кадры посетителю.
///
/// `reserve_frame` вызывается до выделения payload и decoded points. Возвращаемое
/// значение живёт до завершения `visit_frame`, поэтому вызывающий код может
/// привязать пиковую память одного кадра к RAII reservation.
pub(crate) fn inspect_directory_with<G>(
    directory: &Path,
    storage_uuid: [u8; 16],
    replay_after_commit: u64,
    mut reserve_frame: impl FnMut(u64) -> Result<G>,
    mut visit_frame: impl FnMut(RecoveredFrame) -> Result<()>,
) -> Result<WalInspection> {
    if !directory.exists() {
        return Ok(WalInspection {
            maximum_wal_id: 0,
            replay_frames: 0,
            replay_operations: 0,
            truncated_tail_bytes: 0,
            truncated_tail_path: None,
        });
    }
    let mut paths = fs::read_dir(directory)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    let mut maximum_wal_id = 0;
    let mut previous_commit = replay_after_commit;
    let mut replay_frames = 0_u64;
    let mut replay_operations = 0_u64;
    let path_count = paths.len();
    let mut truncated_tail_bytes = 0_u64;
    let mut truncated_tail_path = None;
    for (path_index, path) in paths.into_iter().enumerate() {
        let (wal_id, truncated) =
            recover_file_with(&path, storage_uuid, &mut reserve_frame, &mut |frame| {
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
                replay_operations = replay_operations.saturating_add(frame.points.len() as u64);
                visit_frame(frame)
            })?;
        if truncated != 0 {
            if path_index + 1 != path_count || truncated_tail_bytes != 0 {
                return Err(corrupt("only the final WAL may contain a truncated tail"));
            }
            truncated_tail_bytes = truncated;
            truncated_tail_path = Some(path.clone());
        }
        maximum_wal_id = maximum_wal_id.max(wal_id);
    }
    Ok(WalInspection {
        maximum_wal_id,
        replay_frames,
        replay_operations,
        truncated_tail_bytes,
        truncated_tail_path,
    })
}

pub(crate) fn repair_truncated_tail(directory: &Path, inspection: &WalInspection) -> Result<()> {
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
    Ok(())
}

fn recover_file_with<G>(
    path: &Path,
    storage_uuid: [u8; 16],
    reserve_frame: &mut impl FnMut(u64) -> Result<G>,
    visit_frame: &mut impl FnMut(RecoveredFrame) -> Result<()>,
) -> Result<(u64, u64)> {
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
        let durability = read_u16(&frame_header, 6);
        let minimum_payload = operation_count.saturating_mul(OPERATION_SIZE);
        let maximum_payload = operation_count.saturating_mul(PUT_SIZE);
        let valid_lengths = frame_size == FRAME_HEADER_SIZE + payload_size
            && payload_size >= minimum_payload
            && payload_size <= maximum_payload
            && payload_size <= MAX_FRAME_PAYLOAD;
        if read_u16(&frame_header, 4) != VERSION
            || !matches!(durability, 1 | 2 | 4)
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
        let mut points = Vec::with_capacity(operation_count);
        let mut offset = 0_usize;
        for index in 0..operation_count {
            if payload.len().saturating_sub(offset) < OPERATION_SIZE {
                return Err(corrupt(format!(
                    "truncated WAL operation: {}",
                    path.display()
                )));
            }
            let operation_type = payload[offset];
            if !matches!(operation_type, 1 | 2) || read_u32(&payload, offset + 1) != index as u32 {
                return Err(corrupt(format!(
                    "unsupported WAL operation: {}",
                    path.display()
                )));
            }
            let is_delete = operation_type == 2;
            let value_bits = if is_delete {
                0
            } else {
                if payload.len().saturating_sub(offset) < PUT_SIZE {
                    return Err(corrupt(format!(
                        "truncated WAL put operation: {}",
                        path.display()
                    )));
                }
                read_u64(&payload, offset + 29)
            };
            if !is_delete && !f64::from_bits(value_bits).is_finite() {
                return Err(corrupt(format!("non-finite WAL value: {}", path.display())));
            }
            points.push(VersionedPoint {
                key: PointKey {
                    series: SeriesKey {
                        space_id: read_u32(&payload, offset + 5),
                        bucket_id: read_u32(&payload, offset + 9),
                        key: read_u64(&payload, offset + 13),
                    },
                    timestamp_us: read_i64(&payload, offset + 21),
                },
                commit_id,
                ordinal: index as u32,
                value_bits,
                is_delete,
            });
            offset += if is_delete { OPERATION_SIZE } else { PUT_SIZE };
        }
        if offset != payload.len() {
            return Err(corrupt(format!(
                "WAL payload has trailing bytes: {}",
                path.display()
            )));
        }
        visit_frame(RecoveredFrame { commit_id, points })?;
    }
    Ok((wal_id, truncated_tail_bytes))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn v1_wal_round_trip_preserves_versions() -> Result<()> {
        let temporary = TempDir::new()?;
        let uuid = [7_u8; 16];
        let mut writer = WalWriter::create(temporary.path(), uuid, 1, 1)?;
        let points = vec![VersionedPoint {
            key: PointKey {
                series: SeriesKey::new(1, 2, 3),
                timestamp_us: -5,
            },
            commit_id: 1,
            ordinal: 0,
            value_bits: 4.5_f64.to_bits(),
            is_delete: false,
        }];
        writer.append(1, &points, Durability::Sync)?;
        writer.sync()?;
        drop(writer);

        let (recovered, maximum_wal_id) = recover_directory(temporary.path(), uuid, 0)?;
        assert_eq!(maximum_wal_id, 1);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].points, points);
        Ok(())
    }

    #[test]
    fn v1_wal_round_trip_supports_mixed_puts_and_deletes() -> Result<()> {
        let temporary = TempDir::new()?;
        let uuid = [8_u8; 16];
        let series = SeriesKey::new(1, 1, 1);
        let operations = vec![
            VersionedPoint {
                key: PointKey {
                    series,
                    timestamp_us: 10,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: 2.5_f64.to_bits(),
                is_delete: false,
            },
            VersionedPoint {
                key: PointKey {
                    series,
                    timestamp_us: 11,
                },
                commit_id: 1,
                ordinal: 1,
                value_bits: 0,
                is_delete: true,
            },
        ];
        let mut writer = WalWriter::create(temporary.path(), uuid, 1, 1)?;
        writer.append(1, &operations, Durability::Sync)?;
        writer.sync()?;
        drop(writer);

        let (recovered, _) = recover_directory(temporary.path(), uuid, 0)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].points, operations);
        Ok(())
    }

    #[test]
    fn recovery_rejects_a_gap_after_the_materialized_commit() -> Result<()> {
        let temporary = TempDir::new()?;
        let uuid = [9_u8; 16];
        let series = SeriesKey::new(1, 1, 1);
        let point = |commit_id| VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: commit_id as i64,
            },
            commit_id,
            ordinal: 0,
            value_bits: (commit_id as f64).to_bits(),
            is_delete: false,
        };
        let mut writer = WalWriter::create(temporary.path(), uuid, 1, 1)?;
        writer.append(1, &[point(1)], Durability::Sync)?;
        writer.append(3, &[point(3)], Durability::Sync)?;
        writer.sync()?;
        drop(writer);

        assert!(matches!(
            recover_directory(temporary.path(), uuid, 0),
            Err(crate::error::Error::Corruption(message))
                if message.contains("continuously follow")
        ));
        Ok(())
    }

    #[test]
    fn unpublished_file_guard_removes_only_uncommitted_renames() -> Result<()> {
        let temporary = TempDir::new()?;
        let removed = temporary.path().join("removed");
        fs::write(&removed, b"temporary")?;
        let mut guard = UnpublishedFileGuard::new(removed.clone());
        guard.mark_present();
        drop(guard);
        assert!(!removed.exists());

        let retained = temporary.path().join("retained");
        fs::write(&retained, b"published")?;
        let mut guard = UnpublishedFileGuard::new(retained.clone());
        guard.mark_present();
        guard.commit();
        assert_eq!(fs::read(retained)?, b"published");
        Ok(())
    }
}
