use std::fs::{self, File, OpenOptions as FileOpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chimaera::{Durability, Error, OpenOptions, Point, Result, SeriesKey, Storage};
use serde_json::Value;
use tempfile::TempDir;

const SEGMENT_HEADER_BYTES: u64 = 112;
const BLOCK_HEADER_BYTES: u64 = 88;
const SEGMENT_FOOTER_BYTES: u64 = 64;

#[test]
fn manifest_and_segment_corruption_matrix_is_rejected() -> Result<()> {
    let source = segment_storage()?;

    let truncated_manifest = clone_storage(source.path())?;
    let manifest_path = truncated_manifest.path().join("manifest/MANIFEST.json");
    let manifest_size = fs::metadata(&manifest_path)?.len();
    FileOpenOptions::new()
        .write(true)
        .open(&manifest_path)?
        .set_len(manifest_size / 2)?;
    assert_verify_and_open_reject(truncated_manifest.path());

    for corruption in [SegmentCorruption::Header, SegmentCorruption::Truncate] {
        let damaged = clone_storage(source.path())?;
        damage_segment(damaged.path(), corruption)?;
        assert_verify_and_open_reject(damaged.path());
    }

    let damaged_index = clone_storage(source.path())?;
    damage_segment(damaged_index.path(), SegmentCorruption::Index)?;
    assert_verify_and_open_reject(damaged_index.path());

    let damaged_block = clone_storage(source.path())?;
    damage_segment(damaged_block.path(), SegmentCorruption::BlockPayload)?;
    assert!(!Storage::verify(damaged_block.path()).valid);
    let storage = open_existing(damaged_block.path())?;
    let read = storage
        .scan(SeriesKey::new(1, 1, 1), 0, 10)
        .and_then(|stream| stream.collect::<Result<Vec<_>>>());
    assert!(matches!(read, Err(Error::Corruption(_))));
    storage.close()?;

    let damaged_block_header = clone_storage(source.path())?;
    damage_segment(
        damaged_block_header.path(),
        SegmentCorruption::BlockHeaderIndex,
    )?;
    assert!(!Storage::verify(damaged_block_header.path()).valid);
    let storage = open_existing(damaged_block_header.path())?;
    let read = storage
        .scan(SeriesKey::new(1, 1, 1), 0, 10)
        .and_then(|stream| stream.collect::<Result<Vec<_>>>());
    assert!(matches!(read, Err(Error::Corruption(_))));
    storage.close()?;
    Ok(())
}

#[test]
fn manifest_checkpoint_bit_flip_is_rejected() -> Result<()> {
    let storage = wal_storage(false)?;
    let path = storage.path().join("manifest/MANIFEST.json");
    let mut bytes = fs::read(&path)?;
    let needle = b"\"checkpoint_commit\":0";
    let offset = bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("checkpoint field in manifest")
        + needle.len()
        - 1;
    bytes[offset] ^= 1;
    let mut file = FileOpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    assert_verify_and_open_reject(storage.path());
    Ok(())
}

#[test]
fn missing_identity_never_reinitializes_or_cleans_existing_storage() -> Result<()> {
    let storage = segment_storage()?;
    let manifest_path = storage.path().join("manifest/MANIFEST.json");
    let manifest_before = fs::read(&manifest_path)?;
    let manifest: Value = serde_json::from_slice(&manifest_before)?;
    let segment_path = storage.path().join(
        manifest["segments"][0]["relative_path"]
            .as_str()
            .expect("segment path"),
    );
    fs::remove_file(storage.path().join("IDENTITY"))?;

    assert!(matches!(
        Storage::open(storage.path(), quiet_options()),
        Err(Error::Corruption(message)) if message.contains("IDENTITY is missing")
    ));
    assert_eq!(fs::read(&manifest_path)?, manifest_before);
    assert!(segment_path.exists());
    Ok(())
}

#[test]
fn wal_corruption_matrix_distinguishes_a_repairable_final_tail() -> Result<()> {
    let single_wal = wal_storage(false)?;

    let damaged_frame = clone_storage(single_wal.path())?;
    let wal = wal_paths(damaged_frame.path())?
        .into_iter()
        .next()
        .expect("fixture WAL");
    flip_byte(&wal, 64 + 32)?;
    assert_verify_and_open_reject(damaged_frame.path());

    let final_tail = clone_storage(single_wal.path())?;
    let wal = wal_paths(final_tail.path())?
        .into_iter()
        .next()
        .expect("fixture WAL");
    let size = fs::metadata(&wal)?.len();
    FileOpenOptions::new()
        .write(true)
        .open(&wal)?
        .set_len(size - 3)?;
    let report = Storage::verify(final_tail.path());
    assert!(report.valid, "{:?}", report.issues);
    assert!(report.repair_needed);
    assert!(report.truncated_wal_tail_bytes > 0);
    let storage = open_existing(final_tail.path())?;
    assert_eq!(
        storage
            .scan(SeriesKey::new(1, 1, 1), 0, 10)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        0
    );
    storage.close()?;
    let repaired = Storage::verify(final_tail.path());
    assert!(repaired.valid, "{:?}", repaired.issues);
    assert!(!repaired.repair_needed);

    let multiple_wals = wal_storage(true)?;
    let truncated_non_final = clone_storage(multiple_wals.path())?;
    let wals = wal_paths(truncated_non_final.path())?;
    assert!(wals.len() >= 2);
    let size = fs::metadata(&wals[0])?.len();
    FileOpenOptions::new()
        .write(true)
        .open(&wals[0])?
        .set_len(size - 3)?;
    assert_verify_and_open_reject(truncated_non_final.path());
    Ok(())
}

#[derive(Clone, Copy)]
enum SegmentCorruption {
    Header,
    BlockHeaderIndex,
    BlockPayload,
    Index,
    Truncate,
}

fn segment_storage() -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), quiet_options())?;
    storage.write_batch(
        vec![Point::new(SeriesKey::new(1, 1, 1), 1, 1.25)?],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.close()?;
    Ok(temporary)
}

fn wal_storage(multiple: bool) -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            checkpoint_on_close: false,
            ..quiet_options()
        },
    )?;
    let series = SeriesKey::new(1, 1, 1);
    storage.write_batch(vec![Point::new(series, 1, 1.0)?], Durability::Sync)?;
    if multiple {
        storage.flush()?;
        storage.write_batch(vec![Point::new(series, 2, 2.0)?], Durability::Sync)?;
    }
    storage.close()?;
    Ok(temporary)
}

fn quiet_options() -> OpenOptions {
    OpenOptions {
        virtual_shards: 1,
        wal_checkpoint_bytes: u64::MAX,
        checkpoint_interval: Duration::from_secs(86_400),
        cold_flush_idle_interval: Duration::from_secs(86_400),
        l0_compaction_trigger: 100,
        l0_slowdown_trigger: 101,
        l0_stop_trigger: 102,
        ..OpenOptions::default()
    }
}

fn open_existing(path: &Path) -> Result<Storage> {
    Storage::open(
        path,
        OpenOptions {
            create_if_missing: false,
            checkpoint_on_close: false,
            virtual_shards: 1,
            ..quiet_options()
        },
    )
}

fn assert_verify_and_open_reject(path: &Path) {
    let report = Storage::verify(path);
    assert!(!report.valid, "verification accepted damaged storage");
    assert!(
        Storage::open(
            path,
            OpenOptions {
                create_if_missing: false,
                virtual_shards: 1,
                ..quiet_options()
            }
        )
        .is_err(),
        "writable open accepted damaged storage"
    );
}

fn damage_segment(root: &Path, corruption: SegmentCorruption) -> Result<()> {
    let manifest: Value = serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let descriptor = &manifest["segments"][0];
    let file_offset = descriptor["file_offset"].as_u64().expect("file offset");
    let file_size = descriptor["file_size"].as_u64().expect("file size");
    let path = root.join(descriptor["relative_path"].as_str().expect("segment path"));
    match corruption {
        SegmentCorruption::Header => flip_byte(&path, file_offset)?,
        SegmentCorruption::BlockHeaderIndex => {
            let block_offset = file_offset + SEGMENT_HEADER_BYTES;
            let mut file = FileOpenOptions::new().read(true).write(true).open(&path)?;
            file.seek(SeekFrom::Start(block_offset))?;
            let mut header = [0_u8; BLOCK_HEADER_BYTES as usize];
            file.read_exact(&mut header)?;
            header[8] ^= 0xff;
            let header_crc = crc32c::crc32c(&header[..84]);
            header[84..88].copy_from_slice(&header_crc.to_le_bytes());
            file.seek(SeekFrom::Start(block_offset))?;
            file.write_all(&header)?;
            file.sync_all()?;
        }
        SegmentCorruption::BlockPayload => {
            flip_byte(
                &path,
                file_offset + SEGMENT_HEADER_BYTES + BLOCK_HEADER_BYTES,
            )?;
        }
        SegmentCorruption::Index => {
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(
                file_offset + file_size - SEGMENT_FOOTER_BYTES + 12,
            ))?;
            let mut bytes = [0_u8; 8];
            file.read_exact(&mut bytes)?;
            let index_offset = u64::from_le_bytes(bytes);
            flip_byte(&path, file_offset + index_offset)?;
        }
        SegmentCorruption::Truncate => {
            let physical_size = fs::metadata(&path)?.len();
            FileOpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(physical_size - 1)?;
        }
    }
    Ok(())
}

fn flip_byte(path: &Path, offset: u64) -> Result<()> {
    let mut file = FileOpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    Ok(())
}

fn wal_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(root.join("wal"))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    Ok(paths)
}

fn clone_storage(source: &Path) -> Result<TempDir> {
    let destination = TempDir::new()?;
    let mut pending = vec![(source.to_path_buf(), destination.path().to_path_buf())];
    while let Some((from, to)) = pending.pop() {
        fs::create_dir_all(&to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            let target = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                pending.push((entry.path(), target));
            } else {
                fs::copy(entry.path(), target)?;
            }
        }
    }
    Ok(destination)
}
