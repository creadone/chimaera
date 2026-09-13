use super::*;

#[test]
fn physical_snapshot_restores_one_commit_while_writes_and_compaction_continue() -> Result<()> {
    let source = TempDir::new()?;
    let snapshot_parent = TempDir::new()?;
    let restore_parent = TempDir::new()?;
    let snapshot_path = snapshot_parent.path().join("snapshot");
    let restored_path = restore_parent.path().join("restored");
    let options = OpenOptions {
        virtual_shards: 1,
        l0_compaction_trigger: 64,
        l0_slowdown_trigger: 96,
        l0_stop_trigger: 128,
        ..OpenOptions::default()
    };
    let storage = Storage::open(source.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 89);
    for batch in 0..2 {
        let from = batch * 100;
        storage.write_batch(
            (from..from + 100)
                .map(|timestamp_us| point(series, timestamp_us, timestamp_us as f64))
                .collect(),
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    assert_eq!(storage.stats()?.l0_segments, 2);

    let first_writes_done = Arc::new(Barrier::new(2));
    let writer_storage = storage.clone();
    let writer_barrier = Arc::clone(&first_writes_done);
    let writer = thread::spawn(move || -> Result<Vec<(u64, i64)>> {
        let mut writes = Vec::new();
        for timestamp_us in 1_000..1_008 {
            let receipt = writer_storage.write_batch(
                vec![point(series, timestamp_us, timestamp_us as f64)],
                Durability::Sync,
            )?;
            writes.push((receipt.commit_id, timestamp_us));
        }
        writer_barrier.wait();
        for timestamp_us in 1_008..1_032 {
            let receipt = writer_storage.write_batch(
                vec![point(series, timestamp_us, timestamp_us as f64)],
                Durability::Sync,
            )?;
            writes.push((receipt.commit_id, timestamp_us));
        }
        Ok(writes)
    });
    let compaction_storage = storage.clone();
    let compaction = thread::spawn(move || compaction_storage.compact());

    first_writes_done.wait();
    let snapshot = storage.snapshot(&snapshot_path)?;
    let concurrent_writes = writer.join().expect("snapshot writer panicked")?;
    compaction.join().expect("snapshot compaction panicked")?;

    assert_eq!(wal_file_count(&snapshot_path)?, 0);
    let restored = Storage::restore_snapshot(&snapshot_path, &restored_path)?;
    assert_eq!(restored.commit_id, snapshot.commit_id);
    assert_eq!(restored.logical_segments, snapshot.logical_segments);
    assert_eq!(restored.physical_files, snapshot.physical_files);
    assert_eq!(wal_file_count(&restored_path)?, 0);

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(snapshot_path.join("manifest/MANIFEST.json"))?)?;
    let first_segment = manifest["segments"][0]["relative_path"]
        .as_str()
        .expect("snapshot must contain a segment");
    let first_segment_path = snapshot_path.join(first_segment);
    let payload_offset = manifest["segments"][0]["file_offset"]
        .as_u64()
        .expect("snapshot segment offset")
        + 112
        + 88;
    let mut segment = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&first_segment_path)?;
    segment.seek(SeekFrom::Start(payload_offset))?;
    let mut byte = [0_u8; 1];
    segment.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    segment.seek(SeekFrom::Start(payload_offset))?;
    segment.write_all(&byte)?;
    segment.sync_all()?;
    let rejected_bitflip_path = restore_parent.path().join("rejected-bitflip");
    assert!(matches!(
        Storage::restore_snapshot(&snapshot_path, &rejected_bitflip_path),
        Err(Error::Corruption(_))
    ));
    assert!(!rejected_bitflip_path.exists());

    segment.set_len(1)?;
    segment.sync_all()?;
    let rejected_path = restore_parent.path().join("rejected-corrupt");
    assert!(matches!(
        Storage::restore_snapshot(&snapshot_path, &rejected_path),
        Err(Error::Corruption(_))
    ));
    assert!(!rejected_path.exists());

    let restored_storage = Storage::open(
        &restored_path,
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        restored_storage.stats()?.visible_commit_id,
        snapshot.commit_id
    );
    let actual = restored_storage
        .scan(series, 0, 2_000)?
        .collect::<Result<Vec<_>>>()?;
    let mut expected_timestamps = (0..200).collect::<Vec<_>>();
    expected_timestamps.extend(
        concurrent_writes
            .into_iter()
            .filter(|(commit_id, _)| *commit_id <= snapshot.commit_id)
            .map(|(_, timestamp_us)| timestamp_us),
    );
    assert_eq!(actual.len(), expected_timestamps.len());
    assert!(
        actual
            .iter()
            .zip(expected_timestamps)
            .all(|(point, timestamp_us)| point.timestamp_us == timestamp_us
                && point.value == timestamp_us as f64)
    );
    restored_storage.close()?;
    storage.close()
}

#[test]
fn gzip_jsonl_export_import_streams_catalog_and_visible_points_in_batches() -> Result<()> {
    let source = TempDir::new()?;
    let source_storage = Storage::open(source.path(), OpenOptions::default())?;
    source_storage.create_space("production")?;
    let policy = source_storage.create_policy(
        StoragePolicyOptions::new("exchange", DAY, DAY)?
            .with_write_mode(WriteMode::Mutable)
            .with_compression(
                CompressionProfile::balanced(),
                CompressionProfile::cold_dense(),
            )?,
    )?;
    source_storage.create_bucket("production", "signals", policy.id)?;
    let first = SeriesRef::new("production", "signals", u64::MAX - 1);
    let second = SeriesRef::new("production", "signals", u64::MAX);
    source_storage.write_named_operations(
        vec![
            NamedPoint::new(first.clone(), 30, 3.0)?.into(),
            NamedPoint::new(first.clone(), 10, 1.0)?.into(),
            NamedPoint::new(first.clone(), 20, 2.0)?.into(),
            NamedPoint::new(second.clone(), -10, 4.0)?.into(),
        ],
        Durability::Sync,
    )?;
    source_storage.upsert(first.clone(), 20, 22.0, Durability::Sync)?;
    source_storage.delete_named_point(first.clone(), 30, Durability::Sync)?;

    let mut dump = Vec::new();
    let exported = source_storage.export_jsonl(
        &mut dump,
        ExportOptions {
            gzip: true,
            max_series: 10,
            max_points: 10,
        },
    )?;
    assert_eq!(&dump[..2], &[0x1f, 0x8b]);
    assert_eq!(exported.spaces, 1);
    assert_eq!(exported.policies, 1);
    assert_eq!(exported.buckets, 1);
    assert_eq!(exported.series, 2);
    assert_eq!(exported.points, 3);
    assert_eq!(exported.bytes_written, dump.len() as u64);

    let destination = TempDir::new()?;
    let destination_storage = Storage::open(destination.path(), OpenOptions::default())?;
    let imported = destination_storage.import_jsonl(
        dump.as_slice(),
        ImportOptions {
            gzip: true,
            batch_size: 2,
            ..ImportOptions::default()
        },
    )?;
    assert_eq!(imported.points_imported, 3);
    assert_eq!(imported.batches_committed, 2);
    assert!(imported.last_commit_id.is_some());
    assert_eq!(
        destination_storage
            .scan_named(&first, -100, 100)?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(10, 1.0), (20, 22.0)]
    );
    assert_eq!(
        destination_storage
            .scan_named(&second, -100, 100)?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(-10, 4.0)]
    );
    assert_eq!(
        destination_storage.catalog()?.policies()[0].cold_compression,
        CompressionProfile::cold_dense()
    );
    assert!(matches!(
        destination_storage.import_jsonl(
            dump.as_slice(),
            ImportOptions {
                gzip: true,
                max_line_bytes: 8,
                ..ImportOptions::default()
            }
        ),
        Err(Error::ResourceLimit(_))
    ));
    destination_storage.close()?;
    source_storage.close()
}

#[test]
fn read_only_info_and_verify_do_not_mutate_and_full_verify_detects_block_corruption() -> Result<()>
{
    let source = TempDir::new()?;
    let storage = Storage::open(
        source.path(),
        OpenOptions {
            virtual_shards: 1,
            wal_checkpoint_bytes: u64::MAX,
            checkpoint_interval: Duration::from_secs(86_400),
            cold_flush_idle_interval: Duration::from_secs(86_400),
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 93);
    storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.write_batch(vec![point(series, 2, 2.0)], Durability::Sync)?;

    let before = storage_file_image(source.path())?;
    let info = Storage::info(source.path())?;
    assert_eq!(info.storage_format, 1);
    assert_eq!(info.checkpoint_commit_id, 1);
    assert_eq!(info.logical_segments, 1);
    assert_eq!(info.wal_files, 1);
    let report = Storage::verify(source.path());
    assert!(report.valid, "{:?}", report.issues);
    assert!(!report.repair_needed);
    assert_eq!(report.verified_segment_operations, 1);
    assert_eq!(report.wal_frames, 1);
    assert_eq!(report.wal_operations, 1);
    assert_eq!(report.orphan_files, 0);
    assert_eq!(storage_file_image(source.path())?, before);

    let active_wal = fs::read_dir(source.path().join("wal"))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .expect("active WAL file");
    let mut wal_tail = std::fs::OpenOptions::new().append(true).open(active_wal)?;
    wal_tail.write_all(&[1, 2, 3])?;
    wal_tail.sync_all()?;
    let repairable = Storage::verify(source.path());
    assert!(repairable.valid, "{:?}", repairable.issues);
    assert!(repairable.repair_needed);
    assert_eq!(repairable.truncated_wal_tail_bytes, 3);
    assert_eq!(repairable.wal_frames, 1);

    let snapshot_parent = TempDir::new()?;
    let snapshot_path = snapshot_parent.path().join("snapshot");
    storage.snapshot(&snapshot_path)?;
    assert!(!snapshot_path.join("LOCK").exists());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(snapshot_path.join("manifest/MANIFEST.json"))?)?;
    let descriptor = &manifest["segments"][0];
    let segment_path = snapshot_path.join(
        descriptor["relative_path"]
            .as_str()
            .expect("snapshot segment path"),
    );
    let payload_offset = descriptor["file_offset"]
        .as_u64()
        .expect("snapshot segment offset")
        + 112
        + 88;
    let mut segment = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment_path)?;
    segment.seek(SeekFrom::Start(payload_offset))?;
    let mut byte = [0_u8; 1];
    segment.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    segment.seek(SeekFrom::Start(payload_offset))?;
    segment.write_all(&byte)?;
    segment.sync_all()?;

    let corrupt = Storage::verify(&snapshot_path);
    assert!(!corrupt.valid);
    assert_eq!(corrupt.issues.len(), 1);
    assert_eq!(corrupt.issues[0].code, "corruption");
    storage.close()
}

#[test]
fn salvage_repairs_only_the_final_wal_tail_into_a_new_verified_storage() -> Result<()> {
    let source = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        wal_checkpoint_bytes: u64::MAX,
        checkpoint_interval: Duration::from_secs(86_400),
        cold_flush_idle_interval: Duration::from_secs(86_400),
        ..OpenOptions::default()
    };
    let storage = Storage::open(source.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 95);
    storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.write_batch(vec![point(series, 2, 2.0)], Durability::Sync)?;
    let active_wal = fs::read_dir(source.path().join("wal"))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .expect("active WAL file");
    let mut tail = std::fs::OpenOptions::new().append(true).open(active_wal)?;
    tail.write_all(&[9, 8, 7])?;
    tail.sync_all()?;
    let before = storage_file_image(source.path())?;

    let destination_parent = TempDir::new()?;
    let destination = destination_parent.path().join("salvaged");
    let salvaged = Storage::salvage(source.path(), &destination)?;
    assert_eq!(salvaged.checkpoint_commit_id, 2);
    assert_eq!(salvaged.repaired_wal_tail_bytes, 3);
    assert!(salvaged.files_copied >= 4);
    assert!(salvaged.bytes_copied > 0);
    assert_eq!(storage_file_image(source.path())?, before);
    let source_report = Storage::verify(source.path());
    assert!(source_report.valid);
    assert!(source_report.repair_needed);

    let report = Storage::verify(&destination);
    assert!(report.valid, "{:?}", report.issues);
    assert!(!report.repair_needed);
    assert_eq!(report.verified_segment_operations, 2);
    let restored = Storage::open(
        &destination,
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        restored
            .scan(series, 0, 10)?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(1, 1.0), (2, 2.0)]
    );
    restored.close()?;
    assert!(matches!(
        Storage::salvage(source.path(), &destination),
        Err(Error::Invalid(message)) if message.contains("destination already exists")
    ));
    storage.close()
}

#[test]
fn salvage_must_include_retained_aggregate_archives() -> Result<()> {
    assert_salvage_preserves_retained_aggregate_archives(true)?;
    assert_salvage_preserves_retained_aggregate_archives(false)
}
