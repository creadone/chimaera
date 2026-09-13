use super::*;

#[test]
fn zstd_preserves_mvcc_across_compaction_snapshot_and_reopen() -> Result<()> {
    let temporary = TempDir::new()?;
    let db = temporary.path().join("db");
    let options = OpenOptions {
        history_seal_lag: None,
        clock: Arc::new(ManualClock::new(10 * DAY)),
        ..OpenOptions::default()
    };
    let storage = Storage::open(&db, options.clone())?;
    let space = storage.create_space("zstd")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("cold", DAY, DAY)?
            .with_compression(CompressionProfile::columnar(), CompressionProfile::zstd())?
            .with_write_mode(WriteMode::Mutable),
    )?;
    let bucket = storage.create_bucket("zstd", "data", policy.id)?;
    let series = SeriesKey::new(space.id, bucket.id, 1);
    let original = (0..5000)
        .map(|t| point(series, t, t as f64))
        .collect::<Vec<_>>();
    storage.write_batch(original.clone(), Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    let held = storage.scan(series, 0, 5000)?;
    storage.write_operations(
        vec![
            point(series, 1, 91.0).into(),
            WriteOperation::delete(series, 1),
            point(series, 1, -0.0).into(),
            point(series, 2, 92.0).into(),
            WriteOperation::delete(series, 2),
            point(series, 5000, 12.0).into(),
        ],
        Durability::GroupCommit,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    let old = held.collect::<Result<Vec<_>>>()?;
    assert_eq!(old, original);
    let mut expected = original;
    expected[1].value = -0.0;
    expected.remove(2);
    expected.push(point(series, 5000, 12.0));
    let snapshot = temporary.path().join("snapshot");
    storage.snapshot(&snapshot)?;
    storage.close()?;
    let restored = temporary.path().join("restored");
    Storage::restore_snapshot(&snapshot, &restored)?;
    for root in [&db, &restored] {
        let storage = Storage::open(root, options.clone())?;
        let actual = storage.scan(series, 0, 5001)?.collect::<Result<Vec<_>>>()?;
        assert_eq!(actual, expected);
        assert_eq!(actual[1].value.to_bits(), (-0.0_f64).to_bits());
        assert!(storage.point(series, 2)?.is_none());
        // Exclude the appended point: the upper endpoint stays half-open.
        let grouped = storage.group(GroupRequest::new(vec![series], 0, 5000)?)?;
        assert_eq!(grouped.rows.iter().map(|r| r.count).sum::<u64>(), 4999);
        assert_eq!(
            grouped.rows.iter().map(|r| r.sum).sum::<f64>(),
            12_497_497.0
        );
        storage.close()?;
        let report = Storage::verify(root);
        assert!(report.valid, "{:?}", report.issues);
    }
    // Full verify and restore must reject a damaged compressed payload.
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(snapshot.join("manifest/MANIFEST.json"))?)?;
    let descriptor = &manifest["segments"][0];
    let path = snapshot.join(descriptor["relative_path"].as_str().expect("segment path"));
    let offset = descriptor["file_offset"].as_u64().unwrap_or(0) + 112;
    let mut file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0; 88];
    file.read_exact(&mut header)?;
    assert_eq!(&header[68..72], &[2, 1, 2, 4]);
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset + 88))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    assert!(!Storage::verify(&snapshot).valid);
    assert!(Storage::restore_snapshot(&snapshot, temporary.path().join("damaged")).is_err());
    Ok(())
}
