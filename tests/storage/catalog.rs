use super::*;

#[test]
fn catalog_is_durable_resolvable_and_published_with_storage_changes() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(1_000));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            clock: clock.clone(),
            ..OpenOptions::default()
        },
    )?;
    let space = storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("fast", DAY, DAY * 30)?
            .with_retention_us(Some(DAY * 90))?
            .with_max_lateness_us(Some(30 * 60 * 1_000_000))?
            .with_compression(
                CompressionProfile::hot_fast(),
                CompressionProfile::cold_dense(),
            )?,
    )?;
    let bucket = storage.create_bucket("production", "latency", "fast")?;
    let other_policy =
        storage.create_policy(StoragePolicyOptions::new("archive", DAY * 7, DAY * 30)?)?;
    let other_bucket = storage.create_bucket("production", "requests", other_policy.id)?;

    assert_eq!(space.id, 1);
    assert_eq!(policy.id, 1);
    assert_eq!(bucket.id, 1);
    assert_eq!(other_policy.id, 2);
    assert_eq!(other_bucket.id, 2);
    let reference = SeriesRef::new("production", "latency", 42);
    let (series, resolved_policy) = storage.resolve_series(&reference)?;
    assert_eq!(series, SeriesKey::new(space.id, bucket.id, 42));
    assert_eq!(resolved_policy, policy);
    assert!(matches!(
        storage.create_space("production"),
        Err(Error::AlreadyExists(_))
    ));
    assert!(matches!(
        storage.create_bucket("missing", "data", policy.id),
        Err(Error::NotFound(_))
    ));

    // Catalog publication and checkpoint publication share one owner. Interleaving
    // them must preserve both the catalog and storage state in the manifest.
    let write_storage = storage.clone();
    let writer = thread::spawn(move || -> Result<()> {
        write_storage.write_batch(vec![point(series, 123, 7.5)], Durability::Sync)?;
        write_storage.checkpoint()?;
        Ok(())
    });
    let created = storage.create_space("staging")?;
    writer.join().expect("catalog storage writer")?;
    assert_eq!(created.id, 2);
    let expected_catalog = storage.catalog()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            clock: clock.clone(),
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reopened.catalog()?, expected_catalog);
    assert_eq!(
        reopened.resolve_series(&reference)?.0,
        SeriesKey::new(space.id, bucket.id, 42)
    );
    assert_eq!(
        reopened.point(series, 123)?.map(|point| point.value),
        Some(7.5)
    );
    let next = reopened.create_space("next")?;
    assert_eq!(next.id, 3);
    reopened.close()
}

#[test]
fn named_writes_use_their_policy_domains_and_survive_compaction() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 4,
            partition_span_us: DAY * 30,
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("production")?;
    let daily = storage.create_policy(
        StoragePolicyOptions::new("daily", DAY, DAY)?.with_write_mode(WriteMode::Mutable),
    )?;
    let weekly = storage.create_policy(
        StoragePolicyOptions::new("weekly", DAY, DAY * 7)?.with_write_mode(WriteMode::Mutable),
    )?;
    storage.create_bucket("production", "hot", daily.id)?;
    storage.create_bucket("production", "archive", "weekly")?;
    let hot = SeriesRef::new("production", "hot", 7);
    let archive = SeriesRef::new("production", "archive", 7);

    storage.write_named_operations(
        vec![
            NamedPoint::new(hot.clone(), DAY + 10, 1.5)?.into(),
            NamedPoint::new(archive.clone(), DAY + 10, 2.5)?.into(),
            NamedWriteOperation::delete(hot.clone(), DAY + 20),
        ],
        Durability::Sync,
    )?;
    storage.upsert(hot.clone(), DAY + 20, 3.5, Durability::Sync)?;
    storage.checkpoint()?;

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let segments = manifest["segments"]
        .as_array()
        .expect("manifest segment array");
    assert!(segments.iter().any(|segment| {
        segment["policy_id"] == daily.id
            && segment["window_start_us"] == DAY
            && segment["window_end_us"] == DAY * 2
    }));
    assert!(segments.iter().any(|segment| {
        segment["policy_id"] == weekly.id
            && segment["window_start_us"] == 0
            && segment["window_end_us"] == DAY * 7
    }));
    assert_eq!(
        storage
            .point_named(&hot, DAY + 20)?
            .map(|point| point.value),
        Some(3.5)
    );
    assert_eq!(
        storage
            .scan_named(&archive, DAY, DAY * 2)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        1
    );
    storage.compact()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            virtual_shards: 4,
            partition_span_us: DAY * 30,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reopened.point_named(&hot, DAY + 10)?.unwrap().value, 1.5);
    assert_eq!(
        reopened.point_named(&archive, DAY + 10)?.unwrap().value,
        2.5
    );
    reopened.delete_named_point(hot.clone(), DAY + 10, Durability::Sync)?;
    assert!(reopened.point_named(&hot, DAY + 10)?.is_none());
    reopened.close()
}

#[test]
fn numeric_writes_honor_registered_policy_lateness() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(1_000));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            clock,
            ..OpenOptions::default()
        },
    )?;
    let space = storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("realtime", 100, 1_000)?.with_max_lateness_us(Some(100))?,
    )?;
    let bucket = storage.create_bucket("production", "live", policy.id)?;
    let series = SeriesKey::new(space.id, bucket.id, 7);

    assert!(matches!(
        storage.write_batch(vec![point(series, 899, 1.0)], Durability::Sync),
        Err(Error::ExpiredPoint(_))
    ));
    assert!(storage.point(series, 899)?.is_none());
    storage.close()
}

#[test]
fn catalog_bucket_rejects_a_numeric_namespace_that_already_contains_data() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            ..OpenOptions::default()
        },
    )?;
    let raw_series = SeriesKey::new(1, 1, 7);
    storage.write_batch(vec![point(raw_series, 1, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.create_space("production")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("daily", 100, DAY)?)?;

    assert!(matches!(
        storage.create_bucket("production", "measurements", policy.id),
        Err(Error::Catalog(message)) if message.contains("numeric series namespace")
    ));
    let report = Storage::verify(temporary.path());
    assert!(report.valid, "{:?}", report.issues);
    assert_eq!(
        storage.point(raw_series, 1)?.map(|point| point.value),
        Some(1.0)
    );
    storage.close()
}

#[test]
fn hot_span_selects_policy_compression_and_compaction_cools_windows() -> Result<()> {
    for (cold_profile, cold_signature) in [
        (CompressionProfile::balanced(), [2, 1, 1, 4]),
        (CompressionProfile::zstd(), [2, 1, 2, 4]),
    ] {
        let temporary = TempDir::new()?;
        let clock = Arc::new(ManualClock::new(10 * DAY));
        let storage = Storage::open(
            temporary.path(),
            OpenOptions {
                clock: clock.clone(),
                ..OpenOptions::default()
            },
        )?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("tiered", 2 * DAY, DAY)?
                .with_compression(CompressionProfile::raw(), cold_profile)?
                .with_write_mode(WriteMode::Mutable),
        )?;
        storage.create_bucket("production", "measurements", policy.id)?;
        let series = SeriesRef::new("production", "measurements", 1);
        storage.write_named_operations(
            vec![
                NamedPoint::new(series.clone(), DAY + 1, 1.0)?.into(),
                NamedPoint::new(series.clone(), 9 * DAY + 1, 2.0)?.into(),
            ],
            Durability::Sync,
        )?;
        storage.checkpoint()?;

        let codecs = segment_block_codecs(temporary.path(), policy.id)?;
        assert_eq!(codecs.get(&DAY), Some(&cold_signature));
        assert_eq!(codecs.get(&(9 * DAY)), Some(&[0, 0, 0, 0]));
        assert!(storage.compact()? > 0);
        let first_l1 = segment_block_codecs(temporary.path(), policy.id)?;
        assert_eq!(first_l1.get(&DAY), Some(&cold_signature));
        assert_eq!(first_l1.get(&(9 * DAY)), Some(&[0, 0, 0, 0]));

        clock.set(20 * DAY);
        assert!(storage.compact()? > 0);
        let cooled = segment_block_codecs(temporary.path(), policy.id)?;
        assert_eq!(cooled.len(), 2);
        assert!(cooled.values().all(|codecs| codecs == &cold_signature));
        storage.close()?;
    }
    Ok(())
}

#[test]
fn invalid_zstd_level_is_rejected_before_catalog_publication_or_write() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    storage.create_space("production")?;
    let unsupported = CompressionProfile {
        timestamp_codec: TimestampCodec::DeltaOfDelta,
        value_codec: ValueCodec::GorillaXor,
        block_codec: BlockCodec::Zstd,
        block_level: Some(1),
    };
    let mut rejected = StoragePolicyOptions::new("measurements", MINUTE, DAY)?;
    rejected.hot_compression = unsupported;
    assert!(matches!(
        storage.create_policy(rejected),
        Err(Error::Invalid(message))
            if message.contains("zstd block level")
    ));

    let policy = storage.create_policy(StoragePolicyOptions::new("measurements", MINUTE, DAY)?)?;
    storage.create_bucket("production", "measurements", policy.id)?;
    let series = storage
        .resolve_series(&SeriesRef::new("production", "measurements", 1))?
        .0;
    storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    Ok(())
}
