use super::*;

#[test]
fn retention_and_lateness_use_a_snapshot_cutoff_persisted_at_checkpoint() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(1_000));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            clock: clock.clone(),
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("production")?;
    let retention = storage.create_policy(
        StoragePolicyOptions::new("retained", 100, 1_000)?.with_retention_us(Some(1_000))?,
    )?;
    let lateness = storage.create_policy(
        StoragePolicyOptions::new("realtime", 100, 1_000)?.with_max_lateness_us(Some(100))?,
    )?;
    storage.create_bucket("production", "history", retention.id)?;
    storage.create_bucket("production", "live", lateness.id)?;
    let history = SeriesRef::new("production", "history", 1);
    let live = SeriesRef::new("production", "live", 1);

    storage.write_named_operations(
        vec![
            NamedPoint::new(history.clone(), 0, 1.0)?.into(),
            NamedPoint::new(history.clone(), 500, 2.0)?.into(),
            NamedPoint::new(history.clone(), 1_000, 3.0)?.into(),
        ],
        Durability::Sync,
    )?;
    storage.upsert(live.clone(), 900, 4.0, Durability::Sync)?;
    assert!(matches!(
        storage.upsert(live.clone(), 899, 5.0, Durability::Sync),
        Err(Error::ExpiredPoint(_))
    ));
    let old_snapshot = storage.scan_named(&history, -1, 2_000)?;

    clock.set(1_600);
    let current = storage
        .scan_named(&history, -1, 2_000)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        current
            .iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [1_000]
    );
    assert!(storage.point_named(&history, 500)?.is_none());
    let key = storage.resolve_series(&history)?.0;
    let grouped = storage
        .group_stream(GroupRequest::new(vec![key], -1, 2_000)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(grouped.iter().map(|row| row.count).sum::<u64>(), 1);
    assert_eq!(
        old_snapshot
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [0, 500, 1_000]
    );
    assert_eq!(
        retention.temperature(1_550, 1_600, Some(600)),
        DataTemperature::Hot
    );
    assert_eq!(
        retention.temperature(1_000, 1_600, Some(600)),
        DataTemperature::Cold
    );
    assert_eq!(
        retention.temperature(500, 1_600, Some(600)),
        DataTemperature::Expired
    );
    storage.checkpoint()?;
    storage.close()?;

    // A wall clock rollback must not resurrect data hidden by a durable cutoff.
    clock.set(1_000);
    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            clock: clock.clone(),
            ..OpenOptions::default()
        },
    )?;
    assert!(reopened.point_named(&history, 500)?.is_none());
    assert_eq!(reopened.scan_named(&history, -1, 2_000)?.count(), 1);
    assert!(matches!(
        reopened.upsert(history.clone(), 599, 7.0, Durability::Sync),
        Err(Error::ExpiredPoint(_))
    ));
    reopened.close()
}

#[test]
fn retention_packs_do_not_mix_policies_with_different_expiry() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(2_000));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: 1_000,
            history_seal_lag: Some(Duration::from_micros(100)),
            history_window: Duration::from_micros(100),
            clock: clock.clone(),
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("production")?;
    let short_policy = storage.create_policy(
        StoragePolicyOptions::new("short", 100, 1_000)?
            .with_retention_us(Some(1_000))?
            .with_write_mode(WriteMode::Mutable),
    )?;
    let long_policy = storage.create_policy(
        StoragePolicyOptions::new("long", 100, 1_000)?
            .with_retention_us(Some(10_000))?
            .with_write_mode(WriteMode::Mutable),
    )?;
    storage.create_bucket("production", "short", short_policy.id)?;
    storage.create_bucket("production", "long", long_policy.id)?;
    let short = SeriesRef::new("production", "short", 1);
    let long = SeriesRef::new("production", "long", 1);
    storage.write_named_operations(
        vec![
            NamedPoint::new(short.clone(), 1_500, 1.0)?.into(),
            NamedPoint::new(short.clone(), 2_000, 2.0)?.into(),
            NamedPoint::new(long.clone(), 1_500, 3.0)?.into(),
            NamedPoint::new(long.clone(), 2_000, 4.0)?.into(),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let records = manifest_segment_records(temporary.path())?;
    let short_history = records
        .iter()
        .find(|segment| segment.policy_id == short_policy.id && segment.level == 2)
        .expect("short history");
    let long_history = records
        .iter()
        .find(|segment| segment.policy_id == long_policy.id && segment.level == 2)
        .expect("long history");
    assert_ne!(short_history.relative_path, long_history.relative_path);
    let short_path = temporary.path().join(&short_history.relative_path);
    let long_path = temporary.path().join(&long_history.relative_path);
    assert!(short_path.exists());
    assert!(long_path.exists());

    clock.set(2_600);
    storage.compact()?;
    assert!(storage.point_named(&short, 1_500)?.is_none());
    assert_eq!(
        storage.point_named(&long, 1_500)?.map(|point| point.value),
        Some(3.0)
    );
    wait_for_removed_paths(std::slice::from_ref(&short_path))?;
    assert!(long_path.exists());
    assert!(storage.stats()?.manifest_expired_segments >= 1);
    storage.close()
}

#[test]
fn compaction_physically_applies_retention_without_breaking_old_snapshots() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(0));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            clock: clock.clone(),
            virtual_shards: 1,
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("retained", 100, 2_000)?.with_retention_us(Some(1_000))?,
    )?;
    storage.create_bucket("production", "measurements", policy.id)?;
    let series = SeriesRef::new("production", "measurements", 7);
    storage.write_named_operations(
        vec![
            NamedPoint::new(series.clone(), -900, 1.0)?.into(),
            NamedPoint::new(series.clone(), 100, 2.0)?.into(),
            NamedPoint::new(series.clone(), 900, 3.0)?.into(),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    assert!(storage.compact()? > 0);

    let old_snapshot = storage.scan_named(&series, -2_000, 2_000)?;
    let old_paths = manifest_segment_paths(temporary.path())?;
    assert_eq!(old_paths.len(), 1, "both domains share one physical pack");

    clock.set(1_600);
    assert!(storage.compact()? > 0);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let segments = manifest["segments"].as_array().expect("manifest segments");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0]["record_count"], 1);
    assert_eq!(segments[0]["min_timestamp_us"], 900);
    assert_eq!(segments[0]["max_timestamp_us"], 900);
    assert!(old_paths.iter().all(|path| path.exists()));

    let current = storage
        .scan_named(&series, -2_000, 2_000)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        current
            .iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [900]
    );
    let old = old_snapshot.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        old.iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [-900, 100, 900]
    );
    wait_for_removed_paths(&old_paths)?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            clock,
            virtual_shards: 1,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reopened.scan_named(&series, -2_000, 2_000)?.count(), 1);
    reopened.close()
}

#[test]
fn aggregate_retention_archives_before_raw_deletion_and_survives_restart() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(DAY));
    let options = OpenOptions {
        clock: clock.clone(),
        virtual_shards: 1,
        partition_span_us: DAY,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("rollups", MINUTE, DAY)?
            .with_retention_us(Some(2 * DAY))?
            // This fixture advances an artificial event clock independently of
            // the write clock; model that source as externally watermarked.
            .with_max_future_skew_us(None)?
            .with_aggregate_retention_us(
                Some(5 * DAY),
                vec![AggregateResolution::Minute, AggregateResolution::Day],
            )?,
    )?;
    storage.create_bucket("production", "measurements", policy.id)?;
    let named = SeriesRef::new("production", "measurements", 7);
    let series = storage.resolve_series(&named)?.0;
    let mut points = Vec::new();
    for day in 0..3_i64 {
        for minute in 0..100_i64 {
            points.push(NamedPoint::new(
                named.clone(),
                day * DAY + minute * MINUTE,
                (day * 100 + minute) as f64,
            )?);
        }
    }
    storage.write_named_operations(
        points.into_iter().map(Into::into).collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let old_snapshot = storage.group_stream(
        GroupRequest::new(vec![series], 0, 3 * DAY)?.with_aggregates(AggregateSet::ALL),
    )?;
    clock.set(4 * DAY);
    storage.compact()?;

    let raw = storage
        .scan(series, 0, 3 * DAY)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(raw.len(), 100);
    assert!(raw.iter().all(|point| point.timestamp_us >= 2 * DAY));
    storage.upsert(
        named.clone(),
        2 * DAY + 120 * MINUTE,
        777.0,
        Durability::Sync,
    )?;
    let raw_group = storage
        .group_stream(
            GroupRequest::new(vec![series], 0, 3 * DAY)?
                .with_source(GroupSource::Raw)
                .with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(raw_group.len(), 1);
    assert_eq!(raw_group[0].count, 101);

    let archived = storage
        .group_stream(
            GroupRequest::new(vec![series], 0, 3 * DAY)?.with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(archived.len(), 3);
    assert_eq!(archived.iter().map(|row| row.count).sum::<u64>(), 301);
    assert_eq!(
        archived[0].source_resolution,
        Some(AggregateResolution::Day)
    );
    assert_eq!(old_snapshot.collect::<Result<Vec<_>>>()?.len(), 3);
    assert!(matches!(
        storage.upsert(named.clone(), MINUTE, 999.0, Durability::Sync),
        Err(Error::ExpiredPoint(_))
    ));

    let info = storage.series_info(series)?;
    assert_eq!(info.retention.raw_available_from_us, Some(2 * DAY));
    assert_eq!(info.retention.aggregate_available_from_us, Some(-DAY));
    assert_eq!(
        info.retention.aggregate_resolutions,
        [AggregateResolution::Minute, AggregateResolution::Day]
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    assert_eq!(
        manifest["aggregate_archives"].as_array().map(Vec::len),
        Some(2)
    );
    storage.checkpoint()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let after_restart = reopened
        .group_stream(
            GroupRequest::new(vec![series], 0, 3 * DAY)?.with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(after_restart.len(), 3);
    for (actual, expected) in after_restart.iter().zip(&archived) {
        assert_extended_close(actual, expected);
    }
    clock.set(7 * DAY);
    reopened.compact()?;
    let after_aggregate_expiry = reopened
        .group_stream(
            GroupRequest::new(vec![series], 0, 3 * DAY)?.with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(after_aggregate_expiry.len(), 1);
    assert_eq!(after_aggregate_expiry[0].bucket_start_us, 2 * DAY);
    assert_eq!(after_aggregate_expiry[0].count, 101);
    reopened.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let archive_path = temporary.path().join(
        manifest["aggregate_archives"][0]["relative_path"]
            .as_str()
            .expect("aggregate archive path"),
    );
    let mut archive = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(archive_path)?;
    let last = archive.metadata()?.len() - 1;
    archive.seek(SeekFrom::Start(last))?;
    let mut byte = [0_u8; 1];
    archive.read_exact(&mut byte)?;
    archive.seek(SeekFrom::Start(last))?;
    archive.write_all(&[byte[0] ^ 1])?;
    archive.sync_all()?;
    let invalid = Storage::verify(temporary.path());
    assert!(!invalid.valid);
    assert!(invalid.issues.iter().any(|issue| {
        issue.message.contains("aggregate archive checksum")
            || issue.message.contains("aggregate record page CRC32C")
    }));
    Ok(())
}

#[test]
fn retained_aggregates_publish_multiple_bounded_chunks_atomically() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(DAY));
    let options = OpenOptions {
        clock: clock.clone(),
        virtual_shards: 1,
        partition_span_us: DAY,
        aggregate_archive_chunk_bytes: 512,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("chunked-rollups", MINUTE, DAY)?
            .with_retention_us(Some(DAY))?
            .with_aggregate_retention_us(
                Some(4 * DAY),
                vec![AggregateResolution::Minute, AggregateResolution::Day],
            )?,
    )?;
    storage.create_bucket("production", "chunked", policy.id)?;
    let mut series = Vec::new();
    let mut points = Vec::new();
    for key in 0..4_u64 {
        let named = SeriesRef::new("production", "chunked", key);
        series.push(storage.resolve_series(&named)?.0);
        for minute in 0..20_i64 {
            points.push(NamedPoint::new(
                named.clone(),
                minute * MINUTE,
                (key as i64 * 100 + minute) as f64,
            )?);
        }
    }
    storage.write_named_operations(
        points.into_iter().map(Into::into).collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    clock.set(2 * DAY);
    storage.compact()?;

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let archives = manifest["aggregate_archives"]
        .as_array()
        .expect("aggregate archive descriptors");
    assert!(archives.len() > 1);
    assert!(archives.iter().all(|archive| {
        archive["file_size"]
            .as_u64()
            .is_some_and(|bytes| bytes <= 96 + 512)
            && archive["chunk_count"].as_u64() == Some(archives.len() as u64)
    }));
    for (chunk_index, archive) in archives.iter().enumerate() {
        assert_eq!(archive["chunk_index"].as_u64(), Some(chunk_index as u64));
    }
    for series in series {
        assert_eq!(storage.scan(series, 0, DAY)?.count(), 0);
        let grouped = storage
            .group(GroupRequest::new(vec![series], 0, DAY)?.with_aggregates(AggregateSet::ALL))?;
        assert_eq!(grouped.rows.len(), 1);
        assert_eq!(grouped.rows[0].count, 20);
        assert_eq!(
            grouped.rows[0].source_resolution,
            Some(AggregateResolution::Day)
        );
    }
    let verified = Storage::verify(temporary.path());
    assert!(verified.valid, "{:?}", verified.issues);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert!(Storage::verify(temporary.path()).valid);
    reopened.close()
}

#[test]
fn aggregate_archive_build_resumes_after_a_cutoff_restart() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(0));
    let options = OpenOptions {
        clock: clock.clone(),
        virtual_shards: 1,
        partition_span_us: DAY,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("resume", MINUTE, DAY)?
            .with_retention_us(Some(2 * DAY))?
            .with_aggregate_retention_us(Some(5 * DAY), vec![AggregateResolution::Day])?,
    )?;
    storage.create_bucket("production", "measurements", policy.id)?;
    let named = SeriesRef::new("production", "measurements", 8);
    storage.upsert(named.clone(), MINUTE, 5.0, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    clock.set(3 * DAY);
    assert_eq!(
        storage.retention_info(policy.id)?.raw_available_from_us,
        Some(DAY)
    );
    storage.checkpoint()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    reopened.compact()?;
    let series = reopened.resolve_series(&named)?.0;
    assert!(reopened.scan(series, 0, DAY)?.next().is_none());
    let rows = reopened
        .group_stream(GroupRequest::new(vec![series], 0, DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 1);
    assert_eq!(rows[0].sum, 5.0);
    assert_eq!(rows[0].source_resolution, Some(AggregateResolution::Day));
    reopened.close()
}
