use super::*;

#[test]
fn offline_pack_directory_bypasses_segment_indexes_and_rejects_stale_writes() -> Result<()> {
    let temp = TempDir::new()?;
    let root = temp.path().join("storage");
    let directory = temp.path().join("pack-directory.bin");
    let storage = Storage::open(
        &root,
        OpenOptions {
            l0_compaction_trigger: 16,
            l0_slowdown_trigger: 32,
            l0_stop_trigger: 64,
            ..OpenOptions::default()
        },
    )?;
    let space = storage.create_space("space")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("strict", DAY * 30, DAY)?)?;
    let bucket = storage.create_bucket("space", "bucket", policy.id)?;
    let series = (0..200_u64)
        .map(|key| SeriesKey::new(space.id, bucket.id, key))
        .collect::<Vec<_>>();
    for batch in 0..4_i64 {
        let points = series
            .iter()
            .flat_map(|series| {
                (0..16_i64).map(move |offset| {
                    point(
                        *series,
                        batch * 16 * MINUTE + offset * MINUTE,
                        (series.key + 1) as f64,
                    )
                })
            })
            .collect::<Vec<_>>();
        storage.write_batch(points, Durability::Sync)?;
        storage.flush()?;
    }
    let request = GroupRequest::new(series.clone(), 0, 64 * MINUTE)?.with_source(GroupSource::Raw);
    let baseline = storage.group(request.clone())?;
    let info = storage.build_offline_pack_directory(&directory)?;
    assert!(info.block_locators > 0);
    assert!(info.pages > 0);
    assert!(info.pages < series.len());
    storage.close()?;

    let query_only = Storage::open(
        &root,
        OpenOptions {
            create_if_missing: false,
            offline_pack_directory: Some(directory.clone()),
            ..OpenOptions::default()
        },
    )?;
    let indexed = query_only.group(request)?;
    assert_eq!(indexed.rows, baseline.rows);
    assert!(indexed.stats.pack_directory_roots_consulted > 0);
    assert!(indexed.stats.pack_directory_pages_read > 0);
    assert_eq!(
        indexed.stats.pack_directory_pages_read,
        u64::try_from(info.pages).unwrap_or(u64::MAX)
    );
    assert_eq!(indexed.stats.pack_directory_page_cache_hits, 0);
    assert!(indexed.stats.pack_directory_extents_returned > 0);
    assert_eq!(indexed.stats.index_roots_consulted, 0);
    assert!(matches!(
        query_only.write_batch(vec![point(series[0], 65 * MINUTE, 1.0)], Durability::Sync),
        Err(Error::Invalid(_))
    ));
    query_only.close()?;

    let corrupted_directory = temp.path().join("pack-directory-corrupt.bin");
    let mut bytes = fs::read(&directory)?;
    let last = bytes
        .last_mut()
        .expect("offline directory contains at least one page");
    *last ^= 0x80;
    fs::write(&corrupted_directory, bytes)?;
    let corrupted = Storage::open(
        &root,
        OpenOptions {
            create_if_missing: false,
            offline_pack_directory: Some(corrupted_directory),
            ..OpenOptions::default()
        },
    )?;
    let corrupt_request =
        GroupRequest::new(series.clone(), 0, 64 * MINUTE)?.with_source(GroupSource::Raw);
    assert!(matches!(
        corrupted.group(corrupt_request),
        Err(Error::Corruption(_))
    ));
    corrupted.close()?;

    let storage = Storage::open(
        &root,
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    storage.write_batch(vec![point(series[0], 65 * MINUTE, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;
    assert!(matches!(
        Storage::open(
            &root,
            OpenOptions {
                create_if_missing: false,
                offline_pack_directory: Some(directory),
                ..OpenOptions::default()
            }
        ),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[test]
fn published_segment_index_tracks_flush_compaction_and_reopen() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 1_000,
        history_seal_lag: None,
        wal_checkpoint_bytes: u64::MAX,
        checkpoint_interval: Duration::from_secs(86_400),
        cold_flush_idle_interval: Duration::from_secs(86_400),
        ..OpenOptions::default()
    };
    let series = SeriesKey::new(1, 1, 71);
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.write_batch(
        vec![
            point(series, 1, 1.0),
            point(series, 1_001, 2.0),
            point(series, 2_001, 3.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;

    // The second L0 overlaps the already published interval and represents a
    // late correction. Publishing the new index generation must retain both.
    storage.write_batch(vec![point(series, 1_001, 20.0)], Durability::Sync)?;
    storage.checkpoint()?;
    assert_eq!(
        storage.point(series, 1_001)?.map(|point| point.value),
        Some(20.0)
    );
    assert_eq!(
        storage
            .scan(series, 0, 3_000)?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(1, 1.0), (1_001, 20.0), (2_001, 3.0)]
    );
    let stats = storage.stats()?;
    assert_eq!(stats.logical_segments, 4);
    assert!(stats.query_segment_index_lookups >= 2);
    assert!(stats.query_segment_index_entries_examined >= 3);
    assert!(stats.query_segment_index_candidates >= 3);

    storage.compact()?;
    assert_eq!(
        storage.point(series, 1_001)?.map(|point| point.value),
        Some(20.0)
    );
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        reopened.point(series, 1_001)?.map(|point| point.value),
        Some(20.0)
    );
    assert_eq!(reopened.scan(series, 3_000, 4_000)?.count(), 0);
    reopened.close()
}

#[test]
fn published_segment_index_filters_disjoint_series_before_metadata_reads() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 10_000,
        history_seal_lag: None,
        l0_compaction_trigger: 64,
        l0_slowdown_trigger: 96,
        l0_stop_trigger: 128,
        wal_checkpoint_bytes: u64::MAX,
        checkpoint_interval: Duration::from_secs(86_400),
        cold_flush_idle_interval: Duration::from_secs(86_400),
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = (0..32)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    for (key, series) in series.iter().copied().enumerate() {
        storage.write_batch(vec![point(series, 100, key as f64)], Durability::Sync)?;
        storage.checkpoint()?;
    }
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let before = reopened.stats()?;
    assert_eq!(
        reopened.point(series[0], 100)?.map(|point| point.value),
        Some(0.0)
    );
    let after = reopened.stats()?;
    let examined = after
        .query_segment_index_entries_examined
        .saturating_sub(before.query_segment_index_entries_examined);
    let candidates = after
        .query_segment_index_candidates
        .saturating_sub(before.query_segment_index_candidates);
    assert_eq!(examined, 32);
    assert!(
        candidates <= 2,
        "unexpected Bloom false positives: {candidates}"
    );
    assert!(before.pinned_metadata_cache_bytes > 0);
    reopened.close()
}

#[test]
fn strict_sealed_l2_publishes_immutable_epoch_directories_and_uses_them_after_restart() -> Result<()>
{
    let parent = TempDir::new()?;
    let root = parent.path().join("storage");
    let snapshot_root = parent.path().join("snapshot");
    let restored_root = parent.path().join("restored");
    let clock = Arc::new(ManualClock::new(100 * DAY));
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: DAY,
        history_seal_lag: Some(Duration::from_secs(60 * 60)),
        history_window: Duration::from_secs(24 * 60 * 60),
        clock,
        ..OpenOptions::default()
    };
    let storage = Storage::open(&root, options.clone())?;
    let space = storage.create_space("production")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("strict", MINUTE, DAY)?)?;
    let bucket = storage.create_bucket("production", "points", policy.id)?;
    let series = SeriesKey::new(space.id, bucket.id, 7);
    storage.write_batch(
        vec![
            point(series, DAY + 1, 1.0),
            point(series, 69 * DAY + 1, 2.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let directories = manifest["published_read_directories"]
        .as_array()
        .expect("published read directories");
    assert_eq!(directories.len(), 1);
    let directory = directories[0]
        .as_object()
        .expect("published read directory");
    assert_eq!(directory["segment_count"].as_u64(), Some(1));
    assert_eq!(manifest["segments"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        manifest["published_read_payload_files"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    let relative_path = directory["relative_path"]
        .as_str()
        .expect("directory path")
        .to_owned();
    assert!(root.join(&relative_path).is_file());
    let directory_stats = storage.stats()?;
    assert!(
        directory_stats
            .published_read_directory_generation_id
            .is_some()
    );
    assert_eq!(directory_stats.published_read_directory_generations, 1);
    assert_eq!(directory_stats.published_read_directory_segments, 1);
    assert!(directory_stats.published_read_directory_pages >= 1);
    assert_eq!(
        directory_stats.published_read_directory_bytes,
        fs::metadata(root.join(&relative_path))?.len()
    );

    let raw = storage.group(
        GroupRequest::new(vec![series], DAY, 2 * DAY)?
            .with_source(GroupSource::Raw)
            .with_aggregates(AggregateSet::ALL),
    )?;
    assert_eq!(raw.rows.len(), 1);
    assert_eq!(raw.rows[0].count, 1);
    assert_eq!(raw.stats.pack_directory_roots_consulted, 1);
    assert_eq!(raw.stats.index_roots_consulted, 0);
    assert!(raw.stats.pack_directory_extents_returned >= 1);
    assert!(Storage::verify(&root).valid);

    storage.snapshot(&snapshot_root)?;
    assert!(snapshot_root.join(&relative_path).is_file());
    assert!(Storage::verify(&snapshot_root).valid);
    let restored = Storage::restore_snapshot(&snapshot_root, &restored_root)?;
    assert_eq!(restored.logical_segments, 2);
    assert!(Storage::verify(&restored_root).valid);
    let snapshot_directory = snapshot_root.join(&relative_path);
    let mut directory_bytes = fs::read(&snapshot_directory)?;
    let last = directory_bytes
        .last_mut()
        .ok_or_else(|| Error::Invalid("empty published directory".into()))?;
    *last ^= 0x01;
    fs::write(&snapshot_directory, directory_bytes)?;
    assert!(!Storage::verify(&snapshot_root).valid);

    let old_stream = storage.group_stream(
        GroupRequest::new(vec![series], DAY, 2 * DAY)?
            .with_source(GroupSource::Raw)
            .with_aggregates(AggregateSet::ALL),
    )?;
    let old_directory_bytes = fs::read(root.join(&relative_path))?;
    storage.write_batch(vec![point(series, 99 * DAY, 3.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    let next_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let next_directories = next_manifest["published_read_directories"]
        .as_array()
        .expect("next published read directories");
    assert_eq!(next_directories.len(), 2);
    assert_eq!(next_directories[0]["relative_path"], relative_path);
    let next_relative_path = next_directories[1]["relative_path"]
        .as_str()
        .expect("next directory path");
    assert_ne!(next_relative_path, relative_path);
    assert!(root.join(&relative_path).is_file());
    assert_eq!(fs::read(root.join(&relative_path))?, old_directory_bytes);
    assert_eq!(old_stream.collect::<Result<Vec<_>>>()?[0].count, 1);
    storage.close()?;

    let reopened = Storage::open(
        &root,
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let raw = reopened.group(
        GroupRequest::new(vec![series], DAY, 2 * DAY)?
            .with_source(GroupSource::Raw)
            .with_aggregates(AggregateSet::ALL),
    )?;
    assert_eq!(raw.rows[0].count, 1);
    assert_eq!(raw.stats.pack_directory_roots_consulted, 1);
    assert_eq!(raw.stats.index_roots_consulted, 0);
    let reopened_stats = reopened.stats()?;
    assert_eq!(reopened_stats.published_read_directory_generations, 2);
    assert_eq!(
        reopened_stats.recovery_read_directory_pages as usize,
        reopened_stats.published_read_directory_pages
    );
    assert!(reopened_stats.recovery_read_directory_bytes > 0);
    reopened.close()
}

#[test]
fn strict_sealed_projection_matches_raw_without_reading_raw_blocks() -> Result<()> {
    let temporary = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(70 * DAY));
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: DAY,
        history_seal_lag: Some(Duration::from_secs(60 * 60)),
        history_window: Duration::from_secs(24 * 60 * 60),
        clock,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let space = storage.create_space("production")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("strict", MINUTE, DAY)?)?;
    let bucket = storage.create_bucket("production", "dense", policy.id)?;
    let series = SeriesKey::new(space.id, bucket.id, 19);
    let mut points = (0..5_000_i64)
        .map(|offset| point(series, DAY + offset, offset as f64 * 0.25 - 10.0))
        .collect::<Vec<_>>();
    points.push(point(series, 69 * DAY + 1, 9.0));
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    let request = || {
        GroupRequest::new(vec![series], DAY, 2 * DAY)
            .map(|request| request.with_aggregates(AggregateSet::ALL))
    };
    let projected = storage.group(request()?)?;
    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(projected.rows.len(), 1);
    assert_extended_close(&projected.rows[0], &raw.rows[0]);
    assert_eq!(projected.stats.blocks_read, 0);
    assert_eq!(projected.stats.operation_bytes_decoded, 0);
    assert!(projected.stats.aggregate_records_read > 0);
    assert!(raw.stats.blocks_read > 0);
    assert!(
        storage
            .stats()?
            .published_read_directory_generation_id
            .is_some()
    );
    assert!(Storage::verify(temporary.path()).valid);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let after_restart = reopened.group(request()?)?;
    assert_eq!(after_restart.rows.len(), 1);
    assert_extended_close(&after_restart.rows[0], &raw.rows[0]);
    assert_eq!(after_restart.stats.blocks_read, 0);
    assert!(after_restart.stats.aggregate_records_read > 0);
    reopened.close()
}
