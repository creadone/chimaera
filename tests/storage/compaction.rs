use super::*;

#[test]
fn compaction_publication_has_no_gaps_or_duplicates_for_active_reader() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        memtable_max_bytes: 64 * 1024,
        l0_compaction_trigger: 8,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options)?;
    let series = SeriesKey::new(3, 4, 5);
    for batch in 0_i32..4 {
        storage.write_batch(
            (0..5_000)
                .map(|offset| {
                    let timestamp = i64::from(batch) * 10 * MINUTE + i64::from(offset);
                    point(series, timestamp, f64::from(batch + 1))
                })
                .collect(),
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    let before = storage.group_stream(GroupRequest::new(vec![series], 0, DAY)?)?;
    let gate = Arc::new(Barrier::new(2));
    let compacting_storage = storage.clone();
    let compacting_gate = Arc::clone(&gate);
    let compacting = thread::spawn(move || {
        compacting_gate.wait();
        compacting_storage.compact()
    });
    gate.wait();
    let during = before.collect::<Result<Vec<_>>>()?;
    compacting.join().expect("compaction thread")?;
    let after = rows(&storage, vec![series])?;
    assert_eq!(during, after);
    assert_eq!(after[0].count, 20_000);
    assert_close(after[0].sum, 50_000.0);
    let stats = storage.stats()?;
    assert_eq!(stats.l0_segments, 0);
    assert_eq!(stats.l1_segments, 1);
    storage.close()
}

#[test]
fn compaction_splits_a_wide_l0_on_stable_time_boundaries() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            l0_compaction_trigger: 8,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 71);
    let mut points = Vec::with_capacity(3 * 4_096);
    for slice in 0_i64..3 {
        points
            .extend((1_i64..=4_096).map(|offset| point(series, slice * 60 * MINUTE + offset, 1.0)));
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    assert_eq!(storage.stats()?.l0_segments, 2);
    assert_eq!(storage.stats()?.l0_debt, 2);
    assert!(storage.compact()? > 0);
    let initially_compacted = storage.stats()?;
    assert_eq!(initially_compacted.l1_segments, 1);
    assert_eq!(initially_compacted.sealed_l2_segments, 1);
    storage.write_batch(
        vec![point(series, 60 * MINUTE + 2_048, 2.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    assert!(storage.compact()? > 0);

    let stats = storage.stats()?;
    assert_eq!(stats.l0_segments, 0);
    assert_eq!(stats.l1_segments, 2);
    assert_eq!(stats.sealed_l2_segments, 1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let ranges = manifest["segments"]
        .as_array()
        .expect("manifest segments")
        .iter()
        .map(|segment| {
            (
                segment["min_timestamp_us"].as_i64().expect("minimum"),
                segment["max_timestamp_us"].as_i64().expect("maximum"),
            )
        })
        .collect::<Vec<_>>();
    let mut ranges = ranges;
    ranges.sort_unstable();
    assert_eq!(
        ranges,
        [
            (1, 4_096),
            (60 * MINUTE + 1, 120 * MINUTE + 4_096),
            (60 * MINUTE + 2_048, 60 * MINUTE + 2_048),
        ]
    );
    let before_query = storage.stats()?;
    let grouped = storage
        .group_stream(GroupRequest::new(vec![series], 0, DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    let after_query = storage.stats()?;
    let raw =
        storage.group(GroupRequest::new(vec![series], 0, DAY)?.with_source(GroupSource::Raw))?;
    assert_eq!(grouped[0].count, 3 * 4_096);
    assert_close(grouped[0].sum, f64::from(3 * 4_096) + 1.0);
    assert_eq!(grouped, raw.rows);
    let automatic_operations =
        after_query.query_operations_scanned - before_query.query_operations_scanned;
    assert!(automatic_operations > 2 * 4_096);
    assert!(automatic_operations < raw.stats.operations_scanned);
    assert_eq!(
        after_query.query_aggregate_records_read - before_query.query_aggregate_records_read,
        1
    );
    storage.close()
}

#[test]
fn active_delta_compaction_does_not_revisit_the_large_base_run() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            l0_compaction_trigger: 8,
            ..OpenOptions::default()
        },
    )?;
    let mut points = Vec::with_capacity(128 * 3);
    for key in 0_u64..128 {
        let series = SeriesKey::new(1, 1, key);
        points.extend([
            point(series, 1, 1.0),
            point(series, 60 * MINUTE + 1, 2.0),
            point(series, 120 * MINUTE + 1, 3.0),
        ]);
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    storage.write_batch(
        vec![point(SeriesKey::new(1, 1, 64), 60 * MINUTE + 1, 4.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    let before = storage.stats()?;
    storage.compact()?;
    let after = storage.stats()?;

    let visits = after.compaction_block_visits - before.compaction_block_visits;
    let unique = after.compaction_unique_blocks - before.compaction_unique_blocks;
    let physical_reads =
        after.compaction_physical_blocks_read - before.compaction_physical_blocks_read;
    let unique_bytes = after.compaction_unique_block_bytes - before.compaction_unique_block_bytes;
    let physical_bytes =
        after.compaction_physical_bytes_read - before.compaction_physical_bytes_read;
    let decoded_bytes =
        after.compaction_decoded_input_bytes - before.compaction_decoded_input_bytes;
    assert!(unique > 0);
    assert_eq!(visits, unique);
    assert_eq!(physical_reads, visits);
    assert_eq!(physical_bytes, unique_bytes);
    assert_eq!(decoded_bytes, physical_bytes);
    assert!(after.compaction_decode_seconds > before.compaction_decode_seconds);
    storage.close()
}

#[test]
fn active_l1_uses_bounded_deltas_instead_of_rewriting_the_base_per_flush() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            history_seal_lag: None,
            l0_compaction_trigger: 2,
            l0_slowdown_trigger: 16,
            l0_stop_trigger: 32,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 711);
    storage.write_batch(
        (0..50_000)
            .map(|timestamp_us| point(series, timestamp_us, timestamp_us as f64 * 0.25))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    for timestamp_us in 50_000..50_016 {
        storage.write_batch(
            vec![point(series, timestamp_us, timestamp_us as f64)],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while storage.stats()?.l0_segments != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    let stats = storage.stats()?;
    assert_eq!(stats.l0_segments, 0);
    assert!((2..ACTIVE_L1_TEST_MAX_RUNS).contains(&stats.active_l1_segments));
    assert!(stats.active_max_rewrite_count <= 3);
    assert!(stats.active_compaction_jobs > 0);
    assert!(stats.active_compaction_max_input_bytes <= 64 * 1024 * 1024);
    assert!(stats.active_compaction_max_output_bytes <= 64 * 1024 * 1024);
    assert!(stats.active_compaction_max_fan_in <= 16);
    assert!(stats.manifest_bytes > 0);
    assert_eq!(
        stats.manifest_bytes + stats.manifest_headroom_bytes,
        stats.manifest_limit_bytes
    );
    assert_eq!(stats.manifest_segment_descriptors, stats.logical_segments);
    assert!(stats.manifest_publications > 0);
    assert_eq!(
        storage
            .scan(series, 0, DAY)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        50_016
    );
    storage.close()
}

#[test]
fn active_compaction_coalesces_disjoint_flushes_without_changing_snapshots() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        history_seal_lag: None,
        l0_compaction_trigger: 8,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 712);
    for generation in 0..4 {
        storage.write_batch(
            vec![point(series, generation * 100 + 1, 1.0)],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    assert_eq!(storage.stats()?.l0_segments, 4);
    let old_snapshot = storage.scan(series, 0, 1_000)?;
    storage.compact()?;
    assert_eq!(storage.stats()?.l1_segments, 1);
    assert!(storage.scan(series, 2, 100)?.next().is_none());

    // Each forced flush creates a separate L1 run. The size/count policy must
    // eventually consolidate these disjoint runs as well as overlapping ones.
    for generation in 4..16 {
        storage.write_batch(
            vec![point(series, generation * 100 + 1, 1.0)],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
    }
    assert!(storage.stats()?.active_l1_segments < ACTIVE_L1_TEST_MAX_RUNS);
    assert_eq!(old_snapshot.collect::<Result<Vec<_>>>()?.len(), 4);
    let expected = (0..16)
        .map(|generation| point(series, generation * 100 + 1, 1.0))
        .collect::<Vec<_>>();
    assert_eq!(
        storage
            .scan(series, 0, 2_000)?
            .collect::<Result<Vec<_>>>()?,
        expected
    );
    storage.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    let reopened = Storage::open(temporary.path(), options)?;
    assert_eq!(
        reopened
            .scan(series, 0, 2_000)?
            .collect::<Result<Vec<_>>>()?,
        expected
    );
    reopened.close()
}

#[test]
fn l1_consolidation_preserves_mutable_day_boundaries_and_held_streams() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 2 * DAY,
        history_seal_lag: None,
        l0_compaction_trigger: 8,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 913);
    let initial = (DAY - 2_048..DAY + 2_048)
        .map(|timestamp| point(series, timestamp, 1.0))
        .collect::<Vec<_>>();
    storage.write_batch(initial.clone(), Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    let held = storage.scan(series, DAY - 2_048, DAY + 2_048)?;
    let request = || GroupRequest::new(vec![series], DAY - 1_000, DAY + 1_000);
    let held_group = storage.group_stream(request()?)?;
    let before_rows = storage.group(request()?)?.rows;
    for generation in 2..=4 {
        storage.write_batch(
            (DAY - 1_500..DAY + 1_500)
                .map(|timestamp| point(series, timestamp, f64::from(generation)))
                .collect(),
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
    }
    storage.delete_point(series, DAY - 1, Durability::Sync)?;
    storage.delete_point(series, DAY, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    let automatic = storage.group(request()?)?;
    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(automatic.rows, raw.rows);
    assert_eq!(raw.rows.iter().map(|row| row.count).sum::<u64>(), 1_998);
    assert_eq!(raw.rows.iter().map(|row| row.sum).sum::<f64>(), 7_992.0);
    assert_eq!(held.collect::<Result<Vec<_>>>()?, initial);
    assert_eq!(held_group.collect::<Result<Vec<_>>>()?, before_rows);
    let stats = storage.stats()?;
    assert_eq!(
        stats
            .l1_compaction_debt
            .iter()
            .map(|debt| debt.parts)
            .sum::<usize>(),
        stats.active_l1_segments
    );
    storage.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    let reopened = Storage::open(temporary.path(), options)?;
    assert_eq!(reopened.group(request()?)?.rows, raw.rows);
    assert_eq!(
        reopened
            .group(request()?.with_source(GroupSource::Raw))?
            .rows,
        raw.rows
    );
    reopened.close()
}

#[test]
fn compaction_skips_empty_time_outputs() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            l0_compaction_trigger: 8,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 73);
    storage.write_batch(
        vec![point(series, 1, 1.0), point(series, 180 * MINUTE + 1, 2.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    storage.write_batch(
        vec![
            point(series, 60 * MINUTE + 1, 3.0),
            point(series, 120 * MINUTE + 1, 4.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let visible = storage.scan(series, 0, DAY)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        visible
            .iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [1, 60 * MINUTE + 1, 120 * MINUTE + 1, 180 * MINUTE + 1,]
    );
    let stats = storage.stats()?;
    assert_eq!(stats.l1_segments, 1);
    assert_eq!(stats.sealed_l2_segments, 2);
    assert_eq!(fs::read_dir(temporary.path().join("tmp"))?.count(), 0);
    storage.close()
}

#[test]
fn late_delete_delta_keeps_non_intersecting_sealed_history() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: DAY,
        l0_compaction_trigger: 8,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 72);
    storage.write_batch(
        vec![
            point(series, 1, 1.0),
            point(series, 60 * MINUTE + 1, 2.0),
            point(series, 120 * MINUTE + 1, 3.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    storage.write_batch(vec![point(series, 60 * MINUTE + 1, 4.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    let before = manifest_segment_records(temporary.path())?;
    assert_eq!(before.len(), 3);
    let sealed = before
        .iter()
        .find(|segment| segment.max_timestamp_us < 60 * MINUTE)
        .expect("initial compaction sealed the first range");
    let sealed_id = sealed.id;
    let sealed_path = temporary.path().join(&sealed.relative_path);

    let bytes_before = storage.stats()?.compaction_bytes_read;
    storage.delete_point(series, 60 * MINUTE + 1, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    let after = manifest_segment_records(temporary.path())?;
    let active_ids = after.iter().map(|segment| segment.id).collect::<Vec<_>>();
    assert!(active_ids.contains(&sealed_id));
    assert!(sealed_path.exists());
    assert!(storage.stats()?.compaction_bytes_read > bytes_before);
    let visible = storage.scan(series, 0, DAY)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        visible
            .iter()
            .map(|point| point.timestamp_us)
            .collect::<Vec<_>>(),
        [1, 120 * MINUTE + 1]
    );
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(reopened.scan(series, 0, DAY)?.count(), 2);
    reopened.close()
}

#[test]
fn sealed_l2_defers_historical_repairs_and_preserves_mvcc_visibility() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 1_000,
        l0_compaction_trigger: 8,
        history_seal_lag: Some(Duration::from_micros(100)),
        history_window: Duration::from_micros(100),
        history_segment_target_bytes: 1024 * 1024,
        history_rewrite_trigger: 4,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 700);
    storage.write_batch(
        vec![
            point(series, 10, 1.0),
            point(series, 20, 1.5),
            point(series, 200, 2.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let sealed = storage.stats()?;
    assert_eq!(sealed.l1_segments, 1);
    assert_eq!(sealed.sealed_l2_segments, 1);
    assert_eq!(sealed.history_watermarks.len(), 1);
    assert_eq!(sealed.history_watermarks[0].event_watermark_us, 200);
    assert_eq!(sealed.history_watermarks[0].sealed_before_us, 100);
    let initial_parts = manifest_segment_records(temporary.path())?;
    assert_eq!(
        initial_parts
            .iter()
            .map(|segment| (segment.level, segment.rewrite_count))
            .collect::<Vec<_>>(),
        [(2, 1), (1, 1)]
    );

    storage.write_operations(
        vec![
            point(series, 10, 3.0).into(),
            WriteOperation::delete(series, 20),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    let before_repair = storage.stats()?;
    assert_eq!(before_repair.l0_segments, 1);
    assert_eq!(
        storage.compact_with_options(CompactionOptions { full: false })?,
        0
    );
    assert_eq!(
        storage.point(series, 10)?.map(|point| point.value),
        Some(3.0)
    );
    assert!(storage.point(series, 20)?.is_none());
    assert_eq!(storage.stats()?.l0_segments, 1);

    for (timestamp, value) in [(30, 4.0), (40, 5.0), (50, 6.0)] {
        storage.write_batch(vec![point(series, timestamp, value)], Durability::Sync)?;
        storage.checkpoint()?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    // The publisher removes L0 before the coordinator records completed I/O.
    // Wait for both observations; publication alone is not a metrics barrier.
    let repaired = loop {
        let current = storage.stats()?;
        if (current.l0_segments == 0
            && current.sealed_compaction_physical_bytes_read
                > before_repair.sealed_compaction_physical_bytes_read)
            || Instant::now() >= deadline
        {
            break current;
        }
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(repaired.l0_segments, 0);
    assert!((1..=4).contains(&repaired.sealed_l2_segments));
    assert!(repaired.sealed_max_rewrite_count >= 2);
    assert!(
        repaired.sealed_compaction_physical_bytes_read
            > before_repair.sealed_compaction_physical_bytes_read
    );
    assert!(repaired.sealed_compaction_jobs > 0);
    assert!(repaired.sealed_compaction_max_fan_in <= 16);
    assert_eq!(
        storage.point(series, 10)?.map(|point| point.value),
        Some(3.0)
    );
    assert!(storage.point(series, 20)?.is_none());
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        reopened.point(series, 10)?.map(|point| point.value),
        Some(3.0)
    );
    assert!(reopened.point(series, 20)?.is_none());
    assert_eq!(
        reopened.stats()?.sealed_l2_segments,
        repaired.sealed_l2_segments
    );
    reopened.close()
}

#[test]
fn shard_and_time_domains_compact_independently() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 4,
        partition_span_us: DAY,
        memtable_max_bytes: 64 * 1024,
        l0_compaction_trigger: 8,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let first = SeriesKey::new(1, 1, 1);
    let second = SeriesKey::new(1, 1, 2);
    for commit in 0..2 {
        storage.write_batch(
            vec![
                point(first, i64::from(commit), 1.0),
                point(first, DAY + i64::from(commit), 2.0),
                point(second, i64::from(commit), 3.0),
            ],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    let packed = storage.stats()?;
    assert_eq!(packed.physical_segment_files, packed.logical_segments);
    storage.compact()?;
    let stats = storage.stats()?;
    assert_eq!(stats.l0_segments, 0);
    assert_eq!(stats.l1_segments, 1);
    assert_eq!(stats.sealed_l2_segments, 2);
    assert_eq!(stats.physical_segment_files, stats.logical_segments);
    assert_eq!(rows(&storage, vec![first, second])?.len(), 3);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            virtual_shards: 4,
            partition_span_us: DAY,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(rows(&reopened, vec![first, second])?.len(), 3);
    reopened.close()?;

    let mismatch = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            virtual_shards: 8,
            partition_span_us: DAY,
            ..OpenOptions::default()
        },
    );
    assert!(matches!(mismatch, Err(chimaera::Error::Invalid(_))));
    Ok(())
}

#[test]
fn flush_keeps_wal_until_checkpoint_and_compaction_can_be_selective() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 16,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 31);
    let first = storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    assert_eq!(storage.flush()?, first.commit_id);
    let flushed = storage.stats()?;
    assert_eq!(flushed.checkpoint_commit_id, 0);
    assert_eq!(flushed.active_memtable_bytes, 0);
    assert_eq!(flushed.frozen_memtable_bytes, 0);
    assert_eq!(flushed.l0_segments, 1);
    assert_eq!(wal_file_count(temporary.path())?, 2);
    assert_eq!(storage.checkpoint()?, first.commit_id);
    assert_eq!(storage.stats()?.checkpoint_commit_id, first.commit_id);
    assert_eq!(wal_file_count(temporary.path())?, 1);

    let second = storage.write_batch(vec![point(series, 2, 2.0)], Durability::Sync)?;
    assert_eq!(storage.checkpoint()?, second.commit_id);
    assert_eq!(storage.stats()?.l0_segments, 2);
    assert_eq!(
        storage.compact_with_options(CompactionOptions { full: false })?,
        0
    );
    assert_eq!(storage.stats()?.l0_segments, 2);
    assert!(storage.compact()? > 0);
    let compacted = storage.stats()?;
    assert_eq!(compacted.l0_segments, 0);
    assert_eq!(compacted.l1_segments, 1);
    let rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 2);
    assert_close(rows[0].sum, 3.0);
    storage.close()
}

#[test]
fn compaction_rejects_insufficient_disk_space_without_degrading_reads() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            minimum_free_disk_bytes: u64::MAX,
            compaction_output_percent: 100,
            l0_compaction_trigger: 2,
            l0_slowdown_trigger: 4,
            l0_stop_trigger: 8,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 91);
    storage.write_batch(vec![point(series, 1, 7.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.write_batch(vec![point(series, 2, 8.0)], Durability::Sync)?;
    storage.checkpoint()?;
    wait_for_compaction_rejection(&storage)?;
    assert_eq!(storage.stats()?.background_errors, 0);

    let compacted = storage.compact();
    assert!(matches!(
        compacted,
        Err(Error::InsufficientDiskSpace {
            available,
            required: u64::MAX,
        }) if available < u64::MAX
    ));
    let stats = storage.stats()?;
    assert!(stats.compaction_space_rejections >= 2);
    assert_eq!(stats.last_compaction_required_bytes, u64::MAX);
    assert_eq!(stats.background_errors, 0);
    assert_eq!(stats.l0_segments, 2);
    assert_eq!(
        storage.point(series, 1)?.map(|point| point.value),
        Some(7.0)
    );
    assert_eq!(
        storage.point(series, 2)?.map(|point| point.value),
        Some(8.0)
    );
    storage.close()
}
