use super::*;

#[test]
fn sparse_sealed_l2_uses_index_count_sum_and_overlays_late_mutations() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 3 * DAY,
        l0_compaction_trigger: 8,
        history_seal_lag: Some(Duration::from_micros(1)),
        history_window: Duration::from_micros(DAY as u64),
        history_segment_target_bytes: 1024 * 1024,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options)?;
    let series = SeriesKey::new(1, 1, 701);
    storage.write_batch(
        vec![
            point(series, 1, 1.0),
            point(series, 2, 2.0),
            point(series, 2 * DAY, 9.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    assert_eq!(storage.stats()?.sealed_l2_segments, 1);

    let request = || {
        GroupRequest::new(vec![series], 0, DAY)
            .map(|request| request.with_aggregates(AggregateSet::COUNT_SUM))
    };
    let indexed = storage.group(request()?)?;
    assert_eq!(indexed.rows.len(), 1);
    assert_eq!(indexed.rows[0].count, 2);
    assert_close(indexed.rows[0].sum, 3.0);
    assert_eq!(indexed.stats.blocks_read, 0);
    assert_eq!(indexed.stats.operation_bytes_decoded, 0);
    assert!(indexed.stats.aggregate_records_read >= 1);

    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(raw.rows, indexed.rows);
    assert!(raw.stats.blocks_read >= 1);

    storage.write_batch(vec![point(series, 1, 5.0)], Durability::Sync)?;
    let overlaid = storage.group(request()?)?;
    assert_eq!(overlaid.rows[0].count, 2);
    assert_close(overlaid.rows[0].sum, 7.0);
    assert!(overlaid.stats.operations_scanned < raw.stats.operations_scanned + 2);
    storage.close()
}

#[test]
fn wide_count_sum_uses_index_summaries_without_aggregate_root_discovery() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 3 * DAY,
        l0_compaction_trigger: 8,
        history_seal_lag: Some(Duration::from_micros(1)),
        history_window: Duration::from_micros(DAY as u64),
        history_segment_target_bytes: 1024 * 1024,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options)?;
    let series = (0..32_u64)
        .map(|key| SeriesKey::new(1, 1, 10_000 + key))
        .collect::<Vec<_>>();
    let mut points = series
        .iter()
        .flat_map(|series| [point(*series, 1, 1.0), point(*series, 2, 2.0)])
        .collect::<Vec<_>>();
    points.push(point(series[0], 2 * DAY, 9.0));
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    let request = || {
        GroupRequest::new(series.clone(), 0, DAY)
            .map(|request| request.with_aggregates(AggregateSet::COUNT_SUM))
    };
    let indexed = storage.group(request()?)?;
    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(indexed.rows, raw.rows);
    assert_eq!(indexed.rows.len(), series.len());
    assert!(indexed.rows.iter().all(|row| row.count == 2));
    assert!(indexed.rows.iter().all(|row| row.sum == 3.0));
    assert_eq!(indexed.stats.blocks_read, 0);
    assert_eq!(indexed.stats.operation_bytes_decoded, 0);
    assert_eq!(
        indexed.stats.aggregate_bytes_read,
        indexed.stats.aggregate_records_read * 24
    );
    assert!(raw.stats.blocks_read > 0);
    storage.close()
}

#[test]
fn disjoint_l0_count_sum_uses_index_and_overlapping_versions_fall_back_to_mvcc() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            l0_compaction_trigger: 64,
            l0_slowdown_trigger: 96,
            l0_stop_trigger: 128,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 702);

    storage.write_batch(
        vec![point(series, 1, 1.0), point(series, 2, 2.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.write_batch(
        vec![point(series, 10, 3.0), point(series, 20, 4.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;

    let indexed = storage.group(GroupRequest::new(vec![series], 0, DAY)?)?;
    assert_eq!(indexed.rows.len(), 1);
    assert_eq!(indexed.rows[0].count, 4);
    assert_close(indexed.rows[0].sum, 10.0);
    assert_eq!(indexed.stats.blocks_read, 0);
    assert_eq!(indexed.stats.operation_bytes_decoded, 0);

    storage.write_batch(
        vec![point(series, 10, 30.0), point(series, 30, 5.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    let overlaid = storage.group(GroupRequest::new(vec![series], 0, DAY)?)?;
    assert_eq!(overlaid.rows.len(), 1);
    assert_eq!(overlaid.rows[0].count, 5);
    assert_close(overlaid.rows[0].sum, 42.0);
    assert!(overlaid.stats.blocks_read >= 2);
    assert!(overlaid.stats.operation_bytes_decoded > 0);
    storage.close()
}

#[test]
fn dense_l1_mixes_stable_aggregates_with_exact_dirty_days() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(4, 5, 6);
    storage.write_batch(
        (0..5_000)
            .map(|timestamp| point(series, timestamp, 0.1))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let before = storage.stats()?;
    let aggregate_rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    let after = storage.stats()?;
    assert_eq!(aggregate_rows.len(), 1);
    assert_eq!(aggregate_rows[0].count, 5_000);
    assert_close(aggregate_rows[0].sum, 500.0);
    assert_eq!(
        after.query_operations_scanned,
        before.query_operations_scanned
    );
    assert_eq!(
        after.query_aggregate_records_read - before.query_aggregate_records_read,
        1
    );

    storage.write_batch(vec![point(series, DAY + 1, 2.0)], Durability::Sync)?;
    let before_exact = storage.stats()?;
    let exact_rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    let after_exact = storage.stats()?;
    assert_eq!(exact_rows.len(), 2);
    assert_eq!(exact_rows.iter().map(|row| row.count).sum::<u64>(), 5_001);
    assert_close(exact_rows.iter().map(|row| row.sum).sum(), 502.0);
    assert_eq!(
        after_exact.query_operations_scanned - before_exact.query_operations_scanned,
        1
    );
    assert_eq!(
        after_exact.query_aggregate_records_read - before_exact.query_aggregate_records_read,
        1
    );
    assert_eq!(
        after_exact.query_blocks_read - before_exact.query_blocks_read,
        0
    );
    assert_eq!(
        after_exact.query_planned_segment_reads - before_exact.query_planned_segment_reads,
        0
    );

    storage.write_batch(vec![point(series, 1, 3.0)], Durability::Sync)?;
    let exact_dirty_rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        exact_dirty_rows.iter().map(|row| row.count).sum::<u64>(),
        5_001
    );
    assert_close(exact_dirty_rows.iter().map(|row| row.sum).sum(), 504.9);
    let hits_before = after_exact.block_cache_hits;
    let repeated = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(repeated, exact_dirty_rows);
    assert!(storage.stats()?.block_cache_hits > hits_before);

    storage.delete_point(series, 2, Durability::Sync)?;
    storage.delete_point(series, DAY + 1, Durability::Sync)?;
    let deleted_rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(deleted_rows.iter().map(|row| row.count).sum::<u64>(), 4_999);
    assert_close(deleted_rows.iter().map(|row| row.sum).sum(), 502.8);
    storage.close()
}

#[test]
fn mutable_aggregate_overlap_falls_back_to_raw_mvcc() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(4, 5, 7);
    storage.write_batch(
        (0..5_000)
            .map(|timestamp| point(series, timestamp, 0.1))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    storage.write_batch(
        (0..1_000)
            .map(|timestamp| point(series, timestamp, 2.0))
            .collect(),
        Durability::Sync,
    )?;

    let request = || {
        GroupRequest::new(vec![series], 0, DAY).and_then(|request| {
            request.with_options(QueryOptions {
                max_decoded_bytes: 2 * 1024 * 1024,
                ..QueryOptions::default()
            })
        })
    };
    let automatic = storage.group(request()?)?;
    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(automatic.rows.len(), 1);
    assert_eq!(automatic.rows[0].count, 5_000);
    assert_close(automatic.rows[0].sum, 2_400.0);
    assert_eq!(automatic.rows, raw.rows);
    assert_eq!(automatic.stats.aggregate_records_read, 0);
    assert_eq!(automatic.stats.aggregate_base_point_lookups, 0);
    assert_eq!(
        automatic.stats.operations_scanned,
        raw.stats.operations_scanned
    );
    assert!(automatic.stats.decoded_bytes < 2 * 1024 * 1024);
    assert!(automatic.stats.physical_decoded_bytes < 2 * 1024 * 1024);
    storage.close()
}

#[test]
fn hybrid_summary_falls_back_when_an_overlapping_l1_is_older() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            history_seal_lag: None,
            l0_compaction_trigger: 2,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(4, 5, 70);

    for range in [0..2_500, 2_500..5_000] {
        storage.write_batch(
            range
                .map(|timestamp_us| point(series, timestamp_us, 1.0))
                .collect(),
            Durability::Sync,
        )?;
        storage.flush()?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while storage.stats()?.l0_segments != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    let first = storage.stats()?;
    assert_eq!(first.l0_segments, 0);
    assert_eq!(first.active_l1_segments, 1);

    for range in [0..5_000, 5_000..10_000] {
        storage.write_batch(
            range
                .map(|timestamp_us| {
                    let value = if timestamp_us < 5_000 { 2.0 } else { 1.0 };
                    point(series, timestamp_us, value)
                })
                .collect(),
            Durability::Sync,
        )?;
        storage.flush()?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while storage.stats()?.l0_segments != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    let layered = storage.stats()?;
    assert_eq!(layered.l0_segments, 0);
    assert_eq!(layered.active_l1_segments, 2);

    let request = || {
        GroupRequest::new(vec![series], 0, DAY)
            .map(|request| request.with_aggregates(AggregateSet::COUNT_SUM))
    };
    let automatic = storage.group(request()?)?;
    let raw = storage.group(request()?.with_source(GroupSource::Raw))?;
    assert_eq!(raw.rows.len(), 1);
    assert_eq!(raw.rows[0].count, 10_000);
    assert_close(raw.rows[0].sum, 15_000.0);
    assert_eq!(automatic.rows, raw.rows);
    storage.close()
}

#[test]
fn extended_daily_summary_is_exact_and_falls_back_for_irreversible_edges() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(4, 5, 106);
    storage.write_batch(
        (0..5_000)
            .map(|timestamp| point(series, timestamp, timestamp as f64 * 0.25 - 100.0))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let request = || {
        GroupRequest::new(vec![series], 0, DAY)
            .map(|request| request.with_aggregates(AggregateSet::ALL))
    };
    let raw_request = || request().map(|request| request.with_source(chimaera::GroupSource::Raw));
    let before = storage.stats()?;
    let persisted = storage.group(request()?)?;
    let after = storage.stats()?;
    let raw = storage.group(raw_request()?)?;
    assert_eq!(persisted.rows.len(), 1);
    assert_eq!(raw.rows.len(), 1);
    assert_extended_close(&persisted.rows[0], &raw.rows[0]);
    assert_eq!(
        after.query_operations_scanned,
        before.query_operations_scanned
    );
    assert_eq!(persisted.stats.aggregate_records_read, 1);

    storage.write_batch(
        vec![point(series, 2_500, 2_500.0 * 0.25 - 99.5)],
        Durability::Sync,
    )?;
    let corrected = storage.group(request()?)?;
    let corrected_raw = storage.group(raw_request()?)?;
    assert_extended_close(&corrected.rows[0], &corrected_raw.rows[0]);
    assert_eq!(
        corrected.stats.operations_scanned,
        corrected_raw.stats.operations_scanned
    );
    assert_eq!(corrected.stats.aggregate_records_read, 0);

    storage.delete_point(series, 0, Durability::Sync)?;
    let fallback = storage.group(request()?)?;
    let fallback_raw = storage.group(raw_request()?)?;
    assert_extended_close(&fallback.rows[0], &fallback_raw.rows[0]);
    assert!(fallback.stats.operations_scanned >= 5_000);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let reopened_rows = reopened.group(request()?)?;
    let reopened_raw = reopened.group(raw_request()?)?;
    assert_extended_close(&reopened_rows.rows[0], &reopened_raw.rows[0]);
    reopened.close()
}

#[test]
fn partial_day_merges_disjoint_l1_summary_with_fresh_points() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(4, 5, 8);
    storage.write_batch(
        (0..5_000)
            .map(|timestamp| point(series, timestamp, 0.1))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    storage.write_batch(
        (9_000..9_100)
            .map(|timestamp| point(series, timestamp, 2.0))
            .collect(),
        Durability::Sync,
    )?;

    let before = storage.stats()?;
    let rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 10_000)?)?
        .collect::<Result<Vec<_>>>()?;
    let after = storage.stats()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 5_100);
    assert_close(rows[0].sum, 700.0);
    assert_eq!(after.query_blocks_read - before.query_blocks_read, 0);
    assert_eq!(
        after.query_aggregate_records_read - before.query_aggregate_records_read,
        1
    );
    assert_eq!(
        after.query_operations_scanned - before.query_operations_scanned,
        100
    );
    storage.close()
}

#[test]
fn hundred_points_per_day_use_exact_daily_aggregates() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 30 * DAY,
        metadata_cache_max_bytes: 1,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(4, 5, 7);
    let mut points = Vec::with_capacity(3_000);
    for day in 0..30_i64 {
        for offset in 0..100_i64 {
            points.push(point(series, day * DAY + offset, 1.0));
        }
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    storage.close()?;

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let before = storage.stats()?;
    let mut stream = storage.group_stream(GroupRequest::new(vec![series], 0, 14 * DAY)?)?;
    let rows = stream.by_ref().collect::<Result<Vec<_>>>()?;
    let query = stream.stats();
    let after = storage.stats()?;
    assert_eq!(rows.len(), 14);
    assert!(rows.iter().all(|row| row.count == 100));
    assert_close(rows.iter().map(|row| row.sum).sum(), 1_400.0);
    assert_eq!(
        after.query_operations_scanned,
        before.query_operations_scanned
    );
    assert_eq!(
        after.query_aggregate_records_read - before.query_aggregate_records_read,
        14
    );
    assert_eq!(
        after.query_exact_daily_aggregate_queries - before.query_exact_daily_aggregate_queries,
        1
    );
    assert_eq!(query.file_opens, 1);
    assert!(query.metadata_local_hits >= 1);
    assert_eq!(query.metadata_local_pinned_bytes, 0);
    assert!(query.metadata_local_peak_pinned_bytes > 0);
    assert_eq!(query.planned_segment_reads, 0);
    assert_eq!(query.blocks_read, 0);
    assert!(query.metadata_bytes_read < 16 * 1_024);
    assert!(query.metadata_duration > Duration::ZERO);
    assert_eq!(query.io_duration, Duration::ZERO);
    assert_eq!(query.decode_duration, Duration::ZERO);
    assert_eq!(query.mvcc_duration, Duration::ZERO);
    storage.close()
}

#[test]
fn multi_series_exact_aggregate_loads_each_root_once() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 30 * DAY,
        metadata_cache_max_bytes: 1,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = (0..12)
        .map(|key| SeriesKey::new(4, 5, key))
        .collect::<Vec<_>>();
    let points = series
        .iter()
        .flat_map(|series| (0..100).map(move |timestamp| point(*series, timestamp, 1.0)))
        .collect::<Vec<_>>();
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    storage.close()?;

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let mut stream = storage.group_stream(GroupRequest::new(series, 0, DAY)?)?;
    let rows = stream.by_ref().collect::<Result<Vec<_>>>()?;
    let query = stream.stats();
    assert_eq!(rows.len(), 12);
    assert!(rows.iter().all(|row| row.count == 100));
    assert_eq!(query.file_opens, 1);
    assert!(query.metadata_local_hits >= 12);
    assert!(query.metadata_bytes_read < 5 * 1_024);
    assert_eq!(query.planned_segment_reads, 0);
    storage.close()
}

#[test]
fn finite_values_whose_sum_overflows_survive_segment_publication() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    storage.create_space("production")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("extreme", DAY, DAY)?.with_write_mode(WriteMode::Mutable),
    )?;
    storage.create_bucket("production", "measurements", policy.id)?;
    let series = SeriesRef::new("production", "measurements", 1);
    let values = [
        f64::from_bits(9_207_031_782_422_452_278),
        f64::from_bits(9_217_324_001_301_273_074),
    ];
    storage.write_named_operations(
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                NamedPoint::new(series.clone(), index as i64 + 1, *value).map(Into::into)
            })
            .collect::<Result<Vec<_>>>()?,
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    let actual = storage
        .scan_named(&series, 0, 3)?
        .map(|point| point.map(|point| point.value.to_bits()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(actual, values.map(f64::to_bits));
    assert!(storage.stats()?.healthy);
    storage.close()?;

    let report = Storage::verify(temporary.path());
    assert!(report.valid, "{:?}", report.issues);
    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let actual = reopened
        .scan_named(&series, 0, 3)?
        .map(|point| point.map(|point| point.value.to_bits()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(actual, values.map(f64::to_bits));
    reopened.close()
}

#[test]
fn fixed_grouping_uses_origin_across_negative_timestamps() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 99);
    storage.write_batch(
        vec![
            point(series, 13, 7.0),
            point(series, -8, 3.0),
            point(series, 3, 5.0),
            point(series, -18, 1.0),
            point(series, 12, 6.0),
            point(series, -17, 2.0),
            point(series, 2, 4.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let actual = storage
        .group_stream(GroupRequest::fixed(vec![series], -20, 14, 10, 3)?)?
        .collect::<Result<Vec<_>>>()?;
    let expected = [
        (-27, -17, 1, 1.0),
        (-17, -7, 2, 5.0),
        (-7, 3, 1, 4.0),
        (3, 13, 2, 11.0),
        (13, 23, 1, 7.0),
    ];
    assert_eq!(actual.len(), expected.len());
    for (row, (start, end, count, sum)) in actual.iter().zip(expected) {
        assert_eq!(row.series, Some(series));
        assert_eq!(row.bucket_start_us, start);
        assert_eq!(row.bucket_end_us, end);
        assert_eq!(row.count, count);
        assert_close(row.sum, sum);
    }
    assert!(GroupRequest::fixed(vec![series], 0, 1, 0, 0).is_err());
    storage.close()
}

#[test]
fn extended_aggregates_use_one_mvcc_visible_stream() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 199);
    storage.write_batch(
        vec![
            point(series, 0, 1.0),
            point(series, 1_000_000, 2.0),
            point(series, 3_000_000, 4.0),
        ],
        Durability::Sync,
    )?;
    storage.write_operations(
        vec![
            point(series, 1_000_000, 5.0).into(),
            WriteOperation::delete(series, 0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let before = storage.stats()?;
    let request = GroupRequest::fixed(vec![series], -1, 4_000_000, 10_000_000, 0)?
        .with_aggregates(AggregateSet::ALL);
    let rows = storage.group_stream(request)?.collect::<Result<Vec<_>>>()?;
    let after = storage.stats()?;
    assert_eq!(rows.len(), 1);
    let row = rows[0];
    assert_eq!(row.count, 2);
    assert_close(row.sum, 9.0);
    assert_eq!(row.min, Some(4.0));
    assert_eq!(row.max, Some(5.0));
    assert_eq!(row.mean, Some(4.5));
    assert_eq!(row.population_variance, Some(0.25));
    assert_eq!(row.sample_variance, Some(0.5));
    assert_eq!(row.population_standard_deviation, Some(0.5));
    assert_close(
        row.sample_standard_deviation
            .ok_or_else(|| Error::Invalid("missing sample standard deviation".into()))?,
        0.5_f64.sqrt(),
    );
    assert_eq!(row.first.map(|point| point.timestamp_us), Some(1_000_000));
    assert_eq!(row.first.map(|point| point.value), Some(5.0));
    assert_eq!(row.last.map(|point| point.timestamp_us), Some(3_000_000));
    assert_eq!(row.last.map(|point| point.value), Some(4.0));
    assert_eq!(row.delta, Some(-1.0));
    assert_eq!(row.rate_per_second, Some(-0.5));
    assert!(after.query_operations_scanned > before.query_operations_scanned);
    storage.close()
}

#[test]
fn one_point_extended_aggregate_has_defined_empty_statistics() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 200);
    storage.write_batch(vec![point(series, -5, 7.0)], Durability::Sync)?;
    let row = storage
        .group_stream(
            GroupRequest::fixed(vec![series], -10, 0, 10, 0)?.with_aggregates(AggregateSet::ALL),
        )?
        .next()
        .ok_or_else(|| Error::Invalid("missing aggregate row".into()))??;
    assert_eq!(row.population_variance, Some(0.0));
    assert_eq!(row.sample_variance, None);
    assert_eq!(row.population_standard_deviation, Some(0.0));
    assert_eq!(row.sample_standard_deviation, None);
    assert_eq!(row.delta, Some(0.0));
    assert_eq!(row.rate_per_second, None);
    storage.close()
}

#[test]
fn profitable_minute_hour_and_day_resolutions_are_selected_exactly() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: 30 * DAY,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 201);
    storage.write_batch(
        (0..7_200)
            .map(|second| point(series, second * 1_000_000, second as f64))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    for (duration, to_us, expected_rows) in [
        (MINUTE, 2 * 60 * MINUTE, 120),
        (60 * MINUTE, 2 * 60 * MINUTE, 2),
        (DAY, DAY, 1),
    ] {
        let result = storage.group(
            GroupRequest::fixed(vec![series], 0, to_us, duration, 0)?
                .with_aggregates(AggregateSet::ALL),
        )?;
        assert_eq!(result.rows.len(), expected_rows);
        assert_eq!(result.stats.operations_scanned, 0);
        assert!(result.stats.aggregate_records_read > 0);
        assert!(result.rows.iter().all(|row| row.resolution_us == duration));
        let expected_source = match duration {
            MINUTE => AggregateResolution::Minute,
            value if value == 60 * MINUTE => AggregateResolution::Hour,
            DAY => AggregateResolution::Day,
            _ => unreachable!(),
        };
        assert!(
            result
                .rows
                .iter()
                .all(|row| row.source_resolution == Some(expected_source))
        );
    }

    let downsampled = storage.group(
        GroupRequest::downsampled(vec![series], 0, 2 * 60 * MINUTE, 2)?
            .with_aggregates(AggregateSet::ALL),
    )?;
    assert_eq!(downsampled.rows.len(), 2);
    assert!(
        downsampled
            .rows
            .iter()
            .all(|row| row.resolution_us == 60 * MINUTE)
    );
    assert_eq!(downsampled.stats.operations_scanned, 0);
    assert!(
        downsampled
            .rows
            .iter()
            .all(|row| row.source_resolution == Some(AggregateResolution::Hour))
    );
    storage.close()
}

#[test]
fn calendar_grouping_uses_iana_dst_boundaries() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 100);
    storage.write_batch(
        vec![
            point(series, 1_710_045_000_000_000, 1.0),
            point(series, 1_710_048_600_000_000, 2.0),
            point(series, 1_710_055_800_000_000, 3.0),
            point(series, 1_710_127_800_000_000, 4.0),
            point(series, 1_710_131_400_000_000, 5.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let actual = storage
        .group_stream(GroupRequest::calendar(
            vec![series],
            1_710_040_000_000_000,
            1_710_140_000_000_000,
            CalendarUnit::Day,
            "America/New_York",
            WeekStart::Monday,
        )?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(actual.len(), 3);
    assert!(actual.iter().all(|row| row.source_resolution.is_none()));
    assert_eq!(
        actual
            .iter()
            .map(|row| (row.bucket_start_us, row.bucket_end_us, row.count))
            .collect::<Vec<_>>(),
        [
            (1_709_960_400_000_000, 1_710_046_800_000_000, 1),
            (1_710_046_800_000_000, 1_710_129_600_000_000, 3),
            (1_710_129_600_000_000, 1_710_216_000_000_000, 1),
        ]
    );
    assert_close(actual[0].sum, 1.0);
    assert_close(actual[1].sum, 9.0);
    assert_close(actual[2].sum, 5.0);
    assert!(
        GroupRequest::calendar(
            vec![series],
            0,
            1,
            CalendarUnit::Day,
            "Mars/Olympus_Mons",
            WeekStart::Monday,
        )
        .is_err()
    );
    storage.close()
}

#[test]
fn combined_grouping_merges_series_into_strict_bucket_order() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let first = SeriesKey::new(1, 1, 1);
    let second = SeriesKey::new(1, 1, 2);
    let third = SeriesKey::new(1, 1, 3);
    storage.write_batch(
        vec![
            point(first, -7, 1.0),
            point(first, 2, 2.0),
            point(first, 12, 3.0),
            point(second, -8, 10.0),
            point(second, 8, 20.0),
            point(second, 18, 30.0),
            point(third, 3, 40.0),
            point(third, 13, 50.0),
        ],
        Durability::Sync,
    )?;
    storage.write_operations(
        vec![
            WriteOperation::Put(point(third, 3, 4.0)),
            WriteOperation::delete(second, 8),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;

    let request = GroupRequest::fixed(vec![third, first, second], -10, 20, 10, 0)?
        .with_scope(GroupScope::Combined);
    let stream = storage.group_stream(request.clone())?;
    let read_commit_id = stream.read_commit_id();
    let actual = stream.collect::<Result<Vec<_>>>()?;
    assert_eq!(read_commit_id, 2);
    assert_eq!(actual.len(), 3);
    assert!(actual.iter().all(|row| row.series.is_none()));
    assert!(
        actual
            .windows(2)
            .all(|rows| { rows[0].bucket_start_us < rows[1].bucket_start_us })
    );
    let expected = [(-10, 0, 2, 11.0), (0, 10, 2, 6.0), (10, 20, 3, 83.0)];
    for (row, (start, end, count, sum)) in actual.iter().zip(expected) {
        assert_eq!(row.bucket_start_us, start);
        assert_eq!(row.bucket_end_us, end);
        assert_eq!(row.count, count);
        assert_close(row.sum, sum);
    }
    let materialized = storage.group(request.clone())?;
    assert_eq!(materialized.read_commit_id, read_commit_id);
    assert_eq!(materialized.rows, actual);
    assert_eq!(materialized.stats.points_scanned, 7);
    assert!(materialized.stats.operations_scanned >= materialized.stats.points_scanned);

    let mut callback_rows = Vec::new();
    let streamed = storage.each_group(request.clone(), |row| {
        callback_rows.push(row);
        Ok(())
    })?;
    assert_eq!(streamed.read_commit_id, read_commit_id);
    assert_eq!(streamed.stats.points_scanned, 7);
    assert_eq!(callback_rows, actual);

    let mut callbacks = 0;
    let stopped = storage.each_group(request, |_row| {
        callbacks += 1;
        Err(Error::Invalid("consumer stopped".into()))
    });
    assert!(matches!(stopped, Err(Error::Invalid(message)) if message == "consumer stopped"));
    assert_eq!(callbacks, 1);
    assert!(GroupRequest::new(vec![first, SeriesKey::new(2, 1, 1)], -10, 20).is_err());
    storage.close()
}

#[test]
fn combined_grouping_merges_l1_aggregates_with_a_dirty_series() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let first = SeriesKey::new(1, 1, 11);
    let second = SeriesKey::new(1, 1, 12);
    let mut points = Vec::with_capacity(8_192);
    for timestamp_us in 0..4_096 {
        points.push(point(first, timestamp_us, 0.1));
        points.push(point(second, timestamp_us, 0.2));
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;

    let request = GroupRequest::new(vec![first, second], 0, DAY)?.with_scope(GroupScope::Combined);
    let before = storage.stats()?;
    let aggregate = storage
        .group_stream(request.clone())?
        .collect::<Result<Vec<_>>>()?;
    let after = storage.stats()?;
    assert_eq!(aggregate.len(), 1);
    assert_eq!(aggregate[0].count, 8_192);
    assert_close(aggregate[0].sum, 1_228.8);
    assert_eq!(
        after.query_operations_scanned,
        before.query_operations_scanned
    );
    assert_eq!(
        after.query_aggregate_records_read - before.query_aggregate_records_read,
        2
    );
    assert_eq!(
        after.query_exact_daily_aggregate_queries - before.query_exact_daily_aggregate_queries,
        1
    );
    let measured = storage.group(request.clone())?;
    assert_eq!(measured.stats.operations_scanned, 0);
    assert_eq!(measured.stats.points_scanned, 8_192);
    assert_eq!(measured.stats.blocks_read, 0);
    assert_eq!(measured.stats.decoded_bytes, 224);
    assert_eq!(measured.stats.operation_bytes_decoded, 0);
    assert_eq!(measured.stats.aggregate_records_read, 2);
    assert_eq!(measured.stats.aggregate_bytes_read, 224);
    assert_eq!(measured.stats.result_rows, 1);
    assert!(measured.stats.segment_descriptors_examined >= 2);
    assert!(measured.stats.series_index_lookups >= 2);
    assert!(measured.stats.planning_duration > Duration::ZERO);
    assert!(measured.stats.read_duration > Duration::ZERO);
    let aggregate_memory_limited = request.clone().with_options(QueryOptions {
        max_memory_bytes: 512,
        ..QueryOptions::default()
    })?;
    let aggregate_memory_limited = storage.group(aggregate_memory_limited)?;
    assert_eq!(aggregate_memory_limited.rows.len(), 1);
    assert_eq!(aggregate_memory_limited.stats.operation_bytes_decoded, 0);
    assert!(aggregate_memory_limited.stats.peak_memory_bytes <= 512);
    let aggregate_limited = request.clone().with_options(QueryOptions {
        max_decoded_bytes: 79,
        ..QueryOptions::default()
    })?;
    assert!(matches!(
        storage.group(aggregate_limited),
        Err(Error::ResourceLimit(message)) if message.contains("decoded-byte")
    ));

    storage.write_batch(vec![point(first, 0, 10.0)], Durability::Sync)?;
    let mixed = storage.group_stream(request)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(mixed.len(), 1);
    assert_eq!(mixed[0].count, 8_192);
    assert_close(mixed[0].sum, 1_238.7);
    storage.close()
}
