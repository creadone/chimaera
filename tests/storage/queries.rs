use super::*;

#[test]
fn sparse_group_preserves_versions_and_day_boundaries_across_compaction() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            l0_compaction_trigger: 16,
            l0_slowdown_trigger: 32,
            l0_stop_trigger: 64,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 79);
    storage.write_batch(
        vec![
            point(series, 0, 1.0),
            point(series, DAY - 1, 2.0),
            point(series, DAY, 4.0),
            point(series, 80 * DAY, 8.0),
        ],
        Durability::Sync,
    )?;
    storage.flush()?;
    storage.compact()?;
    let request =
        GroupRequest::new(vec![series], 0, 90 * DAY)?.with_aggregates(AggregateSet::COUNT_SUM);
    let held = storage.group_stream(request.clone())?;
    storage.write_operations(
        vec![
            WriteOperation::Put(point(series, DAY, 16.0)),
            WriteOperation::delete(series, 80 * DAY),
        ],
        Durability::Sync,
    )?;
    storage.flush()?;
    storage.compact()?;
    let old = held.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        old.iter()
            .map(|r| (r.bucket_start_us, r.count, r.sum))
            .collect::<Vec<_>>(),
        [(0, 2, 3.0), (DAY, 1, 4.0), (80 * DAY, 1, 8.0)]
    );
    for (from, to, expected) in [
        (0, 90 * DAY, vec![(0, 2, 3.0), (DAY, 1, 16.0)]),
        (DAY, 80 * DAY, vec![(DAY, 1, 16.0)]),
        (2 * DAY, 80 * DAY, vec![]),
    ] {
        for source in [GroupSource::Auto, GroupSource::Raw] {
            let rows = storage
                .group_stream(
                    GroupRequest::new(vec![series], from, to)?
                        .with_source(source)
                        .with_aggregates(AggregateSet::COUNT_SUM),
                )?
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(
                rows.iter()
                    .map(|r| (r.bucket_start_us, r.count, r.sum))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
    storage.close()
}

#[test]
fn point_and_scan_merge_versions_in_timestamp_order() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 2, 3);
    let other = SeriesKey::new(1, 2, 4);
    storage.write_batch(
        vec![
            point(series, 30, 3.0),
            point(other, 20, 99.0),
            point(series, 10, 1.0),
            point(series, 20, 2.0),
        ],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.write_batch(vec![point(series, 20, 7.0)], Durability::Sync)?;

    assert_eq!(
        storage.point(series, 20)?.map(|value| value.value),
        Some(7.0)
    );
    assert!(storage.point(series, 21)?.is_none());
    assert!(storage.point(series, i64::MAX)?.is_none());
    let stream = storage.scan(series, 10, 30)?;
    assert_eq!(stream.read_commit_id(), 2);
    let actual = stream.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        actual
            .iter()
            .map(|value| (value.timestamp_us, value.value))
            .collect::<Vec<_>>(),
        [(10, 1.0), (20, 7.0)]
    );
    storage.close()
}

#[test]
fn wide_queries_do_not_starve_short_reads_flush_or_compaction() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            history_seal_lag: None,
            memtable_max_bytes: 16 * 1024 * 1024,
            l0_compaction_trigger: 8,
            wal_checkpoint_bytes: u64::MAX,
            checkpoint_interval: Duration::from_secs(86_400),
            cold_flush_idle_interval: Duration::from_secs(86_400),
            ..OpenOptions::default()
        },
    )?;
    let series = (0..300)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    for generation in 0..3 {
        storage.write_batch(fairness_points(&series, generation), Durability::Sync)?;
        storage.checkpoint()?;
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(6));
    let wide_completed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let short_completed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let storage = storage.clone();
        let series = series.clone();
        let stop = Arc::clone(&stop);
        let gate = Arc::clone(&gate);
        let completed = Arc::clone(&wide_completed);
        readers.push(thread::spawn(move || -> Result<()> {
            gate.wait();
            while !stop.load(Ordering::Acquire) {
                let rows = storage
                    .group_stream(GroupRequest::new(series.clone(), 0, DAY)?)?
                    .collect::<Result<Vec<_>>>()?;
                if rows.len() != 300 {
                    return Err(Error::Corruption(format!(
                        "wide fairness query returned {} rows",
                        rows.len()
                    )));
                }
                completed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }));
    }
    let short_storage = storage.clone();
    let short_series = series[0];
    let short_stop = Arc::clone(&stop);
    let short_gate = Arc::clone(&gate);
    let short_counter = Arc::clone(&short_completed);
    readers.push(thread::spawn(move || -> Result<()> {
        short_gate.wait();
        while !short_stop.load(Ordering::Acquire) {
            if short_storage.point(short_series, 0)?.is_none() {
                return Err(Error::Corruption(
                    "short fairness query lost its point".into(),
                ));
            }
            short_counter.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }));

    gate.wait();
    let maintenance = (|| -> Result<()> {
        storage.write_batch(fairness_points(&series, 3), Durability::Sync)?;
        storage.checkpoint()?;
        storage.compact()?;
        Ok(())
    })();

    let progress_deadline = Instant::now() + Duration::from_secs(2);
    while (wide_completed.load(Ordering::Relaxed) == 0
        || short_completed.load(Ordering::Relaxed) == 0)
        && Instant::now() < progress_deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().expect("fairness reader")?;
    }
    maintenance?;
    let stats = storage.stats()?;
    assert!(wide_completed.load(Ordering::Relaxed) > 0);
    assert!(short_completed.load(Ordering::Relaxed) > 0);
    assert!(stats.flushes >= 4);
    assert!(stats.compactions > 0);
    assert_eq!(stats.flush_queue_depth, 0);
    assert_eq!(stats.publication_queue_depth, 0);
    storage.close()
}

#[test]
fn scan_keeps_its_snapshot_during_concurrent_write_and_compaction() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(7, 8, 9);
    storage.write_batch(
        (0..5_000)
            .map(|timestamp| point(series, timestamp, 1.0))
            .collect(),
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    let stream = storage.scan(series, 0, 10_000)?;
    let worker_storage = storage.clone();
    let worker = thread::spawn(move || -> Result<()> {
        worker_storage.write_batch(
            (5_000..10_000)
                .map(|timestamp| point(series, timestamp, 2.0))
                .collect(),
            Durability::Sync,
        )?;
        worker_storage.checkpoint()?;
        worker_storage.compact()?;
        Ok(())
    });

    let snapshot = stream.collect::<Result<Vec<_>>>()?;
    worker.join().expect("storage worker")?;
    assert_eq!(snapshot.len(), 5_000);
    assert!(snapshot.iter().all(|value| value.value == 1.0));
    let current = storage
        .scan(series, 0, 10_000)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(current.len(), 10_000);
    assert!(
        current
            .windows(2)
            .all(|pair| { pair[0].timestamp_us < pair[1].timestamp_us })
    );
    storage.close()
}

#[test]
fn delete_survives_checkpoint_compaction_and_reopen_without_changing_old_snapshot() -> Result<()> {
    let temporary = TempDir::new()?;
    let series = SeriesKey::new(9, 8, 7);
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    storage.write_batch(
        vec![point(series, 10, 1.0), point(series, 20, 2.0)],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.compact()?;
    let old_snapshot = storage.scan(series, 0, 100)?;

    storage.delete_point(series, 10, Durability::Sync)?;
    assert!(storage.point(series, 10)?.is_none());
    let current = storage.scan(series, 0, 100)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        current
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(20, 2.0)]
    );
    let rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 1);
    assert_close(rows[0].sum, 2.0);

    let old = old_snapshot.collect::<Result<Vec<_>>>()?;
    assert_eq!(old.len(), 2);
    storage.checkpoint()?;
    storage.compact()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert!(reopened.point(series, 10)?.is_none());
    assert_eq!(reopened.scan(series, 0, 100)?.count(), 1);
    reopened.delete_point(series, 99, Durability::Sync)?;
    assert!(reopened.point(series, 99)?.is_none());
    reopened.close()
}

#[test]
fn streaming_multi_series_has_stable_per_series_date_order() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let first = SeriesKey::new(1, 1, 1);
    let second = SeriesKey::new(1, 1, 2);
    storage.write_batch(
        vec![
            point(second, DAY * 2 + 1, 4.0),
            point(first, DAY + 1, 2.0),
            point(second, 1, 3.0),
            point(first, 1, 1.0),
        ],
        Durability::Sync,
    )?;
    let actual = rows(&storage, vec![second, first])?;
    let mut previous = BTreeMap::new();
    for row in &actual {
        if let Some(prior) = previous.insert(row.series, row.bucket_start_us) {
            assert!(prior < row.bucket_start_us);
        }
    }
    assert_eq!(actual.iter().map(|row| row.count).sum::<u64>(), 4);
    assert_close(actual.iter().map(|row| row.sum).sum(), 10.0);
    storage.close()
}

#[test]
fn wide_daily_stream_preserves_order_and_mvcc_across_parallel_workers() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 4,
            ..OpenOptions::default()
        },
    )?;
    let series = (0..32_u64)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    storage.write_batch(
        series
            .iter()
            .flat_map(|series| [point(*series, 1, 1.0), point(*series, DAY + 1, 2.0)])
            .collect(),
        Durability::Sync,
    )?;
    storage.flush()?;
    storage.write_operations(
        series
            .iter()
            .flat_map(|series| {
                let replacement = WriteOperation::Put(point(*series, 1, 3.0));
                if series.key.is_multiple_of(2) {
                    vec![replacement, WriteOperation::delete(*series, DAY + 1)]
                } else {
                    vec![replacement]
                }
            })
            .collect(),
        Durability::Sync,
    )?;

    let mut requested = series.clone();
    requested.reverse();
    let actual = storage
        .group_stream(GroupRequest::new(requested, 0, 2 * DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    let mut previous = None;
    for row in &actual {
        if let Some((series, day)) = previous {
            assert!(series < row.series || (series == row.series && day < row.bucket_start_us));
        }
        previous = Some((row.series, row.bucket_start_us));
        if row.bucket_start_us == 0 {
            assert_eq!(row.count, 1);
            assert_close(row.sum, 3.0);
        } else {
            assert!(!row.series.expect("per-key row").key.is_multiple_of(2));
            assert_eq!(row.count, 1);
            assert_close(row.sum, 2.0);
        }
    }
    assert_eq!(actual.len(), 48);

    let mut partial = storage.group_stream(GroupRequest::new(series.clone(), 0, 2 * DAY)?)?;
    assert!(partial.next().transpose()?.is_some());
    drop(partial);
    assert_query_resources_released(&storage)?;

    let cancellation = CancellationToken::new();
    let request = GroupRequest::new(series, 0, 2 * DAY)?.with_options(QueryOptions {
        cancellation: Some(cancellation.clone()),
        ..QueryOptions::default()
    })?;
    let mut cancelled = storage.group_stream(request)?;
    cancellation.cancel();
    assert!(matches!(cancelled.next(), Some(Err(Error::QueryCancelled))));
    assert!(cancelled.next().is_none());
    drop(cancelled);
    assert_query_resources_released(&storage)?;
    storage.close()
}

fn assert_query_resources_released(storage: &Storage) -> Result<()> {
    // Dropping a partial stream cancels its workers without joining them. Their
    // snapshot leases and buffers must be released when cancelled work finishes.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let stats = storage.stats()?;
        let remaining = (
            stats.active_query_snapshots,
            stats.query_plan_memory_bytes,
            stats.query_decoded_memory_bytes,
            stats.query_results_memory_bytes,
        );
        if remaining == (0, 0, 0, 0) {
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "cancelled query retained snapshots/plan/decoded/results: {remaining:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn read_snapshot_remains_consistent_during_write() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 9);
    storage.write_batch(
        (0..1_000)
            .map(|timestamp| point(series, timestamp, 1.0))
            .collect(),
        Durability::Sync,
    )?;
    let stream = storage.group_stream(GroupRequest::new(vec![series], 0, DAY)?)?;
    let read_commit = stream.read_commit_id();
    let writer = storage.clone();
    let handle = thread::spawn(move || {
        writer.write_batch(
            (1_000..2_000)
                .map(|timestamp| point(series, timestamp, 1.0))
                .collect(),
            Durability::Sync,
        )
    });
    let snapshot_rows = stream.collect::<Result<Vec<_>>>()?;
    handle.join().expect("writer thread")?;
    assert_eq!(
        snapshot_rows.iter().map(|row| row.count).sum::<u64>(),
        1_000
    );
    assert_eq!(read_commit, 1);
    assert_eq!(rows(&storage, vec![series])?[0].count, 2_000);
    storage.close()
}

#[test]
fn range_query_skips_metadata_outside_its_time_window() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        create_if_missing: true,
        virtual_shards: 1,
        partition_span_us: 30 * DAY,
        ..OpenOptions::default()
    };
    let series = SeriesKey::new(4, 5, 8);
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.write_batch(vec![point(series, 0, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.write_batch(vec![point(series, 60 * DAY, 2.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let before = storage.stats()?;
    let rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, DAY)?)?
        .collect::<Result<Vec<_>>>()?;
    let after = storage.stats()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 1);
    // Metadata and payload share one cached physical file. The segment in the
    // later 30-day window must be rejected without opening its pack.
    assert_eq!(after.query_file_opens - before.query_file_opens, 1);
    storage.close()
}

#[test]
fn query_limits_deadline_and_cancellation_stop_grouping() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 21);
    let other = SeriesKey::new(1, 1, 22);
    storage.write_batch(
        vec![
            point(series, 0, 1.0),
            point(series, 1, 2.0),
            point(series, 2, 3.0),
        ],
        Durability::Sync,
    )?;

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_request =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            cancellation: Some(cancelled),
            ..QueryOptions::default()
        })?;
    assert!(matches!(
        storage.group_stream(cancelled_request),
        Err(Error::QueryCancelled)
    ));

    let expired = GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
        deadline: Some(Instant::now() - Duration::from_millis(1)),
        ..QueryOptions::default()
    })?;
    assert!(matches!(
        storage.group_stream(expired),
        Err(Error::QueryDeadlineExceeded)
    ));

    let token = CancellationToken::new();
    let mut stream = storage.group_stream(
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            cancellation: Some(token.clone()),
            ..QueryOptions::default()
        })?,
    )?;
    assert_eq!(storage.stats()?.active_query_snapshots, 1);
    assert!(matches!(stream.next(), Some(Ok(_))));
    token.cancel();
    assert!(matches!(stream.next(), Some(Err(Error::QueryCancelled))));
    let cancelled_stats = storage.stats()?;
    assert_eq!(cancelled_stats.active_query_snapshots, 0);
    assert_eq!(cancelled_stats.snapshot_pinned_segments, 0);
    assert!(stream.next().is_none());

    let row_limited =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            max_result_rows: 1,
            ..QueryOptions::default()
        })?;
    assert!(matches!(
        storage.group(row_limited),
        Err(Error::ResourceLimit(message)) if message.contains("result-row")
    ));

    let memory_limited =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            max_memory_bytes: 160,
            ..QueryOptions::default()
        })?;
    assert_eq!(
        storage
            .group_stream(memory_limited.clone())?
            .collect::<Result<Vec<_>>>()?
            .len(),
        3
    );
    assert!(matches!(
        storage.group(memory_limited),
        Err(Error::ResourceLimit(message)) if message.contains("memory")
    ));

    let series_limited = GroupRequest::new(vec![series, other], 0, 3)?.with_options(QueryOptions {
        max_series: 1,
        ..QueryOptions::default()
    });
    assert!(matches!(series_limited, Err(Error::ResourceLimit(_))));

    storage.checkpoint()?;
    let result = storage.group(GroupRequest::fixed(vec![series], 0, 3, 1, 0)?)?;
    assert_eq!(result.stats.operations_scanned, 3);
    assert_eq!(result.stats.points_scanned, 3);
    assert_eq!(result.stats.blocks_read, 1);
    assert!(result.stats.decoded_bytes > 0);
    assert_eq!(result.stats.result_rows, 3);
    assert!(result.stats.peak_memory_bytes >= 128);
    assert!(result.stats.segment_descriptors_examined >= 1);
    assert!(result.stats.series_index_lookups >= 1);
    assert!(result.stats.planning_duration > Duration::ZERO);
    assert!(result.stats.io_duration > Duration::ZERO);
    assert!(result.stats.decode_duration > Duration::ZERO);
    assert!(result.stats.mvcc_duration > Duration::ZERO);
    assert!(result.stats.read_duration > Duration::ZERO);

    let decoded_limited =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            max_decoded_bytes: 1,
            ..QueryOptions::default()
        })?;
    assert!(matches!(
        storage.group(decoded_limited),
        Err(Error::ResourceLimit(message)) if message.contains("decoded-byte")
    ));

    let disk_memory_limited =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            max_memory_bytes: 200,
            ..QueryOptions::default()
        })?;
    assert!(matches!(
        storage.group(disk_memory_limited),
        Err(Error::ResourceLimit(message)) if message.contains("memory")
    ));

    let scanned_limited =
        GroupRequest::fixed(vec![series], 0, 3, 1, 0)?.with_options(QueryOptions {
            max_scanned_points: 1,
            ..QueryOptions::default()
        })?;
    assert!(matches!(
        storage.group(scanned_limited),
        Err(Error::ResourceLimit(message)) if message.contains("scanned-point")
    ));
    storage.close()
}
