use super::*;

#[test]
fn metadata_cache_is_bounded_and_segment_filter_rejects_absent_series() -> Result<()> {
    // Include cache keys and LRU nodes while still forcing eviction across segments.
    const CACHE_BYTES: u64 = 512;
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            metadata_cache_max_bytes: CACHE_BYTES,
            l0_compaction_trigger: 16,
            l0_slowdown_trigger: 32,
            l0_stop_trigger: 64,
            ..OpenOptions::default()
        },
    )?;
    let present = SeriesKey::new(1, 1, 1);
    for timestamp_us in 1..=3 {
        storage.write_batch(
            vec![point(present, timestamp_us, timestamp_us as f64)],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    let populated = storage.stats()?;
    assert!(populated.metadata_cache_bytes <= CACHE_BYTES);
    assert!((1..=3).contains(&populated.metadata_cache_entries));

    let absent = SeriesKey::new(1, 1, 999);
    assert!(
        storage
            .scan(absent, 0, 10)?
            .collect::<Result<Vec<_>>>()?
            .is_empty()
    );
    let filtered = storage.stats()?;
    assert!(filtered.metadata_cache_bytes <= CACHE_BYTES);
    assert!((1..=3).contains(&filtered.metadata_cache_entries));
    assert_eq!(
        filtered.metadata_cache_misses,
        populated.metadata_cache_misses
    );
    assert_eq!(
        filtered.query_segment_index_candidates,
        populated.query_segment_index_candidates
    );
    assert_eq!(
        filtered.membership_filter_negatives,
        populated.membership_filter_negatives
    );
    assert_eq!(filtered.membership_filter_false_positives, 0);

    let points = storage.scan(present, 0, 10)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(points.len(), 3);
    let hits_before = storage.stats()?.metadata_cache_hits;
    assert_eq!(
        storage.point(present, 3)?.map(|point| point.value),
        Some(3.0)
    );
    assert!(storage.stats()?.metadata_cache_hits > hits_before);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            virtual_shards: 1,
            metadata_cache_max_bytes: CACHE_BYTES,
            l0_compaction_trigger: 16,
            l0_slowdown_trigger: 32,
            l0_stop_trigger: 64,
            ..OpenOptions::default()
        },
    )?;
    let cold = reopened.stats()?;
    assert_eq!(cold.metadata_cache_entries, 0);
    assert_eq!(cold.metadata_cache_bytes, 0);
    assert_eq!(cold.metadata_cache_misses, 0);
    assert_eq!(reopened.scan(present, 0, 10)?.count(), 3);
    let reopened_stats = reopened.stats()?;
    assert!(reopened_stats.metadata_cache_bytes <= CACHE_BYTES);
    assert!((1..=3).contains(&reopened_stats.metadata_cache_entries));
    reopened.close()
}

#[test]
fn managed_memory_limit_flushes_memtable_and_rejects_an_oversized_batch() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            memtable_max_bytes: 1_024,
            block_cache_max_bytes: 1_024,
            metadata_cache_max_bytes: 1_024,
            memory_max_bytes: 3 * 48,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 5);
    storage.write_batch(
        vec![
            point(series, 1, 1.0),
            point(series, 2, 2.0),
            point(series, 3, 3.0),
        ],
        Durability::Sync,
    )?;
    storage.write_batch(vec![point(series, 4, 4.0)], Durability::Sync)?;

    let stats = storage.stats()?;
    assert!(stats.memory_backpressure_events >= 1);
    assert!(stats.write_queue_memory_backpressure_events >= 1);
    assert!(stats.managed_memory_bytes <= stats.managed_memory_max_bytes);
    assert_eq!(stats.managed_memory_max_bytes, 3 * 48);
    assert_eq!(stats.managed_memtable_bytes, 48);
    assert_eq!(stats.active_memtable_bytes, 48);
    assert_eq!(stats.frozen_memtable_bytes, 0);
    assert_eq!(stats.l0_segments, 1);

    let rejected = storage.write_batch(
        vec![
            point(series, 5, 5.0),
            point(series, 6, 6.0),
            point(series, 7, 7.0),
            point(series, 8, 8.0),
        ],
        Durability::Sync,
    );
    assert!(matches!(rejected, Err(Error::ResourceLimit(_))));
    assert_eq!(storage.stats()?.visible_commit_id, 2);
    // Raw reads now share the work-memory budget. 144 bytes suffice for this
    // admission test, but cannot hold a disk block and its index.
    assert!(matches!(
        storage.scan(series, 0, 10),
        Err(Error::Overloaded(chimaera::OverloadReason::WorkMemory))
    ));
    storage.close()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            ..OpenOptions::default()
        },
    )?;
    let points = storage.scan(series, 0, 10)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(points.len(), 4);
    storage.close()
}

#[test]
fn live_snapshots_keep_their_memtable_memory_reserved() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            memtable_max_bytes: 1_024,
            block_cache_max_bytes: 1,
            metadata_cache_max_bytes: 1,
            memory_max_bytes: 2 * 48,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 6);

    storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    let first_snapshot = storage.scan(series, 0, 10)?;
    storage.flush()?;
    storage.write_batch(vec![point(series, 2, 2.0)], Durability::Sync)?;
    let second_snapshot = storage.scan(series, 2, 10)?;
    storage.flush()?;

    let pinned = storage.stats()?;
    assert_eq!(pinned.managed_memtable_bytes, 2 * 48);
    assert_eq!(pinned.pinned_memtable_bytes, 2 * 48);
    assert_eq!(pinned.active_memtable_bytes, 0);
    assert_eq!(pinned.frozen_memtable_bytes, 0);
    assert!(matches!(
        storage.enqueue_batch_with_options(
            vec![point(series, 3, 3.0)],
            Durability::Sync,
            chimaera::AdmissionOptions {
                deadline: Instant::now() + Duration::from_millis(20)
            }
        ),
        Err(Error::Overloaded(chimaera::OverloadReason::ManagedMemory))
    ));

    drop(first_snapshot);
    let partially_pinned = storage.stats()?;
    assert_eq!(partially_pinned.managed_memtable_bytes, 48);
    assert_eq!(partially_pinned.pinned_memtable_bytes, 48);
    storage.write_batch(vec![point(series, 3, 3.0)], Durability::Sync)?;
    drop(second_snapshot);
    storage.close()
}

#[test]
fn file_descriptor_budget_streams_many_compaction_inputs() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        flush_worker_count: 1,
        file_descriptor_max: 10,
        l0_compaction_trigger: 16,
        l0_slowdown_trigger: 32,
        l0_stop_trigger: 64,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 77);
    for timestamp_us in 0..12 {
        storage.write_batch(
            vec![point(series, timestamp_us, timestamp_us as f64)],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
    }
    assert_eq!(storage.stats()?.l0_segments, 12);
    assert_eq!(storage.scan(series, 0, 20)?.count(), 12);
    assert!(storage.compact()? > 0);
    assert_eq!(storage.scan(series, 0, 20)?.count(), 12);
    let stats = storage.stats()?;
    assert_eq!(stats.file_permit_capacity, 2);
    assert_eq!(stats.file_permits_in_use, 0);
    assert!(stats.peak_file_permits_in_use <= stats.file_permit_capacity);
    assert_eq!(stats.l0_segments, 0);
    assert_eq!(stats.l1_segments, 1);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(reopened.scan(series, 0, 20)?.count(), 12);
    assert!(reopened.stats()?.peak_file_permits_in_use <= 2);
    reopened.close()
}

#[test]
fn file_cache_keeps_enough_permits_for_compaction_progress() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            flush_worker_count: 1,
            file_descriptor_max: 11,
            block_cache_max_bytes: 1,
            l0_compaction_trigger: 16,
            l0_slowdown_trigger: 32,
            l0_stop_trigger: 64,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 81);
    for timestamp_us in 0..2 {
        storage.write_batch(
            vec![point(series, timestamp_us, timestamp_us as f64)],
            Durability::Sync,
        )?;
        storage.flush()?;
    }
    storage
        .group_stream(GroupRequest::fixed(vec![series], 0, 2, 1, 0)?)?
        .collect::<Result<Vec<_>>>()?;
    let cached = storage.stats()?;
    assert_eq!(cached.file_permit_capacity, 3);
    assert_eq!(cached.open_file_cache_entries, 1);
    assert!(cached.file_permit_capacity - cached.open_file_cache_entries >= 2);

    let other = storage.clone();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(other.compact());
    });
    assert!(
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("compaction must retain enough file permits")?
            > 0
    );
    assert_eq!(storage.scan(series, 0, 2)?.count(), 2);
    storage.close()
}

#[test]
fn cached_group_query_does_not_open_its_segment_file() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 76);
    storage.write_batch(
        (0..100)
            .map(|timestamp_us| point(series, timestamp_us, timestamp_us as f64))
            .collect(),
        Durability::Sync,
    )?;
    storage.flush()?;
    let request = || GroupRequest::fixed(vec![series], 0, 100, 100, 0);
    storage
        .group_stream(request()?)?
        .collect::<Result<Vec<_>>>()?;
    let hits_before = storage.stats()?.block_cache_hits;
    let opens_before = storage.stats()?.open_file_opens;
    let mut cached = storage.group_stream(request()?)?;
    assert!(cached.next().transpose()?.is_some());
    let stats = storage.stats()?;
    assert!(stats.block_cache_hits > hits_before);
    assert_eq!(stats.open_file_opens, opens_before);
    assert_eq!(stats.file_permits_in_use, stats.open_file_cache_entries);
    drop(cached);
    storage.close()
}

#[test]
fn wide_query_does_not_evict_a_hot_single_series_block() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        block_cache_max_bytes: 256 * 1024,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let hot = SeriesKey::new(1, 1, 1);
    let cold = (2..14)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    let mut points = Vec::with_capacity(13 * 4_096);
    for series in std::iter::once(&hot).chain(cold.iter()) {
        for timestamp_us in 0..4_096 {
            points.push(point(*series, timestamp_us, 1.0));
        }
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let single = || GroupRequest::fixed(vec![hot], 0, 4_096, 4_096, 0);
    storage.group(single()?)?;
    let hot_stats = storage.stats()?;
    assert_eq!(hot_stats.block_cache_entries, 1);

    storage.group(GroupRequest::fixed(cold, 0, 4_096, 4_096, 0)?)?;
    let after_wide = storage.stats()?;
    assert_eq!(
        after_wide.block_cache_entries,
        hot_stats.block_cache_entries
    );
    assert_eq!(after_wide.block_cache_bytes, hot_stats.block_cache_bytes);

    let hits_before = after_wide.block_cache_hits;
    storage.group(single()?)?;
    assert!(storage.stats()?.block_cache_hits > hits_before);
    storage.close()
}

#[test]
fn wide_query_plans_pack_reads_and_bounds_prefetch_memory() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: 1_000,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = (0..12)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    let mut points = Vec::with_capacity(series.len() * 5_000);
    for series_key in &series {
        for timestamp_us in 0..5_000 {
            points.push(point(*series_key, timestamp_us, timestamp_us as f64));
        }
    }
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let maximum_query_memory = 16 * 1024 * 1024;
    let measured = reopened.group(
        GroupRequest::fixed(series.clone(), 0, 5_000, 5_000, 0)?.with_options(QueryOptions {
            max_memory_bytes: maximum_query_memory,
            ..QueryOptions::default()
        })?,
    )?;
    assert_eq!(measured.rows.len(), 12);
    assert!(measured.stats.blocks_read >= 12);
    assert!(measured.stats.prefetched_blocks > 0);
    assert!(measured.stats.prefetch_hits > 0);
    assert!(measured.stats.payload_bytes_read > 0);
    assert!(measured.stats.metadata_bytes_read > 0);
    assert_eq!(measured.stats.file_opens, 1);
    assert!(measured.stats.planned_segment_reads >= 5);
    assert!(measured.stats.unique_planned_segments >= 5);
    assert_eq!(measured.stats.unique_planned_files, 1);
    assert!(measured.stats.index_roots_consulted >= measured.stats.unique_planned_segments);
    assert_eq!(measured.stats.index_roots_read, 0);
    assert!(measured.stats.index_pages_read > 0);
    assert!(measured.stats.planned_block_references >= measured.stats.unique_planned_blocks);
    assert_eq!(
        measured.stats.unused_prefetched_blocks,
        measured
            .stats
            .prefetched_blocks
            .saturating_sub(measured.stats.prefetch_hits)
    );
    assert_eq!(
        measured.stats.operation_bytes_decoded,
        measured.stats.decoded_bytes
    );
    assert!(measured.stats.peak_memory_bytes <= maximum_query_memory);
    assert!(measured.stats.file_opens * 5 <= measured.stats.blocks_read);
    assert!(measured.stats.file_opens * 5 <= measured.stats.planned_segment_reads);
    let stats = reopened.stats()?;
    assert_eq!(stats.open_file_opens, 1);
    assert!(stats.open_file_cache_hits + stats.open_file_cache_misses >= 1);
    assert!(stats.peak_file_permits_in_use <= stats.file_permit_capacity);
    let too_wide = GroupRequest::fixed(series, 0, 5_000, 5_000, 0)?.with_options(QueryOptions {
        max_memory_bytes: 2 * 1024,
        ..QueryOptions::default()
    })?;
    assert!(matches!(
        reopened.group_stream(too_wide),
        Err(Error::ResourceLimit(message)) if message.contains("memory")
    ));
    reopened.close()
}

#[test]
fn hybrid_grouping_does_not_deadlock_a_small_file_permit_pool() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            file_descriptor_max: 10,
            flush_worker_count: 1,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 77);
    storage.write_batch(
        (0..4_096)
            .map(|timestamp_us| point(series, timestamp_us, 1.0))
            .collect(),
        Durability::Sync,
    )?;
    storage.flush()?;
    storage.compact()?;
    for timestamp_us in 5_000..5_002 {
        storage.write_batch(vec![point(series, timestamp_us, 1.0)], Durability::Sync)?;
        storage.flush()?;
    }

    let request = GroupRequest::new(vec![series], 0, DAY)?.with_options(QueryOptions {
        deadline: Some(Instant::now() + Duration::from_secs(1)),
        ..QueryOptions::default()
    })?;
    let other = storage.clone();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result = other
            .group_stream(request)
            .and_then(|mut stream| stream.next().transpose());
        let _ = sender.send(result);
    });

    let row = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("hybrid query must not wait forever for its own file permits")?
        .expect("hybrid query must return its UTC-day row");
    assert_eq!(row.count, 4_098);
    assert_close(row.sum, 4_098.0);
    let stats = storage.stats()?;
    assert_eq!(stats.file_permit_capacity, 2);
    assert_eq!(stats.file_permits_in_use, stats.open_file_cache_entries);
    storage.close()
}

#[test]
fn shared_file_cache_avoids_query_permit_starvation() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            file_descriptor_max: 11,
            flush_worker_count: 1,
            block_cache_max_bytes: 1,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 79);
    storage.write_batch(
        vec![point(series, 1, 1.0), point(series, 2, 2.0)],
        Durability::Sync,
    )?;
    storage.flush()?;

    let request = || GroupRequest::fixed(vec![series], 0, 10, 1, 0);
    let mut first = storage.group_stream(request()?)?;
    let mut second = storage.group_stream(request()?)?;
    assert!(first.next().is_some());
    assert!(second.next().is_some());
    assert_eq!(storage.stats()?.file_permits_in_use, 1);

    let mut third = storage.group_stream(request()?.with_options(QueryOptions {
        deadline: Some(Instant::now() + Duration::from_millis(50)),
        ..QueryOptions::default()
    })?)?;
    let started = Instant::now();
    assert!(matches!(third.next(), Some(Ok(_))));
    assert!(started.elapsed() < Duration::from_secs(1));
    let shared = storage.stats()?;
    assert!(shared.open_file_cache_hits >= 2);
    assert_eq!(shared.open_file_opens, 1);
    assert_eq!(shared.file_permit_capacity, 3);

    drop(first);
    drop(second);
    drop(third);
    assert_eq!(storage.stats()?.file_permits_in_use, 1);
    storage.close()
}

#[test]
fn client_holding_streams_does_not_trigger_read_latency_pacing() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            l0_compaction_trigger: 64,
            l0_slowdown_trigger: 96,
            l0_stop_trigger: 128,
            read_latency_slowdown_threshold: Duration::from_millis(5),
            write_pacing_max_delay: Duration::from_millis(5),
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 80);
    storage.write_batch(vec![point(series, 1, 1.0)], Durability::Sync)?;
    let request = || GroupRequest::fixed(vec![series], 0, 10, 10, 0);

    for _ in 0..32 {
        drop(storage.group_stream(request()?)?);
    }
    for _ in 0..32 {
        let held = storage.group_stream(request()?)?;
        thread::sleep(Duration::from_millis(12));
        drop(held);
    }
    for timestamp_us in 2..6 {
        storage.write_batch(
            vec![point(series, timestamp_us, timestamp_us as f64)],
            Durability::Sync,
        )?;
    }
    let stats = storage.stats()?;
    assert!(!stats.write_pacing_active);
    assert_eq!(stats.write_pacing_read_latency_events, 0);
    storage.close()
}

#[test]
fn uncached_metadata_must_respect_query_and_engine_memory_limits() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            memtable_max_bytes: 1024 * 1024,
            memory_max_bytes: 4 * 1024 * 1024,
            block_cache_max_bytes: 1,
            metadata_cache_max_bytes: 1,
            ..OpenOptions::default()
        },
    )?;
    let points = (0..10_000)
        .map(|key| Point::new(SeriesKey::new(1, 1, key), 1, 7.0))
        .collect::<Result<Vec<_>>>()?;
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.close()?;
    drop(storage);

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            partition_span_us: DAY,
            memtable_max_bytes: 32 * 1024,
            memory_max_bytes: 64 * 1024,
            block_cache_max_bytes: 1,
            metadata_cache_max_bytes: 1,
            ..OpenOptions::default()
        },
    )?;
    let result = reopened.group(
        GroupRequest::new(vec![SeriesKey::new(1, 1, 1)], 0, DAY)?.with_options(QueryOptions {
            max_memory_bytes: 4 * 1024,
            ..QueryOptions::default()
        })?,
    );
    assert!(
        matches!(result, Err(Error::ResourceLimit(_))),
        "uncached metadata exceeded query and engine memory limits without rejection"
    );
    let measured = reopened.group(
        GroupRequest::new(vec![SeriesKey::new(1, 1, 1)], 0, DAY)?.with_options(QueryOptions {
            max_memory_bytes: 32 * 1024,
            ..QueryOptions::default()
        })?,
    )?;
    assert_eq!(measured.rows.len(), 1);
    assert_eq!(measured.rows[0].count, 1);
    // The paged root is verified as a stream: bytes read may exceed the live
    // memory budget, while memory remains bounded independently of root size.
    assert!(
        measured.stats.metadata_bytes_read < 128 * 1024,
        "streamed metadata read {} bytes",
        measured.stats.metadata_bytes_read
    );
    assert!(measured.stats.metadata_local_hits >= 1);
    reopened.close()?;
    Ok(())
}
