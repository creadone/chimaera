use super::*;

#[test]
fn storage_lock_excludes_a_second_writer_and_releases_on_close() -> Result<()> {
    let temporary = TempDir::new()?;
    let first = Storage::open(temporary.path(), OpenOptions::default())?;
    let stale_clone = first.clone();
    let second = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    );
    assert!(matches!(second, Err(chimaera::Error::Lock(_))));

    first.close()?;
    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert!(matches!(
        stale_clone.point(SeriesKey::new(1, 1, 1), 0),
        Err(chimaera::Error::Closed)
    ));
    reopened.close()
}

#[test]
fn dropping_the_last_storage_handle_stops_workers_and_releases_the_lock() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions::default();
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 12);
    storage.write_batch(vec![point(series, 1, 12.0)], Durability::Sync)?;
    drop(storage);

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        reopened.point(series, 1)?.map(|point| point.value),
        Some(12.0)
    );
    reopened.close()
}

#[test]
fn checkpoint_and_reopen_preserve_exact_results() -> Result<()> {
    let temporary = TempDir::new()?;
    let series = SeriesKey::new(2, 3, 11);
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let receipt = storage.write_batch(
        vec![point(series, 1, 1.25), point(series, DAY + 1, 2.5)],
        Durability::Sync,
    )?;
    assert_eq!(storage.checkpoint()?, receipt.commit_id);
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let actual = rows(&reopened, vec![series])?;
    assert_eq!(actual.iter().map(|row| row.count).sum::<u64>(), 2);
    assert_close(actual.iter().map(|row| row.sum).sum(), 3.75);
    assert_eq!(reopened.stats()?.visible_commit_id, receipt.commit_id);
    reopened.close()
}

#[test]
fn cancelled_group_stream_releases_storage_lock_before_drop() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions::default();
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 82);
    storage.write_batch(
        vec![point(series, 0, 1.0), point(series, 1, 2.0)],
        Durability::Sync,
    )?;
    let cancellation = CancellationToken::new();
    let mut stream = storage.group_stream(
        GroupRequest::fixed(vec![series], 0, 2, 1, 0)?.with_options(QueryOptions {
            cancellation: Some(cancellation.clone()),
            ..QueryOptions::default()
        })?,
    )?;
    assert!(matches!(stream.next(), Some(Ok(_))));
    cancellation.cancel();
    assert!(matches!(stream.next(), Some(Err(Error::QueryCancelled))));
    storage.close()?;

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert!(stream.next().is_none());
    reopened.close()
}

#[test]
fn automatic_checkpoint_honors_wal_interval_and_cold_idle_triggers() -> Result<()> {
    let wal_directory = TempDir::new()?;
    let wal_storage = Storage::open(
        wal_directory.path(),
        OpenOptions {
            wal_checkpoint_bytes: 1,
            checkpoint_interval: Duration::from_secs(60),
            cold_flush_idle_interval: Duration::from_secs(60),
            ..OpenOptions::default()
        },
    )?;
    let raw_series = SeriesKey::new(1, 1, 1);
    let wal_commit = wal_storage
        .write_batch(vec![point(raw_series, 1, 1.0)], Durability::Sync)?
        .commit_id;
    wait_for_checkpoint(&wal_storage, wal_commit)?;
    assert_eq!(wal_storage.stats()?.background_checkpoints, 1);
    wal_storage.close()?;

    let interval_directory = TempDir::new()?;
    let interval_storage = Storage::open(
        interval_directory.path(),
        OpenOptions {
            wal_checkpoint_bytes: u64::MAX,
            checkpoint_interval: Duration::from_millis(25),
            cold_flush_idle_interval: Duration::from_secs(60),
            ..OpenOptions::default()
        },
    )?;
    let interval_commit = interval_storage
        .write_batch(vec![point(raw_series, 2, 2.0)], Durability::Sync)?
        .commit_id;
    wait_for_checkpoint(&interval_storage, interval_commit)?;
    assert_eq!(interval_storage.stats()?.background_checkpoints, 1);
    interval_storage.close()?;

    let cold_directory = TempDir::new()?;
    let clock = Arc::new(ManualClock::new(10 * DAY));
    let cold_storage = Storage::open(
        cold_directory.path(),
        OpenOptions {
            clock,
            wal_checkpoint_bytes: u64::MAX,
            checkpoint_interval: Duration::from_secs(60),
            cold_flush_idle_interval: Duration::from_millis(25),
            ..OpenOptions::default()
        },
    )?;
    cold_storage.create_space("production")?;
    let policy = cold_storage.create_policy(
        StoragePolicyOptions::new("tiered", DAY, DAY)?.with_write_mode(WriteMode::Mutable),
    )?;
    cold_storage.create_bucket("production", "measurements", policy.id)?;
    let named_series = SeriesRef::new("production", "measurements", 1);
    let hot_commit = cold_storage
        .upsert(named_series.clone(), 10 * DAY, 3.0, Durability::Sync)?
        .commit_id;
    thread::sleep(Duration::from_millis(100));
    assert_eq!(cold_storage.stats()?.checkpoint_commit_id, 0);

    let cold_commit = cold_storage
        .upsert(named_series, DAY, 4.0, Durability::Sync)?
        .commit_id;
    assert!(cold_commit > hot_commit);
    wait_for_checkpoint(&cold_storage, cold_commit)?;
    let cold_stats = cold_storage.stats()?;
    assert_eq!(cold_stats.background_checkpoints, 1);
    assert!(cold_stats.background_checkpoint_seconds > 0.0);
    cold_storage.close()
}

#[test]
fn close_does_not_wait_for_a_reader_of_retired_segments() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        l0_compaction_trigger: 16,
        l0_slowdown_trigger: 32,
        l0_stop_trigger: 64,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(1, 1, 78);
    storage.write_batch(
        vec![point(series, 1, 1.0), point(series, 2, 2.0)],
        Durability::Sync,
    )?;
    storage.flush()?;
    let held = storage.scan(series, 0, 10)?;
    storage.compact()?;
    assert!(storage.stats()?.retired_segment_groups >= 1);

    let other = storage.clone();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(other.close());
    });
    receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("close must not wait for a user-owned snapshot")?;

    assert!(matches!(
        Storage::open(
            temporary.path(),
            OpenOptions {
                create_if_missing: false,
                ..options.clone()
            }
        ),
        Err(Error::Lock(_))
    ));
    let old = held.collect::<Result<Vec<_>>>()?;
    assert_eq!(old.len(), 2);
    assert_close(old.iter().map(|point| point.value).sum(), 3.0);

    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(reopened.scan(series, 0, 10)?.count(), 2);
    reopened.close()
}
