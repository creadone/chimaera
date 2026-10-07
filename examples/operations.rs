//! Tickets, aggregates, cancellation, deletion, physical backup, and JSONL transfer.

use std::time::{Duration, Instant};

use chimaera::{
    AggregateResolution, AggregateSet, CancellationToken, Clock, CompressionProfile, DAY_US, Error,
    ExportOptions, GroupRequest, HOUR_US, ImportOptions, MINUTE_US, OpenOptions, Point,
    QueryOptions, Result, SeriesKey, SeriesRef, Storage, StoragePolicyOptions, SystemClock,
    WriteStatus,
};

fn read(storage: &Storage, key: SeriesKey, from: i64) -> Result<Vec<Point>> {
    storage.scan(key, from, from + MINUTE_US)?.collect()
}

fn assert_shared_handle<T: Send + Sync + Clone>() {}

fn main() -> Result<()> {
    assert_shared_handle::<Storage>();
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source");
    let snapshot_path = directory.path().join("snapshot");
    let restore_path = directory.path().join("restored");
    let import_path = directory.path().join("imported");
    let storage = Storage::open(&source_path, OpenOptions::default())?;
    storage.create_space("metrics")?;
    let policy = StoragePolicyOptions::new("history", HOUR_US, DAY_US)?
        .with_retention_us(Some(7 * DAY_US))?
        .with_aggregate_retention_us(Some(30 * DAY_US), vec![AggregateResolution::Minute])?
        .with_compression(CompressionProfile::hot_fast(), CompressionProfile::zstd())?;
    storage.create_policy(policy)?;
    storage.create_bucket("metrics", "host", "history")?;
    let first = storage
        .resolve_series(&SeriesRef::new("metrics", "host", 1))?
        .0;
    let second = storage
        .resolve_series(&SeriesRef::new("metrics", "host", 2))?
        .0;
    let start = SystemClock.now_us().div_euclid(MINUTE_US) * MINUTE_US - 10 * MINUTE_US;
    let points = vec![
        Point::new(first, start, 10.0)?,
        Point::new(first, start + 5_000_000, 14.0)?,
    ];
    let mut ticket = storage.enqueue_batch(points.clone())?;
    if matches!(ticket.wait_until(Instant::now()), WriteStatus::Pending) {
        println!("pending: continue waiting on the accepted ticket");
    }
    let receipt = ticket.wait()?; // Propagate failures; do not resubmit an uncertain batch.
    assert_eq!(receipt.point_count, 2);

    // The new point for another series must not escape a rejected batch.
    let rejected = storage.write_batch(vec![points[1], Point::new(second, start, 99.0)?]);
    assert!(matches!(rejected, Err(Error::Invalid(_))));
    assert!(storage.point(second, start)?.is_none());
    storage.write_batch(vec![Point::new(second, start, 20.0)?])?;

    let request = GroupRequest::fixed(vec![first], start, start + MINUTE_US, MINUTE_US, 0)?
        .with_aggregates(
            AggregateSet::MEAN | AggregateSet::POPULATION_STANDARD_DEVIATION | AggregateSet::RATE,
        );
    let rows = storage.group_stream(request)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].count, rows[0].sum, rows[0].mean),
        (2, 24.0, Some(12.0))
    );
    assert_eq!(rows[0].population_standard_deviation, Some(2.0));
    assert_eq!(rows[0].rate_per_second, Some(0.8));

    let cancel = CancellationToken::new();
    let mut cancelled = storage.scan_with_options(
        first,
        start,
        start + MINUTE_US,
        QueryOptions {
            cancellation: Some(cancel.clone()),
            ..QueryOptions::default()
        },
    )?;
    cancel.cancel();
    assert!(matches!(cancelled.next(), Some(Err(Error::QueryCancelled))));
    assert!(cancelled.next().is_none());
    drop(cancelled);

    let image = storage.snapshot(&snapshot_path)?;
    let restored_image = Storage::restore_snapshot(&snapshot_path, &restore_path)?;
    assert_eq!(image.commit_id, restored_image.commit_id);
    let mut dump = Vec::new();
    let exported = storage.export_jsonl(
        &mut dump,
        ExportOptions {
            gzip: true,
            ..ExportOptions::default()
        },
    )?;
    assert_eq!(exported.points, 3);

    let held = storage.scan(first, start, start + MINUTE_US)?;
    let deleted = storage.delete_series_batch(&[first, second, first])?;
    assert_eq!(deleted.operation_count, 2);
    assert!(storage.point(first, start)?.is_none());
    assert!(storage.point(second, start)?.is_none());
    assert_eq!(held.collect::<Result<Vec<_>>>()?, points);
    let recreated = Point::new(first, start, 30.0)?;
    storage.write_batch(vec![recreated])?;
    assert_eq!(read(&storage, first, start)?, [recreated]);

    let restored = Storage::open(
        &restore_path,
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(read(&restored, first, start)?, points);
    assert_eq!(restored.point(second, start)?.map(|p| p.value), Some(20.0));
    restored.close()?;
    let imported = Storage::open(&import_path, OpenOptions::default())?;
    let imported_result = imported.import_jsonl(
        dump.as_slice(),
        ImportOptions {
            gzip: true,
            ..ImportOptions::default()
        },
    )?;
    assert_eq!(imported_result.points_imported, 3);
    assert_eq!(read(&imported, first, start)?, points);
    imported.close()?;

    let drain = storage.drain(Instant::now() + Duration::from_secs(30))?;
    assert!(drain.checkpoint_commit_id.is_some());
    assert_eq!(drain.writes.outstanding_batches, 0);
    assert!(matches!(
        storage.write_batch(vec![Point::new(first, start + 1, 31.0)?]),
        Err(Error::AdmissionStopped)
    ));
    let close = storage.close_until(Instant::now() + Duration::from_secs(30))?;
    assert!(close.closed);
    for path in [&source_path, &snapshot_path, &restore_path, &import_path] {
        let report = Storage::verify(path);
        assert!(report.valid, "verification failed: {:?}", report.issues);
    }
    println!(
        "PASS: tickets, rejection atomicity, aggregates, cancellation, deletion, snapshot, import, drain, and close"
    );
    Ok(())
}
