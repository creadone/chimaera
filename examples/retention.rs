use chimaera::{
    AggregateResolution, Clock, DAY_US, Durability, GroupRequest, GroupSource, MINUTE_US,
    OpenOptions, Point, SeriesRef, Storage, StoragePolicyOptions,
};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

#[derive(Debug)]
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_us(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(TestClock(AtomicI64::new(2 * DAY_US)));
    let options = OpenOptions {
        clock: clock.clone(),
        virtual_shards: 1,
        partition_span_us: DAY_US,
        ..OpenOptions::default()
    };
    let storage = Storage::open(directory.path(), options.clone())?;
    storage.create_space("demo")?;
    storage.create_policy(
        StoragePolicyOptions::new("retained", MINUTE_US, DAY_US)?
            .with_retention_us(Some(2 * DAY_US))?
            .with_aggregate_retention_us(Some(5 * DAY_US), vec![AggregateResolution::Day])?,
    )?;
    storage.create_bucket("demo", "metrics", "retained")?;
    let (series, _) = storage.resolve_series(&SeriesRef::new("demo", "metrics", 42))?;
    storage.write_batch(
        (0..3)
            .map(|day| Point::new(series, day * DAY_US, (day + 1) as f64))
            .collect::<chimaera::Result<Vec<_>>>()?,
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    clock.0.store(4 * DAY_US, Ordering::SeqCst);
    storage.compact()?;
    let request = GroupRequest::new(vec![series], 0, 3 * DAY_US)?;
    let raw = storage.group(request.clone().with_source(GroupSource::Raw))?;
    let automatic = storage.group(request.clone())?;
    assert_eq!(raw.rows.iter().map(|r| r.count).sum::<u64>(), 1);
    assert_eq!(automatic.rows.iter().map(|r| r.count).sum::<u64>(), 3);
    assert!(storage.point(series, 0)?.is_none());
    assert!(matches!(
        storage.write_batch(vec![Point::new(series, 1, 99.0)?], Durability::Sync),
        Err(chimaera::Error::ExpiredPoint(_))
    ));
    assert_eq!(
        storage.series_info(series)?.retention.raw_available_from_us,
        Some(2 * DAY_US)
    );
    storage.close()?;
    let reopened = Storage::open(
        directory.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        reopened
            .group(request)?
            .rows
            .iter()
            .map(|r| r.count)
            .sum::<u64>(),
        3
    );
    reopened.close()?;
    assert!(Storage::verify(directory.path()).valid);
    println!("retention: raw count=1; Auto count=3; restored from disk count=3");
    Ok(())
}
