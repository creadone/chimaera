use chimaera::{
    DAY_US, Durability, GroupRequest, OpenOptions, Point, SeriesRef, Storage, StoragePolicyOptions,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The example owns this temporary directory; it is removed on exit.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("database");
    let storage = Storage::open(&path, OpenOptions::default())?;
    storage.create_space("demo")?;
    storage.create_policy(StoragePolicyOptions::new("ordered", DAY_US, DAY_US)?)?;
    storage.create_bucket("demo", "metrics", "ordered")?;
    let (series, _) = storage.resolve_series(&SeriesRef::new("demo", "metrics", 42))?;
    let start_us = 1_788_220_800_000_000; // 2026-09-01 00:00:00 UTC
    storage.write_batch(
        vec![
            Point::new(series, start_us, 10.0)?,
            Point::new(series, start_us + 1_000_000, 12.5)?,
        ],
        Durability::Sync,
    )?;
    let point = storage.point(series, start_us)?.ok_or("missing point")?;
    assert_eq!(point.value, 10.0);
    println!("point: {}", point.value);
    let grouped = storage.group(GroupRequest::new(
        vec![series],
        start_us,
        start_us + DAY_US,
    )?)?;
    assert_eq!(grouped.rows.len(), 1);
    assert_eq!(grouped.rows[0].count, 2);
    assert_eq!(grouped.rows[0].sum, 22.5);
    println!(
        "group: count={}, sum={}",
        grouped.rows[0].count, grouped.rows[0].sum
    );
    storage.close()?;
    drop(storage);

    let reopened = Storage::open(&path, OpenOptions::default())?;
    let persisted = reopened
        .point(series, start_us + 1_000_000)?
        .ok_or("missing persisted point")?;
    assert_eq!(persisted.value, 12.5);
    println!("reopen: {}", persisted.value);
    reopened.close()?;
    Ok(())
}
