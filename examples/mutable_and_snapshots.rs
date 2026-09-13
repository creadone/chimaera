use chimaera::{
    Durability, OpenOptions, Point, SeriesRef, Storage, StoragePolicyOptions, WriteMode,
    WriteOperation,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Storage::open(directory.path(), OpenOptions::default())?;
    storage.create_space("demo")?;
    storage.create_policy(
        StoragePolicyOptions::new("mutable", 100, 1000)?.with_write_mode(WriteMode::Mutable),
    )?;
    storage.create_bucket("demo", "metrics", "mutable")?;
    let (series, _) = storage.resolve_series(&SeriesRef::new("demo", "metrics", 42))?;
    storage.write_batch(
        vec![
            Point::new(series, 0, 10.0)?,
            Point::new(series, 10, 20.0)?,
            Point::new(series, 20, 30.0)?,
        ],
        Durability::Sync,
    )?;
    let old = storage.scan(series, 0, 20)?;
    storage.write_operations(
        vec![
            WriteOperation::Put(Point::new(series, 0, 11.0)?),
            WriteOperation::delete(series, 0),
            WriteOperation::Put(Point::new(series, 0, 12.0)?),
            WriteOperation::delete(series, 10),
        ],
        Durability::Sync,
    )?;
    let current = storage
        .scan(series, 0, 20)?
        .collect::<chimaera::Result<Vec<_>>>()?;
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].value, 12.0);
    storage.close()?;
    assert!(matches!(
        Storage::open(directory.path(), OpenOptions::default()),
        Err(chimaera::Error::Lock(_))
    ));
    let previous = old.collect::<chimaera::Result<Vec<_>>>()?;
    assert_eq!(
        previous.iter().map(|p| p.value).collect::<Vec<_>>(),
        [10.0, 20.0]
    );
    let reopened = Storage::open(
        directory.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reopened.point(series, 20)?.map(|p| p.value), Some(30.0));
    reopened.close()?;
    println!("snapshot: [10, 20]; current: [12]; upper bound 20 excluded");
    Ok(())
}
