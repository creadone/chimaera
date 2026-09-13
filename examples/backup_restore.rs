use chimaera::{Durability, OpenOptions, Point, SeriesKey, Storage};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let snapshot = directory.path().join("snapshot");
    let restored = directory.path().join("restored");
    let storage = Storage::open(&source, OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 42);
    storage.write_batch(vec![Point::new(series, 1, 10.0)?], Durability::Sync)?;
    let saved = storage.snapshot(&snapshot)?;
    storage.write_batch(vec![Point::new(series, 2, 20.0)?], Durability::Sync)?;
    let copy = Storage::restore_snapshot(&snapshot, &restored)?;
    assert_eq!(copy.commit_id, saved.commit_id);
    let verified = Storage::verify(&restored);
    assert!(verified.valid && !verified.repair_needed);
    let reader = Storage::open(
        &restored,
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reader.point(series, 1)?.map(|p| p.value), Some(10.0));
    assert!(reader.point(series, 2)?.is_none());
    reader.close()?;
    storage.close()?;
    println!("backup: verified; snapshot contains first point only");
    Ok(())
}
