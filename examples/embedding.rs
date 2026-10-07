//! Open, write, read, close, and reopen through the public API.

use chimaera::{OpenOptions, Point, Result, SeriesKey, Storage};

fn main() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = Storage::open(directory.path(), OpenOptions::default())?;
    let key = SeriesKey::new(1, 1, 42);
    let points = vec![Point::new(key, 1, 10.0)?, Point::new(key, 2, 14.0)?];
    let receipt = storage.write_batch(points.clone())?;
    assert_eq!(receipt.point_count, 2);
    let read = storage.scan(key, 1, 3)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(read, points);
    storage.close()?;

    let reopened = Storage::open(
        directory.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(reopened.point(key, 2)?, Some(points[1]));
    reopened.close()?;
    let report = Storage::verify(directory.path());
    assert!(report.valid, "verification failed: {:?}", report.issues);
    println!("PASS: durable batch, exact scan, reopen, and verification");
    Ok(())
}
