use super::*;

fn fixture(mode: WriteMode) -> Result<(TempDir, Storage, Vec<SeriesKey>)> {
    let root = TempDir::new()?;
    let storage = Storage::open(
        root.path(),
        OpenOptions {
            virtual_shards: 1,
            history_seal_lag: None,
            ..OpenOptions::default()
        },
    )?;
    let space = storage.create_space("summary-test")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("summary-test", 10 * DAY, 30 * DAY)?.with_write_mode(mode),
    )?;
    let bucket = storage.create_bucket("summary-test", "points", policy.id)?;
    let series = (0..32)
        .map(|key| SeriesKey::new(space.id, bucket.id, key))
        .collect::<Vec<_>>();
    let heavy = series[31];
    let mut points = series[..31]
        .iter()
        .map(|key| point(*key, DAY + 10, 1.0))
        .collect::<Vec<_>>();
    points.extend((1..=8192).map(|index| point(heavy, DAY + index, (index % 113) as f64)));
    points.extend([point(heavy, 2 * DAY - 1, 7.0), point(heavy, 2 * DAY, 11.0)]);
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    Ok((root, storage, series))
}

#[test]
fn wide_summary_fallback_uses_persisted_day_when_one_block_crosses_midnight() -> Result<()> {
    for mode in [WriteMode::StrictlyIncreasing, WriteMode::Mutable] {
        let (_root, storage, series) = fixture(mode)?;
        let request = GroupRequest::new(series.clone(), DAY, 3 * DAY)?;
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert!(raw.stats.operations_scanned >= 8194);
        assert!(
            automatic.stats.operations_scanned < 64,
            "a boundary must not force 8192 whole-block points through MVCC"
        );
        assert!(automatic.stats.aggregate_records_read > 0);
        let heavy = automatic
            .rows
            .iter()
            .find(|row| row.series == Some(series[31]) && row.bucket_start_us == DAY)
            .unwrap();
        assert_eq!(heavy.count, 8193);
        storage.close()?;
    }
    Ok(())
}

#[test]
fn wide_summary_fallback_preserves_half_open_clipped_days() -> Result<()> {
    let (_root, storage, series) = fixture(WriteMode::StrictlyIncreasing)?;
    for (from, to) in [
        (DAY + 2000, 3 * DAY),
        (DAY, 2 * DAY),
        (DAY, 2 * DAY - 1),
        (DAY + 4096, 2 * DAY + 1),
    ] {
        let request = GroupRequest::new(series.clone(), from, to)?;
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows, "range [{from}, {to})");
    }
    storage.close()
}

#[test]
fn wide_summary_fallback_preserves_strict_tail_and_pinned_snapshot() -> Result<()> {
    let (_root, storage, series) = fixture(WriteMode::StrictlyIncreasing)?;
    let request = GroupRequest::new(series.clone(), DAY, 3 * DAY)?;
    let before = storage.group(request.clone().with_source(GroupSource::Raw))?;
    let old = storage.group_stream(request.clone())?;
    storage.write_batch(
        (1..=32)
            .map(|index| point(series[31], 2 * DAY + index, 1.0))
            .collect(),
        Durability::Sync,
    )?;
    assert_eq!(old.collect::<Result<Vec<_>>>()?, before.rows);
    for checkpoint in [false, true] {
        if checkpoint {
            storage.checkpoint()?;
        }
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.clone().with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert_eq!(automatic.stats.aggregate_base_point_lookups, 0);
    }
    storage.close()
}

#[test]
fn wide_summary_fallback_rejects_overlapping_versions_and_keeps_batch_ordinal() -> Result<()> {
    let (_root, storage, series) = fixture(WriteMode::Mutable)?;
    let request = GroupRequest::new(series.clone(), DAY, 3 * DAY)?;
    let before = storage.group(request.clone().with_source(GroupSource::Raw))?;
    let old = storage.group_stream(request.clone())?;
    let key = series[31];
    storage.write_operations(
        vec![
            WriteOperation::Put(point(key, DAY + 4096, 123.0)),
            WriteOperation::delete(key, DAY + 4096),
            WriteOperation::delete(key, DAY + 4097),
            WriteOperation::Put(point(key, DAY + 4097, -13.0)),
            WriteOperation::delete(key, 2 * DAY - 1),
            WriteOperation::Put(point(key, 2 * DAY, 17.0)),
        ],
        Durability::Sync,
    )?;
    assert_eq!(old.collect::<Result<Vec<_>>>()?, before.rows);
    for stage in 0..3 {
        match stage {
            1 => {
                storage.checkpoint()?;
            }
            2 => {
                storage.compact()?;
            }
            _ => (),
        }
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.clone().with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        let heavy = automatic
            .rows
            .iter()
            .find(|row| row.series == Some(key) && row.bucket_start_us == DAY)
            .unwrap();
        assert_eq!(heavy.count, 8191);
    }
    storage.close()
}

#[test]
fn wide_summary_fallback_keeps_l2_corrections_and_recovery_exact() -> Result<()> {
    let (root, storage, series) = fixture(WriteMode::Mutable)?;
    storage.close()?;
    drop(storage);
    let options = OpenOptions {
        virtual_shards: 1,
        history_seal_lag: Some(Duration::from_secs(10 * 86_400)),
        history_window: Duration::from_secs(3 * 86_400),
        history_rewrite_trigger: 4,
        ..OpenOptions::default()
    };
    let storage = Storage::open(root.path(), options.clone())?;
    storage.write_batch(vec![point(series[0], 16 * DAY, 1.0)], Durability::Sync)?;
    storage.checkpoint()?;
    storage.compact()?;
    assert!(storage.stats()?.sealed_l2_segments > 0);
    let request = GroupRequest::new(series.clone(), DAY, 3 * DAY)?;
    let before = storage.group(request.clone())?;
    assert!(before.stats.operations_scanned < 64);
    let old = storage.group_stream(request.clone())?;
    storage.write_operations(
        vec![
            WriteOperation::Put(point(series[31], DAY + 4096, -31.0)),
            WriteOperation::delete(series[31], DAY + 4097),
        ],
        Durability::Sync,
    )?;
    assert_eq!(old.collect::<Result<Vec<_>>>()?, before.rows);
    storage.checkpoint()?;
    assert!(storage.stats()?.l0_segments > 0);
    let expected = storage.group(request.clone().with_source(GroupSource::Raw))?;
    assert_eq!(storage.group(request.clone())?.rows, expected.rows);
    storage.close()?;
    drop(storage);
    let reopened = Storage::open(root.path(), options)?;
    assert_eq!(reopened.group(request)?.rows, expected.rows);
    reopened.close()
}
