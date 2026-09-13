use super::*;

#[test]
fn strictly_increasing_policy_rejects_mutations_and_skips_overlay_lookups() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: DAY,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("production")?;
    let mutable = storage.create_policy(
        StoragePolicyOptions::new("mutable", MINUTE, DAY)?.with_write_mode(WriteMode::Mutable),
    )?;
    let strict = storage.create_policy(
        StoragePolicyOptions::new("strict", MINUTE, DAY)?
            .with_write_mode(WriteMode::StrictlyIncreasing),
    )?;
    storage.create_bucket("production", "mutable", mutable.id)?;
    storage.create_bucket("production", "strict", strict.id)?;
    let mutable_ref = SeriesRef::new("production", "mutable", 1);
    let strict_ref = SeriesRef::new("production", "strict", 1);
    let mutable_series = storage.resolve_series(&mutable_ref)?.0;
    let strict_series = storage.resolve_series(&strict_ref)?.0;

    let base = |series| {
        (0..100_i64)
            .map(|offset| Point::new(series, offset * MINUTE, offset as f64))
            .collect::<Result<Vec<_>>>()
    };
    storage.write_batch(base(mutable_series)?, Durability::Async)?;
    storage.write_batch(base(strict_series)?, Durability::Async)?;
    storage.checkpoint()?;
    storage.compact()?;

    let tail = |series| {
        (100..200_i64)
            .map(|offset| Point::new(series, offset * MINUTE, offset as f64))
            .collect::<Result<Vec<_>>>()
    };
    storage.write_batch(tail(mutable_series)?, Durability::Async)?;
    storage.write_batch(tail(strict_series)?, Durability::Async)?;

    assert!(matches!(
        storage.upsert(strict_ref.clone(), 199 * MINUTE, 1.0, Durability::Async),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        storage.upsert(strict_ref.clone(), 150 * MINUTE, 1.0, Durability::Async),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        storage.delete_named_point(strict_ref.clone(), 150 * MINUTE, Durability::Async),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        storage.write_batch(
            vec![
                Point::new(strict_series, 201 * MINUTE, 1.0)?,
                Point::new(strict_series, 200 * MINUTE, 1.0)?,
            ],
            Durability::Async,
        ),
        Err(Error::Invalid(_))
    ));

    let request = |series| {
        GroupRequest::new(vec![series], 0, DAY)
            .map(|request| request.with_aggregates(AggregateSet::ALL))
    };
    let mutable_result = storage.group(request(mutable_series)?)?;
    let strict_result = storage.group(request(strict_series)?)?;
    assert_eq!(mutable_result.rows[0].count, 200);
    assert_eq!(strict_result.rows[0].count, 200);
    // Both fresh tails start after the persisted base range, so neither mode
    // needs point lookups. Strict mode additionally guarantees that property.
    assert_eq!(mutable_result.stats.aggregate_base_point_lookups, 0);
    assert_eq!(strict_result.stats.aggregate_base_point_lookups, 0);
    assert_eq!(strict_result.rows[0].source_resolution, None);
    let stats = storage.stats()?;
    assert_eq!(stats.strict_series_watermarks, 1);
    assert_eq!(stats.managed_series_watermark_bytes, 64);
    let mut expected = mutable_result.rows[0];
    expected.series = Some(strict_series);
    assert_extended_close(&strict_result.rows[0], &expected);

    storage.checkpoint()?;
    storage.close()?;
    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert!(matches!(
        reopened.upsert(strict_ref.clone(), 199 * MINUTE, 1.0, Durability::Sync),
        Err(Error::Invalid(_))
    ));
    reopened.upsert(strict_ref, 200 * MINUTE, 200.0, Durability::Sync)?;
    let stats = reopened.stats()?;
    assert_eq!(stats.strict_series_watermarks, 1);
    assert_eq!(stats.managed_series_watermark_bytes, 64);
    reopened.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    Ok(())
}

#[test]
fn strict_policy_seal_rejects_old_new_series_and_far_future_before_wal() -> Result<()> {
    let temporary = TempDir::new()?;
    let now_us = 10 * DAY;
    let clock = Arc::new(ManualClock::new(now_us));
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            history_seal_lag: Some(Duration::from_secs(60 * 60)),
            history_window: Duration::from_secs(60 * 60),
            clock,
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("production")?;
    let sealed = storage.create_policy(
        StoragePolicyOptions::new("sealed", MINUTE, DAY)?
            .with_max_future_skew_us(Some(60 * MINUTE))?,
    )?;
    let overlay = storage.create_policy(
        StoragePolicyOptions::new("overlay", MINUTE, DAY)?.with_max_future_skew_us(None)?,
    )?;
    let sealed_bucket = storage.create_bucket("production", "sealed", sealed.id)?;
    let overlay_bucket = storage.create_bucket("production", "overlay", overlay.id)?;
    let sealed_first = SeriesKey::new(1, sealed_bucket.id, 1);
    let sealed_new = SeriesKey::new(1, sealed_bucket.id, 2);
    let overlay_first = SeriesKey::new(1, overlay_bucket.id, 1);
    let overlay_new = SeriesKey::new(1, overlay_bucket.id, 2);

    storage.write_batch(vec![point(sealed_first, now_us, 1.0)], Durability::Sync)?;
    let visible_commit = storage.stats()?.visible_commit_id;
    assert!(matches!(
        storage.write_batch(
            vec![point(sealed_new, now_us - 2 * 60 * MINUTE, 2.0)],
            Durability::Sync,
        ),
        Err(Error::ExpiredPoint(_))
    ));
    assert!(matches!(
        storage.write_batch(
            vec![point(sealed_first, now_us + 60 * MINUTE + 1, 2.0)],
            Durability::Sync,
        ),
        Err(Error::Invalid(_))
    ));
    assert_eq!(storage.stats()?.visible_commit_id, visible_commit);

    storage.write_batch(vec![point(overlay_first, now_us, 1.0)], Durability::Sync)?;
    storage.write_batch(
        vec![point(overlay_new, now_us - 2 * 60 * MINUTE, 2.0)],
        Durability::Sync,
    )?;
    storage.close()
}
