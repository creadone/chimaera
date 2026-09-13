use super::*;

#[test]
fn write_read_upsert_out_of_order_and_late_points() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(1, 1, 7);
    storage.write_batch(
        vec![
            point(series, DAY + 20, 2.0),
            point(series, 10, 1.0),
            point(series, DAY + 10, 3.0),
            point(series, -10, 4.0),
        ],
        Durability::Sync,
    )?;
    storage.write_batch(vec![point(series, 10, 9.0)], Durability::Sync)?;
    storage.write_batch(
        vec![point(series, DAY + 20, 5.0), point(series, DAY + 20, 6.0)],
        Durability::Sync,
    )?;

    let actual = rows(&storage, vec![series])?;
    assert_eq!(actual.len(), 3);
    assert_eq!(
        actual.iter().map(|row| row.count).collect::<Vec<_>>(),
        [1, 1, 2]
    );
    assert_close(actual[0].sum, 4.0);
    assert_close(actual[1].sum, 9.0);
    assert_close(actual[2].sum, 9.0);
    assert!(
        actual
            .windows(2)
            .all(|pair| pair[0].bucket_start_us < pair[1].bucket_start_us)
    );
    storage.close()
}

#[test]
fn atomic_put_delete_order_uses_the_last_batch_ordinal() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(3, 2, 1);
    let deleted = storage.write_operations(
        vec![
            WriteOperation::Put(point(series, 10, 1.0)),
            WriteOperation::delete(series, 10),
        ],
        Durability::Sync,
    )?;
    assert_eq!(deleted.operation_count, 2);
    assert_eq!(deleted.point_count, 1);
    assert!(storage.point(series, 10)?.is_none());

    storage.write_operations(
        vec![
            WriteOperation::delete(series, 10),
            WriteOperation::Put(point(series, 10, 4.0)),
        ],
        Durability::Sync,
    )?;
    assert_eq!(
        storage.point(series, 10)?.map(|point| point.value),
        Some(4.0)
    );
    let stats = storage.stats()?;
    assert_eq!(stats.written_operations, 4);
    assert_eq!(stats.written_points, 2);
    assert_eq!(stats.deleted_points, 2);
    storage.close()
}

#[test]
fn enqueue_batch_returns_a_ticket_without_changing_atomic_order() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = SeriesKey::new(5, 5, 5);
    let ticket = storage.enqueue_operations(
        vec![
            WriteOperation::Put(point(series, 10, 1.0)),
            WriteOperation::delete(series, 10),
            WriteOperation::Put(point(series, 10, 3.0)),
        ],
        Durability::Sync,
    )?;
    let receipt = ticket.wait()?;
    assert_eq!(receipt.commit_id, 1);
    assert_eq!(receipt.operation_count, 3);
    assert_eq!(
        storage.point(series, 10)?.map(|point| point.value),
        Some(3.0)
    );

    let ignored = storage.enqueue_batch(vec![point(series, 20, 4.0)], Durability::Sync)?;
    drop(ignored);
    storage.checkpoint()?;
    assert_eq!(
        storage.point(series, 20)?.map(|point| point.value),
        Some(4.0)
    );
    storage.close()
}

#[test]
fn group_commit_batches_share_wal_sync_and_keep_commit_order() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        memtable_max_bytes: 48,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    let series = SeriesKey::new(5, 5, 6);
    let tickets = (0..32)
        .map(|timestamp_us| {
            storage.enqueue_batch(
                vec![point(series, timestamp_us, timestamp_us as f64)],
                Durability::GroupCommit,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let receipts = tickets
        .into_iter()
        .map(|ticket| ticket.wait())
        .collect::<Result<Vec<_>>>()?;
    assert!(
        receipts
            .iter()
            .enumerate()
            .all(|(index, receipt)| receipt.commit_id == index as u64 + 1 && receipt.durable)
    );
    let stats = storage.stats()?;
    assert_eq!(stats.group_commit_batches, 32);
    assert!(stats.group_commit_groups < stats.group_commit_batches);
    assert!(stats.wal_syncs >= stats.group_commit_groups);
    assert!(
        stats.wal_syncs
            <= stats
                .group_commit_groups
                .saturating_add(stats.group_commit_groups),
        "each group may add at most one WAL rotation sync when the tiny memtable is frozen"
    );
    assert_eq!(
        storage
            .scan(series, 0, 32)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        32
    );
    storage.checkpoint()?;
    storage.close()?;
    let reopened = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    assert_eq!(
        reopened
            .scan(series, 0, 32)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        32
    );
    reopened.close()
}
