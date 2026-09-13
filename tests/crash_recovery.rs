use std::env;
use std::fs;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use chimaera::{
    CompressionProfile, Durability, GroupRequest, OpenOptions, Point, Result, SeriesKey, SeriesRef,
    Storage, StoragePolicyOptions, WriteMode, WriteOperation,
};
use tempfile::TempDir;

const HELPER_MODE: &str = "CHIMAERA_CRASH_HELPER_MODE";
const HELPER_STORAGE: &str = "CHIMAERA_CRASH_HELPER_STORAGE";
const TEST_FAILPOINT: &str = "CHIMAERA_TEST_FAILPOINT";
const TEST_WAL_PARTIAL_COMMIT: &str = "CHIMAERA_TEST_WAL_PARTIAL_COMMIT";
const TEST_WAL_PARTIAL_BYTES: &str = "CHIMAERA_TEST_WAL_PARTIAL_BYTES";
const TEST_WAL_SYNC_ERROR: &str = "CHIMAERA_TEST_WAL_SYNC_ERROR";
const DAY_US: i64 = 86_400_000_000;

fn fixture_points(series: SeriesKey) -> Result<Vec<Point>> {
    Ok(vec![
        Point::new(series, DAY_US + 7, 2.5)?,
        Point::new(series, 11, 1.25)?,
        Point::new(series, 11, 4.0)?,
    ])
}

#[test]
fn crash_process_helper() -> Result<()> {
    let Ok(mode) = env::var(HELPER_MODE) else {
        return Ok(());
    };
    let root = env::var_os(HELPER_STORAGE)
        .ok_or_else(|| chimaera::Error::Invalid("missing crash helper storage".into()))?;
    let open_started = Instant::now();
    let storage = Storage::open(
        root,
        OpenOptions {
            checkpoint_on_close: mode != "returned_wal_error",
            ..OpenOptions::default()
        },
    )?;
    let open_seconds = open_started.elapsed().as_secs_f64();
    let series = SeriesKey::new(9, 8, 7);
    match mode.as_str() {
        "zstd_wal" => {
            let space = storage.create_space("zstd")?;
            let policy = storage.create_policy(
                StoragePolicyOptions::new("zstd", DAY_US, DAY_US)?
                    .with_compression(CompressionProfile::columnar(), CompressionProfile::zstd())?
                    .with_write_mode(WriteMode::Mutable),
            )?;
            let bucket = storage.create_bucket("zstd", "data", policy.id)?;
            let series = SeriesKey::new(space.id, bucket.id, 1);
            storage.write_batch(
                vec![Point::new(series, 1, 1.0)?, Point::new(series, 2, 2.0)?],
                Durability::Sync,
            )?;
            storage.checkpoint()?;
            storage.write_operations(
                vec![
                    Point::new(series, 1, 3.0)?.into(),
                    WriteOperation::delete(series, 2),
                    Point::new(series, 3, 4.0)?.into(),
                ],
                Durability::Sync,
            )?;
        }
        "initialize" => {
            storage.write_batch(fixture_points(series)?, Durability::Sync)?;
            storage.checkpoint()?;
            storage.close()?;
            return Ok(());
        }
        "inspect" => {
            let stats = storage.stats()?;
            let read_started = Instant::now();
            let points = storage
                .scan(series, 0, 2 * DAY_US)?
                .collect::<Result<Vec<_>>>()?;
            let read_ready_seconds = read_started.elapsed().as_secs_f64();
            println!(
                "[chimaera crash inspect] {}",
                serde_json::json!({
                    "open_seconds": open_seconds,
                    "engine_recovery_seconds": stats.recovery_seconds,
                    "first_read_seconds": read_ready_seconds,
                    "read_ready_seconds": open_seconds + read_ready_seconds,
                    "write_ready_seconds": open_seconds,
                    "visible_commit_id": stats.visible_commit_id,
                    "checkpoint_commit_id": stats.checkpoint_commit_id,
                    "points": points.len(),
                    "healthy": stats.healthy,
                    "background_errors": stats.background_errors,
                })
            );
            storage.close()?;
            return Ok(());
        }
        "wal" | "flush" | "checkpoint" => {
            storage.write_batch(fixture_points(series)?, Durability::Sync)?;
            if mode == "checkpoint" {
                storage.checkpoint()?;
            } else if mode == "flush" {
                storage.flush()?;
            }
        }
        "large_wal" => {
            let points = (0..512)
                .map(|timestamp_us| Point::new(series, timestamp_us, timestamp_us as f64))
                .collect::<Result<Vec<_>>>()?;
            storage.write_batch(points, Durability::Sync)?;
        }
        "fail_write" => {
            storage.write_batch(fixture_points(series)?, Durability::Sync)?;
        }
        "fail_flush" => {
            storage.write_batch(fixture_points(series)?, Durability::Sync)?;
            storage.flush()?;
        }
        "fail_checkpoint" => {
            storage.write_batch(fixture_points(series)?, Durability::Sync)?;
            storage.checkpoint()?;
        }
        "fail_compact" => {
            storage.compact()?;
            std::thread::sleep(Duration::from_secs(1));
        }
        "returned_wal_error" => {
            storage.write_batch(vec![Point::new(series, 1, 1.0)?], Durability::Sync)?;
            let mut ticket =
                storage.enqueue_batch(vec![Point::new(series, 2, 2.0)?], Durability::Sync)?;
            assert!(matches!(
                ticket.wait_until(std::time::Instant::now() + Duration::from_secs(5)),
                chimaera::WriteStatus::OutcomeUnknown(chimaera::Error::Io(_))
            ));
            assert!(matches!(ticket.wait(), Err(chimaera::Error::Io(_))));
            assert!(matches!(
                storage.write_batch(vec![Point::new(series, 3, 3.0)?], Durability::Sync,),
                Err(chimaera::Error::Corruption(_))
            ));
            let stats = storage.stats()?;
            assert!(!stats.healthy);
            assert!(stats.background_errors >= 1);
            assert!(stats.last_background_error.is_some());
            assert!(storage.close().is_err());
            return Ok(());
        }
        "returned_group_wal_error" => {
            let tickets = (1..=3)
                .map(|timestamp_us| {
                    storage.enqueue_batch(
                        vec![Point::new(series, timestamp_us, timestamp_us as f64)?],
                        Durability::GroupCommit,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            for mut ticket in tickets {
                assert!(matches!(
                    ticket.wait_until(std::time::Instant::now() + Duration::from_secs(5)),
                    chimaera::WriteStatus::OutcomeUnknown(_)
                ));
                assert!(ticket.wait().is_err());
            }
            let stats = storage.stats()?;
            assert!(!stats.healthy);
            assert_eq!(stats.visible_commit_id, 0);
            assert_eq!(stats.group_commit_groups, 0);
            assert!(storage.close().is_err());
            return Ok(());
        }
        _ => {
            return Err(chimaera::Error::Invalid("invalid crash helper mode".into()));
        }
    }
    std::process::abort();
}

fn crash_storage(mode: &str) -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let executable = env::current_exe()?;
    let status = Command::new(executable)
        .arg("--exact")
        .arg("crash_process_helper")
        .arg("--nocapture")
        .env(HELPER_MODE, mode)
        .env(HELPER_STORAGE, temporary.path())
        .status()?;
    assert!(
        !status.success(),
        "crash helper unexpectedly exited cleanly"
    );
    Ok(temporary)
}

#[test]
fn zstd_segments_recover_with_wal_updates_after_process_abort() -> Result<()> {
    let temporary = crash_storage("zstd_wal")?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    let series = storage
        .resolve_series(&SeriesRef::new("zstd", "data", 1))?
        .0;
    assert_eq!(
        storage.point(series, 1)?.map(|point| point.value),
        Some(3.0)
    );
    assert!(storage.point(series, 2)?.is_none());
    assert_eq!(
        storage.point(series, 3)?.map(|point| point.value),
        Some(4.0)
    );
    storage.checkpoint()?;
    storage.compact()?;
    storage.close()?;
    let report = Storage::verify(temporary.path());
    assert!(report.valid, "{:?}", report.issues);
    Ok(())
}

fn assert_recovers_after(mode: &str) -> Result<()> {
    let temporary = crash_storage(mode)?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(9, 8, 7);
    let rows = storage
        .group_stream(GroupRequest::new(vec![series], 0, 2 * DAY_US)?)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().map(|row| row.count).sum::<u64>(), 2);
    let sum = rows.iter().map(|row| row.sum).sum::<f64>();
    assert!((sum - 6.5).abs() <= 6.5e-12, "unexpected sum {sum}");
    storage.close()
}

#[test]
fn recovery_rejects_a_wal_frame_above_its_memory_budget_without_mutating_wal() -> Result<()> {
    let temporary = crash_storage("large_wal")?;
    let wal_path = fs::read_dir(temporary.path().join("wal"))?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "tsw"))
        .expect("crash helper created a WAL");
    let wal_before = fs::read(&wal_path)?;
    let error = match Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            memtable_max_bytes: 4 * 1024,
            block_cache_max_bytes: 4 * 1024,
            metadata_cache_max_bytes: 4 * 1024,
            block_load_max_bytes: 4 * 1024,
            memory_max_bytes: 4 * 1024,
            ..OpenOptions::default()
        },
    ) {
        Ok(_) => panic!("oversized recovery frame must respect the work-memory limit"),
        Err(error) => error,
    };
    assert!(matches!(error, chimaera::Error::ResourceLimit(_)));
    assert_eq!(fs::read(&wal_path)?, wal_before);

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(
        storage
            .scan(SeriesKey::new(9, 8, 7), 0, 512)?
            .collect::<Result<Vec<_>>>()?
            .len(),
        512
    );
    storage.close()
}

fn assert_reopens_after_flush(
    mode: WriteMode,
    checkpoint: bool,
    include_wal_tail: bool,
) -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        checkpoint_on_close: false,
        wal_checkpoint_bytes: u64::MAX,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("audit")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("audit", 60_000_000, DAY_US)?.with_write_mode(mode),
    )?;
    storage.create_bucket("audit", "points", policy.id)?;
    let series = storage
        .resolve_series(&SeriesRef::new("audit", "points", 1))?
        .0;
    let timestamp_us = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_micros(),
    )
    .expect("timestamp fits i64");
    let first = Point::new(series, timestamp_us, 42.0)?;
    let first_receipt = storage.write_batch(vec![first], Durability::Sync)?;
    storage.flush()?;
    if checkpoint {
        storage.checkpoint()?;
    }
    let mut expected = vec![first];
    if include_wal_tail {
        let tail = Point::new(series, timestamp_us + 1, 43.0)?;
        storage.write_batch(vec![tail], Durability::Sync)?;
        expected.push(tail);
    }
    storage.close()?;
    drop(storage);

    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    assert_eq!(
        persisted["materialized_commit"].as_u64(),
        Some(first_receipt.commit_id)
    );
    let verification = Storage::verify(temporary.path());
    assert!(verification.valid, "{:?}", verification.issues);
    let reopened = Storage::open(temporary.path(), options)?;
    let points = reopened
        .scan(series, timestamp_us, timestamp_us + 2)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(points, expected);
    reopened.close()
}

#[test]
fn mutable_flushed_wal_reopens_control() -> Result<()> {
    assert_reopens_after_flush(WriteMode::Mutable, false, false)
}

#[test]
fn strict_checkpointed_wal_reopens_control() -> Result<()> {
    assert_reopens_after_flush(WriteMode::StrictlyIncreasing, true, false)
}

#[test]
fn strict_flushed_uncheckpointed_wal_must_reopen() -> Result<()> {
    assert_reopens_after_flush(WriteMode::StrictlyIncreasing, false, false)
}

#[test]
fn strict_materialized_prefix_and_wal_tail_reopen() -> Result<()> {
    assert_reopens_after_flush(WriteMode::StrictlyIncreasing, false, true)
}

#[test]
fn strict_group_commits_recover_across_materialized_domains_and_wal_tail() -> Result<()> {
    let temporary = TempDir::new()?;
    let options = OpenOptions {
        virtual_shards: 1,
        partition_span_us: DAY_US,
        checkpoint_on_close: false,
        wal_checkpoint_bytes: u64::MAX,
        ..OpenOptions::default()
    };
    let storage = Storage::open(temporary.path(), options.clone())?;
    storage.create_space("audit")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("strict", 60_000_000, DAY_US)?
            .with_write_mode(WriteMode::StrictlyIncreasing),
    )?;
    storage.create_bucket("audit", "points", policy.id)?;
    let first_series = storage
        .resolve_series(&SeriesRef::new("audit", "points", 1))?
        .0;
    let second_series = storage
        .resolve_series(&SeriesRef::new("audit", "points", 2))?
        .0;

    let prefix = [
        vec![
            Point::new(first_series, 1, 1.0)?,
            Point::new(second_series, DAY_US + 1, 2.0)?,
        ],
        vec![
            Point::new(first_series, 2 * DAY_US + 1, 3.0)?,
            Point::new(second_series, 3 * DAY_US + 1, 4.0)?,
        ],
    ];
    let prefix_tickets = prefix
        .iter()
        .map(|batch| storage.enqueue_batch(batch.clone(), Durability::GroupCommit))
        .collect::<Result<Vec<_>>>()?;
    for ticket in prefix_tickets {
        ticket.wait()?;
    }
    storage.flush()?;
    storage.compact()?;

    let tail = [
        vec![
            Point::new(first_series, 4 * DAY_US + 1, 5.0)?,
            Point::new(second_series, 5 * DAY_US + 1, 6.0)?,
        ],
        vec![
            Point::new(first_series, 6 * DAY_US + 1, 7.0)?,
            Point::new(second_series, 7 * DAY_US + 1, 8.0)?,
        ],
    ];
    let tail_tickets = tail
        .iter()
        .map(|batch| storage.enqueue_batch(batch.clone(), Durability::GroupCommit))
        .collect::<Result<Vec<_>>>()?;
    for ticket in tail_tickets {
        ticket.wait()?;
    }
    storage.close()?;
    drop(storage);

    let reopened = Storage::open(temporary.path(), options)?;
    let first_points = reopened
        .scan(first_series, 0, 8 * DAY_US)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        first_points,
        [
            Point::new(first_series, 1, 1.0)?,
            Point::new(first_series, 2 * DAY_US + 1, 3.0)?,
            Point::new(first_series, 4 * DAY_US + 1, 5.0)?,
            Point::new(first_series, 6 * DAY_US + 1, 7.0)?,
        ]
    );
    let second_points = reopened
        .scan(second_series, 0, 8 * DAY_US)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        second_points,
        [
            Point::new(second_series, DAY_US + 1, 2.0)?,
            Point::new(second_series, 3 * DAY_US + 1, 4.0)?,
            Point::new(second_series, 5 * DAY_US + 1, 6.0)?,
            Point::new(second_series, 7 * DAY_US + 1, 8.0)?,
        ]
    );
    reopened.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    Ok(())
}

#[test]
fn durable_wal_recovers_after_process_abort() -> Result<()> {
    assert_recovers_after("wal")
}

#[test]
fn flushed_segments_and_uncheckpointed_wal_recover_without_duplicates() -> Result<()> {
    assert_recovers_after("flush")
}

#[test]
fn materialized_segment_reopens_without_redundant_wal() -> Result<()> {
    let temporary = crash_storage("flush")?;
    for entry in fs::read_dir(temporary.path().join("wal"))? {
        fs::remove_file(entry?.path())?;
    }
    let report = Storage::verify(temporary.path());
    assert!(report.valid, "{:?}", report.issues);
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let points = storage
        .scan(SeriesKey::new(9, 8, 7), 0, 2 * DAY_US)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        points,
        [
            Point::new(SeriesKey::new(9, 8, 7), 11, 4.0)?,
            Point::new(SeriesKey::new(9, 8, 7), DAY_US + 7, 2.5)?,
        ]
    );
    storage.close()
}

#[test]
fn published_checkpoint_recovers_after_process_abort() -> Result<()> {
    assert_recovers_after("checkpoint")
}

#[test]
fn returned_partial_wal_write_poison_writer_and_preserves_valid_prefix() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let temporary = TempDir::new()?;
    let output = Command::new(env::current_exe()?)
        .arg("--exact")
        .arg("crash_process_helper")
        .arg("--nocapture")
        .env(HELPER_MODE, "returned_wal_error")
        .env(HELPER_STORAGE, temporary.path())
        .env(TEST_WAL_PARTIAL_COMMIT, "2")
        .env(TEST_WAL_PARTIAL_BYTES, "16")
        .output()?;
    assert!(
        output.status.success(),
        "partial-write helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            create_if_missing: false,
            checkpoint_on_close: false,
            ..OpenOptions::default()
        },
    )?;
    let points = storage
        .scan(SeriesKey::new(9, 8, 7), 0, 4)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        points
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(1, 1.0)]
    );
    storage.close()?;
    Ok(())
}

#[test]
fn group_commit_replies_wait_for_one_successful_wal_sync() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for environment in [
        vec![
            (TEST_WAL_PARTIAL_COMMIT, "2"),
            (TEST_WAL_PARTIAL_BYTES, "16"),
        ],
        vec![(TEST_WAL_SYNC_ERROR, "1")],
    ] {
        let temporary = TempDir::new()?;
        let mut command = Command::new(env::current_exe()?);
        command
            .arg("--exact")
            .arg("crash_process_helper")
            .arg("--nocapture")
            .env(HELPER_MODE, "returned_group_wal_error")
            .env(HELPER_STORAGE, temporary.path());
        for (name, value) in environment {
            command.env(name, value);
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "group WAL failure helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reopened = Storage::open(
            temporary.path(),
            OpenOptions {
                create_if_missing: false,
                checkpoint_on_close: false,
                ..OpenOptions::default()
            },
        )?;
        assert!(reopened.stats()?.visible_commit_id <= 3);
        reopened.close()?;
    }
    Ok(())
}

#[test]
fn storage_publication_failpoints_recover_an_atomic_state() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for (failpoint, mode, requires_new_state) in [
        ("wal_after_append", "fail_write", false),
        ("wal_after_sync", "fail_write", true),
        ("segment_after_sync", "fail_flush", true),
        ("segment_after_rename", "fail_flush", true),
        ("manifest_after_sync", "fail_checkpoint", true),
        ("manifest_after_rename", "fail_checkpoint", true),
    ] {
        let temporary = single_commit_storage()?;
        let output = run_failpoint(temporary.path(), failpoint, mode)?;
        assert_failpoint_abort(&output, failpoint);
        assert_single_commit_recovery(temporary.path(), requires_new_state)?;
    }
    Ok(())
}

#[test]
fn compaction_reaping_failpoints_keep_the_published_state_readable() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for failpoint in ["reap_before_remove", "reap_after_remove"] {
        let temporary = compaction_storage()?;
        let output = run_failpoint(temporary.path(), failpoint, "fail_compact")?;
        assert_failpoint_abort(&output, failpoint);
        let storage = Storage::open(
            temporary.path(),
            OpenOptions {
                create_if_missing: false,
                ..OpenOptions::default()
            },
        )?;
        let points = storage
            .scan(SeriesKey::new(9, 8, 7), 0, DAY_US)?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            points
                .iter()
                .map(|point| (point.timestamp_us, point.value))
                .collect::<Vec<_>>(),
            [(1, 10.0), (2, 20.0)]
        );
        storage.close()?;
        let report = Storage::verify(temporary.path());
        assert!(report.valid);
        assert_eq!(report.orphan_files, 0);
    }
    Ok(())
}

#[test]
fn read_generation_publication_failpoints_leave_no_unpublished_files() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for failpoint in ["read_directory_after_sync", "read_directory_after_rename"] {
        let temporary = read_generation_storage()?;
        let output = run_failpoint(temporary.path(), failpoint, "fail_compact")?;
        assert_failpoint_abort(&output, failpoint);
        let storage = Storage::open(
            temporary.path(),
            OpenOptions {
                create_if_missing: false,
                ..OpenOptions::default()
            },
        )?;
        let series = storage
            .resolve_series(&SeriesRef::new("audit", "points", 1))?
            .0;
        let points = storage
            .scan(series, 0, 70 * DAY_US)?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(points.len(), 2);
        storage.close()?;
        let report = Storage::verify(temporary.path());
        assert!(report.valid, "{:?}", report.issues);
        assert_eq!(report.orphan_files, 0);
    }
    Ok(())
}

fn single_commit_storage() -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(temporary.path(), OpenOptions::default())?;
    storage.write_batch(
        vec![Point::new(SeriesKey::new(9, 8, 7), 1, 10.0)?],
        Durability::Sync,
    )?;
    storage.checkpoint()?;
    storage.close()?;
    Ok(temporary)
}

fn compaction_storage() -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            l0_compaction_trigger: 100,
            l0_slowdown_trigger: 101,
            l0_stop_trigger: 102,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(9, 8, 7);
    for (timestamp_us, value) in [(1, 10.0), (2, 20.0)] {
        storage.write_batch(
            vec![Point::new(series, timestamp_us, value)?],
            Durability::Sync,
        )?;
        storage.flush()?;
    }
    storage.checkpoint()?;
    storage.close()?;
    Ok(temporary)
}

fn read_generation_storage() -> Result<TempDir> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            l0_compaction_trigger: 100,
            l0_slowdown_trigger: 101,
            l0_stop_trigger: 102,
            ..OpenOptions::default()
        },
    )?;
    storage.create_space("audit")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("strict", 60_000_000, DAY_US)?)?;
    storage.create_bucket("audit", "points", policy.id)?;
    let series = storage
        .resolve_series(&SeriesRef::new("audit", "points", 1))?
        .0;
    storage.write_batch(
        vec![
            Point::new(series, DAY_US + 1, 1.0)?,
            Point::new(series, 69 * DAY_US + 1, 2.0)?,
        ],
        Durability::Sync,
    )?;
    storage.flush()?;
    storage.checkpoint()?;
    storage.close()?;
    Ok(temporary)
}

fn run_failpoint(root: &std::path::Path, failpoint: &str, mode: &str) -> Result<Output> {
    Ok(Command::new(env::current_exe()?)
        .arg("--exact")
        .arg("crash_process_helper")
        .arg("--nocapture")
        .env(HELPER_MODE, mode)
        .env(HELPER_STORAGE, root)
        .env(TEST_FAILPOINT, failpoint)
        .output()?)
}

fn assert_failpoint_abort(output: &Output, failpoint: &str) {
    assert!(
        !output.status.success(),
        "failpoint helper exited cleanly before {failpoint}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("[chimaera failpoint] {failpoint}")),
        "failpoint {failpoint} was not reached; stderr: {stderr}"
    );
}

fn assert_single_commit_recovery(root: &std::path::Path, requires_new_state: bool) -> Result<()> {
    let storage = Storage::open(
        root,
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let points = storage
        .scan(SeriesKey::new(9, 8, 7), 0, 2 * DAY_US)?
        .collect::<Result<Vec<_>>>()?;
    let actual = points
        .iter()
        .map(|point| (point.timestamp_us, point.value))
        .collect::<Vec<_>>();
    let old = vec![(1, 10.0)];
    let new = vec![(1, 10.0), (11, 4.0), (DAY_US + 7, 2.5)];
    if requires_new_state {
        assert_eq!(actual, new);
    } else {
        assert!(
            actual == old || actual == new,
            "non-atomic state: {actual:?}"
        );
    }
    storage.close()?;
    let report = Storage::verify(root);
    assert!(report.valid);
    assert_eq!(report.orphan_files, 0);
    Ok(())
}
