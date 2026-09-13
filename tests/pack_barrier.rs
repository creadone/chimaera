//! Process-isolated failures around a group of independently packed files.
use chimaera::{Durability, OpenOptions, Point, Result, SeriesKey, Storage};
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn options() -> OpenOptions {
    OpenOptions {
        checkpoint_on_close: false,
        ..OpenOptions::default()
    }
}

fn points(timestamp: i64) -> Result<Vec<Point>> {
    (0..128)
        .map(|key| {
            Point::new(
                SeriesKey::new(9, 8, key),
                timestamp,
                timestamp as f64 + key as f64,
            )
        })
        .collect()
}

fn fixture() -> Result<TempDir> {
    let dir = TempDir::new()?;
    let storage = Storage::open(dir.path(), options())?;
    for timestamp in 1..=2 {
        storage.write_batch(points(timestamp)?, Durability::Sync)?;
        storage.flush()?;
    }
    storage.close()?;
    Ok(dir)
}

fn verify_points(root: &Path, last: i64) -> Result<()> {
    let storage = Storage::open(root, options())?;
    for key in 0..128 {
        let actual = storage
            .scan(SeriesKey::new(9, 8, key), 0, 4)?
            .map(|point| point.map(|p| (p.timestamp_us, p.value)))
            .collect::<Result<Vec<_>>>()?;
        let expected = (1..=last)
            .map(|timestamp| (timestamp, timestamp as f64 + key as f64))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "series {key}");
    }
    storage.close()?;
    let report = Storage::verify(root);
    assert!(report.valid, "{report:?}");
    assert_eq!(report.orphan_files, 0);
    Ok(())
}

#[test]
fn pack_barrier_process_helper() -> Result<()> {
    let Ok(mode) = env::var("CHIMAERA_E4_MODE") else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(env::var_os("CHIMAERA_E4_ROOT").unwrap());
    let storage = Storage::open(&root, options())?;
    // The stream pins inputs across retirement and failed publication.
    let held = storage.scan(SeriesKey::new(9, 8, 0), 0, 4)?;
    let old_manifest = fs::read(root.join("manifest/MANIFEST.json"))?;
    let outcome = if mode == "flush" {
        storage.write_batch(points(3)?, Durability::Sync)?;
        storage.flush()
    } else {
        storage.compact()
    };
    if env::var_os("CHIMAERA_TEST_IO_ERROR").is_some() {
        assert!(outcome.is_err(), "injected failure was not returned");
        let post_commit = env::var("CHIMAERA_TEST_IO_ERROR").unwrap() == "manifest_after_commit";
        let current_manifest = fs::read(root.join("manifest/MANIFEST.json"))?;
        if post_commit {
            assert_ne!(old_manifest, current_manifest);
        } else {
            assert_eq!(
                old_manifest, current_manifest,
                "published before the barrier"
            );
        }
        if mode == "flush" || post_commit {
            assert!(!storage.stats()?.healthy);
        } else {
            // Explicit compaction reports a pre-publication error to its caller
            // and remains retryable, unlike an unhandled background failure.
            assert!(storage.stats()?.healthy);
            storage.compact()?;
        }
        assert_eq!(held.collect::<Result<Vec<_>>>()?.len(), 2);
        // With checkpoint_on_close=false, close only stops workers; the failed
        // flush/compaction has already reported its error and WAL stays durable.
        storage.close()?;
    } else {
        // A kill failpoint must abort before reaching this point.
        panic!("failpoint was not reached: {outcome:?}");
    }
    Ok(())
}

fn helper(root: &Path, mode: &str) -> Result<Command> {
    let mut cmd = Command::new(env::current_exe()?);
    cmd.args(["--exact", "pack_barrier_process_helper", "--nocapture"])
        .env("CHIMAERA_E4_ROOT", root)
        .env("CHIMAERA_E4_MODE", mode);
    Ok(cmd)
}

#[test]
fn pack_barrier_io_failures_preserve_manifest_wal_and_pinned_inputs() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for mode in ["flush", "compact"] {
        for stage in [
            "pack_file_sync",
            "pack_rename",
            "pack_group_tmp_sync",
            "pack_group_packs_sync",
            "pack_group_parent_sync",
            "manifest_after_commit",
        ] {
            for code in [libc::EIO, libc::ENOSPC] {
                let dir = fixture()?;
                // A third-worker failure also tests cleanup of successful siblings.
                let occurrence = if matches!(stage, "pack_file_sync" | "pack_rename") {
                    3
                } else {
                    1
                };
                let output = helper(dir.path(), mode)?
                    .env("CHIMAERA_TEST_IO_ERROR", stage)
                    .env("CHIMAERA_TEST_IO_ERROR_CODE", code.to_string())
                    .env("CHIMAERA_TEST_IO_ERROR_AT", occurrence.to_string())
                    .output()?;
                assert!(
                    output.status.success(),
                    "{mode}/{stage}/{code}: {}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                verify_points(dir.path(), if mode == "flush" { 3 } else { 2 })?;
            }
        }
    }
    Ok(())
}

#[test]
fn pack_barrier_process_crashes_recover_acknowledged_data() -> Result<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    for mode in ["flush", "compact"] {
        for stage in [
            "segment_after_sync",
            "segment_after_rename",
            "pack_group_before_barrier",
            "pack_group_after_tmp_sync",
            "pack_group_after_barrier",
            "manifest_after_rename",
        ] {
            let dir = fixture()?;
            let output = helper(dir.path(), mode)?
                .env("CHIMAERA_TEST_FAILPOINT", stage)
                .output()?;
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains(&format!("[chimaera failpoint] {stage}")),
                "{mode}/{stage}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            verify_points(dir.path(), if mode == "flush" { 3 } else { 2 })?;
        }
    }
    Ok(())
}
