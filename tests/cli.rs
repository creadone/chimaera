use std::collections::BTreeMap;
use std::fs;
use std::process::{Command, Output};

use chimaera::{
    Durability, NamedPoint, OpenOptions, Result, SeriesRef, Storage, StoragePolicyOptions,
    VerificationOptions,
};
use serde_json::Value;
use tempfile::TempDir;

const DAY: i64 = 86_400_000_000;

#[test]
fn admin_cli_runs_read_only_diagnostics_and_the_operational_round_trip() -> Result<()> {
    let source = TempDir::new()?;
    let storage = Storage::open(source.path(), OpenOptions::default())?;
    storage.create_space("production")?;
    let policy = storage.create_policy(StoragePolicyOptions::new("cli", DAY, DAY)?)?;
    storage.create_bucket("production", "signals", policy.id)?;
    let series = SeriesRef::new("production", "signals", 42);
    storage.write_named_operations(
        vec![
            NamedPoint::new(series.clone(), 1, 1.5)?.into(),
            NamedPoint::new(series.clone(), 2, 2.5)?.into(),
        ],
        Durability::Sync,
    )?;
    storage.close()?;

    let before = file_image(source.path())?;
    let info = admin_json(["info", path(source.path())]);
    assert!(info.status.success(), "{}", stderr(&info));
    let info_json: Value = serde_json::from_slice(&info.stdout)?;
    assert_eq!(info_json["ok"], true);
    assert_eq!(info_json["operation"], "info");
    assert_eq!(info_json["logical_segments"], 1);
    let verify = admin_json(["verify", path(source.path())]);
    assert!(verify.status.success(), "{}", stderr(&verify));
    let verify_json: Value = serde_json::from_slice(&verify.stdout)?;
    assert_eq!(verify_json["ok"], true);
    assert_eq!(verify_json["valid"], true);
    assert_eq!(verify_json["workers"], 4);
    assert_eq!(verify_json["verified_physical_files"], 1);
    for workers in [1, 2, 4] {
        let report = Storage::verify_with_options(source.path(), VerificationOptions { workers });
        assert!(
            report.valid,
            "{workers}-worker verification: {:?}",
            report.issues
        );
        assert_eq!(report.workers, workers);
        assert_eq!(report.verified_physical_files, 1);
        assert_eq!(report.verified_segment_operations, 2);
    }
    assert_eq!(file_image(source.path())?, before);

    let outputs = TempDir::new()?;
    let dump = outputs.path().join("storage.jsonl.gz");
    let loaded = outputs.path().join("loaded");
    let snapshot = outputs.path().join("snapshot");
    let restored = outputs.path().join("restored");
    let salvaged = outputs.path().join("salvaged");

    assert_success(admin_json([
        "dump",
        path(source.path()),
        "--output",
        path(&dump),
    ]));
    assert_success(admin_json(["load", path(&dump), "--output", path(&loaded)]));
    let loaded_storage = Storage::open(
        &loaded,
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(
        loaded_storage
            .scan_named(&series, 0, 10)?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(|point| (point.timestamp_us, point.value))
            .collect::<Vec<_>>(),
        [(1, 1.5), (2, 2.5)]
    );
    loaded_storage.close()?;

    let rejected = admin_json(["flush", path(&loaded)]);
    assert_eq!(rejected.status.code(), Some(74));
    assert_eq!(
        serde_json::from_slice::<Value>(&rejected.stdout)?["ok"],
        false
    );
    assert_success(admin_json(["flush", path(&loaded), "--force"]));
    assert_success(admin_json(["checkpoint", path(&loaded), "--force"]));
    assert_success(admin_json(["compact", path(&loaded), "--force"]));
    assert_success(admin_json([
        "snapshot",
        path(&loaded),
        "--output",
        path(&snapshot),
    ]));
    assert_success(admin_json([
        "restore",
        path(&snapshot),
        "--output",
        path(&restored),
    ]));
    assert_success(admin_json([
        "salvage",
        path(&restored),
        "--output",
        path(&salvaged),
    ]));
    assert!(Storage::verify(&salvaged).valid);

    let manifest: Value =
        serde_json::from_slice(&fs::read(snapshot.join("manifest/MANIFEST.json"))?)?;
    let segment = manifest["segments"][0]["relative_path"]
        .as_str()
        .expect("snapshot segment path");
    fs::OpenOptions::new()
        .write(true)
        .open(snapshot.join(segment))?
        .set_len(1)?;
    let invalid = admin_json(["verify", path(&snapshot)]);
    assert_eq!(invalid.status.code(), Some(65));
    let invalid_json: Value = serde_json::from_slice(&invalid.stdout)?;
    assert_eq!(invalid_json["ok"], false);
    assert_eq!(invalid_json["valid"], false);
    Ok(())
}

#[test]
fn parallel_verify_reports_the_same_first_corruption_as_one_worker() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            virtual_shards: 1,
            l0_compaction_trigger: 8,
            ..OpenOptions::default()
        },
    )?;
    for (key, value) in [(1, 1.0), (2, 2.0)] {
        storage.write_batch(
            vec![chimaera::Point::new(
                chimaera::SeriesKey::new(1, 1, key),
                key as i64,
                value,
            )?],
            Durability::Sync,
        )?;
        storage.flush()?;
    }
    storage.close()?;

    let manifest: Value =
        serde_json::from_slice(&fs::read(temporary.path().join("manifest/MANIFEST.json"))?)?;
    let mut paths = manifest["segments"]
        .as_array()
        .expect("manifest segments")
        .iter()
        .map(|segment| {
            temporary.path().join(
                segment["relative_path"]
                    .as_str()
                    .expect("segment relative path"),
            )
        })
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.dedup();
    assert!(paths.len() >= 2);
    for path in paths.iter().take(2) {
        let mut bytes = fs::read(path)?;
        let last = bytes.last_mut().expect("segment is non-empty");
        *last ^= 0x80;
        fs::write(path, bytes)?;
    }

    let serial = Storage::verify_with_options(temporary.path(), VerificationOptions { workers: 1 });
    let parallel =
        Storage::verify_with_options(temporary.path(), VerificationOptions { workers: 4 });
    assert!(!serial.valid);
    assert!(!parallel.valid);
    assert_eq!(parallel.issues, serial.issues);
    Ok(())
}

fn admin_json<const N: usize>(arguments: [&str; N]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_chimaera-admin"))
        .arg("--json")
        .args(arguments)
        .output()
        .expect("chimaera-admin must run")
}

fn assert_success(output: Output) {
    assert!(output.status.success(), "{}", stderr(&output));
    let value: Value = serde_json::from_slice(&output.stdout).expect("valid admin JSON");
    assert_eq!(value["ok"], true);
}

fn path(path: &std::path::Path) -> &str {
    path.to_str().expect("temporary path is UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn file_image(root: &std::path::Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut image = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                let path = entry.path();
                let relative = path
                    .strip_prefix(root)
                    .expect("entry stays below storage root")
                    .to_string_lossy()
                    .into_owned();
                image.insert(relative, fs::read(path)?);
            }
        }
    }
    Ok(image)
}
