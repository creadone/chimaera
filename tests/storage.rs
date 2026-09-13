//! Интеграционные проверки Storage; общие фикстуры доступны тематическим модулям.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use chimaera::{
    AggregateResolution, AggregateSet, BlockCodec, CalendarUnit, CancellationToken, Clock,
    CompactionOptions, CompressionProfile, DataTemperature, Durability, Error, ExportOptions,
    GroupRequest, GroupScope, GroupSource, ImportOptions, NamedPoint, NamedWriteOperation,
    OpenOptions, Point, QueryOptions, Result, SeriesKey, SeriesRef, Storage, StoragePolicyOptions,
    TimestampCodec, ValueCodec, WeekStart, WriteMode, WriteOperation,
};
use tempfile::TempDir;

const DAY: i64 = 86_400_000_000;
const MINUTE: i64 = 60_000_000;

#[derive(Debug)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(now_us: i64) -> Self {
        Self(AtomicI64::new(now_us))
    }

    fn set(&self, now_us: i64) {
        self.0.store(now_us, Ordering::Release);
    }
}

impl Clock for ManualClock {
    fn now_us(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

fn point(series: SeriesKey, timestamp_us: i64, value: f64) -> Point {
    Point::new(series, timestamp_us, value).expect("finite test point")
}

fn rows(storage: &Storage, series: Vec<SeriesKey>) -> Result<Vec<chimaera::GroupRow>> {
    storage
        .group_stream(GroupRequest::new(series, -DAY, DAY * 10)?)?
        .collect()
}

fn assert_close(actual: f64, expected: f64) {
    // Neumaier summation is deterministic for a fixed visible point order. The
    // tolerance only covers a different legal addition order in the oracle.
    let tolerance = 1.0e-12_f64.max(expected.abs() * 1.0e-12);
    assert!(
        (actual - expected).abs() <= tolerance,
        "actual={actual} expected={expected} tolerance={tolerance}"
    );
}

fn assert_extended_close(actual: &chimaera::GroupRow, expected: &chimaera::GroupRow) {
    assert_eq!(actual.series, expected.series);
    assert_eq!(actual.bucket_start_us, expected.bucket_start_us);
    assert_eq!(actual.bucket_end_us, expected.bucket_end_us);
    assert_eq!(actual.count, expected.count);
    assert_close(actual.sum, expected.sum);
    for (actual, expected) in [
        (actual.min, expected.min),
        (actual.max, expected.max),
        (actual.mean, expected.mean),
        (actual.population_variance, expected.population_variance),
        (actual.sample_variance, expected.sample_variance),
        (
            actual.population_standard_deviation,
            expected.population_standard_deviation,
        ),
        (
            actual.sample_standard_deviation,
            expected.sample_standard_deviation,
        ),
        (actual.delta, expected.delta),
        (actual.rate_per_second, expected.rate_per_second),
    ] {
        match (actual, expected) {
            (Some(actual), Some(expected)) => assert_close(actual, expected),
            (None, None) => {}
            pair => panic!("aggregate availability differs: {pair:?}"),
        }
    }
    assert_eq!(actual.first, expected.first);
    assert_eq!(actual.last, expected.last);
}

fn fairness_points(series: &[SeriesKey], generation: i64) -> Vec<Point> {
    series
        .iter()
        .flat_map(|series| {
            (0..32).map(move |offset| {
                point(
                    *series,
                    generation * 100 + i64::from(offset),
                    generation as f64,
                )
            })
        })
        .collect()
}

const ACTIVE_L1_TEST_MAX_RUNS: usize = 8;

fn assert_salvage_preserves_retained_aggregate_archives(include_recent_raw: bool) -> Result<()> {
    let parent = TempDir::new()?;
    let source = parent.path().join("source");
    let destination = parent.path().join("salvaged");
    let now_us = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_micros(),
    )
    .expect("timestamp fits i64");
    let today = now_us.div_euclid(DAY) * DAY;
    let old_base = today - 4 * DAY;
    let clock = Arc::new(ManualClock::new(old_base + DAY));
    let options = OpenOptions {
        clock: clock.clone(),
        virtual_shards: 1,
        partition_span_us: DAY,
        ..OpenOptions::default()
    };
    let storage = Storage::open(&source, options.clone())?;
    storage.create_space("audit")?;
    let policy = storage.create_policy(
        StoragePolicyOptions::new("rollups", MINUTE, DAY)?
            .with_retention_us(Some(2 * DAY))?
            .with_aggregate_retention_us(Some(10 * DAY), vec![AggregateResolution::Day])?,
    )?;
    storage.create_bucket("audit", "points", policy.id)?;
    let series = storage
        .resolve_series(&SeriesRef::new("audit", "points", 1))?
        .0;
    storage.write_batch(
        (0..100)
            .map(|offset| Point::new(series, old_base + offset * MINUTE, offset as f64))
            .collect::<Result<Vec<_>>>()?,
        Durability::Sync,
    )?;
    storage.checkpoint()?;

    clock.set(today);
    if include_recent_raw {
        storage.write_batch(
            (0..10)
                .map(|offset| {
                    Point::new(
                        series,
                        today - DAY + offset * MINUTE,
                        1_000.0 + offset as f64,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            Durability::Sync,
        )?;
    }
    storage.compact()?;

    let expected_raw = storage
        .scan(series, old_base, today)?
        .collect::<Result<Vec<_>>>()?;
    let expected_groups = storage
        .group_stream(
            GroupRequest::new(vec![series], old_base, today)?.with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(expected_raw.len(), usize::from(include_recent_raw) * 10);
    assert_eq!(
        expected_groups.iter().map(|row| row.count).sum::<u64>(),
        100 + u64::from(include_recent_raw) * 10
    );
    storage.close()?;
    drop(storage);

    let verification = Storage::verify(&source);
    assert!(verification.valid, "{:?}", verification.issues);
    assert!(
        verification
            .info
            .as_ref()
            .expect("verified storage has info")
            .aggregate_archives
            > 0
    );
    Storage::salvage(&source, &destination)?;
    let destination_verification = Storage::verify(&destination);
    assert!(
        destination_verification.valid,
        "{:?}",
        destination_verification.issues
    );

    let restored = Storage::open(
        &destination,
        OpenOptions {
            create_if_missing: false,
            ..options
        },
    )?;
    let actual_raw = restored
        .scan(series, old_base, today)?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(actual_raw, expected_raw);
    let actual_groups = restored
        .group_stream(
            GroupRequest::new(vec![series], old_base, today)?.with_aggregates(AggregateSet::ALL),
        )?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(actual_groups, expected_groups);
    restored.close()?;

    fs::write(destination.join("aggregates/ORPHAN.tsa"), [1_u8, 2, 3])?;
    let with_orphan = Storage::verify(&destination);
    assert!(with_orphan.valid, "{:?}", with_orphan.issues);
    assert_eq!(with_orphan.orphan_files, 1);

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("manifest/MANIFEST.json"))?)?;
    let archive = manifest["aggregate_archives"][0]["relative_path"]
        .as_str()
        .expect("aggregate archive path");
    fs::remove_file(destination.join(archive))?;
    let missing_archive = Storage::verify(&destination);
    assert!(!missing_archive.valid);
    assert_eq!(missing_archive.issues.len(), 1);
    Ok(())
}

fn wal_file_count(root: &std::path::Path) -> Result<usize> {
    Ok(fs::read_dir(root.join("wal"))?
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "tsw")
        })
        .count())
}

fn storage_file_image(root: &std::path::Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut image = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                let relative = entry
                    .path()
                    .strip_prefix(root)
                    .expect("storage entry must remain below root")
                    .to_string_lossy()
                    .into_owned();
                image.insert(relative, fs::read(entry.path())?);
            }
        }
    }
    Ok(image)
}

fn wait_for_checkpoint(storage: &Storage, expected_commit: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let stats = storage.stats()?;
        if stats.checkpoint_commit_id >= expected_commit && stats.background_checkpoints > 0 {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("checkpoint {expected_commit} was not published before timeout");
}

fn wait_for_compaction_rejection(storage: &Storage) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if storage.stats()?.compaction_space_rejections > 0 {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("background compaction was not rejected before timeout");
}

fn manifest_segment_paths(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let mut paths = manifest["segments"]
        .as_array()
        .expect("manifest segments")
        .iter()
        .map(|segment| {
            root.join(
                segment["relative_path"]
                    .as_str()
                    .expect("segment relative path"),
            )
        })
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.dedup();
    Ok(paths)
}

struct ManifestSegmentRecord {
    id: u64,
    level: u8,
    policy_id: u32,
    min_timestamp_us: i64,
    max_timestamp_us: i64,
    relative_path: String,
    rewrite_count: u32,
}

fn manifest_segment_records(root: &std::path::Path) -> Result<Vec<ManifestSegmentRecord>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let mut segments = manifest["segments"]
        .as_array()
        .expect("manifest segments")
        .iter()
        .map(|segment| ManifestSegmentRecord {
            id: segment["id"].as_u64().expect("segment ID"),
            level: u8::try_from(segment["level"].as_u64().expect("level")).expect("level fits u8"),
            policy_id: u32::try_from(segment["policy_id"].as_u64().expect("policy ID"))
                .expect("policy ID fits u32"),
            min_timestamp_us: segment["min_timestamp_us"].as_i64().expect("minimum"),
            max_timestamp_us: segment["max_timestamp_us"].as_i64().expect("maximum"),
            relative_path: segment["relative_path"]
                .as_str()
                .expect("relative path")
                .to_owned(),
            rewrite_count: u32::try_from(segment["rewrite_count"].as_u64().unwrap_or_default())
                .expect("rewrite count fits u32"),
        })
        .collect::<Vec<_>>();
    segments.sort_by_key(|segment| {
        (
            segment.min_timestamp_us,
            segment.max_timestamp_us,
            segment.id,
        )
    });
    Ok(segments)
}

fn wait_for_removed_paths(paths: &[std::path::PathBuf]) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if paths.iter().all(|path| !path.exists()) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("retired segment files were not removed before timeout");
}

fn segment_block_codecs(root: &std::path::Path, policy_id: u32) -> Result<BTreeMap<i64, [u8; 4]>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("manifest/MANIFEST.json"))?)?;
    let mut codecs = BTreeMap::new();
    for segment in manifest["segments"].as_array().expect("manifest segments") {
        if segment["policy_id"] != policy_id {
            continue;
        }
        let relative_path = segment["relative_path"]
            .as_str()
            .expect("segment relative path");
        let file_offset = segment["file_offset"]
            .as_u64()
            .expect("segment file offset");
        let window_start = segment["window_start_us"]
            .as_i64()
            .expect("segment window start");
        let mut file = fs::File::open(root.join(relative_path))?;
        file.seek(SeekFrom::Start(file_offset + 112))?;
        let mut header = [0_u8; 88];
        file.read_exact(&mut header)?;
        codecs.insert(
            window_start,
            header[68..72].try_into().expect("codec bytes"),
        );
    }
    Ok(codecs)
}

#[path = "storage/administration.rs"]
mod administration;
#[path = "storage/aggregates.rs"]
mod aggregates;
#[path = "storage/catalog.rs"]
mod catalog;
#[path = "storage/compaction.rs"]
mod compaction;
#[path = "storage/compression.rs"]
mod compression;
#[path = "storage/lifecycle.rs"]
mod lifecycle;
#[path = "storage/queries.rs"]
mod queries;
#[path = "storage/read_directory.rs"]
mod read_directory;
#[path = "storage/resources.rs"]
mod resources;
#[path = "storage/retention.rs"]
mod retention;
#[path = "storage/strict_writes.rs"]
mod strict_writes;
#[path = "storage/writes.rs"]
mod writes;

#[path = "storage/summary_fallback.rs"]
mod summary_fallback;
