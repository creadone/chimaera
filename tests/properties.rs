use std::collections::BTreeMap;
use std::time::Duration;

use chimaera::{
    CalendarGrouping, CalendarUnit, CompressionProfile, Durability, Grouping, NamedPoint,
    OpenOptions, Point, SeriesKey, SeriesRef, Storage, StoragePolicyOptions, WeekStart, WriteMode,
    WriteOperation,
};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence};
use tempfile::TempDir;

const DAY_US: i64 = 86_400_000_000;

fn storage_cases() -> Config {
    Config {
        cases: 24,
        max_shrink_iters: 2_048,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "tests/mvcc.proptest-regressions",
        ))),
        ..Config::default()
    }
}

fn codec_cases() -> Config {
    Config {
        cases: 16,
        max_shrink_iters: 1_024,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "tests/codecs.proptest-regressions",
        ))),
        ..Config::default()
    }
}

fn calendar_cases() -> Config {
    Config {
        cases: 512,
        max_shrink_iters: 4_096,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "tests/calendar.proptest-regressions",
        ))),
        ..Config::default()
    }
}

fn operation() -> impl Strategy<Value = (i16, bool, i32)> {
    (-256_i16..256, any::<bool>(), -1_000_000_i32..1_000_000)
}

fn finite_bits() -> impl Strategy<Value = u64> {
    any::<u64>().prop_filter("value must be finite", |bits| {
        f64::from_bits(*bits).is_finite()
    })
}

proptest! {
    #![proptest_config(storage_cases())]

    #[test]
    fn mvcc_merge_matches_a_sequential_model(
        batches in vec(vec(operation(), 1..8), 1..16),
    ) {
        let temporary = TempDir::new().expect("temporary storage");
        let series = SeriesKey::new(1, 1, 7);
        let storage = Storage::open(temporary.path(), storage_options())
            .expect("open property storage");
        let mut model = BTreeMap::<i64, Option<u64>>::new();
        for (batch_index, batch) in batches.iter().enumerate() {
            let writes = batch
                .iter()
                .map(|(timestamp, delete, value)| {
                    let timestamp = i64::from(*timestamp);
                    if *delete {
                        model.insert(timestamp, None);
                        WriteOperation::delete(series, timestamp)
                    } else {
                        let value = f64::from(*value) / 16.0;
                        model.insert(timestamp, Some(value.to_bits()));
                        WriteOperation::Put(
                            Point::new(series, timestamp, value).expect("finite model value"),
                        )
                    }
                })
                .collect::<Vec<_>>();
            storage
                .write_operations(writes, Durability::Sync)
                .expect("property write");
            if batch_index % 4 == 3 {
                storage.flush().expect("property flush");
            }
        }
        storage.checkpoint().expect("property checkpoint");
        storage.compact().expect("property compaction");
        storage.close().expect("property close");

        let reopened = Storage::open(
            temporary.path(),
            OpenOptions {
                create_if_missing: false,
                ..storage_options()
            },
        )
        .expect("property reopen");
        let actual = reopened
            .scan(series, -256, 256)
            .expect("property scan")
            .map(|point| {
                let point = point.expect("property point");
                (point.timestamp_us, point.value.to_bits())
            })
            .collect::<Vec<_>>();
        let expected = model
            .into_iter()
            .filter_map(|(timestamp, value)| value.map(|value| (timestamp, value)))
            .collect::<Vec<_>>();
        prop_assert_eq!(actual, expected);
        reopened.close().expect("property reopened close");
    }
}

proptest! {
    #![proptest_config(codec_cases())]

    #[test]
    fn every_storage_profile_round_trips_finite_float_bits(
        start in -10_000_000_i64..10_000_000,
        samples in vec((1_u16..10_000, finite_bits()), 1..96),
    ) {
        let profiles = [
            CompressionProfile::raw(),
            CompressionProfile::hot_fast(),
            CompressionProfile::columnar(),
            CompressionProfile::balanced(),
            CompressionProfile::cold_dense(),
        ];
        let temporary = TempDir::new().expect("temporary codec storage");
        let storage = Storage::open(temporary.path(), storage_options())
            .expect("open codec storage");
        storage.create_space("properties").expect("create space");
        let mut timestamp = start;
        let mut expected = Vec::with_capacity(samples.len());
        for (delta, bits) in samples {
            timestamp += i64::from(delta);
            expected.push((timestamp, bits));
        }
        let mut series = Vec::with_capacity(profiles.len());
        for (index, profile) in profiles.into_iter().enumerate() {
            let policy_name = format!("codec_{index}");
            let bucket_name = format!("values_{index}");
            let policy = storage
                .create_policy(
                    StoragePolicyOptions::new(&policy_name, DAY_US, DAY_US)
                        .expect("codec policy")
                        .with_write_mode(WriteMode::Mutable)
                        .with_compression(profile, profile)
                        .expect("codec compression"),
                )
                .expect("create policy");
            storage
                .create_bucket("properties", &bucket_name, policy.id)
                .expect("create bucket");
            let series_ref = SeriesRef::new("properties", bucket_name, 9);
            let operations = expected
                .iter()
                .rev()
                .map(|(timestamp, bits)| {
                    NamedPoint::new(series_ref.clone(), *timestamp, f64::from_bits(*bits))
                        .expect("finite generated point")
                        .into()
                })
                .collect::<Vec<_>>();
            storage
                .write_named_operations(operations, Durability::Sync)
                .expect("codec write");
            series.push(series_ref);
        }
        storage.checkpoint().expect("codec checkpoint");
        storage.compact().expect("codec compaction");
        for series_ref in series {
            let actual = storage
                .scan_named(&series_ref, start, timestamp + 1)
                .expect("codec scan")
                .map(|point| {
                    let point = point.expect("codec point");
                    (point.timestamp_us, point.value.to_bits())
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(&actual, &expected);
        }
        storage.close().expect("codec close");
    }
}

proptest! {
    #![proptest_config(calendar_cases())]

    #[test]
    fn calendar_buckets_are_monotonic_and_contain_the_input(
        timestamp_seconds in -2_208_988_800_i64..4_102_444_800,
        micros in 0_i64..1_000_000,
        unit_index in 0_usize..6,
        timezone_index in 0_usize..5,
        monday in any::<bool>(),
    ) {
        let units = [
            CalendarUnit::Day,
            CalendarUnit::Week,
            CalendarUnit::Month,
            CalendarUnit::Quarter,
            CalendarUnit::HalfYear,
            CalendarUnit::Year,
        ];
        let timezones = [
            "UTC",
            "America/New_York",
            "Europe/Berlin",
            "Australia/Lord_Howe",
            "Pacific/Apia",
        ];
        let week_start = if monday {
            WeekStart::Monday
        } else {
            WeekStart::Sunday
        };
        let calendar = CalendarGrouping::new(
            units[unit_index],
            timezones[timezone_index],
            week_start,
        )
        .expect("valid generated calendar");
        let grouping = Grouping::Calendar(calendar);
        let timestamp = timestamp_seconds * 1_000_000 + micros;
        let bucket = grouping.bucket(timestamp).expect("calendar bucket");
        prop_assert!(bucket.start_us <= timestamp);
        prop_assert!(timestamp < bucket.end_us);
        prop_assert!(bucket.start_us < bucket.end_us);
        prop_assert_eq!(grouping.bucket(bucket.start_us).expect("start bucket"), bucket);
        prop_assert_eq!(
            grouping.bucket(bucket.end_us - 1).expect("end bucket"),
            bucket,
        );

        let mut previous = None;
        for hour in -72_i64..=72 {
            let sampled = timestamp + hour * 3_600_000_000;
            let sampled_bucket = grouping.bucket(sampled).expect("neighbor bucket");
            if let Some(previous_start) = previous {
                prop_assert!(previous_start <= sampled_bucket.start_us);
            }
            previous = Some(sampled_bucket.start_us);
        }
    }
}

fn storage_options() -> OpenOptions {
    OpenOptions {
        virtual_shards: 1,
        partition_span_us: 1_000_000_000,
        wal_checkpoint_bytes: u64::MAX,
        checkpoint_interval: Duration::from_secs(86_400),
        cold_flush_idle_interval: Duration::from_secs(86_400),
        l0_compaction_trigger: 100,
        l0_slowdown_trigger: 101,
        l0_stop_trigger: 102,
        ..OpenOptions::default()
    }
}
