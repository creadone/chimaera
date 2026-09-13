//! Регрессионные проверки взаимодействия внутренних ролей движка.

use super::batch::{OrderedBatchCursor, QueryBatch, QueryBatchOverlap};
use super::compaction_ranges::append_size_split_range;
use super::compaction_selection::{
    CompactionClass, CompactionDebt, can_coalesce_without_l1_overlap, l1_domain_debt,
    select_l1_input_indices, select_l1_repair_indices, validate_l1_publication,
    weighted_compaction_order_for_debt,
};
use super::executor::{QueryPlanningTask, QueryWork};
use super::group_cursor::SeriesCursor;
use super::options::OpenOptions;
use super::planning::HybridReadOverlap;
use super::publication::UnpublishedArtifacts;
use super::read_directory::prepare_published_read_directories;
use super::regulator::{PACING_READ_LATENCY, WritePressure, WriteRegulator};
use super::support::{read, write};
use super::{PublishCommand, READ_LATENCY_WIDE_WINDOW, Storage, TimeRange, WriteCommand};
use crate::admission::{WorkMemoryClass, WorkMemoryGovernor};
use crate::cache::{MemoryClass, MemoryGovernor};
use crate::catalog::{StoragePolicyOptions, WriteMode};
use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::layout::shard_for;
use crate::lifecycle::{AdmissionOptions, OverloadReason};
use crate::manifest::Manifest;
use crate::query::{QueryBudget, QueryOptions};
use crate::segment::{OperationCursor, PointBatch, SliceCursor, VisibleCursor};
use crate::types::{
    AggregateSet, DAY_US, Durability, GroupRequest, GroupSource, Point, PointKey, SeriesKey,
    VersionedPoint,
};
use crossbeam_channel::bounded;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
mod write_regulator_tests {
    use super::*;

    fn no_structural_pressure() -> WritePressure {
        WritePressure {
            l0_segments: 0,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 16,
            compaction_queue_depth: 0,
            compaction_queue_capacity: 2,
            managed_non_cache_bytes: 0,
            managed_memory_max_bytes: 1024,
            memory_backpressure_events: 0,
        }
    }

    #[test]
    fn read_latency_pacing_ignores_one_spike_and_expires_after_idle() -> Result<()> {
        let regulator = WriteRegulator::new(
            Duration::from_millis(5),
            Duration::from_millis(40),
            Duration::from_millis(5),
        );
        for _ in 0..31 {
            regulator.record_read(Duration::from_millis(1), false);
        }
        regulator.record_read(Duration::from_millis(20), false);
        for _ in 0..3 {
            assert!(
                regulator
                    .regulate(no_structural_pressure())?
                    .delay
                    .is_zero()
            );
        }
        assert_eq!(regulator.stats()?.read_latency_events, 0);

        for _ in 0..32 {
            regulator.record_read(Duration::from_millis(20), false);
        }
        for _ in 0..3 {
            let _ = regulator.regulate(no_structural_pressure())?;
        }
        let active = regulator.stats()?;
        assert!(active.active);
        assert_ne!(active.reasons & PACING_READ_LATENCY, 0);

        thread::sleep(Duration::from_millis(110));
        assert!(
            regulator
                .regulate(no_structural_pressure())?
                .delay
                .is_zero()
        );
        let recovered = regulator.stats()?;
        assert!(!recovered.active);
        assert_eq!(recovered.reasons, 0);
        assert_eq!(recovered.read_latency_samples, 0);
        assert!(recovered.recoveries >= 1);
        Ok(())
    }

    #[test]
    fn wide_read_latency_has_an_independent_window() -> Result<()> {
        let regulator = WriteRegulator::new(
            Duration::from_millis(5),
            Duration::from_millis(40),
            Duration::from_millis(5),
        );
        for _ in 0..128 {
            regulator.record_read(Duration::from_millis(1), false);
        }
        regulator.record_read(Duration::from_millis(60), true);
        for _ in 0..3 {
            assert!(
                regulator
                    .regulate(no_structural_pressure())?
                    .delay
                    .is_zero()
            );
        }
        for _ in 0..2 {
            regulator.record_read(Duration::from_millis(60), true);
        }
        for _ in 0..3 {
            let _ = regulator.regulate(no_structural_pressure())?;
        }
        let stats = regulator.stats()?;
        assert!(stats.active);
        assert_eq!(stats.read_latency_single_samples, 128);
        assert_eq!(stats.read_latency_wide_samples, 3);
        assert_eq!(stats.read_latency_single_p95, Duration::from_millis(1));
        assert_eq!(stats.read_latency_wide_p95, Duration::from_millis(60));
        for _ in 0..READ_LATENCY_WIDE_WINDOW {
            regulator.record_read(Duration::from_millis(1), true);
        }
        assert!(
            regulator
                .regulate(no_structural_pressure())?
                .delay
                .is_zero()
        );
        assert!(!regulator.stats()?.active);
        Ok(())
    }
}

#[cfg(test)]
mod compaction_fairness_tests {
    use super::*;

    #[test]
    fn weighted_order_accounts_for_bytes_age_and_stable_ties() {
        let active = weighted_compaction_order_for_debt(
            CompactionDebt {
                active_bytes: 2,
                historical_bytes: 1,
                ..CompactionDebt::default()
            },
            CompactionClass::Historical,
        );
        assert_eq!(active.0, CompactionClass::Active);

        let historical = weighted_compaction_order_for_debt(
            CompactionDebt {
                active_bytes: 2 * 1024 * 1024,
                historical_bytes: 1,
                historical_oldest_age_us: 3_000_000,
                ..CompactionDebt::default()
            },
            CompactionClass::Active,
        );
        assert_eq!(historical.0, CompactionClass::Historical);

        let tie = weighted_compaction_order_for_debt(
            CompactionDebt::default(),
            CompactionClass::Historical,
        );
        assert_eq!(tie.0, CompactionClass::Historical);
    }
}

#[cfg(test)]
mod l1_selection_tests {
    use super::*;
    use crate::segment::SegmentDescriptor;

    fn part(id: u64, from: i64, to: i64, mib: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            id,
            level: 1,
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 1_000,
            min_timestamp_us: from,
            max_timestamp_us: to - 1,
            relative_path: String::new(),
            pack_id: None,
            file_offset: 0,
            min_commit: 1,
            max_commit: 1,
            record_count: 1,
            file_size: mib * 1024 * 1024,
            rewrite_count: 0,
        }
    }

    fn select(parts: &[SegmentDescriptor]) -> Vec<usize> {
        select_l1_input_indices(&parts.iter().collect::<Vec<_>>())
    }

    fn assert_no_new_overlap(parts: &[SegmentDescriptor], selected: &[usize]) {
        assert!(selected.len() >= 2);
        let from = selected
            .iter()
            .map(|i| parts[*i].min_timestamp_us)
            .min()
            .unwrap();
        let to = selected
            .iter()
            .map(|i| parts[*i].max_timestamp_us)
            .max()
            .unwrap();
        for (i, part) in parts.iter().enumerate() {
            if !selected.contains(&i) {
                assert!(
                    part.max_timestamp_us < from || part.min_timestamp_us > to,
                    "selected {selected:?} bridges unselected part {}",
                    part.id
                );
            }
        }
    }

    #[test]
    fn l1_connected_components_are_indivisible_and_bounded() {
        let parts = [
            part(1, 0, 20, 58),
            part(2, 10, 30, 9),
            part(3, 20, 40, 9),
            part(4, 40, 50, 9),
            part(5, 50, 60, 9),
        ];
        assert_eq!(select(&parts), [3, 4]);
        assert_eq!(
            select_l1_repair_indices(&parts.iter().collect::<Vec<_>>()),
            Some(vec![0, 1, 2])
        );
        let oversized = [part(1, 0, 20, 60), part(2, 10, 30, 60), part(3, 20, 40, 10)];
        assert!(select(&oversized).is_empty());
        assert!(select_l1_repair_indices(&oversized.iter().collect::<Vec<_>>()).is_none());
        let fan_in = (0..17).map(|id| part(id, 0, 10, 5)).collect::<Vec<_>>();
        assert!(select(&fan_in).is_empty());
        assert!(select_l1_repair_indices(&fan_in.iter().collect::<Vec<_>>()).is_none());
    }

    #[test]
    fn l0_coalescing_cannot_bridge_remaining_l1_and_uses_half_open_ranges() {
        let ranges = [
            TimeRange {
                from_us: 10,
                to_us: 20,
            },
            TimeRange {
                from_us: 30,
                to_us: 40,
            },
        ];
        assert!(!can_coalesce_without_l1_overlap(
            &ranges,
            [&part(1, 20, 30, 1)].into_iter()
        ));
        assert!(can_coalesce_without_l1_overlap(
            &ranges,
            [&part(1, 0, 10, 1), &part(2, 40, 50, 1)].into_iter()
        ));
    }

    #[test]
    fn l1_repair_output_cuts_preserve_exact_coverage_even_at_timestamp_extremes() {
        for range in [
            TimeRange {
                from_us: -1,
                to_us: 100,
            },
            TimeRange {
                from_us: i64::MIN,
                to_us: i64::MAX,
            },
            TimeRange {
                from_us: 1,
                to_us: 2,
            },
        ] {
            let mut split = Vec::new();
            append_size_split_range(&mut split, range, 80 * 1024 * 1024, 64 * 1024 * 1024, true);
            assert_eq!(split.first().unwrap().from_us, range.from_us);
            assert_eq!(split.last().unwrap().to_us, range.to_us);
            assert!(split.len() <= 2);
            assert!(split.iter().all(|part| part.from_us < part.to_us));
            assert!(
                split
                    .windows(2)
                    .all(|pair| pair[0].to_us == pair[1].from_us)
            );
        }
    }

    #[test]
    fn l1_publication_rejects_a_new_gap_overlap_but_keeps_mutable_deltas() -> Result<()> {
        let inputs = [part(1, 10, 20, 1), part(2, 30, 40, 1)];
        let remaining = [part(3, 0, 50, 10)];
        let bad = [part(4, 10, 40, 2)];
        assert!(validate_l1_publication(inputs.iter(), bad.iter(), remaining.iter()).is_err());
        validate_l1_publication(inputs.iter(), inputs.iter(), remaining.iter())?;
        let full = [part(5, 0, 50, 12)];
        validate_l1_publication(
            inputs.iter().chain(remaining.iter()),
            full.iter(),
            [].iter(),
        )
    }

    #[test]
    fn l1_debt_distinguishes_waiting_small_parts_from_saturated_runs() {
        let mut parts = vec![part(1, 0, 10, 58)];
        parts.extend((2..9).map(|id| part(id, id as i64 * 10, (id as i64 + 1) * 10, 9)));
        let now = Instant::now();
        let debt = l1_domain_debt(
            parts[0].domain(),
            &parts.iter().collect::<Vec<_>>(),
            None,
            now,
        );
        assert_eq!(debt.stats.eligible_input_parts, 7);
        assert_eq!(debt.stats.eligible_input_bytes, 63 * 1024 * 1024);
        assert_eq!(debt.oldest_eligible, Some(now));
        assert_eq!(debt.stats.overlap_components, 0);
        let full = (0..8)
            .map(|id| part(id, id as i64 * 10, (id as i64 + 1) * 10, 60))
            .collect::<Vec<_>>();
        let debt = l1_domain_debt(
            full[0].domain(),
            &full.iter().collect::<Vec<_>>(),
            None,
            now,
        );
        assert_eq!(debt.stats.eligible_input_parts, 0);
        assert!(debt.stats.no_adjacent_pack);
        assert_eq!(debt.stats.overlap_components_over_byte_limit, 0);
        assert_eq!(debt.oldest_eligible, None);
        let blocked = [part(1, 0, 20, 70), part(2, 10, 30, 70)];
        let debt = l1_domain_debt(
            blocked[0].domain(),
            &blocked.iter().collect::<Vec<_>>(),
            None,
            now,
        );
        assert_eq!(debt.stats.eligible_input_parts, 0);
        assert_eq!(debt.stats.overlap_components, 1);
        assert_eq!(debt.stats.overlap_input_bytes, 140 * 1024 * 1024);
        assert_eq!(debt.stats.overlap_components_over_byte_limit, 1);
    }

    #[test]
    fn l1_wait_age_starts_when_work_becomes_eligible_and_survives_other_publications() {
        let parts = [part(1, 0, 10, 30), part(2, 10, 20, 30)];
        let descriptors = parts.iter().collect::<Vec<_>>();
        let now = Instant::now();
        let alone = l1_domain_debt(parts[0].domain(), &descriptors[..1], None, now);
        let ready_at = now + Duration::from_secs(100);
        let ready = l1_domain_debt(parts[0].domain(), &descriptors, Some(&alone), ready_at);
        assert_eq!(
            ready.oldest_eligible,
            Some(ready_at),
            "old file was not waiting before its neighbor existed"
        );
        let waiting = l1_domain_debt(
            parts[0].domain(),
            &descriptors,
            Some(&ready),
            ready_at + Duration::from_secs(5),
        );
        assert_eq!(
            waiting.oldest_eligible,
            Some(ready_at),
            "publication must not reset an existing wait"
        );
        let no_work = l1_domain_debt(
            parts[0].domain(),
            &descriptors[..1],
            Some(&waiting),
            ready_at + Duration::from_secs(10),
        );
        assert!(no_work.eligible_since.is_empty());
        let next_ready_at = ready_at + Duration::from_secs(20);
        let ready_again = l1_domain_debt(
            parts[0].domain(),
            &descriptors,
            Some(&no_work),
            next_ready_at,
        );
        assert_eq!(ready_again.oldest_eligible, Some(next_ready_at));
    }

    #[test]
    fn l1_large_old_part_does_not_block_a_fitting_pair() {
        let parts = [part(1, 0, 10, 58), part(2, 10, 20, 9), part(3, 20, 30, 9)];
        assert_eq!(select(&parts), [1, 2]);
    }

    #[test]
    fn l1_selection_never_bridges_a_skipped_part() {
        let parts = [
            part(1, 0, 10, 20),
            part(2, 10, 20, 50),
            part(3, 20, 30, 20),
            part(4, 30, 40, 20),
        ];
        let selected = select(&parts);
        assert_no_new_overlap(&parts, &selected);
    }

    #[test]
    fn l1_selection_keeps_progress_after_publication_changes_id_order() {
        let mut parts = vec![part(1, 0, 10, 58)];
        parts.extend((2..12).map(|id| part(id, id as i64 * 10, (id as i64 + 1) * 10, 9)));
        for id in 12..14 {
            let selected = select(&parts);
            assert_no_new_overlap(&parts, &selected);
            let from = selected
                .iter()
                .map(|i| parts[*i].min_timestamp_us)
                .min()
                .unwrap();
            let to = selected
                .iter()
                .map(|i| parts[*i].max_timestamp_us)
                .max()
                .unwrap()
                + 1;
            let mib = selected.iter().map(|i| parts[*i].file_size).sum::<u64>() / (1024 * 1024);
            parts = parts
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !selected.contains(i))
                .map(|(_, p)| p)
                .collect();
            parts.push(part(id, from, to, mib));
        }
        assert!(select(&parts).is_empty(), "only full runs should remain");
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::{CancellationToken, WriteStatus};
    use tempfile::TempDir;

    fn point(timestamp: i64) -> Point {
        Point::new(SeriesKey::new(1, 1, 1), timestamp, 1.0).expect("finite point")
    }

    fn wait_for(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !condition() {
            assert!(Instant::now() < deadline, "condition did not become true");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn packed_flush_releases_work_memory_before_waiting_for_publication() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(
            directory.path(),
            OpenOptions {
                virtual_shards: 1,
                ..OpenOptions::default()
            },
        )?;
        storage.write_batch((0..1000).map(point).collect(), Durability::Sync)?;
        let (entered, entries) = bounded(1);
        let (release, releases) = bounded(1);
        storage
            .inner
            .publish_tx
            .send(PublishCommand::Pause {
                entered,
                release: releases,
            })
            .expect("pause publisher");
        entries
            .recv_timeout(Duration::from_secs(3))
            .expect("publisher paused");
        let (reply, response) = bounded(1);
        storage
            .inner
            .write_tx
            .send(WriteCommand::Flush { reply })
            .expect("request flush");
        // Packing has finished and the output now waits for the paused publisher.
        wait_for(|| !storage.inner.publish_tx.is_empty());
        let work = storage.inner.work_memory_governor.stats()?;
        let frozen = read(&storage.inner.state)?.frozen.len();
        release.send(()).expect("resume publisher");
        response
            .recv_timeout(Duration::from_secs(3))
            .expect("flush completed")?;
        assert_eq!(storage.scan(point(0).series, 0, 1000)?.count(), 1000);
        storage.close()?;
        assert_eq!(frozen, 1, "MVCC input must remain until publication");
        assert_eq!(
            work.flush_bytes, 0,
            "packing scratch must not block the publisher"
        );
        Ok(())
    }

    #[test]
    fn strict_count_sum_consumes_a_disjoint_memory_tail_once() -> Result<()> {
        let root = TempDir::new()?;
        let storage = Storage::open(
            root.path(),
            OpenOptions {
                virtual_shards: 1,
                history_seal_lag: None,
                ..OpenOptions::default()
            },
        )?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("strict", 60_000_000, DAY_US)?
                .with_write_mode(WriteMode::StrictlyIncreasing),
        )?;
        let bucket = storage.create_bucket("production", "points", policy.id)?;
        let series = SeriesKey::new(1, bucket.id, 7);
        storage.write_batch(
            (1..=4096)
                .map(|index| Point::new(series, DAY_US + index, (index * 17 % 113) as f64))
                .collect::<Result<Vec<_>>>()?,
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
        for index in 0..32 {
            storage.write_batch(
                vec![Point::new(
                    series,
                    DAY_US + 10_000 + index,
                    (index as f64 + 1.0) / 8.0,
                )?],
                Durability::Sync,
            )?;
        }
        let request = GroupRequest::new(vec![series], DAY_US, 2 * DAY_US)?;
        let mut stream = storage.group_stream(request.clone())?;
        assert_eq!(
            stream.stats().planned_block_references,
            0,
            "summarized disk blocks must not remain in the memory-tail prefetch plan"
        );
        assert!(stream.open_next_series(false)?);
        let Some(SeriesCursor::Hybrid(cursor)) = stream.current.as_ref() else {
            panic!("persisted summary with a memory tail must use hybrid aggregation");
        };
        let plan = cursor.aggregate_days.get(&DAY_US).expect("summary day");
        assert!(!plan.persisted_segments.is_empty());
        assert_eq!(
            cursor
                .aggregate_day(DAY_US, 2 * DAY_US, plan)?
                .state
                .available,
            AggregateSet::COUNT_SUM,
            "persisted extended state must not enable unrequested tail aggregates"
        );
        let rows = stream.by_ref().collect::<Result<Vec<_>>>()?;
        let stats = stream.stats();
        let raw = storage
            .group_stream(request.clone().with_source(GroupSource::Raw))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows, raw);
        assert_eq!(rows[0].count, 4128);
        assert_eq!(stats.aggregate_base_point_lookups, 0);
        assert_eq!(
            stats.operations_scanned, 32,
            "memory tail must be consumed once, without consolidation"
        );
        drop(stream);

        // Count and sum remain finite, although a Welford variance update
        // would overflow. Unrequested statistics must not fail this query.
        storage.write_batch(
            vec![
                Point::new(series, DAY_US + 20_000, 1.0e308)?,
                Point::new(series, DAY_US + 20_001, -1.0e308)?,
            ],
            Durability::Sync,
        )?;
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert_eq!(automatic.rows[0].count, 4130);
        storage.close()
    }

    #[test]
    fn strict_partial_query_reuses_complete_disjoint_block_summaries() -> Result<()> {
        let root = TempDir::new()?;
        let storage = Storage::open(
            root.path(),
            OpenOptions {
                virtual_shards: 1,
                history_seal_lag: None,
                ..OpenOptions::default()
            },
        )?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("strict", 60_000_000, 10 * DAY_US)?
                .with_write_mode(WriteMode::StrictlyIncreasing),
        )?;
        let bucket = storage.create_bucket("production", "points", policy.id)?;
        let series = SeriesKey::new(1, bucket.id, 7);
        storage.write_batch(
            (1..=8192)
                .map(|index| Point::new(series, DAY_US + index, (index % 113) as f64))
                .collect::<Result<Vec<_>>>()?,
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
        storage.write_batch(
            (0..32)
                .map(|index| Point::new(series, DAY_US + 10_000 + index, 1.0))
                .collect::<Result<Vec<_>>>()?,
            Durability::Sync,
        )?;
        storage.inner.metadata_cache.clear()?;
        let request = GroupRequest::new(vec![series], DAY_US + 1, DAY_US + 20_000)?;
        let mut stream = storage.group_stream(request.clone())?;
        assert!(stream.open_next_series(false)?);
        let Some(SeriesCursor::Hybrid(cursor)) = stream.current.as_ref() else {
            panic!("complete disjoint blocks and a fresh tail must use hybrid aggregation");
        };
        let day = cursor.aggregate_days.get(&DAY_US).expect("summary day");
        assert!(
            day.persisted_segments.is_empty(),
            "the block plan already contains count/sum"
        );
        assert_eq!(
            day.block_summary
                .as_ref()
                .expect("block summary")
                .state
                .count,
            8192
        );
        let rows = stream.by_ref().collect::<Result<Vec<_>>>()?;
        let stats = stream.stats();
        drop(stream);
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(rows, raw.rows);
        assert_eq!(rows[0].count, 8224);
        assert_eq!(stats.operations_scanned, 32);
        assert_eq!(stats.planned_block_references, 0);

        // A boundary through the first block must retain the existing fallback.
        let clipped = GroupRequest::new(vec![series], DAY_US + 2000, DAY_US + 20_000)?;
        let automatic = storage.group(clipped.clone())?;
        let raw = storage.group(clipped.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert!(automatic.stats.operations_scanned > 32);
        storage.close()
    }

    #[test]
    fn strict_hybrid_keeps_raw_blocks_at_partial_day_boundaries() -> Result<()> {
        let root = TempDir::new()?;
        let storage = Storage::open(
            root.path(),
            OpenOptions {
                virtual_shards: 1,
                history_seal_lag: None,
                ..OpenOptions::default()
            },
        )?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("strict", 60_000_000, 10 * DAY_US)?
                .with_write_mode(WriteMode::StrictlyIncreasing),
        )?;
        let bucket = storage.create_bucket("production", "points", policy.id)?;
        let series = SeriesKey::new(1, bucket.id, 7);
        // 4100 points per day deliberately makes a block span midnight.
        let mut points = Vec::new();
        for day in 1..=2 {
            for index in 1..=4100 {
                points.push(Point::new(
                    series,
                    day * DAY_US + index,
                    (index % 113) as f64,
                )?);
            }
        }
        storage.write_batch(points, Durability::Sync)?;
        storage.checkpoint()?;
        storage.compact()?;
        for index in 0..32 {
            storage.write_batch(
                vec![Point::new(series, 2 * DAY_US + 10_000 + index, 1.0)?],
                Durability::Sync,
            )?;
        }
        let request = GroupRequest::new(vec![series], DAY_US + 2000, 3 * DAY_US)?;
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert_eq!(automatic.rows[0].count, 2101);
        assert_eq!(automatic.rows[1].count, 4132);
        assert!(automatic.stats.planned_block_references > 0);
        assert!(automatic.stats.planned_block_references < raw.stats.planned_block_references);

        let request = GroupRequest::new(vec![series], DAY_US, 3 * DAY_US)?;
        let automatic = storage.group(request.clone())?;
        let raw = storage.group(request.with_source(GroupSource::Raw))?;
        assert_eq!(automatic.rows, raw.rows);
        assert_eq!(automatic.stats.planned_block_references, 0);
        storage.close()
    }

    #[test]
    fn ordered_batches_match_mvcc_merge_and_reject_overlap() -> Result<()> {
        let governor = WorkMemoryGovernor::new(1024 * 1024);
        let budget =
            QueryBudget::new_with_admission(QueryOptions::default(), Arc::clone(&governor))?;
        let series = SeriesKey::new(1, 1, 7);
        let batches = (0_i64..32)
            .map(|batch| {
                let operation = |offset: i64, commit_id, ordinal, is_delete| VersionedPoint {
                    key: PointKey {
                        series,
                        timestamp_us: batch * 100 + offset,
                    },
                    commit_id,
                    ordinal,
                    value_bits: (commit_id as f64).to_bits(),
                    is_delete,
                };
                Arc::new(vec![
                    operation(1, 1, 0, false),
                    operation(1, 1, 1, true),
                    operation(2, 2, 0, false),
                    operation(2, 3, 0, false),
                ])
            })
            .collect::<Vec<_>>();
        let cursors = || {
            batches
                .iter()
                .rev()
                .map(|batch| SliceCursor::all(Arc::clone(batch)))
                .collect::<Vec<_>>()
        };
        let collect = |sources| -> Result<Vec<VersionedPoint>> {
            let mut visible = VisibleCursor::new(sources, 2)?;
            let mut points = Vec::new();
            while let Some(point) = visible.next_visible()? {
                points.push(point);
            }
            Ok(points)
        };
        let expected = collect(
            cursors()
                .into_iter()
                .map(|cursor| Box::new(cursor) as Box<dyn OperationCursor>)
                .collect(),
        )?;
        assert_eq!(expected.len(), 32);
        assert!(
            expected
                .iter()
                .all(|point| point.commit_id == 2 && point.ordinal == 0 && !point.is_delete)
        );
        let ordered = OrderedBatchCursor::try_new(cursors(), Arc::clone(&budget))
            .ok()
            .expect("disjoint batches");
        assert!(governor.stats()?.query_decoded_bytes > 0);
        assert!(
            !ordered.only_visible_unique_puts(2)?,
            "versions and tombstones must keep the MVCC path"
        );
        assert_eq!(collect(vec![Box::new(ordered)])?, expected);
        assert_eq!(governor.stats()?.query_decoded_bytes, 0);
        let mut overlapping = cursors();
        overlapping.push(SliceCursor::all(Arc::clone(&batches[0])));
        let fallback = OrderedBatchCursor::try_new(overlapping, Arc::clone(&budget))
            .err()
            .expect("overlapping keys require MVCC merge");
        assert_eq!(
            collect(
                fallback
                    .into_iter()
                    .map(|cursor| Box::new(cursor) as Box<dyn OperationCursor>)
                    .collect()
            )?,
            expected
        );
        let occupied = governor.reserve_background(WorkMemoryClass::Flush, 1024 * 1024)?;
        assert!(OrderedBatchCursor::try_new(cursors(), budget).is_err());
        drop(occupied);
        assert_eq!(governor.stats()?.used_bytes, 0);
        Ok(())
    }

    #[test]
    fn memory_envelope_preserves_gaps_and_half_open_overlap() {
        let ranges = [(i64::MIN, -100), (-5, -1), (10, 20), (100, i64::MAX)];
        let governor = Arc::new(MemoryGovernor::new(1024));
        let reservation = Arc::new(
            governor
                .try_reserve_token(MemoryClass::Memtable, 0)
                .expect("zero-byte batch"),
        );
        let batches = ranges
            .iter()
            .rev()
            .map(|&(minimum_timestamp_us, maximum_timestamp_us)| QueryBatch {
                batch: Arc::new(PointBatch::managed(Vec::new(), Arc::clone(&reservation))),
                minimum_timestamp_us,
                maximum_timestamp_us,
            })
            .collect::<Vec<_>>();
        let boundaries = [
            i64::MIN,
            -101,
            -100,
            -99,
            -5,
            -1,
            0,
            10,
            20,
            21,
            99,
            100,
            i64::MAX,
        ];
        for subset in [&batches[..], &batches[1..3], &batches[..0]] {
            let envelope = QueryBatchOverlap::new(subset);
            for &from in &boundaries {
                for &to in boundaries.iter().filter(|&&to| to > from) {
                    assert_eq!(
                        envelope.intersects(from, to),
                        subset.iter().any(|batch| batch.intersects(from, to)),
                        "[{from}, {to})"
                    );
                }
            }
        }
        let middle = QueryBatchOverlap::new(&batches[1..3]);
        assert!(!middle.intersects(0, 10));
        assert!(middle.intersects(20, 21));
        assert!(!middle.intersects(21, 99));
    }

    #[test]
    fn hybrid_overlap_index_matches_exhaustive_block_search() -> Result<()> {
        let governor = WorkMemoryGovernor::new(1024 * 1024);
        let budget =
            QueryBudget::new_with_admission(QueryOptions::default(), Arc::clone(&governor))?;
        // Duplicate ranges, nested blocks, disjoint gaps and both timestamp limits.
        let mut ranges = vec![
            (0, i64::MIN, i64::MIN),
            (1, i64::MAX - 1, i64::MAX),
            (0, -10, 10),
            (0, -8, 12),
            (1, -5, 8),
            (2, -5, 8),
        ];
        for number in 0..100 {
            let start = (number * 17 % 53) - 26;
            ranges.push((number as usize % 7, start, start + number % 11));
        }
        let index = HybridReadOverlap::from_ranges(
            ranges.len(),
            ranges.iter().copied(),
            Arc::clone(&budget),
        )
        .expect("index budget");
        for owner in 0..8 {
            for from in -30..40 {
                for to in from + 1..41 {
                    let expected = ranges.iter().any(|(source, start, end)| {
                        *source != owner && *start < to && *end >= from
                    });
                    assert_eq!(
                        index.overlaps_other(owner, from, to),
                        expected,
                        "owner={owner} range=[{from}, {to})"
                    );
                }
            }
        }
        assert!(!index.overlaps_other(0, i64::MIN, i64::MIN + 1));
        assert!(index.overlaps_other(1, i64::MIN, i64::MIN + 1));
        assert!(!index.overlaps_other(1, i64::MAX - 1, i64::MAX));
        assert!(index.overlaps_other(0, i64::MAX - 1, i64::MAX));
        assert!(governor.stats()?.query_plan_bytes > 0);
        drop(index);
        assert_eq!(governor.stats()?.query_plan_bytes, 0);
        let occupied = governor.reserve_background(WorkMemoryClass::Flush, 1024 * 1024)?;
        assert!(
            HybridReadOverlap::from_ranges(ranges.len(), ranges.iter().copied(), budget).is_none()
        );
        drop(occupied);
        assert_eq!(governor.stats()?.used_bytes, 0);
        Ok(())
    }

    #[test]
    fn read_directory_rebuilds_only_changed_shards_and_preserves_snapshots() -> Result<()> {
        #[derive(Debug)]
        struct FixedClock;
        impl Clock for FixedClock {
            fn now_us(&self) -> i64 {
                100 * DAY_US
            }
        }
        let root = TempDir::new()?;
        let options = OpenOptions {
            virtual_shards: 4,
            partition_span_us: DAY_US,
            history_window: Duration::from_secs(86_400),
            clock: Arc::new(FixedClock),
            ..OpenOptions::default()
        };
        let storage = Storage::open(root.path(), options.clone())?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("strict", 60_000_000, DAY_US)?
                .with_write_mode(WriteMode::StrictlyIncreasing),
        )?;
        let bucket = storage.create_bucket("production", "points", policy.id)?;
        let mut series_by_shard = BTreeMap::new();
        for key in 0..1000 {
            let series = SeriesKey::new(1, bucket.id, key);
            series_by_shard
                .entry(shard_for(series, 4))
                .or_insert(series);
            if series_by_shard.len() == 4 {
                break;
            }
        }
        assert_eq!(series_by_shard.len(), 4);
        let changed = series_by_shard[&0];
        let mut points = series_by_shard
            .values()
            .map(|series| Point::new(*series, DAY_US + 1, 1.0))
            .collect::<Result<Vec<_>>>()?;
        points.push(Point::new(changed, 5 * DAY_US + 1, 5.0)?);
        storage.write_batch(points, Durability::Sync)?;
        storage.checkpoint()?;
        storage.compact()?;
        let before = Manifest::load(root.path())?;
        assert_eq!(before.published_read_directories.len(), 4);
        let generations = before
            .published_read_directories
            .iter()
            .map(|directory| (directory.shard_id, directory.generation_id))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(generations.len(), 4);
        let old = storage.scan(changed, 0, 10 * DAY_US)?;
        let builds = storage.stats()?.read_directory_build_attempts;
        storage.write_batch(
            vec![Point::new(changed, 7 * DAY_US + 1, 7.0)?],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
        let after = Manifest::load(root.path())?;
        assert_eq!(after.published_read_directories.len(), 4);
        for directory in &after.published_read_directories {
            let prior = generations[&directory.shard_id];
            if directory.shard_id == Some(0) {
                assert_ne!(directory.generation_id, prior);
            } else {
                assert_eq!(directory.generation_id, prior);
            }
        }
        assert_eq!(storage.stats()?.read_directory_build_attempts - builds, 1);
        assert_eq!(old.collect::<Result<Vec<_>>>()?.len(), 2);
        let snapshot = storage.capture_snapshot(
            &[series_by_shard[&1]],
            0,
            10 * DAY_US,
            &QueryOptions::default(),
        )?;
        assert_eq!(snapshot.published_read_directories.len(), 1);
        assert_eq!(
            snapshot.published_read_directories[0].shard_scope(),
            Some(1)
        );
        drop(snapshot);
        storage.close()?;
        let reopened = Storage::open(root.path(), options.clone())?;
        for (shard, series) in series_by_shard {
            assert_eq!(
                reopened
                    .scan(series, 0, 10 * DAY_US)?
                    .collect::<Result<Vec<_>>>()?
                    .len(),
                if shard == 0 { 3 } else { 1 }
            );
        }
        reopened.close()?;
        assert!(Storage::verify(root.path()).valid);
        let manifest = Manifest::load(root.path())?;
        let mut overlapping = manifest.clone();
        overlapping.published_read_directories[0].shard_id = None;
        overlapping.persist(root.path())?;
        assert!(
            Manifest::load(root.path()).is_err(),
            "legacy scope cannot overlap shard directories"
        );
        let mut mislabeled = manifest;
        for directory in &mut mislabeled.published_read_directories {
            directory.shard_id = directory.shard_id.map(|shard| (shard + 1) % 4);
        }
        mislabeled.persist(root.path())?;
        Manifest::load(root.path())?;
        assert!(
            Storage::open(root.path(), options).is_err(),
            "embedded descriptors must match the declared shard"
        );
        Ok(())
    }

    #[test]
    fn published_read_directory_skips_busy_work_budget_and_retries() -> Result<()> {
        #[derive(Debug)]
        struct FixedClock;
        impl Clock for FixedClock {
            fn now_us(&self) -> i64 {
                100 * DAY_US
            }
        }

        let directory = TempDir::new()?;
        let storage = Storage::open(
            directory.path(),
            OpenOptions {
                virtual_shards: 1,
                partition_span_us: DAY_US,
                history_window: Duration::from_secs(86_400),
                clock: Arc::new(FixedClock),
                ..OpenOptions::default()
            },
        )?;
        storage.create_space("production")?;
        let policy = storage.create_policy(
            StoragePolicyOptions::new("strict", 60_000_000, DAY_US)?
                .with_write_mode(WriteMode::StrictlyIncreasing),
        )?;
        let bucket = storage.create_bucket("production", "points", policy.id)?;
        let series = SeriesKey::new(1, bucket.id, 7);
        storage.write_batch(
            vec![
                Point::new(series, DAY_US + 1, 1.0)?,
                Point::new(series, 69 * DAY_US + 1, 2.0)?,
            ],
            Durability::Sync,
        )?;
        storage.checkpoint()?;
        storage.compact()?;
        let mut manifest = Manifest::load(directory.path())?;
        assert_eq!(manifest.published_read_directories.len(), 1);
        // Force a fresh directory build over real published L2 segments.
        manifest.published_read_directories.clear();
        manifest.published_read_payload_files.clear();
        let segments = read(&storage.inner.state)?.segments.clone();
        let occupied = storage.inner.work_memory_governor.reserve_background(
            WorkMemoryClass::QueryPlan,
            storage.inner.work_memory_governor.stats()?.maximum_bytes,
        )?;
        let inner = Arc::clone(&storage.inner);
        let retry_segments = segments.clone();
        let (reply, response) = bounded(1);
        let worker = thread::spawn(move || {
            let mut unpublished = UnpublishedArtifacts::default();
            let result = prepare_published_read_directories(
                &inner,
                &mut manifest,
                &segments,
                &mut unpublished,
            )
            .map(|directories| (manifest, directories.len()));
            let _ = reply.send(result);
        });
        let outcome = response.recv_timeout(Duration::from_secs(3));
        // Also let the old blocking implementation finish on test failure.
        drop(occupied);
        worker.join().expect("directory worker");
        let (fallback, count) = outcome.expect("optional directory must not wait for memory")?;
        assert_eq!(count, 0);
        assert!(fallback.published_read_directories.is_empty());
        assert!(fallback.published_read_payload_files.is_empty());
        assert_eq!(fallback.segments.len(), retry_segments.len());
        for segment in &retry_segments {
            assert!(
                fallback
                    .segments
                    .iter()
                    .any(|entry| entry.id == segment.descriptor().id)
            );
        }
        let mut retry = fallback;
        let mut unpublished = UnpublishedArtifacts::default();
        let built = prepare_published_read_directories(
            &storage.inner,
            &mut retry,
            &retry_segments,
            &mut unpublished,
        )?;
        assert_eq!(built.len(), 1);
        assert_eq!(retry.published_read_directories.len(), 1);
        assert_eq!(
            retry.segments.len() + built[0].covered_segment_count(),
            retry_segments.len()
        );
        assert_eq!(
            storage.inner.work_memory_governor.stats()?.compaction_bytes,
            0
        );
        assert_eq!(storage.scan(series, 0, 70 * DAY_US)?.count(), 2);
        drop(built);
        drop(unpublished);
        storage.close()
    }

    #[test]
    fn full_write_queue_rejects_before_wal_and_ticket_survives_timeout() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(
            directory.path(),
            OpenOptions {
                write_queue_capacity: 1,
                ..OpenOptions::default()
            },
        )?;
        storage.flush()?; // Wait for writer initialization before holding state.
        let state = write(&storage.inner.state)?;
        let mut first = storage.enqueue_batch(vec![point(1)], Durability::Sync)?;
        wait_for(|| storage.inner.write_tx.is_empty());
        let second = storage.enqueue_batch(vec![point(2)], Durability::Sync)?;
        let started = Instant::now();
        let error = storage
            .enqueue_batch_with_options(
                vec![point(3)],
                Durability::Sync,
                AdmissionOptions {
                    deadline: started + Duration::from_millis(20),
                },
            )
            .expect_err("full queue");
        assert!(matches!(
            error,
            Error::Overloaded(OverloadReason::WriteQueue)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            first.wait_until(Instant::now() + Duration::from_millis(5)),
            WriteStatus::Pending
        ));
        assert_eq!(storage.write_admission_stats().outstanding_batches, 2);
        drop(state);
        assert_eq!(first.wait()?.commit_id, 1);
        assert_eq!(second.wait()?.commit_id, 2);
        // A rejected packet did not consume a commit or enter WAL.
        assert_eq!(
            storage
                .write_batch(vec![point(3)], Durability::Sync)?
                .commit_id,
            3
        );
        let stats = storage.write_admission_stats();
        assert_eq!(stats.accepted_batches, 3);
        assert_eq!(stats.completed_batches, 3);
        assert_eq!(stats.rejected_overload, 1);
        assert_eq!(stats.wait_timeouts, 1);
        assert_eq!(stats.outstanding_operations, 0);
        storage.close()?;
        let reopened = Storage::open(directory.path(), OpenOptions::default())?;
        assert_eq!(reopened.scan(point(1).series, 0, 4)?.count(), 3);
        reopened.close()
    }

    #[test]
    fn memory_admission_deadline_includes_flush_wait() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        let reservation = storage
            .inner
            .memory_governor
            .try_reserve_token(
                MemoryClass::WriteQueue,
                storage.inner.memory_governor.maximum_bytes(),
            )
            .expect("empty memory budget");
        let state = write(&storage.inner.state)?;
        let started = Instant::now();
        assert!(matches!(
            storage.enqueue_batch_with_options(
                vec![point(1)],
                Durability::Sync,
                AdmissionOptions {
                    deadline: started + Duration::from_millis(20)
                }
            ),
            Err(Error::Overloaded(OverloadReason::ManagedMemory))
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(storage.write_admission_stats().accepted_batches, 0);
        drop(state);
        drop(reservation);
        assert_eq!(
            storage
                .write_batch(vec![point(1)], Durability::Sync)?
                .commit_id,
            1
        );
        storage.close()
    }

    #[test]
    fn stop_and_drain_preserve_accepted_writes_and_existing_snapshot() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        storage.write_batch(vec![point(1)], Durability::Sync)?;
        let snapshot = storage.scan(point(1).series, 0, 4)?;
        let state = write(&storage.inner.state)?;
        let ticket = storage.enqueue_batch(vec![point(2)], Durability::Sync)?;
        let pending = storage.drain(Instant::now() + Duration::from_millis(20))?;
        assert_eq!(pending.checkpoint_commit_id, None);
        assert_eq!(pending.writes.outstanding_operations, 1);
        assert!(matches!(
            storage.enqueue_batch(vec![point(3)], Durability::Sync),
            Err(Error::AdmissionStopped)
        ));
        assert!(matches!(
            storage.scan(point(1).series, 0, 4),
            Err(Error::AdmissionStopped)
        ));
        drop(state);
        drop(ticket); // Completion accounting must not depend on observing a ticket.
        let drained = storage.drain(Instant::now() + Duration::from_secs(3))?;
        assert_eq!(drained.checkpoint_commit_id, Some(2));
        assert_eq!(drained.writes.outstanding_batches, 0);
        assert_eq!(drained.writes.completed_batches, 2);
        assert_eq!(snapshot.collect::<Result<Vec<_>>>()?, vec![point(1)]);
        assert_eq!(storage.drain(Instant::now())?, drained);
        storage.close()
    }

    #[test]
    fn close_deadline_retains_directory_lock_and_continues_same_shutdown() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        let state = write(&storage.inner.state)?;
        let ticket = storage.enqueue_batch(vec![point(1)], Durability::Sync)?;
        let started = Instant::now();
        let report = storage.close_until(started + Duration::from_millis(20))?;
        assert!(!report.closed);
        assert_eq!(report.writes.outstanding_batches, 1);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!storage.close_until(Instant::now())?.closed);
        assert!(matches!(
            Storage::open(directory.path(), OpenOptions::default()),
            Err(Error::Lock(_))
        ));
        drop(state);
        assert_eq!(ticket.wait()?.commit_id, 1);
        assert!(
            storage
                .close_until(Instant::now() + Duration::from_secs(3))?
                .closed
        );
        let reopened = Storage::open(directory.path(), OpenOptions::default())?;
        assert_eq!(reopened.point(point(1).series, 1)?, Some(point(1)));
        reopened.close()
    }

    fn planning_task() -> QueryPlanningTask {
        QueryPlanningTask::Segments {
            segments: Vec::new(),
            series_ranges: Vec::new(),
            from_us: 0,
            to_us: 1,
            commit_id: 0,
        }
    }

    #[test]
    fn query_queue_and_worker_deadlines_return_without_joining_workers() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        let executor = Arc::clone(&storage.inner.query_executor);
        let (entered, entries) = bounded(executor.worker_count * 3);
        let (release, releases) = bounded::<()>(1);
        for _ in 0..executor.worker_count {
            executor
                .sender
                .send(QueryWork::Pause {
                    entered: entered.clone(),
                    release: releases.clone(),
                })
                .expect("pause job");
        }
        for _ in 0..executor.worker_count {
            entries
                .recv_timeout(Duration::from_secs(1))
                .expect("paused");
        }
        let snapshot =
            storage.capture_snapshot(&[point(1).series], 0, 4, &QueryOptions::default())?;
        let budget = || {
            QueryBudget::new_with_admission(
                QueryOptions {
                    deadline: Some(Instant::now() + Duration::from_millis(20)),
                    ..QueryOptions::default()
                },
                Arc::clone(&storage.inner.work_memory_governor),
            )
        };
        let started = Instant::now();
        assert!(matches!(
            executor.plan(vec![planning_task()], snapshot.query_lease(), &budget()?),
            Err(Error::QueryDeadlineExceeded)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        // Fill the remaining slots while all workers are still paused.
        while !executor.sender.is_full() {
            executor
                .sender
                .try_send(QueryWork::Pause {
                    entered: entered.clone(),
                    release: releases.clone(),
                })
                .expect("queue slot");
        }
        assert!(matches!(
            executor.plan(vec![planning_task()], snapshot.query_lease(), &budget()?),
            Err(Error::Overloaded(OverloadReason::QueryQueue))
        ));
        drop(snapshot);
        storage.close()?;
        // The abandoned queued read still owns the snapshot and directory lease.
        assert!(matches!(
            Storage::open(directory.path(), OpenOptions::default()),
            Err(Error::Lock(_))
        ));
        drop(release);
        wait_for(|| {
            storage
                .inner
                .snapshot_tracker
                .stats()
                .is_ok_and(|stats| stats.0 == 0)
        });
        let reopened = Storage::open(directory.path(), OpenOptions::default())?;
        reopened.close()
    }

    #[test]
    fn query_deadline_covers_waiting_for_snapshot_state() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        storage.flush()?;
        let state = write(&storage.inner.state)?;
        thread::scope(|scope| {
            let (reply, replies) = bounded(2);
            let storage = &storage;
            scope.spawn(move || {
                let options = || QueryOptions {
                    deadline: Some(Instant::now() + Duration::from_millis(20)),
                    ..QueryOptions::default()
                };
                reply
                    .send(matches!(
                        storage.scan_with_options(point(1).series, 0, 4, options()),
                        Err(Error::QueryDeadlineExceeded)
                    ))
                    .expect("scan result");
                reply
                    .send(matches!(
                        storage.point_with_options(point(1).series, 1, options()),
                        Err(Error::QueryDeadlineExceeded)
                    ))
                    .expect("point result");
            });
            let first = replies.recv_timeout(Duration::from_secs(1));
            let second = replies.recv_timeout(Duration::from_secs(1));
            drop(state);
            assert!(first.expect("scan deadline while state is held"));
            assert!(second.expect("point deadline while state is held"));
        });
        // An options/request template carries no already-ticking absolute deadline.
        let template = QueryOptions::default();
        assert_eq!(template.deadline, None);
        storage.scan_with_options(point(1).series, 0, 4, template.clone())?;
        storage.point_with_options(point(1).series, 1, template)?;
        storage.close()
    }

    #[test]
    fn raw_scan_honors_cancellation_and_deadline_with_persisted_data() -> Result<()> {
        let directory = TempDir::new()?;
        let storage = Storage::open(directory.path(), OpenOptions::default())?;
        storage.write_batch((0..100).map(point).collect(), Durability::Sync)?;
        storage.checkpoint()?;
        let cancel = CancellationToken::new();
        let mut stream = storage.scan_with_options(
            point(1).series,
            0,
            100,
            QueryOptions {
                cancellation: Some(cancel.clone()),
                ..QueryOptions::default()
            },
        )?;
        assert_eq!(stream.next().transpose()?, Some(point(0)));
        cancel.cancel();
        assert!(matches!(stream.next(), Some(Err(Error::QueryCancelled))));
        assert!(stream.next().is_none());
        drop(stream);
        assert!(matches!(
            storage.scan_with_options(
                point(1).series,
                0,
                100,
                QueryOptions {
                    deadline: Some(Instant::now()),
                    ..QueryOptions::default()
                }
            ),
            Err(Error::QueryDeadlineExceeded)
        ));
        assert!(matches!(
            storage.point_with_options(
                point(1).series,
                1,
                QueryOptions {
                    deadline: Some(Instant::now()),
                    ..QueryOptions::default()
                }
            ),
            Err(Error::QueryDeadlineExceeded)
        ));
        storage.close()
    }
}
