//! Page selection must reserve exactly the selected plan and return both budgets.
use super::*;
use crate::cache::BlockCache;
use crate::layout::Domain;
use crate::segment::cursor::SliceCursor;
use crate::segment::pack::write_segment;
use crate::segment::read_plan::QUERY_PLAN_BYTES_PER_BLOCK;
use crate::types::{PointKey, VersionedPoint};
use crate::{CancellationToken, Error, OverloadReason, QueryOptions};
use tempfile::TempDir;

fn directory() -> Result<(TempDir, Arc<OfflinePackDirectory>)> {
    directory_with_cache(0)
}

fn directory_with_cache(cache_bytes: u64) -> Result<(TempDir, Arc<OfflinePackDirectory>)> {
    directory_with_resources(
        cache_bytes,
        &Arc::new(MemoryGovernor::new(1024 * 1024)),
        &DirectoryPagePool::new(),
    )
}

pub(super) fn directory_with_resources(
    cache_bytes: u64,
    governor: &Arc<MemoryGovernor>,
    page_pool: &Arc<DirectoryPagePool>,
) -> Result<(TempDir, Arc<OfflinePackDirectory>)> {
    let temporary = TempDir::new()?;
    let mut manifest = Manifest::create(temporary.path(), 1, 100)?;
    let point = VersionedPoint {
        key: PointKey {
            series: SeriesKey::new(1, 1, 7),
            timestamp_us: 10,
        },
        commit_id: 1,
        ordinal: 0,
        value_bits: 1.0_f64.to_bits(),
        is_delete: false,
    };
    let written = write_segment(
        temporary.path(),
        manifest.storage_uuid,
        1,
        1,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
        },
        vec![Box::new(SliceCursor::all(Arc::new(vec![point])))],
        &Arc::new(BlockCache::new(1024 * 1024)),
    )?;
    manifest.segments.push(written.segment.descriptor().clone());
    let path = temporary.path().join("directory.bin");
    let segments = [written.segment];
    OfflinePackDirectory::build(&path, &manifest, &segments)?;
    let file_pool = FilePermitPool::new(8)?;
    let directory = OfflinePackDirectory::open(
        &path,
        &manifest,
        &segments,
        governor,
        page_pool,
        &file_pool,
        OpenFileCache::new(file_pool.clone(), 2),
        cache_bytes,
    )?;
    Ok((temporary, directory))
}

#[test]
fn resident_directory_page_needs_no_open_file_and_cold_page_still_reads() -> Result<()> {
    let (_temporary, directory) = directory_with_cache(65536)?;
    let query = |options| -> Result<crate::QueryStats> {
        let budget =
            QueryBudget::new_with_admission(options, WorkMemoryGovernor::new(1024 * 1024))?;
        let mut reservation = QueryPlanReservation::new(Arc::clone(&budget));
        let plan = directory.plan_query_series(
            &[(SeriesKey::new(1, 1, 7), 10)],
            11,
            &budget,
            &mut reservation,
        )?;
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].1.blocks.len(), 1);
        Ok(budget.stats())
    };
    let cold = query(QueryOptions::default())?;
    assert_eq!(cold.file_opens, 1);
    assert_eq!(cold.pack_directory_pages_read, 1);
    for _ in 0..100 {
        directory.file_cache.invalidate(&directory.path)?;
        let warm = query(QueryOptions::default())?;
        assert_eq!(warm.file_opens, 0);
        assert_eq!(warm.pack_directory_pages_read, 0);
        assert_eq!(warm.pack_directory_page_cache_hits, 1);
    }
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        query(QueryOptions {
            cancellation: Some(token),
            ..QueryOptions::default()
        }),
        Err(Error::QueryCancelled)
    ));
    directory.clear_page_cache()?;
    let cold = query(QueryOptions::default())?;
    assert_eq!(cold.file_opens, 1);
    assert_eq!(cold.pack_directory_pages_read, 1);
    Ok(())
}

#[test]
fn planned_cursor_preserves_half_open_boundaries_around_empty_days() -> Result<()> {
    use crate::cache::BlockCacheAdmission;
    use crate::segment::read_plan::QueryReadPlan;
    let (_temporary, directory) = directory()?;
    let budget = QueryBudget::new_with_admission(
        QueryOptions::default(),
        WorkMemoryGovernor::new(1024 * 1024),
    )?;
    let mut reservation = QueryPlanReservation::new(Arc::clone(&budget));
    let planned = directory.plan_query_series(
        &[(SeriesKey::new(1, 1, 7), 0)],
        100,
        &budget,
        &mut reservation,
    )?;
    let read = &planned[0].1;
    let plan = QueryReadPlan::new(
        std::iter::once(read),
        Arc::clone(&budget),
        Arc::clone(&directory.file_cache),
        reservation,
    )?;
    for (from, to, expected) in [
        (9, 10, None),
        (10, 11, Some(10)),
        (11, 12, None),
        (-crate::types::DAY_US, 0, None),
        (crate::types::DAY_US, 90 * crate::types::DAY_US, None),
    ] {
        let cursor = read.cursor(
            from,
            to,
            BlockCacheAdmission::Always,
            Arc::clone(&budget),
            Arc::clone(&plan),
        )?;
        let actual = match cursor {
            Some(mut c) => c.next_operation()?.map(|p| p.key.timestamp_us),
            None => None,
        };
        assert_eq!(actual, expected);
    }
    Ok(())
}

fn page(locators: &[Locator]) -> (PageDescriptor, Vec<u8>) {
    let mut bytes = Vec::new();
    encode_page(locators, &mut bytes);
    (
        PageDescriptor {
            first_series: locators[0].series,
            last_series: locators[locators.len() - 1].series,
            offset: 0,
            length: bytes.len() as u32,
            entries: locators.len() as u32,
            crc32c: crc32c::crc32c(&bytes),
        },
        bytes,
    )
}

#[test]
fn directory_page_reserves_only_selected_blocks_and_returns_both_budgets() -> Result<()> {
    let (_temporary, directory) = directory()?;
    let series = SeriesKey::new(1, 1, 7);
    let requested = HashMap::from([(series, 10)]);
    let mut locators = [super::tests::locator(7, 0); 5];
    // Only timestamps 10 and 19 intersect [10, 20); the other series is absent.
    for (locator, timestamp) in locators.iter_mut().zip([9, 10, 19, 20, 15]) {
        locator.block.min_timestamp_us = timestamp;
        locator.block.max_timestamp_us = timestamp;
        locator.block.offset = timestamp as u64;
    }
    locators[4].series = SeriesKey::new(1, 1, 8);
    locators[4].block.series = locators[4].series;
    let (page, bytes) = page(&locators);
    let required = 2 * QUERY_PLAN_BYTES_PER_BLOCK;
    for (query_limit, work_limit) in [
        (required - 1, required),
        (required, required - 1),
        (required, required),
    ] {
        let work = WorkMemoryGovernor::new(work_limit);
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                max_memory_bytes: query_limit,
                ..QueryOptions::default()
            },
            Arc::clone(&work),
        )?;
        let mut reservation = QueryPlanReservation::new(Arc::clone(&budget));
        let mut planned = BTreeMap::new();
        let mut returned = 0;
        let result = directory.decode_page(
            page,
            &bytes,
            &requested,
            20,
            &budget,
            &mut reservation,
            &mut planned,
            &mut returned,
        );
        if query_limit < required {
            assert!(matches!(result, Err(Error::ResourceLimit(_))));
        } else if work_limit < required {
            assert!(matches!(
                result,
                Err(Error::Overloaded(OverloadReason::WorkMemory))
            ));
        } else {
            result?;
            assert_eq!(returned, 2);
            assert_eq!(
                planned[&series][&1]
                    .iter()
                    .map(|b| b.offset)
                    .collect::<Vec<_>>(),
                [10, 19]
            );
            assert_eq!(work.stats()?.used_bytes, required);
            assert_eq!(budget.stats().plan_peak_memory_bytes, required);
        }
        if query_limit < required || work_limit < required {
            assert!(planned.is_empty());
            assert_eq!(returned, 0);
            assert_eq!(work.stats()?.used_bytes, 0);
        }
        drop(planned);
        drop(reservation);
        assert_eq!(work.stats()?.used_bytes, 0);
        // A failed page and a dropped successful plan must leave no query charge.
        let reusable = query_limit.min(work_limit);
        budget.reserve_memory(reusable)?;
        budget.release_memory(reusable);
        assert_eq!(work.stats()?.used_bytes, 0);
    }
    Ok(())
}

#[test]
fn directory_page_rejects_cancellation_and_corruption_before_materialization() -> Result<()> {
    let (_temporary, directory) = directory()?;
    let requested = HashMap::from([(SeriesKey::new(1, 1, 7), 0)]);
    for case in 0..3 {
        let mut locators = [super::tests::locator(7, 0), super::tests::locator(7, 1)];
        if case == 2 {
            locators[1].segment_id = 99;
        }
        let (page, mut bytes) = page(&locators);
        let cancellation = CancellationToken::new();
        if case == 1 {
            bytes[0] ^= 1;
        }
        let work = WorkMemoryGovernor::new(1024 * 1024);
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                cancellation: Some(cancellation.clone()),
                ..QueryOptions::default()
            },
            Arc::clone(&work),
        )?;
        if case == 0 {
            cancellation.cancel();
        }
        let mut reservation = QueryPlanReservation::new(Arc::clone(&budget));
        let mut planned = BTreeMap::new();
        let mut returned = 0;
        let result = directory.decode_page(
            page,
            &bytes,
            &requested,
            1,
            &budget,
            &mut reservation,
            &mut planned,
            &mut returned,
        );
        if case == 0 {
            assert!(matches!(result, Err(Error::QueryCancelled)));
        } else {
            assert!(matches!(result, Err(Error::Corruption(_))));
        }
        assert!(planned.is_empty());
        assert_eq!(returned, 0);
        drop(reservation);
        assert_eq!(work.stats()?.used_bytes, 0);
    }
    Ok(())
}
