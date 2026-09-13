//! Shared page admission must preserve data, budgets and directory lifetimes.
use super::super::budget_tests::directory_with_resources;
use super::*;
use crate::QueryOptions;
use crate::admission::WorkMemoryGovernor;
use crate::query::{QueryBudget, QueryStats};
use crate::segment::QueryPlanReservation;
use crate::types::SeriesKey;
use tempfile::TempDir;

fn fixture(
    governor: Arc<MemoryGovernor>,
    cache_bytes: u64,
    pool: &Arc<DirectoryPagePool>,
) -> Result<(TempDir, Arc<OfflinePackDirectory>)> {
    directory_with_resources(cache_bytes, &governor, pool)
}

fn query(directory: &OfflinePackDirectory, from: i64, to: i64) -> Result<QueryStats> {
    let budget = QueryBudget::new_with_admission(
        QueryOptions::default(),
        WorkMemoryGovernor::new(1024 * 1024),
    )?;
    let mut reservation = QueryPlanReservation::new(Arc::clone(&budget));
    let plan = directory.plan_query_series(
        &[(SeriesKey::new(1, 1, 7), from)],
        to,
        &budget,
        &mut reservation,
    )?;
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].1.blocks.len(), 1);
    Ok(budget.stats())
}

#[test]
fn foreign_cold_pages_make_room_for_real_directory_reads() -> Result<()> {
    let governor = Arc::new(MemoryGovernor::new(132 * 1024));
    let pool = DirectoryPagePool::new();
    let (_ta, a) = fixture(Arc::clone(&governor), 65536, &pool)?;
    let (_tb, b) = fixture(Arc::clone(&governor), 65536, &pool)?;
    let (_tc, c) = fixture(Arc::clone(&governor), 65536, &pool)?;
    a.cache_page(1000, Arc::new(vec![0xa; 65536]))?;
    b.cache_page(1000, Arc::new(vec![0xb; 65536]))?;
    let held_b = b.cached_page(1000)?.expect("pin B");
    let filled = governor.maximum_bytes() - governor.stats().1;
    let fill = governor
        .try_reserve_token(MemoryClass::WriteQueue, filled)
        .expect("fill remaining managed budget");
    let mut reads = 0;
    let mut hits = 0;
    for _ in 0..100 {
        let stats = query(&c, 10, 11)?;
        assert_eq!(stats.pack_directory_extents_returned, 1);
        reads += stats.pack_directory_pages_read;
        hits += stats.pack_directory_page_cache_hits;
    }
    assert_eq!(held_b.bytes[0], 0xb);
    assert_eq!(reads, 1);
    assert_eq!(hits, 99);
    assert_eq!(pool.stats().reclaimed_bytes, 65536);
    assert_eq!(pool.stats().admission_failures, 0);
    assert!(pool.stats().pressure_probes <= 6);
    drop((a, b, c, held_b, fill));
    assert_eq!(governor.stats().1, 0);
    assert!(pool.registry.lock().expect("registry").entries.is_empty());
    Ok(())
}

#[test]
fn pinned_pages_keep_their_charge_and_payload() -> Result<()> {
    let governor = Arc::new(MemoryGovernor::new(132 * 1024));
    let pool = DirectoryPagePool::new();
    let (_ta, a) = fixture(Arc::clone(&governor), 65536, &pool)?;
    let (_tb, b) = fixture(Arc::clone(&governor), 65536, &pool)?;
    let (_tc, c) = fixture(Arc::clone(&governor), 65536, &pool)?;
    a.cache_page(1000, Arc::new(vec![0xa; 65536]))?;
    b.cache_page(1000, Arc::new(vec![0xb; 65536]))?;
    let held_a = a.cached_page(1000)?.expect("pin A");
    let held_b = b.cached_page(1000)?.expect("pin B");
    c.cache_page(1000, Arc::new(vec![0xc; 65536]))?;
    assert!(c.cached_page(1000)?.is_none());
    let before = governor.stats().1;
    a.clear_page_cache()?;
    assert_eq!(governor.stats().1, before);
    c.cache_page(1000, Arc::new(vec![0xc; 65536]))?;
    assert!(c.cached_page(1000)?.is_none());
    assert_eq!(held_a.bytes[0], 0xa);
    drop(held_a);
    assert_eq!(governor.stats().1, before - 65536);
    c.cache_page(1000, Arc::new(vec![0xc; 65536]))?;
    assert_eq!(
        c.cached_page(1000)?
            .expect("admitted after last reader")
            .bytes[0],
        0xc
    );
    assert_eq!(pool.stats().admission_failures, 2);
    drop((a, b, c, held_b));
    assert_eq!(governor.stats().1, 0);
    assert!(pool.registry.lock().expect("registry").entries.is_empty());
    Ok(())
}

#[test]
fn concurrent_reading_and_directory_lifetimes_return_all_memory() -> Result<()> {
    let governor = Arc::new(MemoryGovernor::new(192 * 1024));
    let pool = DirectoryPagePool::new();
    let (_temp, directory) = fixture(Arc::clone(&governor), 65536, &pool)?;
    std::thread::scope(|scope| -> Result<()> {
        let workers = (0..4)
            .map(|_| {
                let directory = &directory;
                scope.spawn(move || -> Result<()> {
                    for _ in 0..1000 {
                        let stats = query(directory, 10, 11)?;
                        assert_eq!(stats.pack_directory_extents_returned, 1);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..100 {
            let (_temp, other) = fixture(Arc::clone(&governor), 256 * 1024, &pool)?;
            for page in 0..4 {
                other.cache_page(page, Arc::new(vec![0xab; 65536]))?;
            }
            directory.clear_page_cache()?;
        }
        for worker in workers {
            worker.join().expect("reader")?;
        }
        Ok(())
    })?;
    drop(directory);
    assert_eq!(governor.stats().1, 0);
    assert!(pool.registry.lock().expect("registry").entries.is_empty());
    Ok(())
}
