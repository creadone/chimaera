use super::*;
use crate::admission::WorkMemoryGovernor;
use crate::cache::BlockCache;
use crate::layout::Domain;
use crate::segment::Segment;
use crate::segment::cursor::SliceCursor;
use crate::segment::pack::write_segment;
use crate::types::{PointKey, VersionedPoint};
use std::io::Write;
use std::sync::Barrier;
use tempfile::TempDir;

fn fixture(cache_bytes: u64) -> Result<(TempDir, Arc<Segment>)> {
    let temporary = TempDir::new()?;
    let blocks = Arc::new(BlockCache::new(1024 * 1024));
    let points = (0..64)
        .flat_map(|key| {
            [10, 20].map(move |timestamp_us| VersionedPoint {
                key: PointKey {
                    series: SeriesKey::new(1, 1, key),
                    timestamp_us,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: (key as f64).to_bits(),
                is_delete: false,
            })
        })
        .collect();
    let written = write_segment(
        temporary.path(),
        [19; 16],
        1,
        0,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
        },
        vec![Box::new(SliceCursor::all(Arc::new(points)))],
        &blocks,
    )?;
    let cache = Arc::new(MetadataCache::with_governor(
        cache_bytes,
        Arc::new(MemoryGovernor::new(1024 * 1024)),
    ));
    let segment = Segment::open(
        temporary.path(),
        [19; 16],
        written.segment.descriptor.clone(),
        blocks,
        cache,
        FilePermitPool::new(8)?,
    )?;
    segment.metadata_cache.clear()?;
    assert!(segment.index_root.is_some());
    Ok((temporary, segment))
}

fn selected(segment: &Segment, keys: &[u64], budget: &QueryBudget) -> Result<Arc<SegmentMetadata>> {
    let series = keys
        .iter()
        .map(|key| SeriesKey::new(1, 1, *key))
        .collect::<Vec<_>>();
    let metadata = segment.metadata_for_query_series(&series, budget)?;
    assert_eq!(metadata.index.len(), keys.len());
    for (entry, series) in metadata.index.iter().zip(series) {
        assert_eq!(entry.series, series);
    }
    Ok(metadata)
}

#[test]
fn r1_changed_series_selection_reuses_page_without_opening_file() -> Result<()> {
    let (_temporary, segment) = fixture(1024 * 1024)?;
    let first = QueryBudget::new_with_admission(
        crate::QueryOptions::default(),
        WorkMemoryGovernor::new(64 * 1024 * 1024),
    )?;
    selected(&segment, &[0, 1], &first)?;
    assert_eq!(first.stats().index_pages_read, 1);
    let hidden = segment.path.with_extension("temporarily-hidden");
    std::fs::rename(&segment.path, &hidden)?;
    let second = QueryBudget::new_with_admission(
        crate::QueryOptions::default(),
        WorkMemoryGovernor::new(64 * 1024 * 1024),
    )?;
    let hit = selected(&segment, &[2, 3], &second);
    let cold = QueryBudget::new_with_admission(
        crate::QueryOptions::default(),
        WorkMemoryGovernor::new(64 * 1024 * 1024),
    )?;
    let miss = selected(&segment, &[63], &cold);
    std::fs::rename(&hidden, &segment.path)?;
    hit?;
    assert!(miss.is_err());
    assert_eq!(second.stats().metadata_bytes_read, 0);
    assert_eq!(second.stats().file_opens, 0);
    assert_eq!(second.stats().index_pages_read, 0);
    Ok(())
}

#[test]
fn r1_disabled_cache_and_nonresident_root_use_exact_fallback() -> Result<()> {
    let (_temporary, mut segment) = fixture(0)?;
    let unique = Arc::get_mut(&mut segment).ok_or_else(|| corrupt("fixture segment shared"))?;
    unique.index_root = None;
    unique._index_root_reservation = None;
    for _ in 0..2 {
        let budget = QueryBudget::new_with_admission(
            crate::QueryOptions::default(),
            WorkMemoryGovernor::new(64 * 1024 * 1024),
        )?;
        selected(&segment, &[0, 9, 63], &budget)?;
        assert_eq!(budget.stats().index_pages_read, 3);
        assert_eq!(budget.stats().index_roots_read, 1);
        assert!(budget.stats().metadata_bytes_read > 0);
    }
    assert_eq!(segment.metadata_cache.stats()?.bytes, 0);
    Ok(())
}

#[test]
fn r1_corrupt_and_truncated_pages_are_rejected_and_work_is_released() -> Result<()> {
    let (_temporary, segment) = fixture(1024 * 1024)?;
    let work = WorkMemoryGovernor::new(1024 * 1024);
    let data = std::fs::read(&segment.path)?;
    let page = segment.index_root.as_ref().unwrap().pages[0];
    let offset = segment.descriptor.file_offset + segment.index_offset + page.offset;
    for truncated in [false, true] {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&segment.path)?;
        if truncated {
            file.set_len(offset + 1)?;
        } else {
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(b"BROKEN!!")?;
        }
        let budget =
            QueryBudget::new_with_admission(crate::QueryOptions::default(), Arc::clone(&work))?;
        assert!(selected(&segment, &[0, 1], &budget).is_err());
        assert_eq!(segment.metadata_cache.stats()?.bytes, 0);
        assert_eq!(work.stats()?.used_bytes, 0);
        std::fs::write(&segment.path, &data)?;
    }
    let budget =
        QueryBudget::new_with_admission(crate::QueryOptions::default(), Arc::clone(&work))?;
    selected(&segment, &[0, 1], &budget)?;
    Ok(())
}

#[test]
fn r1_pinned_page_retains_managed_budget_after_eviction() -> Result<()> {
    let (_temporary, segment) = fixture(1024 * 1024)?;
    let resident = segment.metadata_cache.governor.stats().6;
    let budget = QueryBudget::new_with_admission(
        crate::QueryOptions::default(),
        WorkMemoryGovernor::new(64 * 1024 * 1024),
    )?;
    selected(&segment, &[0, 1], &budget)?;
    let page = segment.index_root.as_ref().unwrap().pages[0];
    let key = MetadataCacheKey::IndexPage(
        segment.descriptor.id,
        segment.descriptor.file_offset + segment.index_offset + page.offset,
    );
    let pinned = segment.metadata_cache.get_blocks_entry(key)?.unwrap();
    let charged = segment.metadata_cache.governor.stats().6;
    assert!(charged > resident);
    segment.metadata_cache.clear()?;
    assert_eq!(segment.metadata_cache.stats()?.bytes, 0);
    assert_eq!(segment.metadata_cache.governor.stats().6, charged);
    drop(pinned);
    assert_eq!(segment.metadata_cache.governor.stats().6, resident);
    Ok(())
}

#[test]
fn r1_simultaneous_misses_admit_one_page_and_cancelled_hits_fail() -> Result<()> {
    let (_temporary, segment) = fixture(1024 * 1024)?;
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        let threads = (0..8)
            .map(|key| {
                let barrier = &barrier;
                let segment = &segment;
                scope.spawn(move || -> Result<()> {
                    let budget = QueryBudget::new_with_admission(
                        crate::QueryOptions::default(),
                        WorkMemoryGovernor::new(64 * 1024 * 1024),
                    )?;
                    barrier.wait();
                    selected(segment, &[key], &budget)?;
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap()?;
        }
        Ok::<_, crate::Error>(())
    })?;
    assert_eq!(segment.metadata_cache.lock_state()?.entries.len(), 1);
    let budget = QueryBudget::new_with_admission(
        crate::QueryOptions::default(),
        WorkMemoryGovernor::new(64 * 1024 * 1024),
    )?;
    budget.cancel();
    assert!(selected(&segment, &[0], &budget).is_err());
    assert_eq!(budget.stats().file_opens, 0);
    Ok(())
}

#[test]
fn r1_small_work_budget_rejects_before_allocating_and_cache_pressure_falls_back() -> Result<()> {
    let (_temporary, segment) = fixture(512)?;
    let work = WorkMemoryGovernor::new(128);
    let budget =
        QueryBudget::new_with_admission(crate::QueryOptions::default(), Arc::clone(&work))?;
    assert!(selected(&segment, &[0, 1], &budget).is_err());
    assert_eq!(work.stats()?.used_bytes, 0);
    for _ in 0..2 {
        let budget = QueryBudget::new_with_admission(
            crate::QueryOptions::default(),
            WorkMemoryGovernor::new(64 * 1024 * 1024),
        )?;
        selected(&segment, &[0, 1], &budget)?;
        assert_eq!(budget.stats().index_pages_read, 1);
        assert_eq!(segment.metadata_cache.stats()?.bytes, 0);
    }
    Ok(())
}
