use super::*;
use crate::catalog::CompressionProfile;
use crate::engine::support::write;
use crate::engine::{OpenOptions, Storage};
use crate::layout::Domain;
use crate::segment::{
    SegmentPackResources, SegmentWriteRequest, SliceCursor, prepare_segment_pack,
};
use crate::types::{PointKey, SeriesKey, VersionedPoint};
use tempfile::TempDir;

#[test]
fn retirement_keeps_shared_pack_until_active_and_held_regions_are_released() -> crate::Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            checkpoint_on_close: false,
            ..OpenOptions::default()
        },
    )?;
    let requests = (1..=2)
        .map(|id| SegmentWriteRequest {
            segment_id: id,
            level: 0,
            domain: Domain {
                shard_id: 0,
                policy_id: 1,
                window_start_us: 0,
                window_end_us: 100,
            },
            profile: CompressionProfile::hot_fast(),
            rewrite_count: 0,
            sources: vec![Box::new(SliceCursor::all(Arc::new(vec![VersionedPoint {
                key: PointKey {
                    series: SeriesKey::new(1, 1, id),
                    timestamp_us: 10,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: 1.0_f64.to_bits(),
                is_delete: false,
            }])))],
        })
        .collect();
    let (mut written, unpublished) = prepare_segment_pack(
        temporary.path(),
        storage.inner.storage_uuid,
        100,
        0,
        requests,
        SegmentPackResources {
            block_cache: &storage.inner.block_cache,
            metadata_cache: &storage.inner.metadata_cache,
            file_pool: &storage.inner.file_pool,
            open_file_cache: Some(&storage.inner.open_file_cache),
            work_memory: None,
        },
    )?;
    crate::segment::sync_segment_pack_group(temporary.path())?;
    unpublished.commit();
    let retired_region = written.pop().expect("second region").segment;
    let active_region = written.pop().expect("first region").segment;
    let path = active_region.path().to_path_buf();
    assert_eq!(retired_region.path(), path);
    write(&storage.inner.state)?.segments.push(active_region);
    let mut retired = vec![vec![retired_region]];
    assert!(reap_ready_segments(&storage.inner, &mut retired));
    assert!(retired.is_empty());
    assert!(path.exists(), "another logical region is still active");
    assert!(!reap_ready_segments(&storage.inner, &mut retired));
    let remaining = write(&storage.inner.state)?
        .segments
        .pop()
        .expect("active region");
    let reader = Arc::clone(&remaining);
    retired.push(vec![remaining]);
    assert!(!reap_ready_segments(&storage.inner, &mut retired));
    assert!(path.exists(), "last region is held by a reader");
    drop(reader);
    assert!(reap_ready_segments(&storage.inner, &mut retired));
    assert!(retired.is_empty());
    assert!(!path.exists());
    assert!(!reap_ready_segments(&storage.inner, &mut retired));
    storage.close()
}
