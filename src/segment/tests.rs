//! Проверки формата, кэша, планирования и курсоров сегментов.

use super::codecs::encode_gorilla_values;
use super::cursor::SliceCursor;
use super::metadata_cache::{MetadataCache, metadata_cache_entry_bytes};
use super::pack::{SegmentPackResources, SegmentWriteRequest, write_segment, write_segment_pack};
use super::read_plan::{QueryPlanReservation, QueryReadPlan};
use super::{BLOCK_HEADER_SIZE, HEADER_SIZE, SegmentMetadata};
use crate::admission::WorkMemoryGovernor;
use crate::cache::{BlockCache, BlockKey, MemoryGovernor};
use crate::catalog::{BlockCodec, CompressionProfile, TimestampCodec, ValueCodec};
use crate::error::{Result, corrupt, invalid};
use crate::file_pool::{FilePermitPool, OpenFileCache};
use crate::layout::Domain;
use crate::query::QueryBudget;
use crate::types::{AggregateResolution, PointKey, SeriesKey, VersionedPoint};
use crate::v1::{OperationKind, decode_block};
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn selected_aggregate_pages_are_reused_across_queries_and_keep_their_budget() -> Result<()> {
    let temporary = TempDir::new()?;
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let series = [SeriesKey::new(1, 1, 7), SeriesKey::new(1, 2, 7)];
    let points = series
        .iter()
        .enumerate()
        .flat_map(|(index, series)| {
            (0..4096).map(move |timestamp_us| VersionedPoint {
                key: PointKey {
                    series: *series,
                    timestamp_us,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: (index as f64 + 1.0).to_bits(),
                is_delete: false,
            })
        })
        .collect();
    let written = write_segment(
        temporary.path(),
        [12; 16],
        1,
        1,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: crate::types::DAY_US,
        },
        vec![Box::new(SliceCursor::all(Arc::new(points)))],
        &block_cache,
    )?;
    let cache = &written.segment.metadata_cache;
    cache.clear()?;
    let resident_bytes = cache.governor.stats().6;
    let new_budget = || {
        QueryBudget::new_with_admission(
            crate::QueryOptions::default(),
            WorkMemoryGovernor::new(1024 * 1024),
        )
    };
    for round in 0..2 {
        for (index, series) in series.iter().enumerate() {
            for resolution in [AggregateResolution::Day, AggregateResolution::Hour] {
                let budget = new_budget()?;
                let mut cursor = written
                    .segment
                    .aggregate_cursor_for_query(
                        *series,
                        resolution,
                        0,
                        crate::types::DAY_US,
                        &budget,
                    )?
                    .ok_or_else(|| corrupt("test aggregate page is missing"))?;
                let record = cursor
                    .next_record()?
                    .ok_or_else(|| corrupt("test aggregate record is missing"))?;
                assert_eq!(record.state.count, 4096);
                assert_eq!(record.total()?, 4096.0 * (index as f64 + 1.0));
                assert_eq!(record.bucket_end_us, resolution.duration_us());
                assert!(cursor.next_record()?.is_none());
                if round == 0 {
                    assert!(budget.stats().metadata_bytes_read > 0);
                } else {
                    assert_eq!(budget.stats().metadata_bytes_read, 0);
                }
            }
        }
    }
    assert!(cache.stats()?.bytes <= cache.maximum_bytes);
    let budget = new_budget()?;
    let mut held = written
        .segment
        .aggregate_cursor_for_query(
            series[0],
            AggregateResolution::Day,
            0,
            crate::types::DAY_US,
            &budget,
        )?
        .ok_or_else(|| corrupt("cached aggregate page is missing"))?;
    cache.clear()?;
    assert_eq!(cache.stats()?.bytes, 0);
    assert!(cache.governor.stats().6 > resident_bytes);
    drop(budget);
    assert_eq!(
        held.next_record()?.map(|record| record.state.count),
        Some(4096)
    );
    assert!(cache.governor.stats().6 > resident_bytes);
    drop(held);
    assert_eq!(cache.governor.stats().6, resident_bytes);
    Ok(())
}

#[test]
fn query_prefetch_does_not_reread_blocks_across_out_of_order_cursors() -> Result<()> {
    let temporary = TempDir::new()?;
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let points = Arc::new(
        (0..5)
            .map(|index| VersionedPoint {
                key: PointKey {
                    series: SeriesKey::new(1, 1, index),
                    timestamp_us: 10,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: (index as f64).to_bits(),
                is_delete: false,
            })
            .collect(),
    );
    let written = write_segment(
        temporary.path(),
        [11; 16],
        1,
        0,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
        },
        vec![Box::new(SliceCursor::all(points))],
        &block_cache,
    )?;
    for order in [[2, 0, 1, 3, 4], [4, 3, 2, 1, 0]] {
        let query_budget = QueryBudget::new_with_admission(
            crate::QueryOptions::default(),
            WorkMemoryGovernor::new(1024 * 1024),
        )?;
        let mut reservation = QueryPlanReservation::new(Arc::clone(&query_budget));
        let reads = written.segment.plan_query_series(
            &(0..5)
                .map(|key| (SeriesKey::new(1, 1, key), 0))
                .collect::<Vec<_>>(),
            100,
            &query_budget,
            &mut reservation,
        )?;
        let plan = QueryReadPlan::new(
            reads.iter().map(|(_, read)| read),
            Arc::clone(&query_budget),
            OpenFileCache::new(FilePermitPool::new(8)?, 2),
            reservation,
        )?;
        let mut expected_bytes = 0;
        for index in order {
            let descriptor = reads[index].1.blocks[0];
            expected_bytes += u64::from(descriptor.size);
            let bytes = plan.read_block(BlockKey::new(1, descriptor.offset))?;
            let operations = decode_block(&bytes)?;
            assert_eq!(operations.len(), 1);
            assert_eq!(operations[0].series, reads[index].0);
            assert_eq!(operations[0].value, Some(index as f64));
        }
        let stats = query_budget.stats();
        assert_eq!(stats.blocks_read, 5, "cursor order {order:?}");
        assert_eq!(stats.payload_bytes_read, expected_bytes);
        assert_eq!(stats.unused_prefetched_blocks, 0);
        assert_eq!(plan.state.lock().unwrap().used_bytes, 0);
    }
    Ok(())
}

#[test]
fn evicted_metadata_stays_accounted_while_a_cursor_holds_it() -> Result<()> {
    let sample = Arc::new(SegmentMetadata::new(Vec::new()));
    let bytes = metadata_cache_entry_bytes(sample.bytes());
    let governor = Arc::new(MemoryGovernor::new(bytes * 2));
    let cache = MetadataCache::with_governor(bytes, Arc::clone(&governor));
    let first = cache.insert(1, sample)?;
    let _second = cache.insert(2, Arc::new(SegmentMetadata::new(Vec::new())))?;
    let stats = cache.stats()?;
    assert_eq!(stats.bytes, bytes);
    assert_eq!(stats.victims_scanned, 1);
    assert_eq!(governor.stats().6, bytes * 2);
    drop(first);
    assert_eq!(governor.stats().6, bytes);
    Ok(())
}

#[test]
fn v1_segment_round_trip_and_deduplication() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage_uuid = [7_u8; 16];
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let series = SeriesKey::new(1, 2, 3);
    let points = Arc::new(vec![
        VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: 10,
            },
            commit_id: 1,
            ordinal: 0,
            value_bits: 1.0_f64.to_bits(),
            is_delete: false,
        },
        VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: 10,
            },
            commit_id: 2,
            ordinal: 0,
            value_bits: 2.0_f64.to_bits(),
            is_delete: false,
        },
    ]);
    let written = write_segment(
        temporary.path(),
        storage_uuid,
        1,
        1,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
        },
        vec![Box::new(SliceCursor::all(points))],
        &block_cache,
    )?;
    assert_eq!(written.segment.descriptor.record_count, 1);
    assert!(
        written
            .segment
            .path()
            .extension()
            .is_some_and(|value| value == "tss")
    );
    let mut cursor = written
        .segment
        .series_cursor_for_query(
            series,
            i64::MIN,
            i64::MAX,
            QueryBudget::new_with_admission(
                crate::QueryOptions::default(),
                crate::admission::WorkMemoryGovernor::new(1024 * 1024),
            )?,
        )?
        .ok_or_else(|| corrupt("missing series"))?;
    let recovered = cursor
        .next_operation()?
        .ok_or_else(|| corrupt("missing point"))?;
    assert_eq!(recovered.put_value()?, 2.0);
    assert!(cursor.next_operation()?.is_none());
    let decoded = crate::v1::read_operations(written.segment.path())?;
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].value, Some(2.0));
    Ok(())
}

#[test]
fn v1_segment_preserves_a_latest_delete() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage_uuid = [8_u8; 16];
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let series = SeriesKey::new(1, 2, 3);
    let operations = Arc::new(vec![
        VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: 10,
            },
            commit_id: 1,
            ordinal: 0,
            value_bits: 1.0_f64.to_bits(),
            is_delete: false,
        },
        VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: 10,
            },
            commit_id: 2,
            ordinal: 0,
            value_bits: 0,
            is_delete: true,
        },
    ]);
    let written = write_segment(
        temporary.path(),
        storage_uuid,
        1,
        1,
        Domain {
            shard_id: 0,
            policy_id: 1,
            window_start_us: 0,
            window_end_us: 100,
        },
        vec![Box::new(SliceCursor::all(operations))],
        &block_cache,
    )?;
    let decoded = crate::v1::read_operations(written.segment.path())?;
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].kind, OperationKind::Delete);
    assert_eq!(decoded[0].value, None);
    assert!(
        written
            .segment
            .metadata_cache
            .get_aggregate(written.segment.descriptor.id)?
            .is_some_and(|aggregate| aggregate.is_empty())
    );
    Ok(())
}

#[test]
fn gorilla_round_trip_uses_v1_decoder() -> Result<()> {
    let series = SeriesKey::new(1, 1, 1);
    let values = [1.0, 1.0, 1.5, 1.75, 1.75, -2.0];
    let operations = values
        .iter()
        .enumerate()
        .map(|(index, value)| VersionedPoint {
            key: PointKey {
                series,
                timestamp_us: index as i64,
            },
            commit_id: 1,
            ordinal: index as u32,
            value_bits: f64::to_bits(*value),
            is_delete: false,
        })
        .collect::<Vec<_>>();
    let bytes = encode_gorilla_values(&operations);
    assert_eq!(crate::v1::decode_values(&bytes, 1, values.len())?, values);
    Ok(())
}

#[test]
fn all_supported_policy_profiles_write_v1_blocks() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage_uuid = [10_u8; 16];
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let profiles = [
        (CompressionProfile::raw(), [0, 0, 0, 0]),
        (CompressionProfile::hot_fast(), [2, 0, 0, 0]),
        (CompressionProfile::columnar(), [2, 1, 0, 0]),
        (CompressionProfile::balanced(), [2, 1, 1, 4]),
        (CompressionProfile::cold_dense(), [2, 1, 1, 7]),
        (CompressionProfile::zstd(), [2, 1, 2, 4]),
        (
            CompressionProfile::new(
                TimestampCodec::DeltaVarint,
                ValueCodec::GorillaXor,
                BlockCodec::None,
                None,
            )?,
            [1, 1, 0, 0],
        ),
    ];
    let requests = profiles
        .iter()
        .enumerate()
        .map(|(index, (profile, _))| {
            let start = i64::try_from(index).map_err(|_| invalid("test index overflow"))? * 100;
            let series = SeriesKey::new(1, u32::try_from(index + 1).unwrap_or(u32::MAX), 1);
            let operations = (0..8)
                .map(|point| VersionedPoint {
                    key: PointKey {
                        series,
                        timestamp_us: start + i64::from(point) * 3,
                    },
                    commit_id: u64::MAX - u64::from(point),
                    ordinal: u32::MAX - point,
                    value_bits: (-f64::from(point) * 0.25).to_bits(),
                    is_delete: point == 7,
                })
                .collect::<Vec<_>>();
            Ok(SegmentWriteRequest {
                segment_id: u64::try_from(index + 1)
                    .map_err(|_| invalid("test segment ID overflow"))?,
                level: 0,
                domain: Domain {
                    shard_id: 0,
                    policy_id: u32::try_from(index + 1)
                        .map_err(|_| invalid("test policy ID overflow"))?,
                    window_start_us: start,
                    window_end_us: start + 100,
                },
                profile: *profile,
                rewrite_count: 0,
                sources: vec![Box::new(SliceCursor::all(Arc::new(operations)))],
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata_cache = Arc::new(MetadataCache::new(1024 * 1024));
    let file_pool = FilePermitPool::new(8)?;
    let written = write_segment_pack(
        temporary.path(),
        storage_uuid,
        1,
        0,
        requests,
        SegmentPackResources {
            block_cache: &block_cache,
            metadata_cache: &metadata_cache,
            file_pool: &file_pool,
            open_file_cache: None,
            work_memory: None,
        },
    )?;
    assert_eq!(written.len(), profiles.len());
    for (result, (_, expected_codecs)) in written.iter().zip(profiles) {
        let descriptor = result.segment.descriptor();
        let mut file = File::open(result.segment.path())?;
        file.seek(SeekFrom::Start(descriptor.file_offset + HEADER_SIZE as u64))?;
        let mut header = [0_u8; BLOCK_HEADER_SIZE];
        file.read_exact(&mut header)?;
        assert_eq!(&header[68..72], &expected_codecs);
        let mut operation_count = 0_u64;
        crate::v1::visit_segment(
            result.segment.path(),
            descriptor.file_offset,
            descriptor.file_size,
            |operation| {
                assert_eq!(operation.commit_id, u64::MAX - operation_count);
                assert_eq!(operation.ordinal, u32::MAX - operation_count as u32);
                if operation_count == 7 {
                    assert_eq!(operation.kind, OperationKind::Delete);
                    assert_eq!(operation.value, None);
                } else {
                    assert_eq!(
                        operation.value.map(f64::to_bits),
                        Some((-(operation_count as f64) * 0.25).to_bits())
                    );
                }
                operation_count += 1;
                Ok(())
            },
        )?;
        assert_eq!(operation_count, 8);
        if expected_codecs[2] == 2 {
            let size = u32::from_le_bytes(header[76..80].try_into().unwrap()) as usize;
            let mut bytes = header.to_vec();
            bytes.resize(BLOCK_HEADER_SIZE + size, 0);
            file.read_exact(&mut bytes[BLOCK_HEADER_SIZE..])?;
            assert_eq!(decode_block(&bytes)?.len(), 8);
            assert_zstd_block_corruption_rejected(&bytes);
        }
    }
    Ok(())
}

fn assert_zstd_block_corruption_rejected(original: &[u8]) {
    fn refresh_header(bytes: &mut [u8]) {
        let checksum = crc32c::crc32c(&bytes[..84]);
        bytes[84..88].copy_from_slice(&checksum.to_le_bytes());
    }
    let mut bytes = original.to_vec();
    bytes[BLOCK_HEADER_SIZE] ^= 1;
    assert!(
        matches!(decode_block(&bytes), Err(crate::Error::Corruption(message))
        if message.contains("payload CRC32C"))
    );
    // A valid outer checksum must not hide a malformed Zstd frame.
    let checksum = crc32c::crc32c(&bytes[BLOCK_HEADER_SIZE..]);
    bytes[80..84].copy_from_slice(&checksum.to_le_bytes());
    refresh_header(&mut bytes);
    assert!(matches!(
        decode_block(&bytes),
        Err(crate::Error::Corruption(_))
    ));
    for marker in [0, 2, 255] {
        let mut bytes = original.to_vec();
        bytes[71] = marker;
        refresh_header(&mut bytes);
        assert!(matches!(
            decode_block(&bytes),
            Err(crate::Error::Corruption(_))
        ));
    }
    for size in [1_u32, 4 * 1024 * 1024 + 1, u32::MAX] {
        let mut bytes = original.to_vec();
        bytes[72..76].copy_from_slice(&size.to_le_bytes());
        refresh_header(&mut bytes);
        assert!(matches!(
            decode_block(&bytes),
            Err(crate::Error::Corruption(_))
        ));
    }
}

#[test]
fn packed_regions_open_at_their_logical_offsets() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage_uuid = [9_u8; 16];
    let block_cache = Arc::new(BlockCache::new(1024 * 1024));
    let series = SeriesKey::new(1, 1, 1);
    let requests = (0_u64..2)
        .map(|index| {
            let start = i64::try_from(index).map_err(|_| invalid("test index overflow"))? * 100;
            let points = Arc::new(vec![VersionedPoint {
                key: PointKey {
                    series,
                    timestamp_us: start,
                },
                commit_id: index + 1,
                ordinal: 0,
                value_bits: (index as f64).to_bits(),
                is_delete: false,
            }]);
            Ok(SegmentWriteRequest {
                segment_id: index + 1,
                level: 0,
                domain: Domain {
                    shard_id: 0,
                    policy_id: 1,
                    window_start_us: start,
                    window_end_us: start + 100,
                },
                profile: CompressionProfile::hot_fast(),
                rewrite_count: 0,
                sources: vec![Box::new(SliceCursor::all(points))],
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata_cache = Arc::new(MetadataCache::new(1024 * 1024));
    let file_pool = FilePermitPool::new(8)?;
    let written = write_segment_pack(
        temporary.path(),
        storage_uuid,
        1,
        0,
        requests,
        SegmentPackResources {
            block_cache: &block_cache,
            metadata_cache: &metadata_cache,
            file_pool: &file_pool,
            open_file_cache: None,
            work_memory: None,
        },
    )?;
    assert_eq!(written.len(), 2);
    assert_eq!(written[0].segment.descriptor.file_offset, 0);
    assert_eq!(
        written[1].segment.descriptor.file_offset,
        written[0].segment.descriptor.file_size
    );
    for result in &written {
        let summary = crate::v1::visit_segment(
            result.segment.path(),
            result.segment.descriptor.file_offset,
            result.segment.descriptor.file_size,
            |_| Ok(()),
        )?;
        assert_eq!(summary.operation_count, 1);
    }
    Ok(())
}
