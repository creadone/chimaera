//! Публичные снимки статистики и сбор показателей Storage.

use super::Storage;
use super::compaction_selection::{compaction_debt_for_state, sealed_before_for_policy};
use super::regulator::pacing_reason_text;
use super::support::{
    available_disk_bytes, directory_bytes, lock, micros_seconds, nanos_seconds, open_file_count,
    read,
};
use crate::error::Result;
use crate::layout::Domain;
use crate::lifecycle::WriteAdmissionStats;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::sync::atomic::Ordering;

/// Снимок диагностических счётчиков.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct HistoryWatermarkStats {
    pub policy_id: u32,
    pub event_watermark_us: i64,
    pub sealed_before_us: i64,
}

/// Долг активного L1 на последней публикации затронутого шарда.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct L1CompactionDebtStats {
    pub shard_id: u16,
    pub policy_id: u32,
    pub window_start_us: i64,
    pub parts: usize,
    pub eligible_input_parts: usize,
    pub eligible_input_bytes: u64,
    pub oldest_eligible_seconds: f64,
    pub overlap_components: usize,
    pub overlap_input_parts: usize,
    pub overlap_input_bytes: u64,
    /// Число компонентов, превышающих ремонтный предел байтов.
    pub overlap_components_over_byte_limit: usize,
    /// Число компонентов, превышающих ремонтный предел входов.
    pub overlap_components_over_fan_in_limit: usize,
    pub below_consolidation_trigger: bool,
    pub no_adjacent_pack: bool,
}

/// Снимок диагностических счётчиков.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct StorageStats {
    pub write_admission: WriteAdmissionStats,
    pub healthy: bool,
    pub last_background_error: Option<String>,
    pub visible_commit_id: u64,
    pub checkpoint_commit_id: u64,
    pub writes: u64,
    pub written_operations: u64,
    pub written_points: u64,
    pub deleted_points: u64,
    pub wal_bytes_written: u64,
    pub wal_syncs: u64,
    pub wal_sync_seconds: f64,
    pub group_commit_groups: u64,
    pub group_commit_batches: u64,
    pub active_memtable_bytes: u64,
    pub frozen_memtable_bytes: u64,
    pub l0_segments: usize,
    pub l1_segments: usize,
    pub active_l1_segments: usize,
    pub l1_compaction_debt: Vec<L1CompactionDebtStats>,
    pub l1_overlap_repair_jobs: u64,
    pub l1_overlap_repair_input_bytes: u64,
    pub l1_overlap_repair_max_input_bytes: u64,
    pub sealed_l2_segments: usize,
    pub history_watermarks: Vec<HistoryWatermarkStats>,
    pub logical_segments: usize,
    pub physical_segment_files: usize,
    pub l0_debt: usize,
    pub flushes: u64,
    pub flush_seconds: f64,
    pub flush_bytes: u64,
    pub compactions: u64,
    pub compacted_domains: u64,
    pub compaction_seconds: f64,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,
    pub compaction_physical_bytes_written: u64,
    pub compaction_block_visits: u64,
    pub compaction_unique_blocks: u64,
    pub compaction_unique_block_bytes: u64,
    pub compaction_physical_blocks_read: u64,
    pub compaction_physical_bytes_read: u64,
    pub compaction_decoded_input_bytes: u64,
    pub compaction_decoded_points: u64,
    pub compaction_decode_seconds: f64,
    pub active_compaction_physical_bytes_read: u64,
    pub active_compaction_physical_bytes_written: u64,
    pub active_compaction_rewrite_passes: u64,
    pub active_compaction_jobs: u64,
    pub active_compaction_max_input_bytes: u64,
    pub active_compaction_max_output_bytes: u64,
    pub active_compaction_max_fan_in: u64,
    pub active_compaction_debt_bytes: u64,
    pub active_compaction_oldest_debt_seconds: f64,
    pub sealed_compaction_physical_bytes_read: u64,
    pub sealed_compaction_physical_bytes_written: u64,
    pub sealed_compaction_rewrite_passes: u64,
    pub sealed_compaction_jobs: u64,
    pub sealed_compaction_max_input_bytes: u64,
    pub sealed_compaction_max_output_bytes: u64,
    pub sealed_compaction_max_fan_in: u64,
    pub sealed_compaction_debt_bytes: u64,
    pub sealed_compaction_oldest_debt_seconds: f64,
    pub compaction_fair_handoffs: u64,
    pub active_max_rewrite_count: u32,
    pub sealed_max_rewrite_count: u32,
    pub manifest_bytes: u64,
    pub manifest_limit_bytes: u64,
    pub manifest_headroom_bytes: u64,
    pub manifest_segment_descriptors: usize,
    pub manifest_archive_descriptors: usize,
    pub manifest_max_domain_descriptors: usize,
    pub published_read_directory_generation_id: Option<u64>,
    pub published_read_directory_generations: usize,
    pub published_read_directory_segments: usize,
    pub published_read_directory_pages: usize,
    pub published_read_directory_bytes: u64,
    pub read_directory_build_attempts: u64,
    pub read_directory_build_resource_skips: u64,
    pub read_directory_build_seconds: f64,
    pub read_directory_build_max_seconds: f64,
    pub read_directory_build_current_segments: u64,
    pub read_directory_build_max_segments: u64,
    pub compaction_current_input_bytes: u64,
    pub publisher_phase: &'static str,
    pub shutdown_phase: &'static str,
    pub manifest_publications: u64,
    pub manifest_encode_seconds: f64,
    pub manifest_write_seconds: f64,
    pub manifest_fsync_seconds: f64,
    pub manifest_lock_wait_seconds: f64,
    pub manifest_expired_segments: u64,
    pub manifest_expired_bytes: u64,
    pub write_queue_backpressure: u64,
    pub write_queue_backpressure_seconds: f64,
    pub flush_queue_backpressure: u64,
    pub flush_queue_backpressure_seconds: f64,
    pub background_errors: u64,
    pub background_checkpoints: u64,
    pub background_checkpoint_seconds: f64,
    pub foreground_maintenance_waits: u64,
    pub foreground_maintenance_wait_seconds: f64,
    pub write_pacing_events: u64,
    pub write_pacing_seconds: f64,
    pub write_pacing_active: bool,
    pub write_pacing_reason: Option<String>,
    pub write_pacing_active_seconds: f64,
    pub write_pacing_recoveries: u64,
    pub write_pacing_l0_events: u64,
    pub write_pacing_compaction_queue_events: u64,
    pub write_pacing_memory_events: u64,
    pub write_pacing_read_latency_events: u64,
    pub read_latency_window_samples: usize,
    pub read_latency_p95_seconds: f64,
    pub read_latency_single_window_samples: usize,
    pub read_latency_single_p95_seconds: f64,
    pub read_latency_wide_window_samples: usize,
    pub read_latency_wide_p95_seconds: f64,
    pub query_operations_scanned: u64,
    pub query_operation_bytes_decoded: u64,
    pub query_metadata_bytes_read: u64,
    pub query_payload_bytes_read: u64,
    pub query_blocks_read: u64,
    pub query_physical_blocks_decoded: u64,
    pub query_physical_decoded_bytes: u64,
    pub query_file_opens: u64,
    pub query_prefetched_blocks: u64,
    pub query_prefetch_hits: u64,
    pub query_planned_segment_reads: u64,
    pub query_segment_index_lookups: u64,
    pub query_segment_index_entries_examined: u64,
    pub query_segment_index_candidates: u64,
    pub query_segment_index_seconds: f64,
    pub query_peak_memory_bytes: u64,
    pub query_exact_daily_aggregate_queries: u64,
    pub query_aggregate_records_read: u64,
    pub query_points_returned: u64,
    pub storage_bytes: u64,
    pub open_files: u64,
    pub recovery_seconds: f64,
    pub recovery_manifest_seconds: f64,
    pub recovery_descriptors_seconds: f64,
    pub recovery_read_directory_pages: u64,
    pub recovery_read_directory_bytes: u64,
    pub recovery_wal_scan_seconds: f64,
    pub recovery_wal_replay_seconds: f64,
    pub recovery_read_ready_seconds: f64,
    pub recovery_write_ready_seconds: f64,
    pub block_cache_bytes: u64,
    pub block_cache_entries: usize,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub block_cache_mutex_wait_seconds: f64,
    pub block_cache_victims_scanned: u64,
    pub block_cache_eviction_seconds: f64,
    pub coalesced_block_loads: u64,
    pub coalesced_block_load_waiters: u64,
    pub block_load_bypasses: u64,
    pub block_load_wait_seconds: f64,
    pub block_loads_in_flight: usize,
    pub block_load_bytes_in_flight: u64,
    pub peak_block_loads_in_flight: u64,
    pub peak_block_load_bytes_in_flight: u64,
    pub metadata_cache_bytes: u64,
    pub metadata_cache_entries: usize,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub metadata_cache_mutex_wait_seconds: f64,
    pub metadata_cache_victims_scanned: u64,
    pub metadata_cache_eviction_seconds: f64,
    pub membership_filter_negatives: u64,
    pub membership_filter_false_positives: u64,
    pub managed_memory_max_bytes: u64,
    pub managed_memory_bytes: u64,
    pub managed_write_queue_bytes: u64,
    pub managed_memtable_bytes: u64,
    pub managed_series_watermark_bytes: u64,
    pub strict_series_watermarks: usize,
    pub pinned_memtable_bytes: u64,
    pub managed_block_cache_bytes: u64,
    pub pinned_block_cache_bytes: u64,
    pub managed_metadata_cache_bytes: u64,
    pub pinned_metadata_cache_bytes: u64,
    pub work_memory_max_bytes: u64,
    pub work_memory_bytes: u64,
    pub work_memory_peak_bytes: u64,
    pub query_plan_memory_bytes: u64,
    pub query_plan_peak_memory_bytes: u64,
    pub query_metadata_memory_bytes: u64,
    pub query_metadata_peak_memory_bytes: u64,
    pub query_compressed_memory_bytes: u64,
    pub query_compressed_peak_memory_bytes: u64,
    pub query_decoded_memory_bytes: u64,
    pub query_decoded_peak_memory_bytes: u64,
    pub query_results_memory_bytes: u64,
    pub query_results_peak_memory_bytes: u64,
    pub flush_memory_bytes: u64,
    pub flush_peak_memory_bytes: u64,
    pub compaction_memory_bytes: u64,
    pub compaction_peak_memory_bytes: u64,
    pub aggregate_builder_memory_bytes: u64,
    pub aggregate_builder_peak_memory_bytes: u64,
    pub recovery_memory_bytes: u64,
    pub recovery_peak_memory_bytes: u64,
    pub unbudgeted_large_allocations: u64,
    pub work_memory_waits: u64,
    pub work_memory_wait_seconds: f64,
    pub memory_backpressure_events: u64,
    /// Раздельные причины общего memory_backpressure; снимок счётчиков не атомарен.
    pub write_queue_memory_backpressure_events: u64,
    pub read_directory_build_memory_backpressure_events: u64,
    pub read_directory_root_memory_backpressure_events: u64,
    /// Directory cache probes made only after shared managed-memory admission failed.
    pub read_directory_page_cache_global_probes: u64,
    /// Page bytes evicted by the shared pool. This is not a measure of RSS returned to the OS.
    pub read_directory_page_cache_global_reclaimed_bytes: u64,
    /// Optional page insertions skipped after the bounded global search found no memory.
    pub read_directory_page_cache_global_admission_failures: u64,

    pub file_permit_capacity: usize,
    pub file_permits_in_use: usize,
    pub peak_file_permits_in_use: usize,
    pub file_permit_waits: u64,
    pub file_permit_wait_seconds: f64,
    pub open_file_cache_capacity: usize,
    pub open_file_cache_entries: usize,
    pub open_file_cache_hits: u64,
    pub open_file_cache_misses: u64,
    pub open_file_opens: u64,
    pub open_file_cache_evictions: u64,
    pub uncached_file_opens: u64,
    pub write_queue_depth: usize,
    pub flush_queue_depth: usize,
    pub publication_queue_depth: usize,
    pub compaction_queue_depth: usize,
    pub history_compaction_queue_depth: usize,
    pub retired_segment_groups: u64,
    pub retired_segment_bytes: u64,
    pub retired_backlog_seconds: f64,
    pub active_query_snapshots: usize,
    pub oldest_query_snapshot_seconds: f64,
    pub snapshot_pinned_memtable_generations: usize,
    pub snapshot_pinned_segments: usize,
    pub available_disk_bytes: u64,
    pub compaction_space_rejections: u64,
    pub last_compaction_required_bytes: u64,
}

impl Storage {
    /// Возвращает счётчики без удержания блокировки во время обхода каталога.
    pub fn stats(&self) -> Result<StorageStats> {
        let last_background_error = lock(&self.inner.degraded)?.clone();
        let cache = self.inner.block_cache.stats()?;
        let cache_bytes = cache.bytes;
        let metadata_cache = self.inner.metadata_cache.stats()?;
        let (
            managed_memory_max_bytes,
            managed_memory_bytes,
            managed_write_queue_bytes,
            managed_memtable_bytes,
            managed_series_watermark_bytes,
            managed_block_cache_bytes,
            managed_metadata_cache_bytes,
        ) = self.inner.memory_governor.stats();
        let (
            file_permit_capacity,
            file_permits_in_use,
            peak_file_permits_in_use,
            file_permit_waits,
            file_permit_wait_nanos,
        ) = self.inner.file_pool.stats();
        let (
            open_file_cache_capacity,
            open_file_cache_entries,
            open_file_cache_hits,
            open_file_cache_misses,
            open_file_opens,
            open_file_cache_evictions,
            uncached_file_opens,
        ) = self.inner.open_file_cache.stats()?;
        let (
            visible,
            checkpoint,
            active,
            frozen,
            l0,
            l1,
            active_l1,
            sealed_l2,
            active_max_rewrites,
            sealed_max_rewrites,
            history_watermarks,
            logical,
            physical,
            l0_debt,
            archive_descriptors,
            max_domain_descriptors,
            published_read_directory_generation_id,
            published_read_directory_generations,
            published_read_directory_segments,
            published_read_directory_pages,
            published_read_directory_bytes,
            published_read_directory_embedded,
            compaction_debt,
            l1_compaction_debt,
        ) = {
            let state = read(&self.inner.state)?;
            let mut l0_domains = BTreeMap::<Domain, usize>::new();
            let mut active_l1 = 0_usize;
            let mut sealed_l2 = 0_usize;
            let mut active_max_rewrites = 0_u32;
            let mut sealed_max_rewrites = 0_u32;
            let history_watermarks = state
                .event_watermarks
                .iter()
                .filter_map(|(policy_id, event_watermark_us)| {
                    sealed_before_for_policy(
                        &self.inner.options,
                        &state.catalog,
                        &state.event_watermarks,
                        *policy_id,
                    )
                    .map(|sealed_before_us| HistoryWatermarkStats {
                        policy_id: *policy_id,
                        event_watermark_us: *event_watermark_us,
                        sealed_before_us,
                    })
                })
                .collect::<Vec<_>>();
            for segment in &state.segments {
                let descriptor = segment.descriptor();
                if descriptor.level == 0 {
                    *l0_domains.entry(descriptor.domain()).or_default() += 1;
                } else if descriptor.level == 1 {
                    active_l1 = active_l1.saturating_add(1);
                    active_max_rewrites = active_max_rewrites.max(descriptor.rewrite_count);
                } else if descriptor.level == 2 {
                    sealed_l2 = sealed_l2.saturating_add(1);
                    sealed_max_rewrites = sealed_max_rewrites.max(descriptor.rewrite_count);
                }
            }
            (
                state.visible_commit,
                state.checkpoint_commit,
                state.active_bytes,
                state.frozen.iter().map(|generation| generation.bytes).sum(),
                state
                    .segments
                    .iter()
                    .filter(|segment| segment.descriptor().level == 0)
                    .count(),
                state
                    .segments
                    .iter()
                    .filter(|segment| segment.descriptor().level == 1)
                    .count(),
                active_l1,
                sealed_l2,
                active_max_rewrites,
                sealed_max_rewrites,
                history_watermarks,
                state.segments.len(),
                state
                    .segments
                    .iter()
                    .map(|segment| segment.path())
                    .collect::<HashSet<_>>()
                    .len(),
                l0_domains.values().copied().max().unwrap_or(0),
                state.aggregate_archives.len(),
                state
                    .segments
                    .iter()
                    .fold(BTreeMap::<Domain, usize>::new(), |mut counts, segment| {
                        *counts.entry(segment.descriptor().domain()).or_default() += 1;
                        counts
                    })
                    .into_values()
                    .max()
                    .unwrap_or(0),
                state
                    .published_read_directories
                    .iter()
                    .map(|directory| directory.generation_id())
                    .max(),
                state.published_read_directories.len(),
                state
                    .published_read_directories
                    .iter()
                    .map(|directory| directory.covered_segment_count())
                    .sum(),
                state
                    .published_read_directories
                    .iter()
                    .map(|directory| directory.root_pages())
                    .sum(),
                state
                    .published_read_directories
                    .iter()
                    .map(|directory| directory.artifact_bytes())
                    .sum(),
                state
                    .published_read_directories
                    .iter()
                    .all(|directory| directory.has_embedded_descriptors()),
                compaction_debt_for_state(&self.inner.options, &state),
                state
                    .l1_debt
                    .values()
                    .map(|debt| {
                        let mut stats = debt.stats.clone();
                        stats.oldest_eligible_seconds = debt
                            .oldest_eligible
                            .map_or(0.0, |at| at.elapsed().as_secs_f64());
                        stats
                    })
                    .collect(),
            )
        };
        let manifest_bytes = fs::metadata(self.inner.root.join("manifest/MANIFEST.json"))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let pinned_memtable_bytes =
            managed_memtable_bytes.saturating_sub(active.saturating_add(frozen));
        let pinned_block_cache_bytes = managed_block_cache_bytes.saturating_sub(cache_bytes);
        let pinned_metadata_cache_bytes =
            managed_metadata_cache_bytes.saturating_sub(metadata_cache.bytes);
        let (
            active_query_snapshots,
            oldest_query_snapshot,
            snapshot_pinned_memtable_generations,
            snapshot_pinned_segments,
        ) = self.inner.snapshot_tracker.stats()?;
        let regulator = self.inner.write_regulator.stats()?;
        let work_memory = self.inner.work_memory_governor.stats()?;
        let directory_page_pool = self.inner.directory_page_pool.stats();
        Ok(StorageStats {
            write_admission: self.inner.admission.stats(),
            healthy: last_background_error.is_none(),
            last_background_error,
            visible_commit_id: visible,
            checkpoint_commit_id: checkpoint,
            writes: self.inner.metrics.writes.load(Ordering::Relaxed),
            written_operations: self
                .inner
                .metrics
                .written_operations
                .load(Ordering::Relaxed),
            written_points: self.inner.metrics.written_points.load(Ordering::Relaxed),
            deleted_points: self.inner.metrics.deleted_points.load(Ordering::Relaxed),
            wal_bytes_written: self.inner.metrics.wal_bytes.load(Ordering::Relaxed),
            wal_syncs: self.inner.metrics.wal_syncs.load(Ordering::Relaxed),
            wal_sync_seconds: nanos_seconds(
                self.inner.metrics.wal_sync_nanos.load(Ordering::Relaxed),
            ),
            group_commit_groups: self
                .inner
                .metrics
                .group_commit_groups
                .load(Ordering::Relaxed),
            group_commit_batches: self
                .inner
                .metrics
                .group_commit_batches
                .load(Ordering::Relaxed),
            active_memtable_bytes: active,
            frozen_memtable_bytes: frozen,
            l0_segments: l0,
            l1_segments: l1,
            active_l1_segments: active_l1,
            l1_compaction_debt,
            l1_overlap_repair_jobs: self
                .inner
                .metrics
                .l1_overlap_repair_jobs
                .load(Ordering::Relaxed),
            l1_overlap_repair_input_bytes: self
                .inner
                .metrics
                .l1_overlap_repair_input_bytes
                .load(Ordering::Relaxed),
            l1_overlap_repair_max_input_bytes: self
                .inner
                .metrics
                .l1_overlap_repair_max_input_bytes
                .load(Ordering::Relaxed),
            sealed_l2_segments: sealed_l2,
            history_watermarks,
            logical_segments: logical,
            physical_segment_files: physical,
            l0_debt,
            flushes: self.inner.metrics.flushes.load(Ordering::Relaxed),
            flush_seconds: nanos_seconds(self.inner.metrics.flush_nanos.load(Ordering::Relaxed)),
            flush_bytes: self.inner.metrics.flush_bytes.load(Ordering::Relaxed),
            compactions: self.inner.metrics.compactions.load(Ordering::Relaxed),
            compacted_domains: self.inner.metrics.compacted_domains.load(Ordering::Relaxed),
            compaction_seconds: nanos_seconds(
                self.inner.metrics.compaction_nanos.load(Ordering::Relaxed),
            ),
            compaction_bytes_read: self
                .inner
                .metrics
                .compaction_bytes_read
                .load(Ordering::Relaxed),
            compaction_bytes_written: self
                .inner
                .metrics
                .compaction_bytes_written
                .load(Ordering::Relaxed),
            compaction_physical_bytes_written: self
                .inner
                .metrics
                .compaction_physical_bytes_written
                .load(Ordering::Relaxed),
            compaction_block_visits: self
                .inner
                .metrics
                .compaction_block_visits
                .load(Ordering::Relaxed),
            compaction_unique_blocks: self
                .inner
                .metrics
                .compaction_unique_blocks
                .load(Ordering::Relaxed),
            compaction_unique_block_bytes: self
                .inner
                .metrics
                .compaction_unique_block_bytes
                .load(Ordering::Relaxed),
            compaction_physical_blocks_read: self
                .inner
                .metrics
                .compaction_physical_blocks_read
                .load(Ordering::Relaxed),
            compaction_physical_bytes_read: self
                .inner
                .metrics
                .compaction_physical_bytes_read
                .load(Ordering::Relaxed),
            compaction_decoded_input_bytes: self
                .inner
                .metrics
                .compaction_decoded_input_bytes
                .load(Ordering::Relaxed),
            compaction_decoded_points: self
                .inner
                .metrics
                .compaction_decoded_points
                .load(Ordering::Relaxed),
            compaction_decode_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .compaction_decode_nanos
                    .load(Ordering::Relaxed),
            ),
            active_compaction_physical_bytes_read: self
                .inner
                .metrics
                .active_compaction_physical_bytes_read
                .load(Ordering::Relaxed),
            active_compaction_physical_bytes_written: self
                .inner
                .metrics
                .active_compaction_physical_bytes_written
                .load(Ordering::Relaxed),
            active_compaction_rewrite_passes: self
                .inner
                .metrics
                .active_compaction_rewrite_passes
                .load(Ordering::Relaxed),
            active_compaction_jobs: self
                .inner
                .metrics
                .active_compaction_jobs
                .load(Ordering::Relaxed),
            active_compaction_max_input_bytes: self
                .inner
                .metrics
                .active_compaction_max_input_bytes
                .load(Ordering::Relaxed),
            active_compaction_max_output_bytes: self
                .inner
                .metrics
                .active_compaction_max_output_bytes
                .load(Ordering::Relaxed),
            active_compaction_max_fan_in: self
                .inner
                .metrics
                .active_compaction_max_fan_in
                .load(Ordering::Relaxed),
            active_compaction_debt_bytes: compaction_debt.active_bytes,
            active_compaction_oldest_debt_seconds: micros_seconds(
                compaction_debt.active_oldest_age_us,
            ),
            sealed_compaction_physical_bytes_read: self
                .inner
                .metrics
                .sealed_compaction_physical_bytes_read
                .load(Ordering::Relaxed),
            sealed_compaction_physical_bytes_written: self
                .inner
                .metrics
                .sealed_compaction_physical_bytes_written
                .load(Ordering::Relaxed),
            sealed_compaction_rewrite_passes: self
                .inner
                .metrics
                .sealed_compaction_rewrite_passes
                .load(Ordering::Relaxed),
            sealed_compaction_jobs: self
                .inner
                .metrics
                .sealed_compaction_jobs
                .load(Ordering::Relaxed),
            sealed_compaction_max_input_bytes: self
                .inner
                .metrics
                .sealed_compaction_max_input_bytes
                .load(Ordering::Relaxed),
            sealed_compaction_max_output_bytes: self
                .inner
                .metrics
                .sealed_compaction_max_output_bytes
                .load(Ordering::Relaxed),
            sealed_compaction_max_fan_in: self
                .inner
                .metrics
                .sealed_compaction_max_fan_in
                .load(Ordering::Relaxed),
            sealed_compaction_debt_bytes: compaction_debt.historical_bytes,
            sealed_compaction_oldest_debt_seconds: micros_seconds(
                compaction_debt.historical_oldest_age_us,
            ),
            compaction_fair_handoffs: self
                .inner
                .metrics
                .compaction_fair_handoffs
                .load(Ordering::Relaxed),
            active_max_rewrite_count: active_max_rewrites,
            sealed_max_rewrite_count: sealed_max_rewrites,
            manifest_bytes,
            manifest_limit_bytes: crate::manifest::MANIFEST_MAX_BYTES,
            manifest_headroom_bytes: crate::manifest::MANIFEST_MAX_BYTES
                .saturating_sub(manifest_bytes),
            manifest_segment_descriptors: logical.saturating_sub(
                published_read_directory_segments * usize::from(published_read_directory_embedded),
            ),
            manifest_archive_descriptors: archive_descriptors,
            manifest_max_domain_descriptors: max_domain_descriptors,
            published_read_directory_generation_id,
            published_read_directory_generations,
            published_read_directory_segments,
            published_read_directory_pages,
            published_read_directory_bytes,
            read_directory_build_attempts: self
                .inner
                .metrics
                .read_directory_build_attempts
                .load(Ordering::Relaxed),
            read_directory_build_resource_skips: self
                .inner
                .metrics
                .read_directory_build_resource_skips
                .load(Ordering::Relaxed),
            read_directory_build_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .read_directory_build_nanos
                    .load(Ordering::Relaxed),
            ),
            read_directory_build_max_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .read_directory_build_max_nanos
                    .load(Ordering::Relaxed),
            ),
            read_directory_build_current_segments: self
                .inner
                .metrics
                .read_directory_build_current_segments
                .load(Ordering::Relaxed),
            read_directory_build_max_segments: self
                .inner
                .metrics
                .read_directory_build_max_segments
                .load(Ordering::Relaxed),
            compaction_current_input_bytes: self
                .inner
                .metrics
                .compaction_current_input_bytes
                .load(Ordering::Relaxed),
            publisher_phase: match self.inner.metrics.publisher_phase.load(Ordering::Relaxed) {
                1 => "flush",
                2 => "compaction",
                3 => "read_directory",
                4 => "checkpoint",
                5 => "catalog",
                6 => "snapshot",
                7 => "stop",
                _ => "idle",
            },
            shutdown_phase: match self.inner.metrics.shutdown_phase.load(Ordering::Relaxed) {
                1 => "checkpoint",
                2 => "maintenance",
                3 => "writer",
                4 => "flush",
                5 => "compaction",
                6 => "pack_workers",
                7 => "publisher",
                8 => "release_lock",
                9 => "complete",
                _ => "open",
            },
            manifest_publications: self
                .inner
                .metrics
                .manifest_publications
                .load(Ordering::Relaxed),
            manifest_encode_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .manifest_encode_nanos
                    .load(Ordering::Relaxed),
            ),
            manifest_write_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .manifest_write_nanos
                    .load(Ordering::Relaxed),
            ),
            manifest_fsync_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .manifest_fsync_nanos
                    .load(Ordering::Relaxed),
            ),
            manifest_lock_wait_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .manifest_lock_wait_nanos
                    .load(Ordering::Relaxed),
            ),
            manifest_expired_segments: self
                .inner
                .metrics
                .manifest_expired_segments
                .load(Ordering::Relaxed),
            manifest_expired_bytes: self
                .inner
                .metrics
                .manifest_expired_bytes
                .load(Ordering::Relaxed),
            write_queue_backpressure: self
                .inner
                .metrics
                .write_backpressure
                .load(Ordering::Relaxed),
            write_queue_backpressure_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .write_backpressure_nanos
                    .load(Ordering::Relaxed),
            ),
            flush_queue_backpressure: self
                .inner
                .metrics
                .flush_backpressure
                .load(Ordering::Relaxed),
            flush_queue_backpressure_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .flush_backpressure_nanos
                    .load(Ordering::Relaxed),
            ),
            background_errors: self.inner.metrics.background_errors.load(Ordering::Relaxed),
            background_checkpoints: self
                .inner
                .metrics
                .background_checkpoints
                .load(Ordering::Relaxed),
            background_checkpoint_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .background_checkpoint_nanos
                    .load(Ordering::Relaxed),
            ),
            foreground_maintenance_waits: self
                .inner
                .metrics
                .foreground_maintenance_waits
                .load(Ordering::Relaxed),
            foreground_maintenance_wait_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .foreground_maintenance_wait_nanos
                    .load(Ordering::Relaxed),
            ),
            write_pacing_events: self
                .inner
                .metrics
                .write_pacing_events
                .load(Ordering::Relaxed),
            write_pacing_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .write_pacing_nanos
                    .load(Ordering::Relaxed),
            ),
            write_pacing_active: regulator.active,
            write_pacing_reason: pacing_reason_text(regulator.reasons),
            write_pacing_active_seconds: regulator.active_duration.as_secs_f64(),
            write_pacing_recoveries: regulator.recoveries,
            write_pacing_l0_events: regulator.l0_events,
            write_pacing_compaction_queue_events: regulator.compaction_queue_events,
            write_pacing_memory_events: regulator.memory_events,
            write_pacing_read_latency_events: regulator.read_latency_events,
            read_latency_window_samples: regulator.read_latency_samples,
            read_latency_p95_seconds: regulator.read_latency_p95.as_secs_f64(),
            read_latency_single_window_samples: regulator.read_latency_single_samples,
            read_latency_single_p95_seconds: regulator.read_latency_single_p95.as_secs_f64(),
            read_latency_wide_window_samples: regulator.read_latency_wide_samples,
            read_latency_wide_p95_seconds: regulator.read_latency_wide_p95.as_secs_f64(),
            query_operations_scanned: self
                .inner
                .metrics
                .query_operations_scanned
                .load(Ordering::Relaxed),
            query_operation_bytes_decoded: self
                .inner
                .metrics
                .query_operation_bytes_decoded
                .load(Ordering::Relaxed),
            query_metadata_bytes_read: self
                .inner
                .metrics
                .query_metadata_bytes_read
                .load(Ordering::Relaxed),
            query_payload_bytes_read: self
                .inner
                .metrics
                .query_payload_bytes_read
                .load(Ordering::Relaxed),
            query_blocks_read: self.inner.metrics.query_blocks_read.load(Ordering::Relaxed),
            query_physical_blocks_decoded: self
                .inner
                .metrics
                .query_physical_blocks_decoded
                .load(Ordering::Relaxed),
            query_physical_decoded_bytes: self
                .inner
                .metrics
                .query_physical_decoded_bytes
                .load(Ordering::Relaxed),
            query_file_opens: self.inner.metrics.query_file_opens.load(Ordering::Relaxed),
            query_prefetched_blocks: self
                .inner
                .metrics
                .query_prefetched_blocks
                .load(Ordering::Relaxed),
            query_prefetch_hits: self
                .inner
                .metrics
                .query_prefetch_hits
                .load(Ordering::Relaxed),
            query_planned_segment_reads: self
                .inner
                .metrics
                .query_planned_segment_reads
                .load(Ordering::Relaxed),
            query_segment_index_lookups: self
                .inner
                .metrics
                .query_segment_index_lookups
                .load(Ordering::Relaxed),
            query_segment_index_entries_examined: self
                .inner
                .metrics
                .query_segment_index_entries_examined
                .load(Ordering::Relaxed),
            query_segment_index_candidates: self
                .inner
                .metrics
                .query_segment_index_candidates
                .load(Ordering::Relaxed),
            query_segment_index_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .query_segment_index_nanos
                    .load(Ordering::Relaxed),
            ),
            query_peak_memory_bytes: self
                .inner
                .metrics
                .query_peak_memory_bytes
                .load(Ordering::Relaxed),
            query_exact_daily_aggregate_queries: self
                .inner
                .metrics
                .query_exact_daily_aggregate_queries
                .load(Ordering::Relaxed),
            query_aggregate_records_read: self
                .inner
                .metrics
                .query_aggregate_records_read
                .load(Ordering::Relaxed),
            query_points_returned: self
                .inner
                .metrics
                .query_points_returned
                .load(Ordering::Relaxed),
            storage_bytes: directory_bytes(&self.inner.root)?,
            open_files: open_file_count(),
            recovery_seconds: nanos_seconds(
                self.inner.metrics.recovery_nanos.load(Ordering::Relaxed),
            ),
            recovery_manifest_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_manifest_nanos
                    .load(Ordering::Relaxed),
            ),
            recovery_descriptors_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_descriptors_nanos
                    .load(Ordering::Relaxed),
            ),
            recovery_read_directory_pages: self
                .inner
                .metrics
                .recovery_read_directory_pages
                .load(Ordering::Relaxed),
            recovery_read_directory_bytes: self
                .inner
                .metrics
                .recovery_read_directory_bytes
                .load(Ordering::Relaxed),
            recovery_wal_scan_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_wal_scan_nanos
                    .load(Ordering::Relaxed),
            ),
            recovery_wal_replay_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_wal_replay_nanos
                    .load(Ordering::Relaxed),
            ),
            recovery_read_ready_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_read_ready_nanos
                    .load(Ordering::Relaxed),
            ),
            recovery_write_ready_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .recovery_write_ready_nanos
                    .load(Ordering::Relaxed),
            ),
            block_cache_bytes: cache_bytes,
            block_cache_entries: cache.entries,
            block_cache_hits: cache.hits,
            block_cache_misses: cache.misses,
            block_cache_mutex_wait_seconds: nanos_seconds(cache.mutex_wait_nanos),
            block_cache_victims_scanned: cache.victims_scanned,
            block_cache_eviction_seconds: nanos_seconds(cache.eviction_nanos),
            coalesced_block_loads: cache.coalesced_loads,
            coalesced_block_load_waiters: cache.coalesced_waiters,
            block_load_bypasses: cache.load_bypasses,
            block_load_wait_seconds: nanos_seconds(cache.load_wait_nanos),
            block_loads_in_flight: cache.loads_in_flight,
            block_load_bytes_in_flight: cache.load_bytes_in_flight,
            peak_block_loads_in_flight: cache.peak_loads_in_flight,
            peak_block_load_bytes_in_flight: cache.peak_load_bytes_in_flight,
            metadata_cache_bytes: metadata_cache.bytes,
            metadata_cache_entries: metadata_cache.entries,
            metadata_cache_hits: metadata_cache.hits,
            metadata_cache_misses: metadata_cache.misses,
            metadata_cache_mutex_wait_seconds: nanos_seconds(metadata_cache.mutex_wait_nanos),
            metadata_cache_victims_scanned: metadata_cache.victims_scanned,
            metadata_cache_eviction_seconds: nanos_seconds(metadata_cache.eviction_nanos),
            membership_filter_negatives: metadata_cache.membership_negatives,
            membership_filter_false_positives: metadata_cache.membership_false_positives,
            managed_memory_max_bytes,
            managed_memory_bytes,
            managed_write_queue_bytes,
            managed_memtable_bytes,
            managed_series_watermark_bytes,
            strict_series_watermarks: usize::try_from(
                self.inner
                    .metrics
                    .strict_series_watermarks
                    .load(Ordering::Relaxed),
            )
            .unwrap_or(usize::MAX),
            pinned_memtable_bytes,
            managed_block_cache_bytes,
            pinned_block_cache_bytes,
            managed_metadata_cache_bytes,
            pinned_metadata_cache_bytes,
            work_memory_max_bytes: work_memory.maximum_bytes,
            work_memory_bytes: work_memory.used_bytes,
            work_memory_peak_bytes: work_memory.peak_bytes,
            query_plan_memory_bytes: work_memory.query_plan_bytes,
            query_plan_peak_memory_bytes: work_memory.query_plan_peak_bytes,
            query_metadata_memory_bytes: work_memory.query_metadata_bytes,
            query_metadata_peak_memory_bytes: work_memory.query_metadata_peak_bytes,
            query_compressed_memory_bytes: work_memory.query_compressed_bytes,
            query_compressed_peak_memory_bytes: work_memory.query_compressed_peak_bytes,
            query_decoded_memory_bytes: work_memory.query_decoded_bytes,
            query_decoded_peak_memory_bytes: work_memory.query_decoded_peak_bytes,
            query_results_memory_bytes: work_memory.query_results_bytes,
            query_results_peak_memory_bytes: work_memory.query_results_peak_bytes,
            flush_memory_bytes: work_memory.flush_bytes,
            flush_peak_memory_bytes: work_memory.flush_peak_bytes,
            compaction_memory_bytes: work_memory.compaction_bytes,
            compaction_peak_memory_bytes: work_memory.compaction_peak_bytes,
            aggregate_builder_memory_bytes: work_memory.aggregate_builder_bytes,
            aggregate_builder_peak_memory_bytes: work_memory.aggregate_builder_peak_bytes,
            recovery_memory_bytes: work_memory.recovery_bytes,
            recovery_peak_memory_bytes: work_memory.recovery_peak_bytes,
            unbudgeted_large_allocations: work_memory.unbudgeted_large_allocations,
            work_memory_waits: work_memory.waits,
            work_memory_wait_seconds: nanos_seconds(work_memory.wait_nanos),
            memory_backpressure_events: self
                .inner
                .metrics
                .memory_backpressure
                .load(Ordering::Relaxed),
            write_queue_memory_backpressure_events: self
                .inner
                .metrics
                .write_queue_memory_backpressure
                .load(Ordering::Relaxed),
            read_directory_build_memory_backpressure_events: self
                .inner
                .metrics
                .read_directory_build_memory_backpressure
                .load(Ordering::Relaxed),
            read_directory_root_memory_backpressure_events: self
                .inner
                .metrics
                .read_directory_root_memory_backpressure
                .load(Ordering::Relaxed),
            read_directory_page_cache_global_probes: directory_page_pool.pressure_probes,
            read_directory_page_cache_global_reclaimed_bytes: directory_page_pool.reclaimed_bytes,
            read_directory_page_cache_global_admission_failures: directory_page_pool
                .admission_failures,
            file_permit_capacity,
            file_permits_in_use,
            peak_file_permits_in_use,
            file_permit_waits,
            file_permit_wait_seconds: nanos_seconds(file_permit_wait_nanos),
            open_file_cache_capacity,
            open_file_cache_entries,
            open_file_cache_hits,
            open_file_cache_misses,
            open_file_opens,
            open_file_cache_evictions,
            uncached_file_opens,
            write_queue_depth: self.inner.write_tx.len(),
            flush_queue_depth: self.inner.flush_tx.len(),
            publication_queue_depth: self.inner.publish_tx.len(),
            compaction_queue_depth: self.inner.compaction_tx.len(),
            history_compaction_queue_depth: self.inner.history_compaction_tx.len(),
            retired_segment_groups: self
                .inner
                .metrics
                .retired_segment_groups
                .load(Ordering::Relaxed),
            retired_segment_bytes: self
                .inner
                .metrics
                .retired_segment_bytes
                .load(Ordering::Relaxed),
            retired_backlog_seconds: nanos_seconds(
                self.inner
                    .metrics
                    .retired_backlog_nanos
                    .load(Ordering::Relaxed),
            ),
            active_query_snapshots,
            oldest_query_snapshot_seconds: oldest_query_snapshot.as_secs_f64(),
            snapshot_pinned_memtable_generations,
            snapshot_pinned_segments,
            available_disk_bytes: available_disk_bytes(&self.inner.root)?,
            compaction_space_rejections: self
                .inner
                .metrics
                .compaction_space_rejections
                .load(Ordering::Relaxed),
            last_compaction_required_bytes: self
                .inner
                .metrics
                .last_compaction_required_bytes
                .load(Ordering::Relaxed),
        })
    }
}
