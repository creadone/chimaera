//! Ownership breakdown of the existing managed metadata reservations.

use crate::memory::cache::MetadataMemoryUsage;

/// Managed metadata reservations in bytes, grouped by their current owners.
///
/// These are budget charges, not allocator measurements or RSS. Root charges
/// retain their existing estimates and include all live generations. Uncached
/// query metadata belongs to the separate work-memory budget.
///
/// Concurrent activity may change ownership while statistics are being sampled.
/// Cached bytes are capped by the sampled reservations so the categories stay
/// disjoint and their sum equals `total_bytes` even during that activity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct MetadataMemoryStats {
    /// Main metadata LRU: block indexes, cached index pages and aggregate sections.
    pub cache_resident_bytes: u64,
    /// Main LRU reservations still alive outside the cache, for example in cursors.
    pub cache_retained_bytes: u64,
    /// File-catalogue page reservations, including briefly pinned pages.
    pub catalog_page_bytes: u64,
    /// Optional segment handle cache reservations.
    pub handle_cache_bytes: u64,
    /// Resident segment index roots, including roots of still-live old segments.
    pub index_root_bytes: u64,
    /// Segment objects and published catalog bookkeeping, including spare capacity.
    pub catalog_bytes: u64,
    /// Read-directory roots, including directories held by old snapshots.
    pub directory_root_bytes: u64,
    /// Pages currently stored in read-directory caches, across all live directories.
    pub directory_page_cache_bytes: u64,
    /// Directory page reservations still alive outside their caches.
    pub directory_page_retained_bytes: u64,
    /// Temporary buffer reservations while loading segment index roots.
    pub loading_bytes: u64,
    /// Sum of the categories above; also `managed_metadata_cache_bytes`.
    pub total_bytes: u64,
}

impl MetadataMemoryStats {
    pub(crate) fn from_usage(
        usage: MetadataMemoryUsage,
        cache_bytes: u64,
        directory_page_cache_bytes: u64,
    ) -> Self {
        let cache_resident_bytes = cache_bytes.min(usage.cache_bytes);
        let directory_page_cache_bytes = directory_page_cache_bytes.min(usage.directory_page_bytes);
        Self {
            cache_resident_bytes,
            cache_retained_bytes: usage.cache_bytes - cache_resident_bytes,
            catalog_page_bytes: usage.catalog_page_bytes,
            handle_cache_bytes: usage.handle_cache_bytes,
            index_root_bytes: usage.index_root_bytes,
            catalog_bytes: usage.catalog_bytes,
            directory_root_bytes: usage.directory_root_bytes,
            directory_page_cache_bytes,
            directory_page_retained_bytes: usage.directory_page_bytes - directory_page_cache_bytes,
            loading_bytes: usage.loading_bytes,
            total_bytes: usage.total_bytes(),
        }
    }
}
