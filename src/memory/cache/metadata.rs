//! Metadata reservations grouped by owner; the global admission budget is unchanged.

use super::{MemoryGovernor, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetadataMemoryUsage {
    pub(crate) cache_bytes: u64,
    pub(crate) catalog_page_bytes: u64,
    pub(crate) handle_cache_bytes: u64,
    pub(crate) index_root_bytes: u64,
    pub(crate) catalog_bytes: u64,
    pub(crate) directory_root_bytes: u64,
    pub(crate) directory_page_bytes: u64,
    pub(crate) loading_bytes: u64,
}

impl MetadataMemoryUsage {
    pub(crate) fn total_bytes(self) -> u64 {
        self.cache_bytes
            .saturating_add(self.catalog_page_bytes)
            .saturating_add(self.handle_cache_bytes)
            .saturating_add(self.index_root_bytes)
            .saturating_add(self.catalog_bytes)
            .saturating_add(self.directory_root_bytes)
            .saturating_add(self.directory_page_bytes)
            .saturating_add(self.loading_bytes)
    }
}

impl MemoryGovernor {
    pub(crate) fn metadata_usage(&self) -> MetadataMemoryUsage {
        MetadataMemoryUsage {
            cache_bytes: self.metadata_cache_bytes.load(Ordering::Relaxed),
            catalog_page_bytes: self.metadata_catalog_page_bytes.load(Ordering::Relaxed),
            handle_cache_bytes: self.metadata_handle_bytes.load(Ordering::Relaxed),
            index_root_bytes: self.metadata_index_root_bytes.load(Ordering::Relaxed),
            catalog_bytes: self.metadata_catalog_bytes.load(Ordering::Relaxed),
            directory_root_bytes: self.metadata_directory_root_bytes.load(Ordering::Relaxed),
            directory_page_bytes: self.metadata_directory_page_bytes.load(Ordering::Relaxed),
            loading_bytes: self.metadata_loading_bytes.load(Ordering::Relaxed),
        }
    }
}
