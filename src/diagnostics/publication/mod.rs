//! Fixed-size, opt-in publication cost counters. Timers are elapsed time, not CPU time.
//! Command and builder totals include nested stages; do not sum all rows.
//! Timers contain completed scopes; counters can include in-flight work.
//! Live snapshots are not atomic. Close Storage before the final snapshot.
//! Recent slow events use a bounded, non-destructive ring: deduplicate snapshots
//! by sequence and check overwrite counts for gaps. Offsets share the session's
//! monotonic clock. Failed operations are timed too; these are not success counts.
//! Manifest sync spans include rename, commit callback and directory syncs.
//! Handle counters here cover opens without a query budget in diagnostic scopes;
//! query-budget opens are reported separately in QueryStats.

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Flush,
    Compaction,
    Other,
    #[cfg(feature = "publication-diagnostics")]
    QueueFlush,
    #[cfg(feature = "publication-diagnostics")]
    QueueCompaction,
    BuilderMetadata,
    BuilderEncode,
    BuilderWrite,
    BuilderSync,
    PersistManifest,
    ApplyState,
    CatalogPrepare,
    CatalogSync,
    Reclaim,
    Stats,
    Selection,
    Priority,
    SelectionSnapshotWait,
    SelectionSnapshotHold,
    #[cfg(feature = "publication-diagnostics")]
    SelectionActiveReady,
    #[cfg(feature = "publication-diagnostics")]
    SelectionActiveEmpty,
    #[cfg(feature = "publication-diagnostics")]
    SelectionActiveError,
    #[cfg(feature = "publication-diagnostics")]
    SelectionHistoricalReady,
    #[cfg(feature = "publication-diagnostics")]
    SelectionHistoricalEmpty,
    #[cfg(feature = "publication-diagnostics")]
    SelectionHistoricalError,
    HistoricalSealing,
    HistoricalFallback,
    PublicationSummary,
    PublicationEligibility,
    PublicationValidation,
    WalSync,
    WalSyncAll,
    WriterVisibilityWait,
    PublisherCatalogWait,
    PublisherStateWait,
    ManifestSyncAndCommit,
}
#[derive(Clone, Copy)]
pub(crate) enum Counter {
    Locators,
    Pages,
    BuildWorkBytesPeak,
    SegmentMetadataReadBytes,
    SegmentHeaderReadBytes,
    DirectoryReadBytes,
    SelectionActiveDomains,
    SelectionHistoricalDomains,
    SelectionActiveOpened,
    SelectionHistoricalOpened,
    SelectionActiveL2,
    SelectionHistoricalL2,
    HistoricalFallbackDescriptors,
    CatalogSummaryNodes,
    CatalogSummaryRecords,
    PublicationHistoryDescriptors,
    PublicationEligibilityDescriptors,
    PublicationValidationDescriptors,
    SegmentHandleCacheHits,
    SegmentHandleCacheMisses,
    SegmentHandleCacheCollisions,
    SegmentHandleOpens,
}

#[cfg(feature = "publication-diagnostics")]
pub(crate) mod compaction;
#[cfg(feature = "publication-diagnostics")]
mod enabled;
#[cfg(feature = "publication-diagnostics")]
pub use compaction::Report as CompactionReport;
#[cfg(feature = "publication-diagnostics")]
pub(crate) use enabled::{Collector, enter, queued, record_elapsed};
#[cfg(feature = "publication-diagnostics")]
pub use enabled::{Report, Session};

pub(crate) struct Timer {
    #[cfg(feature = "publication-diagnostics")]
    inner: Option<(std::sync::Arc<Collector>, Stage, std::time::Instant)>,
}

#[inline]
pub(crate) fn stage(name: Stage) -> Timer {
    #[cfg(feature = "publication-diagnostics")]
    {
        enabled::timer(name)
    }
    #[cfg(not(feature = "publication-diagnostics"))]
    {
        let _ = name;
        Timer {}
    }
}
#[inline]
pub(crate) fn add(name: Counter, value: usize) {
    #[cfg(feature = "publication-diagnostics")]
    enabled::add(name, value);
    #[cfg(not(feature = "publication-diagnostics"))]
    let _ = (name, value);
}

impl Drop for Timer {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "publication-diagnostics")]
        if let Some((collector, name, started)) = &self.inner {
            collector.record(*name, *started, started.elapsed());
        }
    }
}
