//! Embedded time-series storage for strictly increasing numeric series.
//!
//! [`Storage`] owns WAL, immutable data files, read snapshots, and bounded
//! background workers. Clone the handle to share a database between threads.
//! This crate is synchronous: use bounded blocking tasks in an async application.
//! API and on-disk compatibility are not promised during development.
//!
//! # Start here
//!
//! ```
//! use chimaera::{OpenOptions, Point, Result, SeriesKey, Storage};
//! let directory = tempfile::tempdir()?;
//! let storage = Storage::open(directory.path(), OpenOptions::default())?;
//! let key = SeriesKey::new(1, 1, 42);
//! let points = vec![Point::new(key, 1, 10.0)?, Point::new(key, 2, 14.0)?];
//! let receipt = storage.write_batch(points.clone())?;
//! assert_eq!(receipt.point_count, 2);
//! let read = storage.scan(key, 1, 3)?.collect::<Result<Vec<_>>>()?;
//! assert_eq!(read, points);
//! storage.close()?;
//! # Ok::<(), chimaera::Error>(())
//! ```
//!
//! `tempfile` is an example-only development dependency. Use an application-owned
//! path for persistent data. Numeric keys need no catalog registration; use
//! [`SeriesRef`] and [`StoragePolicyOptions`] for named buckets and retention.
//!
//! # Contracts
//!
//! - [`Point`] timestamps are Unix microseconds. Values must be finite `f64`.
//!   Each series must increase strictly, including within a batch. Intervals
//!   may vary; gaps are allowed. Updates and out-of-order insertion are rejected.
//! - [`WriteReceipt`] follows WAL sync and visibility publication (GroupCommit).
//!   Segment flush is asynchronous. [`WriteTicket`] admission is not an ACK.
//!   [`WriteStatus::Pending`] and dropping a ticket do not cancel the batch.
//!   There is no request-ID deduplication: do not blindly resubmit lost ACKs.
//! - [`PointStream`] and [`GroupStream`] hold snapshots. Consume every `Result`
//!   and reach end-of-stream before treating the answer as complete. Drop unused
//!   streams promptly; cancellation alone does not release an idle stream.
//! - [`GroupSource::Auto`] may use exact retained aggregates after raw expiry.
//!   Insufficient archive coverage or precision yields [`Error::QueryUnavailable`].
//! - [`Storage::delete_series_batch`] atomically hides selected histories from
//!   new reads, including archives. Existing streams retain their old view;
//!   physical cleanup follows asynchronously. A later write starts a new history.
//! - [`OpenOptions::memory_max_bytes`] bounds managed engine state, not process RSS.
//!   [`QueryOptions`] imposes additional per-query limits.
//! - [`Storage::snapshot`] and [`Storage::restore_snapshot`] provide physical copies.
//!   JSONL export requires registered buckets and transfers retained raw data,
//!   not aggregate archives.
//! - [`Storage::close`] affects all clones. Check its result; `Drop` cannot report
//!   shutdown errors. Release streams before reopening the directory.
//!
//! Build this reference with `cargo doc --no-deps --lib`. Repository guides:
//! `docs/embedding.md` (integration and operations), `docs/architecture.md`
//! (data paths). Run `cargo run --example embedding` and
//! `cargo run --example operations` for executable examples.

// Unit-test executables own their allocator just like standalone harnesses.

mod admin;
mod aggregate;
mod catalog;
mod deletion;
mod diagnostics;
mod engine;
mod error;
mod memory;
mod persistence;
mod query;
mod segment;
mod types;

// Preserve the published feature APIs while keeping their implementation together.
#[cfg(feature = "publication-diagnostics")]
#[doc(hidden)]
pub use diagnostics::publication as publication_diagnostics;
#[cfg(feature = "read-diagnostics")]
#[doc(hidden)]
pub use diagnostics::query as query_plan_diagnostics;
#[cfg(feature = "pipeline-trace")]
pub use diagnostics::trace as pipeline_trace;
pub use memory::allocator;
pub use persistence::decode;

pub use admin::exchange::{ExportOptions, ExportResult, ImportOptions, ImportResult};
pub use admin::snapshot::SnapshotInfo;
pub use admin::verify::{
    SalvageResult, StorageInfo, VerificationIssue, VerificationOptions, VerificationReport,
};
pub use catalog::{
    BlockCodec, BucketDefinition, CatalogSnapshot, CompressionProfile, DataTemperature, NamedPoint,
    PolicySelector, RetentionInfo, SeriesRef, SeriesStorageInfo, SpaceDefinition, StoragePolicy,
    StoragePolicyOptions, TimestampCodec, ValueCodec,
};
pub use engine::clock::{Clock, SystemClock};
pub use engine::lifecycle::admission::{
    AdmissionOptions, CloseReport, DrainReport, OverloadReason, WriteAdmissionStats,
};
pub use engine::ticket::{WriteStatus, WriteTicket};
pub use engine::{
    BackgroundActivityStats, CompactionOptions, GroupResult, GroupStream, GroupStreamResult,
    HistoryWatermarkStats, L1CompactionDebtStats, MetadataMemoryStats, OpenOptions, PointStream,
    Storage, StorageStats,
};
pub use error::{Error, Result};
pub use query::{CancellationToken, QueryOptions, QueryPriority, QueryStats};
pub use segment::OfflinePackDirectoryInfo;
pub use types::{
    AggregatePoint, AggregateResolution, AggregateSet, CalendarGrouping, CalendarUnit, DAY_US,
    GroupRequest, GroupRow, GroupScope, GroupSource, Grouping, HOUR_US, MINUTE_US, Point,
    SeriesKey, TimeBucket, WeekStart, WriteReceipt,
};

#[cfg(feature = "state-diagnostics")]
pub use diagnostics::state as state_diagnostics;
