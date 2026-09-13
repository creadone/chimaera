//! Chimaera — встраиваемое Rust-ядро хранения временных рядов.
//!
//! Публичный API включает каталог, пакетные `put`/`delete`, согласованные point,
//! scan и group-запросы, checkpoint, snapshot и фоновое слияние L0→L1→L2.

mod admission;
mod aggregate;
mod aggregate_archive;
mod cache;
mod catalog;
mod clock;
mod diagnostics;
mod engine;
mod error;
mod exchange;
mod failpoint;
mod file_pool;
mod layout;
mod lifecycle;
mod manifest;
mod query;
mod segment;
mod snapshot;
mod storage_lock;
mod types;
pub mod v1;
mod wal;
mod write_ticket;

pub use catalog::{
    BlockCodec, BucketDefinition, CatalogSnapshot, CompressionProfile, DataTemperature, NamedPoint,
    NamedWriteOperation, PolicySelector, RetentionInfo, SeriesRef, SeriesStorageInfo,
    SpaceDefinition, StoragePolicy, StoragePolicyOptions, TimestampCodec, ValueCodec, WriteMode,
};
pub use clock::{Clock, SystemClock};
pub use diagnostics::{
    SalvageResult, StorageInfo, VerificationIssue, VerificationOptions, VerificationReport,
};
pub use engine::{
    CompactionOptions, GroupResult, GroupStream, GroupStreamResult, HistoryWatermarkStats,
    L1CompactionDebtStats, OpenOptions, PointStream, Storage, StorageStats,
};
pub use error::{Error, Result};
pub use exchange::{ExportOptions, ExportResult, ImportOptions, ImportResult};
pub use lifecycle::{
    AdmissionOptions, CloseReport, DrainReport, OverloadReason, WriteAdmissionStats,
};
pub use query::{CancellationToken, QueryOptions, QueryPriority, QueryStats};
pub use segment::OfflinePackDirectoryInfo;
pub use snapshot::SnapshotInfo;
pub use types::{
    AggregatePoint, AggregateResolution, AggregateSet, CalendarGrouping, CalendarUnit, DAY_US,
    Durability, GroupRequest, GroupRow, GroupScope, GroupSource, Grouping, HOUR_US, MINUTE_US,
    Point, SeriesKey, TimeBucket, WeekStart, WriteOperation, WriteReceipt,
};
pub use write_ticket::{WriteStatus, WriteTicket};
