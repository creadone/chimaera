use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};

use chrono::{DateTime, SecondsFormat, Utc};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

use crate::catalog::{
    BlockCodec, BucketDefinition, CatalogSnapshot, CompressionProfile, NamedPoint, SeriesRef,
    SpaceDefinition, StoragePolicy, StoragePolicyOptions, TimestampCodec, ValueCodec,
};
use crate::engine::{PointStream, Storage};
use crate::error::{Error, Result, invalid};
use crate::types::{AggregateResolution, Point, SeriesKey};

const JSONL_FORMAT: u32 = 1;
const MAX_IMPORT_BATCH_SIZE: usize = 100_000;
const MAX_IMPORT_LINE_BYTES: usize = 64 * 1024 * 1024;

/// Limits for exporting registered series and their visible raw points as JSONL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportOptions {
    /// Gzip the output stream. Default: `false`.
    pub gzip: bool,
    /// Positive limit on discovered series, including keys without visible points.
    /// Default: 1,000,000. Shared working-memory limits apply as well.
    pub max_series: usize,
    /// Positive limit on emitted raw points. Default: 100,000,000.
    pub max_points: u64,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            gzip: false,
            max_series: 1_000_000,
            max_points: 100_000_000,
        }
    }
}

impl ExportOptions {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_series == 0 || self.max_points == 0 {
            return Err(invalid("export limits must be positive"));
        }
        Ok(())
    }
}

/// Completed export counters. On error, the output sink may contain a partial stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExportResult {
    /// Checkpoint commit captured by the export.
    pub read_commit_id: u64,
    pub spaces: usize,
    pub policies: usize,
    pub buckets: usize,
    /// Discovered keys; some can have no visible exported points.
    pub series: usize,
    /// Visible raw points emitted.
    pub points: u64,
    /// Bytes written to the sink, compressed when gzip is enabled.
    pub bytes_written: u64,
}

/// Limits for a streaming import. Each batch is atomic; the whole import is not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImportOptions {
    /// Decode gzip input. Default: `false`; this is not auto-detected by the library.
    pub gzip: bool,
    /// Points per atomic batch, 1..=100,000. Default: 1000.
    pub batch_size: usize,
    /// Positive decoded line-size limit in bytes, at most 64 MiB. Default: 1 MiB.
    pub max_line_bytes: usize,
    /// Positive input-line limit, including metadata records. Default: 100,000,000.
    pub max_lines: u64,
    /// Checkpoint after successful input processing. Default: `true`.
    /// A checkpoint error does not roll back batches already committed.
    pub checkpoint_after: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            gzip: false,
            batch_size: 1_000,
            max_line_bytes: 1024 * 1024,
            max_lines: 100_000_000,
            checkpoint_after: true,
        }
    }
}

impl ImportOptions {
    pub(crate) fn validate(self) -> Result<()> {
        if self.batch_size == 0 || self.max_line_bytes == 0 || self.max_lines == 0 {
            return Err(invalid("import limits must be positive"));
        }
        if self.batch_size > MAX_IMPORT_BATCH_SIZE {
            return Err(invalid(
                "import batch size exceeds the atomic operation limit",
            ));
        }
        if self.max_line_bytes > MAX_IMPORT_LINE_BYTES {
            return Err(invalid("import line limit exceeds the hard 64 MiB bound"));
        }
        Ok(())
    }
}

/// Counters returned after a successful import; an error can leave a committed prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ImportResult {
    pub lines_read: u64,
    pub points_imported: u64,
    pub batches_committed: u64,
    /// Last point-batch commit, or `None` when no point batch was written.
    pub last_commit_id: Option<u64>,
}

pub(crate) fn export_jsonl<W, F>(
    writer: W,
    options: ExportOptions,
    created_at_us: i64,
    read_commit_id: u64,
    catalog: &CatalogSnapshot,
    series: &[SeriesKey],
    streams: F,
) -> Result<ExportResult>
where
    W: Write,
    F: FnMut(SeriesKey) -> Result<PointStream>,
{
    options.validate()?;
    let mut counting = CountingWriter::new(writer);
    let points = if options.gzip {
        let mut gzip = GzEncoder::new(&mut counting, Compression::default());
        let points = write_export(
            &mut gzip,
            options,
            created_at_us,
            read_commit_id,
            catalog,
            series,
            streams,
        )?;
        gzip.try_finish()?;
        points
    } else {
        write_export(
            &mut counting,
            options,
            created_at_us,
            read_commit_id,
            catalog,
            series,
            streams,
        )?
    };
    counting.flush()?;
    Ok(ExportResult {
        read_commit_id,
        spaces: catalog.spaces().len(),
        policies: catalog.policies().len(),
        buckets: catalog.buckets().len(),
        series: series.len(),
        points,
        bytes_written: counting.bytes_written,
    })
}

fn write_export<W, F>(
    writer: &mut W,
    options: ExportOptions,
    created_at_us: i64,
    read_commit_id: u64,
    catalog: &CatalogSnapshot,
    series: &[SeriesKey],
    mut streams: F,
) -> Result<u64>
where
    W: Write,
    F: FnMut(SeriesKey) -> Result<PointStream>,
{
    write_record(
        writer,
        &HeaderRecord {
            kind: "chronotope_dump",
            format: JSONL_FORMAT,
            created_at: format_timestamp(created_at_us)?,
            read_commit_id: read_commit_id.to_string(),
            timestamp_unit: "microsecond",
        },
    )?;
    for space in catalog.spaces() {
        write_record(writer, &SpaceRecord::from(space))?;
    }
    for policy in catalog.policies() {
        write_record(writer, &PolicyRecord::from(policy))?;
    }
    for bucket in catalog.buckets() {
        write_record(writer, &BucketRecord::from(bucket))?;
    }

    let mut point_count = 0_u64;
    for series_key in series {
        for point in streams(*series_key)? {
            let point = point?;
            point_count = point_count.saturating_add(1);
            if point_count > options.max_points {
                return Err(Error::ResourceLimit("export point limit exceeded".into()));
            }
            write_record(writer, &PointRecord::from(point))?;
        }
    }
    write_record(
        writer,
        &EndRecord {
            kind: "end",
            spaces: catalog.spaces().len(),
            policies: catalog.policies().len(),
            buckets: catalog.buckets().len(),
            points: point_count.to_string(),
        },
    )?;
    writer.flush()?;
    Ok(point_count)
}

pub(crate) fn import_jsonl<R: Read>(
    storage: &Storage,
    reader: R,
    options: ImportOptions,
) -> Result<ImportResult> {
    options.validate()?;
    if options.gzip {
        Importer::new(storage, options).run(BufReader::new(GzDecoder::new(reader)))
    } else {
        Importer::new(storage, options).run(BufReader::new(reader))
    }
}

struct Importer<'a> {
    storage: &'a Storage,
    options: ImportOptions,
    spaces: HashMap<String, SpaceDefinition>,
    policies: HashMap<String, StoragePolicy>,
    buckets: HashMap<String, (String, String)>,
    batch: Vec<NamedPoint>,
    lines: u64,
    points: u64,
    batches: u64,
    last_commit_id: Option<u64>,
    header_seen: bool,
    end_seen: bool,
}

impl<'a> Importer<'a> {
    fn new(storage: &'a Storage, options: ImportOptions) -> Self {
        Self {
            storage,
            options,
            spaces: HashMap::new(),
            policies: HashMap::new(),
            buckets: HashMap::new(),
            batch: Vec::with_capacity(options.batch_size),
            lines: 0,
            points: 0,
            batches: 0,
            last_commit_id: None,
            header_seen: false,
            end_seen: false,
        }
    }

    fn run<R: BufRead>(mut self, mut reader: R) -> Result<ImportResult> {
        let mut line = Vec::with_capacity(self.options.max_line_bytes.min(64 * 1024));
        while read_line_limited(&mut reader, &mut line, self.options.max_line_bytes)? {
            self.lines = self.lines.saturating_add(1);
            if self.lines > self.options.max_lines {
                return Err(Error::ResourceLimit("import line count exceeded".into()));
            }
            if line.is_empty() {
                return Err(self.import_error("blank JSONL records are not allowed"));
            }
            let record: DumpRecord = serde_json::from_slice(&line)
                .map_err(|error| self.import_error(format!("invalid JSONL record: {error}")))?;
            self.consume(record)?;
        }
        if !self.header_seen {
            return Err(self.import_error("dump header is missing"));
        }
        if !self.end_seen {
            return Err(self.import_error("dump end record is missing"));
        }
        self.commit_batch()?;
        if self.options.checkpoint_after {
            self.storage.checkpoint()?;
        }
        Ok(ImportResult {
            lines_read: self.lines,
            points_imported: self.points,
            batches_committed: self.batches,
            last_commit_id: self.last_commit_id,
        })
    }

    fn consume(&mut self, record: DumpRecord) -> Result<()> {
        if self.end_seen {
            return Err(self.import_error("record appears after end"));
        }
        if !self.header_seen {
            return self.consume_header(record);
        }
        match record.kind.as_str() {
            "space" => self.consume_space(record),
            "policy" => self.consume_policy(record),
            "bucket" => self.consume_bucket(record),
            "point" => self.consume_point(record),
            "end" => self.consume_end(record),
            _ => Err(self.import_error("unknown dump record type")),
        }
    }

    fn consume_header(&mut self, record: DumpRecord) -> Result<()> {
        if !matches!(record.kind.as_str(), "chronotope_dump" | "tessima_dump")
            || record.format != Some(JSONL_FORMAT)
            || record.timestamp_unit.as_deref() != Some("microsecond")
        {
            return Err(self.import_error("unsupported or invalid dump header"));
        }
        parse_timestamp_string(required(record.created_at.as_deref(), "created_at")?)?;
        parse_u64(record.read_commit_id.as_deref(), "read_commit_id")?;
        self.header_seen = true;
        Ok(())
    }

    fn consume_space(&mut self, record: DumpRecord) -> Result<()> {
        let dump_id = required(record.id.as_deref(), "id")?.to_owned();
        if self.spaces.contains_key(&dump_id) {
            return Err(self.import_error("duplicate dump space ID"));
        }
        let name = required(record.name.as_deref(), "name")?;
        let catalog = self.storage.catalog()?;
        let definition = match catalog.space(name) {
            Some(space) => space.clone(),
            None => self.storage.create_space(name)?,
        };
        self.spaces.insert(dump_id, definition);
        Ok(())
    }

    fn consume_policy(&mut self, record: DumpRecord) -> Result<()> {
        let dump_id = required(record.id.as_deref(), "id")?.to_owned();
        if self.policies.contains_key(&dump_id) {
            return Err(self.import_error("duplicate dump policy ID"));
        }
        let options = StoragePolicyOptions {
            name: required(record.name.as_deref(), "name")?.to_owned(),
            hot_span_us: parse_positive_i64(record.hot_span_us.as_deref(), "hot_span_us")?,
            retention_us: parse_optional_nonnegative_i64(
                record.retention_us.as_deref(),
                "retention_us",
            )?,
            aggregate_retention_us: parse_optional_nonnegative_i64(
                record.aggregate_retention_us.as_deref(),
                "aggregate_retention_us",
            )?,
            retained_aggregate_resolutions: record
                .retained_aggregate_resolutions
                .unwrap_or_default(),
            max_lateness_us: parse_optional_nonnegative_i64(
                record.max_lateness_us.as_deref(),
                "max_lateness_us",
            )?,
            max_future_skew_us: parse_optional_nonnegative_i64(
                record.max_future_skew_us.as_deref(),
                "max_future_skew_us",
            )?,
            partition_span_us: parse_positive_i64(
                record.partition_span_us.as_deref(),
                "partition_span_us",
            )?,
            hot_compression: parse_profile(record.hot_compression, "hot_compression")?,
            cold_compression: parse_profile(record.cold_compression, "cold_compression")?,
        };
        let catalog = self.storage.catalog()?;
        let policy = match catalog.policy_by_name(&options.name) {
            Some(policy) if policy_matches(policy, &options) => policy.clone(),
            Some(_) => {
                return Err(self.import_error("existing policy is incompatible with dump policy"));
            }
            None => self.storage.create_policy(options)?,
        };
        self.policies.insert(dump_id, policy);
        Ok(())
    }

    fn consume_bucket(&mut self, record: DumpRecord) -> Result<()> {
        let dump_id = required(record.id.as_deref(), "id")?.to_owned();
        if self.buckets.contains_key(&dump_id) {
            return Err(self.import_error("duplicate dump bucket ID"));
        }
        let space_dump_id = required(record.space_id.as_deref(), "space_id")?;
        let policy_dump_id = required(record.policy_id.as_deref(), "policy_id")?;
        let space = self
            .spaces
            .get(space_dump_id)
            .ok_or_else(|| self.import_error("bucket references unknown dump space"))?;
        let policy = self
            .policies
            .get(policy_dump_id)
            .ok_or_else(|| self.import_error("bucket references unknown dump policy"))?;
        let name = required(record.name.as_deref(), "name")?;
        let catalog = self.storage.catalog()?;
        let bucket = match catalog.bucket(space.id, name) {
            Some(bucket) if bucket.policy_id == policy.id => bucket.clone(),
            Some(_) => {
                return Err(self.import_error("existing bucket uses a different policy"));
            }
            None => self.storage.create_bucket(&space.name, name, policy.id)?,
        };
        self.buckets
            .insert(dump_id, (space.name.clone(), bucket.name));
        Ok(())
    }

    fn consume_point(&mut self, record: DumpRecord) -> Result<()> {
        let space_dump_id = required(record.space_id.as_deref(), "space_id")?;
        let bucket_dump_id = required(record.bucket_id.as_deref(), "bucket_id")?;
        let space = self
            .spaces
            .get(space_dump_id)
            .ok_or_else(|| self.import_error("point references unknown dump space"))?;
        let bucket = self
            .buckets
            .get(bucket_dump_id)
            .ok_or_else(|| self.import_error("point references unknown dump bucket"))?;
        if bucket.0 != space.name {
            return Err(self.import_error("point bucket is outside its dump space"));
        }
        let key = parse_u64(record.key.as_deref(), "key")?;
        let timestamp_us = parse_record_timestamp(&record)?;
        let value = record
            .value
            .filter(|value| value.is_finite())
            .ok_or_else(|| self.import_error("point value is required and must be finite"))?;
        self.batch.push(NamedPoint::new(
            SeriesRef::new(&space.name, &bucket.1, key),
            timestamp_us,
            value,
        )?);
        self.points = self.points.saturating_add(1);
        if self.batch.len() >= self.options.batch_size {
            self.commit_batch()?;
        }
        Ok(())
    }

    fn consume_end(&mut self, record: DumpRecord) -> Result<()> {
        let spaces = record
            .spaces
            .ok_or_else(|| self.import_error("end record requires spaces"))?;
        let buckets = record
            .buckets
            .ok_or_else(|| self.import_error("end record requires buckets"))?;
        let points = parse_u64(record.points.as_deref(), "points")?;
        if spaces != self.spaces.len()
            || buckets != self.buckets.len()
            || points != self.points
            || record
                .policies
                .is_some_and(|policies| policies != self.policies.len())
        {
            return Err(self.import_error("end record counts do not match imported records"));
        }
        self.end_seen = true;
        Ok(())
    }

    fn commit_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(self.options.batch_size));
        let receipt = self.storage.write_named_batch(batch)?;
        self.last_commit_id = Some(receipt.commit_id);
        self.batches = self.batches.saturating_add(1);
        Ok(())
    }

    fn import_error(&self, message: impl AsRef<str>) -> Error {
        invalid(format!(
            "import line {} after {} accepted batches and {} points: {}",
            self.lines,
            self.batches,
            self.points,
            message.as_ref()
        ))
    }
}

#[derive(Serialize)]
struct HeaderRecord {
    #[serde(rename = "type")]
    kind: &'static str,
    format: u32,
    created_at: String,
    read_commit_id: String,
    timestamp_unit: &'static str,
}

#[derive(Serialize)]
struct SpaceRecord<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    id: String,
    name: &'a str,
}

impl<'a> From<&'a SpaceDefinition> for SpaceRecord<'a> {
    fn from(space: &'a SpaceDefinition) -> Self {
        Self {
            kind: "space",
            id: space.id.to_string(),
            name: &space.name,
        }
    }
}

#[derive(Serialize)]
struct PolicyRecord<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    id: String,
    name: &'a str,
    hot_span_us: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_us: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregate_retention_us: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    retained_aggregate_resolutions: Vec<AggregateResolution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_lateness_us: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_future_skew_us: Option<String>,
    partition_span_us: String,
    hot_compression: CompressionRecord,
    cold_compression: CompressionRecord,
}

impl<'a> From<&'a StoragePolicy> for PolicyRecord<'a> {
    fn from(policy: &'a StoragePolicy) -> Self {
        Self {
            kind: "policy",
            id: policy.id.to_string(),
            name: &policy.name,
            hot_span_us: policy.hot_span_us.to_string(),
            retention_us: policy.retention_us.map(|value| value.to_string()),
            aggregate_retention_us: policy.aggregate_retention_us.map(|value| value.to_string()),
            retained_aggregate_resolutions: policy.retained_aggregate_resolutions.clone(),
            max_lateness_us: policy.max_lateness_us.map(|value| value.to_string()),
            max_future_skew_us: policy.max_future_skew_us.map(|value| value.to_string()),
            partition_span_us: policy.partition_span_us.to_string(),
            hot_compression: policy.hot_compression.into(),
            cold_compression: policy.cold_compression.into(),
        }
    }
}

#[derive(Serialize)]
struct BucketRecord<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    id: String,
    space_id: String,
    name: &'a str,
    policy_id: String,
}

impl<'a> From<&'a BucketDefinition> for BucketRecord<'a> {
    fn from(bucket: &'a BucketDefinition) -> Self {
        Self {
            kind: "bucket",
            id: bucket.id.to_string(),
            space_id: bucket.space_id.to_string(),
            name: &bucket.name,
            policy_id: bucket.policy_id.to_string(),
        }
    }
}

#[derive(Serialize)]
struct PointRecord {
    #[serde(rename = "type")]
    kind: &'static str,
    space_id: String,
    bucket_id: String,
    key: String,
    timestamp_us: String,
    value: f64,
}

impl From<Point> for PointRecord {
    fn from(point: Point) -> Self {
        Self {
            kind: "point",
            space_id: point.series.space_id.to_string(),
            bucket_id: point.series.bucket_id.to_string(),
            key: point.series.key.to_string(),
            timestamp_us: point.timestamp_us.to_string(),
            value: point.value,
        }
    }
}

#[derive(Serialize)]
struct EndRecord {
    #[serde(rename = "type")]
    kind: &'static str,
    spaces: usize,
    policies: usize,
    buckets: usize,
    points: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct CompressionRecord {
    timestamp: TimestampCodec,
    value: ValueCodec,
    block: BlockCodec,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<u8>,
}

impl From<CompressionProfile> for CompressionRecord {
    fn from(profile: CompressionProfile) -> Self {
        Self {
            timestamp: profile.timestamp_codec,
            value: profile.value_codec,
            block: profile.block_codec,
            level: profile.block_level,
        }
    }
}

#[derive(Deserialize)]
struct DumpRecord {
    #[serde(rename = "type")]
    kind: String,
    format: Option<u32>,
    timestamp_unit: Option<String>,
    created_at: Option<String>,
    read_commit_id: Option<String>,
    id: Option<String>,
    name: Option<String>,
    space_id: Option<String>,
    bucket_id: Option<String>,
    policy_id: Option<String>,
    hot_span_us: Option<String>,
    retention_us: Option<String>,
    aggregate_retention_us: Option<String>,
    retained_aggregate_resolutions: Option<Vec<AggregateResolution>>,
    max_lateness_us: Option<String>,
    max_future_skew_us: Option<String>,
    partition_span_us: Option<String>,
    hot_compression: Option<CompressionRecord>,
    cold_compression: Option<CompressionRecord>,
    key: Option<String>,
    timestamp: Option<String>,
    timestamp_us: Option<String>,
    value: Option<f64>,
    spaces: Option<usize>,
    policies: Option<usize>,
    buckets: Option<usize>,
    points: Option<String>,
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W> CountingWriter<W> {
    const fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_record(writer: &mut impl Write, record: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn format_timestamp(timestamp_us: i64) -> Result<String> {
    DateTime::<Utc>::from_timestamp_micros(timestamp_us)
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Micros, true))
        .ok_or_else(|| invalid("timestamp is outside RFC 3339 range"))
}

fn parse_record_timestamp(record: &DumpRecord) -> Result<i64> {
    let from_rfc = record
        .timestamp
        .as_deref()
        .map(parse_timestamp_string)
        .transpose()?;
    let from_integer = record
        .timestamp_us
        .as_deref()
        .map(|value| parse_i64(Some(value), "timestamp_us"))
        .transpose()?;
    match (from_rfc, from_integer) {
        (Some(rfc), Some(integer)) if rfc != integer => Err(invalid(
            "timestamp and timestamp_us identify different instants",
        )),
        (Some(timestamp), _) | (_, Some(timestamp)) => Ok(timestamp),
        (None, None) => Err(invalid("point requires timestamp or timestamp_us")),
    }
}

fn parse_timestamp_string(value: &str) -> Result<i64> {
    let timestamp =
        DateTime::parse_from_rfc3339(value).map_err(|_| invalid("invalid RFC 3339 timestamp"))?;
    if timestamp.timestamp_subsec_nanos() % 1_000 != 0 {
        return Err(invalid("timestamp precision exceeds microseconds"));
    }
    let micros = i128::from(timestamp.timestamp()) * 1_000_000
        + i128::from(timestamp.timestamp_subsec_nanos() / 1_000);
    i64::try_from(micros).map_err(|_| invalid("timestamp exceeds Int64 microseconds"))
}

fn parse_profile(profile: Option<CompressionRecord>, field: &str) -> Result<CompressionProfile> {
    let profile = profile.ok_or_else(|| invalid(format!("missing {field}")))?;
    CompressionProfile::new(
        profile.timestamp,
        profile.value,
        profile.block,
        profile.level,
    )
}

fn policy_matches(policy: &StoragePolicy, options: &StoragePolicyOptions) -> bool {
    policy.name == options.name
        && policy.hot_span_us == options.hot_span_us
        && policy.retention_us == options.retention_us
        && policy.aggregate_retention_us == options.aggregate_retention_us
        && policy.retained_aggregate_resolutions == options.retained_aggregate_resolutions
        && policy.max_lateness_us == options.max_lateness_us
        && policy.max_future_skew_us == options.max_future_skew_us
        && policy.partition_span_us == options.partition_span_us
        && policy.hot_compression == options.hot_compression
        && policy.cold_compression == options.cold_compression
}

fn required<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str> {
    value.ok_or_else(|| invalid(format!("missing {field}")))
}

fn parse_u64(value: Option<&str>, field: &str) -> Result<u64> {
    required(value, field)?
        .parse()
        .map_err(|_| invalid(format!("invalid {field}")))
}

fn parse_i64(value: Option<&str>, field: &str) -> Result<i64> {
    required(value, field)?
        .parse()
        .map_err(|_| invalid(format!("invalid {field}")))
}

fn parse_positive_i64(value: Option<&str>, field: &str) -> Result<i64> {
    let value = parse_i64(value, field)?;
    if value <= 0 {
        return Err(invalid(format!("{field} must be positive")));
    }
    Ok(value)
}

fn parse_optional_nonnegative_i64(value: Option<&str>, field: &str) -> Result<Option<i64>> {
    let value = value
        .map(|value| parse_i64(Some(value), field))
        .transpose()?;
    if value.is_some_and(|value| value < 0) {
        return Err(invalid(format!("{field} must not be negative")));
    }
    Ok(value)
}

fn read_line_limited(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    maximum_bytes: usize,
) -> Result<bool> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content = newline.unwrap_or(consumed);
        if line.len().saturating_add(content) > maximum_bytes {
            return Err(Error::ResourceLimit(
                "JSONL line exceeds configured limit".into(),
            ));
        }
        line.extend_from_slice(&available[..content]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(true);
        }
    }
}
