use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, corrupt, invalid};
use crate::types::{AggregateResolution, DAY_US, SeriesKey};

const MAX_NAME_BYTES: usize = 128;

/// A logical series name resolved through the durable catalog.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SeriesRef {
    pub space: String,
    pub bucket: String,
    pub key: u64,
}

/// A point addressed by a logical series name before catalog resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedPoint {
    pub series: SeriesRef,
    pub timestamp_us: i64,
    pub value: f64,
}

impl NamedPoint {
    /// Create a named point, validating its value before queue admission.
    ///
    /// # Errors
    ///
    /// Returns an error for NaN or infinity.
    pub fn new(series: SeriesRef, timestamp_us: i64, value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(invalid("point value must be finite"));
        }
        Ok(Self {
            series,
            timestamp_us,
            value,
        })
    }
}

impl SeriesRef {
    #[must_use]
    pub fn new(space: impl Into<String>, bucket: impl Into<String>, key: u64) -> Self {
        Self {
            space: space.into(),
            bucket: bucket.into(),
            key,
        }
    }
}

/// An immutable namespace definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpaceDefinition {
    pub id: u32,
    pub name: String,
    pub created_at_us: i64,
}

/// An immutable bucket definition and its selected storage policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketDefinition {
    pub id: u32,
    pub space_id: u32,
    pub name: String,
    pub policy_id: u32,
    pub created_at_us: i64,
}

/// Timestamp encoding within a Chimaera block.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampCodec {
    RawInt64,
    DeltaVarint,
    DeltaOfDelta,
}

/// Value encoding within a Chimaera block.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueCodec {
    RawFloat64,
    GorillaXor,
}

/// Compression applied to the encoded block as a whole.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockCodec {
    None,
    Gzip,
    Zstd,
}

/// Three independent stages of block compression.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionProfile {
    pub timestamp_codec: TimestampCodec,
    pub value_codec: ValueCodec,
    pub block_codec: BlockCodec,
    pub block_level: Option<u8>,
}

impl CompressionProfile {
    /// Create and validate a codec profile.
    ///
    /// # Errors
    ///
    /// Returns an error for a level with None, gzip outside `0..=9`, or zstd other than 3.
    pub fn new(
        timestamp_codec: TimestampCodec,
        value_codec: ValueCodec,
        block_codec: BlockCodec,
        block_level: Option<u8>,
    ) -> Result<Self> {
        let profile = Self {
            timestamp_codec,
            value_codec,
            block_codec,
            block_level,
        };
        profile.validate()?;
        Ok(profile)
    }

    #[must_use]
    pub const fn raw() -> Self {
        Self {
            timestamp_codec: TimestampCodec::RawInt64,
            value_codec: ValueCodec::RawFloat64,
            block_codec: BlockCodec::None,
            block_level: None,
        }
    }

    #[must_use]
    pub const fn hot_fast() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::RawFloat64,
            block_codec: BlockCodec::None,
            block_level: None,
        }
    }

    #[must_use]
    pub const fn columnar() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::None,
            block_level: None,
        }
    }

    #[must_use]
    pub const fn balanced() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::Gzip,
            block_level: Some(3),
        }
    }

    #[must_use]
    pub const fn cold_dense() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::Gzip,
            block_level: Some(6),
        }
    }

    /// Columnar encoding followed by Zstd-3; opt-in, without changing policy defaults.
    #[must_use]
    pub const fn zstd() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::Zstd,
            block_level: Some(3),
        }
    }

    /// Effective level: defaults to 6 for gzip and 3 for zstd.
    #[must_use]
    pub const fn effective_block_level(self) -> Option<u8> {
        match self.block_codec {
            BlockCodec::Gzip => Some(match self.block_level {
                Some(level) => level,
                None => 6,
            }),
            BlockCodec::Zstd => Some(match self.block_level {
                Some(level) => level,
                None => 3,
            }),
            BlockCodec::None => None,
        }
    }

    pub(crate) const fn timestamp_codec_id(self) -> u8 {
        match self.timestamp_codec {
            TimestampCodec::RawInt64 => 0,
            TimestampCodec::DeltaVarint => 1,
            TimestampCodec::DeltaOfDelta => 2,
        }
    }

    pub(crate) const fn value_codec_id(self) -> u8 {
        match self.value_codec {
            ValueCodec::RawFloat64 => 0,
            ValueCodec::GorillaXor => 1,
        }
    }

    pub(crate) const fn storage_signature(self) -> [u8; 4] {
        let (block_codec, level_marker) = match self.block_codec {
            BlockCodec::None => (0, 0),
            BlockCodec::Gzip => (
                1,
                match self.block_level {
                    Some(level) => level.saturating_add(1),
                    None => 7,
                },
            ),
            BlockCodec::Zstd => (2, 4),
        };
        [
            self.timestamp_codec_id(),
            self.value_codec_id(),
            block_codec,
            level_marker,
        ]
    }

    fn validate(self) -> Result<()> {
        match self.block_codec {
            BlockCodec::None if self.block_level.is_some() => {
                return Err(invalid("block level is only valid for gzip or zstd"));
            }
            BlockCodec::Gzip if self.block_level.is_some_and(|level| level > 9) => {
                return Err(invalid("gzip block level must be in 0..=9"));
            }
            BlockCodec::Zstd if self.block_level.is_some_and(|level| level != 3) => {
                return Err(invalid("zstd block level must be 3"));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Policy settings before a stable policy ID is assigned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoragePolicyOptions {
    pub name: String,
    pub hot_span_us: i64,
    pub retention_us: Option<i64>,
    /// Separate retention duration for persisted aggregates. Raw points still
    /// expire according to `retention_us`.
    #[serde(default)]
    pub aggregate_retention_us: Option<i64>,
    /// Aggregate resolutions that must outlive raw points.
    #[serde(default)]
    pub retained_aggregate_resolutions: Vec<AggregateResolution>,
    pub max_lateness_us: Option<i64>,
    /// Permitted lead over the trusted clock; `None` prevents treating the
    /// policy-wide event watermark as a final sealing boundary.
    #[serde(
        default = "default_max_future_skew_us",
        skip_serializing_if = "is_default_max_future_skew_us"
    )]
    pub max_future_skew_us: Option<i64>,
    pub partition_span_us: i64,
    pub hot_compression: CompressionProfile,
    pub cold_compression: CompressionProfile,
}

impl StoragePolicyOptions {
    /// Create a policy with hot_fast hot encoding and columnar cold encoding.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid name or interval.
    pub fn new(name: impl Into<String>, hot_span_us: i64, partition_span_us: i64) -> Result<Self> {
        let options = Self {
            name: name.into(),
            hot_span_us,
            retention_us: None,
            aggregate_retention_us: None,
            retained_aggregate_resolutions: Vec::new(),
            max_lateness_us: None,
            max_future_skew_us: default_max_future_skew_us(),
            partition_span_us,
            hot_compression: CompressionProfile::hot_fast(),
            cold_compression: CompressionProfile::columnar(),
        };
        options.validate()?;
        Ok(options)
    }

    /// Set retention and validate the entire policy again.
    ///
    /// # Errors
    ///
    /// Returns an error if retention does not exceed hot span or conflicts with lateness.
    pub fn with_retention_us(mut self, retention_us: Option<i64>) -> Result<Self> {
        self.retention_us = retention_us;
        self.validate()?;
        Ok(self)
    }

    /// Keep selected exact summaries longer than raw points.
    ///
    /// Raw and aggregate expiry cutoffs round down to UTC day boundaries
    /// so no minute, hour, or day bucket is retained only partially. This also
    /// affects read visibility: raw data can outlive its nominal duration by
    /// less than one day. Physical file reclamation can lag further.
    ///
    /// # Errors
    ///
    /// Returns an error if raw retention is unset, aggregate retention is not longer,
    /// resolutions are empty, or the partition span is not a multiple of a UTC day.
    pub fn with_aggregate_retention_us(
        mut self,
        aggregate_retention_us: Option<i64>,
        mut resolutions: Vec<AggregateResolution>,
    ) -> Result<Self> {
        resolutions.sort_unstable();
        resolutions.dedup();
        self.aggregate_retention_us = aggregate_retention_us;
        self.retained_aggregate_resolutions = resolutions;
        self.validate()?;
        Ok(self)
    }

    /// Set the maximum permitted age of an incoming point relative to the clock.
    ///
    /// # Errors
    ///
    /// Returns an error for a negative value or a value greater than retention.
    pub fn with_max_lateness_us(mut self, max_lateness_us: Option<i64>) -> Result<Self> {
        self.max_lateness_us = max_lateness_us;
        self.validate()?;
        Ok(self)
    }

    /// Limit future timestamps relative to the engine clock.
    ///
    /// `None` permits arbitrary future timestamps. Progress in other series does not
    /// by itself prevent a new series from writing into an older window.
    ///
    /// # Errors
    ///
    /// Returns an error for a negative value.
    pub fn with_max_future_skew_us(mut self, max_future_skew_us: Option<i64>) -> Result<Self> {
        self.max_future_skew_us = max_future_skew_us;
        self.validate()?;
        Ok(self)
    }

    /// Set the hot and cold codec profiles.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported codec or invalid compression
    /// level.
    pub fn with_compression(
        mut self,
        hot: CompressionProfile,
        cold: CompressionProfile,
    ) -> Result<Self> {
        hot.validate()?;
        cold.validate()?;
        self.hot_compression = hot;
        self.cold_compression = cold;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_name(&self.name, "policy")?;
        self.hot_compression.validate()?;
        self.cold_compression.validate()?;
        if self.hot_span_us <= 0 {
            return Err(invalid("hot span must be positive"));
        }
        if self.partition_span_us <= 0 {
            return Err(invalid("partition span must be positive"));
        }
        if self
            .retention_us
            .is_some_and(|retention| retention <= self.hot_span_us)
        {
            return Err(invalid("retention must exceed hot span"));
        }
        if self.max_lateness_us.is_some_and(|lateness| lateness < 0) {
            return Err(invalid("max lateness must not be negative"));
        }
        if self.max_future_skew_us.is_some_and(|skew| skew < 0) {
            return Err(invalid("maximum future skew must not be negative"));
        }
        if let (Some(lateness), Some(retention)) = (self.max_lateness_us, self.retention_us)
            && lateness > retention
        {
            return Err(invalid("max lateness must not exceed retention"));
        }
        match self.aggregate_retention_us {
            Some(aggregate_retention) => {
                let raw_retention = self
                    .retention_us
                    .ok_or_else(|| invalid("aggregate retention requires raw retention"))?;
                if aggregate_retention <= raw_retention {
                    return Err(invalid("aggregate retention must exceed raw retention"));
                }
                if self.retained_aggregate_resolutions.is_empty() {
                    return Err(invalid(
                        "aggregate retention requires at least one resolution",
                    ));
                }
                if self.partition_span_us.rem_euclid(DAY_US) != 0 {
                    return Err(invalid(
                        "aggregate retention requires a partition span aligned to UTC days",
                    ));
                }
            }
            None if !self.retained_aggregate_resolutions.is_empty() => {
                return Err(invalid(
                    "retained aggregate resolutions require aggregate retention",
                ));
            }
            None => {}
        }
        Ok(())
    }
}

/// A durable immutable storage policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoragePolicy {
    pub id: u32,
    pub name: String,
    pub hot_span_us: i64,
    pub retention_us: Option<i64>,
    #[serde(default)]
    pub aggregate_retention_us: Option<i64>,
    #[serde(default)]
    pub retained_aggregate_resolutions: Vec<AggregateResolution>,
    pub max_lateness_us: Option<i64>,
    #[serde(
        default = "default_max_future_skew_us",
        skip_serializing_if = "is_default_max_future_skew_us"
    )]
    pub max_future_skew_us: Option<i64>,
    pub partition_span_us: i64,
    pub hot_compression: CompressionProfile,
    pub cold_compression: CompressionProfile,
}

/// Policy time boundaries captured by a retention-info call.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionInfo {
    pub policy_id: u32,
    pub raw_retention_us: Option<i64>,
    pub aggregate_retention_us: Option<i64>,
    pub raw_available_from_us: Option<i64>,
    pub aggregate_available_from_us: Option<i64>,
    /// Legacy alias of `raw_available_from_us`; does not enable corrections.
    /// Write ordering, maximum lateness, and future-skew checks still apply.
    pub corrections_available_from_us: Option<i64>,
    pub available_to_us: Option<i64>,
    pub aggregate_resolutions: Vec<AggregateResolution>,
}

/// The same retention boundaries attached to a resolved numeric series key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SeriesStorageInfo {
    pub series: SeriesKey,
    pub retention: RetentionInfo,
}

/// Point temperature relative to its policy and captured maintenance time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataTemperature {
    Hot,
    Cold,
    Expired,
}

impl StoragePolicy {
    fn options(&self) -> StoragePolicyOptions {
        StoragePolicyOptions {
            name: self.name.clone(),
            hot_span_us: self.hot_span_us,
            retention_us: self.retention_us,
            aggregate_retention_us: self.aggregate_retention_us,
            retained_aggregate_resolutions: self.retained_aggregate_resolutions.clone(),
            max_lateness_us: self.max_lateness_us,
            max_future_skew_us: self.max_future_skew_us,
            partition_span_us: self.partition_span_us,
            hot_compression: self.hot_compression,
            cold_compression: self.cold_compression,
        }
    }

    /// Classify a point without reading the clock or changing state.
    #[must_use]
    pub fn temperature(
        &self,
        timestamp_us: i64,
        now_us: i64,
        retention_cutoff_us: Option<i64>,
    ) -> DataTemperature {
        if retention_cutoff_us.is_some_and(|cutoff| timestamp_us < cutoff) {
            return DataTemperature::Expired;
        }
        let hot_cutoff = i128::from(now_us) - i128::from(self.hot_span_us);
        if i128::from(timestamp_us) >= hot_cutoff {
            DataTemperature::Hot
        } else {
            DataTemperature::Cold
        }
    }
}

/// Policy identifier used when creating a bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicySelector {
    Id(u32),
    Name(String),
}

impl From<u32> for PolicySelector {
    fn from(value: u32) -> Self {
        Self::Id(value)
    }
}

impl From<&str> for PolicySelector {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for PolicySelector {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

/// An immutable catalog snapshot that can be read without locks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogSnapshot {
    spaces: Vec<SpaceDefinition>,
    policies: Vec<StoragePolicy>,
    buckets: Vec<BucketDefinition>,
}

impl CatalogSnapshot {
    #[must_use]
    pub fn spaces(&self) -> &[SpaceDefinition] {
        &self.spaces
    }

    #[must_use]
    pub fn policies(&self) -> &[StoragePolicy] {
        &self.policies
    }

    #[must_use]
    pub fn buckets(&self) -> &[BucketDefinition] {
        &self.buckets
    }

    #[must_use]
    pub fn space(&self, name: &str) -> Option<&SpaceDefinition> {
        self.spaces.iter().find(|space| space.name == name)
    }

    #[must_use]
    pub fn policy_by_id(&self, id: u32) -> Option<&StoragePolicy> {
        self.policies.iter().find(|policy| policy.id == id)
    }

    #[must_use]
    pub fn policy_by_name(&self, name: &str) -> Option<&StoragePolicy> {
        self.policies.iter().find(|policy| policy.name == name)
    }

    #[must_use]
    pub fn bucket(&self, space_id: u32, name: &str) -> Option<&BucketDefinition> {
        self.buckets
            .iter()
            .find(|bucket| bucket.space_id == space_id && bucket.name == name)
    }

    /// Resolve series names to a stable `SeriesKey` and its policy.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the space, bucket, or policy is absent.
    pub fn resolve(&self, series: &SeriesRef) -> Result<(SeriesKey, StoragePolicy)> {
        let space = self
            .space(&series.space)
            .ok_or_else(|| Error::NotFound(format!("space not found: {}", series.space)))?;
        let bucket = self.bucket(space.id, &series.bucket).ok_or_else(|| {
            Error::NotFound(format!(
                "bucket not found: {}/{}",
                series.space, series.bucket
            ))
        })?;
        let policy = self
            .policy_by_id(bucket.policy_id)
            .ok_or_else(|| Error::NotFound(format!("policy not found: {}", bucket.policy_id)))?;
        Ok((
            SeriesKey::new(space.id, bucket.id, series.key),
            policy.clone(),
        ))
    }

    /// Return the policy for a stable series key in this catalog.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the bucket does not belong to the specified space.
    pub fn policy_for(&self, series: SeriesKey) -> Result<&StoragePolicy> {
        self.registered_policy(series).ok_or_else(|| {
            Error::NotFound(format!(
                "bucket ID is not present in space: {}/{}",
                series.space_id, series.bucket_id
            ))
        })
    }

    pub(crate) fn registered_policy(&self, series: SeriesKey) -> Option<&StoragePolicy> {
        let bucket = self
            .buckets
            .iter()
            .find(|bucket| bucket.id == series.bucket_id && bucket.space_id == series.space_id)?;
        self.policy_by_id(bucket.policy_id)
    }

    pub(crate) fn storage_layout(&self, series: SeriesKey) -> Option<(u32, i64)> {
        self.registered_policy(series)
            .map(|policy| (policy.id, policy.partition_span_us))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CatalogState {
    #[serde(default = "first_id")]
    next_space_id: u32,
    #[serde(default = "first_id")]
    next_policy_id: u32,
    #[serde(default = "first_id")]
    next_bucket_id: u32,
    #[serde(default)]
    spaces: Vec<SpaceDefinition>,
    #[serde(default)]
    policies: Vec<StoragePolicy>,
    #[serde(default)]
    buckets: Vec<BucketDefinition>,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            next_space_id: 1,
            next_policy_id: 1,
            next_bucket_id: 1,
            spaces: Vec::new(),
            policies: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

impl CatalogState {
    pub(crate) fn contains_policy_id(&self, policy_id: u32) -> bool {
        self.policies.iter().any(|policy| policy.id == policy_id)
    }

    pub(crate) fn snapshot(&self) -> CatalogSnapshot {
        let mut snapshot = CatalogSnapshot {
            spaces: self.spaces.clone(),
            policies: self.policies.clone(),
            buckets: self.buckets.clone(),
        };
        snapshot.spaces.sort_unstable_by_key(|space| space.id);
        snapshot.policies.sort_unstable_by_key(|policy| policy.id);
        snapshot.buckets.sort_unstable_by_key(|bucket| bucket.id);
        snapshot
    }

    pub(crate) fn validate_loaded(&self) -> Result<()> {
        validate_definitions(&self.spaces, |space| space.id, |space| &space.name, "space")?;
        validate_definitions(
            &self.policies,
            |policy| policy.id,
            |policy| &policy.name,
            "policy",
        )?;
        for policy in &self.policies {
            policy
                .options()
                .validate()
                .map_err(|error| corrupt(format!("invalid catalog policy: {error}")))?;
        }
        let space_ids = self
            .spaces
            .iter()
            .map(|space| space.id)
            .collect::<HashSet<_>>();
        let policy_ids = self
            .policies
            .iter()
            .map(|policy| policy.id)
            .collect::<HashSet<_>>();
        let mut bucket_ids = HashSet::new();
        let mut bucket_names = HashSet::new();
        for bucket in &self.buckets {
            validate_name(&bucket.name, "bucket")
                .map_err(|error| corrupt(format!("invalid catalog bucket: {error}")))?;
            if bucket.id == 0
                || !bucket_ids.insert(bucket.id)
                || !bucket_names.insert((bucket.space_id, bucket.name.as_str()))
                || !space_ids.contains(&bucket.space_id)
                || !policy_ids.contains(&bucket.policy_id)
            {
                return Err(corrupt("catalog contains an invalid or duplicate bucket"));
            }
        }
        validate_cursor(self.next_space_id, self.spaces.iter().map(|space| space.id))?;
        validate_cursor(
            self.next_policy_id,
            self.policies.iter().map(|policy| policy.id),
        )?;
        validate_cursor(
            self.next_bucket_id,
            self.buckets.iter().map(|bucket| bucket.id),
        )?;
        Ok(())
    }

    pub(crate) fn create_space(
        &mut self,
        name: String,
        created_at_us: i64,
    ) -> Result<SpaceDefinition> {
        validate_name(&name, "space")?;
        if self.spaces.iter().any(|space| space.name == name) {
            return Err(Error::AlreadyExists(format!("space: {name}")));
        }
        let id = allocate(&mut self.next_space_id, "space")?;
        let definition = SpaceDefinition {
            id,
            name,
            created_at_us,
        };
        self.spaces.push(definition.clone());
        Ok(definition)
    }

    pub(crate) fn create_policy(&mut self, options: StoragePolicyOptions) -> Result<StoragePolicy> {
        options.validate()?;
        if self
            .policies
            .iter()
            .any(|policy| policy.name == options.name)
        {
            return Err(Error::AlreadyExists(format!("policy: {}", options.name)));
        }
        let id = allocate(&mut self.next_policy_id, "policy")?;
        let policy = StoragePolicy {
            id,
            name: options.name,
            hot_span_us: options.hot_span_us,
            retention_us: options.retention_us,
            aggregate_retention_us: options.aggregate_retention_us,
            retained_aggregate_resolutions: options.retained_aggregate_resolutions,
            max_lateness_us: options.max_lateness_us,
            max_future_skew_us: options.max_future_skew_us,
            partition_span_us: options.partition_span_us,
            hot_compression: options.hot_compression,
            cold_compression: options.cold_compression,
        };
        self.policies.push(policy.clone());
        Ok(policy)
    }

    pub(crate) fn create_bucket(
        &mut self,
        space_name: &str,
        name: String,
        selector: &PolicySelector,
        created_at_us: i64,
    ) -> Result<BucketDefinition> {
        validate_name(&name, "bucket")?;
        let space_id = self
            .spaces
            .iter()
            .find(|space| space.name == space_name)
            .map(|space| space.id)
            .ok_or_else(|| Error::NotFound(format!("space not found: {space_name}")))?;
        let policy_id = match selector {
            PolicySelector::Id(id) => self.policies.iter().find(|policy| policy.id == *id),
            PolicySelector::Name(name) => self.policies.iter().find(|policy| policy.name == *name),
        }
        .map(|policy| policy.id)
        .ok_or_else(|| Error::NotFound(format!("policy not found: {selector:?}")))?;
        if self
            .buckets
            .iter()
            .any(|bucket| bucket.space_id == space_id && bucket.name == name)
        {
            return Err(Error::AlreadyExists(format!("bucket: {space_name}/{name}")));
        }
        let id = allocate(&mut self.next_bucket_id, "bucket")?;
        let definition = BucketDefinition {
            id,
            space_id,
            name,
            policy_id,
            created_at_us,
        };
        self.buckets.push(definition.clone());
        Ok(definition)
    }
}

fn validate_definitions<T, Id, Name>(
    definitions: &[T],
    id: Id,
    name: Name,
    kind: &str,
) -> Result<()>
where
    Id: Fn(&T) -> u32,
    Name: Fn(&T) -> &String,
{
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for definition in definitions {
        validate_name(name(definition), kind)
            .map_err(|error| corrupt(format!("invalid catalog {kind}: {error}")))?;
        if id(definition) == 0
            || !ids.insert(id(definition))
            || !names.insert(name(definition).as_str())
        {
            return Err(corrupt(format!(
                "catalog contains an invalid or duplicate {kind}"
            )));
        }
    }
    Ok(())
}

fn validate_cursor(ids_next: u32, ids: impl Iterator<Item = u32>) -> Result<()> {
    if ids_next == 0 {
        return Err(corrupt("catalog contains a zero next ID"));
    }
    let maximum = ids.max().unwrap_or(0);
    if maximum == u32::MAX {
        if ids_next != u32::MAX {
            return Err(corrupt("catalog next ID moved behind exhausted ID space"));
        }
    } else if ids_next <= maximum {
        return Err(corrupt("catalog next ID moved backwards"));
    }
    Ok(())
}

fn allocate(next: &mut u32, kind: &str) -> Result<u32> {
    if *next == u32::MAX {
        return Err(Error::Catalog(format!("{kind} ID space exhausted")));
    }
    let id = *next;
    *next += 1;
    Ok(id)
}

pub(crate) fn validate_name(name: &str, kind: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(invalid(format!(
            "{kind} name must contain 1..={MAX_NAME_BYTES} UTF-8 bytes"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{kind} name must not contain control characters"
        )));
    }
    Ok(())
}

fn first_id() -> u32 {
    1
}

fn default_max_future_skew_us() -> Option<i64> {
    Some(DAY_US)
}

fn is_default_max_future_skew_us(value: &Option<i64>) -> bool {
    *value == default_max_future_skew_us()
}
