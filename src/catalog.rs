use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, corrupt, invalid};
use crate::types::{AggregateResolution, DAY_US, SeriesKey};

const MAX_NAME_BYTES: usize = 128;

/// Логическое имя ряда, разрешаемое через долговечный каталог.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SeriesRef {
    pub space: String,
    pub bucket: String,
    pub key: u64,
}

/// Точка с логическим именем ряда до разрешения каталога.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedPoint {
    pub series: SeriesRef,
    pub timestamp_us: i64,
    pub value: f64,
}

impl NamedPoint {
    /// Создаёт именованную точку, проверяя значение до постановки в очередь.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для NaN и бесконечности.
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

/// Put или delete с логическим именем ряда.
#[derive(Clone, Debug, PartialEq)]
pub enum NamedWriteOperation {
    Put(NamedPoint),
    Delete {
        series: SeriesRef,
        timestamp_us: i64,
    },
}

impl NamedWriteOperation {
    /// Создаёт идемпотентное удаление именованной точки.
    #[must_use]
    pub const fn delete(series: SeriesRef, timestamp_us: i64) -> Self {
        Self::Delete {
            series,
            timestamp_us,
        }
    }
}

impl From<NamedPoint> for NamedWriteOperation {
    fn from(point: NamedPoint) -> Self {
        Self::Put(point)
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

/// Неизменяемое описание пространства имён.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpaceDefinition {
    pub id: u32,
    pub name: String,
    pub created_at_us: i64,
}

/// Неизменяемое описание bucket и выбранной политики хранения.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketDefinition {
    pub id: u32,
    pub space_id: u32,
    pub name: String,
    pub policy_id: u32,
    pub created_at_us: i64,
}

/// Кодек timestamp в блоке Chimaera v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampCodec {
    RawInt64,
    DeltaVarint,
    DeltaOfDelta,
}

/// Кодек значения в блоке Chimaera v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueCodec {
    RawFloat64,
    GorillaXor,
}

/// Внешнее сжатие блока Chimaera v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockCodec {
    None,
    Gzip,
    Zstd,
}

/// Допустимые изменения временного ряда.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Разрешены повторная запись timestamp, удаление и поздние точки.
    Mutable,
    /// Каждый следующий timestamp ряда обязан быть строго больше предыдущего;
    /// удаления запрещены.
    #[default]
    StrictlyIncreasing,
}

/// Три независимых этапа сжатия блока.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionProfile {
    pub timestamp_codec: TimestampCodec,
    pub value_codec: ValueCodec,
    pub block_codec: BlockCodec,
    pub block_level: Option<u8>,
}

impl CompressionProfile {
    /// Создаёт и проверяет профиль кодеков.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для уровня у None, gzip вне `0..=9` или zstd не равного 3.
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

    /// Columnar с внешним Zstd-3; выбирается явно, defaults политик не меняет.
    #[must_use]
    pub const fn zstd() -> Self {
        Self {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::Zstd,
            block_level: Some(3),
        }
    }

    /// Эффективный уровень: по умолчанию 6 для gzip и 3 для zstd.
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

/// Настройки до выдачи стабильного номера политики.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoragePolicyOptions {
    pub name: String,
    pub hot_span_us: i64,
    pub retention_us: Option<i64>,
    /// Отдельный срок хранения сохранённых сводок. Исходные точки по-прежнему
    /// удаляются по `retention_us`.
    #[serde(default)]
    pub aggregate_retention_us: Option<i64>,
    /// Разрешения, которые должны пережить исходные точки.
    #[serde(default)]
    pub retained_aggregate_resolutions: Vec<AggregateResolution>,
    /// Проверяемый договор допустимых операций ряда.
    #[serde(default)]
    pub write_mode: WriteMode,
    pub max_lateness_us: Option<i64>,
    /// Допустимое опережение над надёжными часами; `None` запрещает считать
    /// policy-wide event watermark окончательной границей seal.
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
    /// Создаёт политику с быстрым горячим и columnar холодным профилем.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для недопустимого имени или интервала.
    pub fn new(name: impl Into<String>, hot_span_us: i64, partition_span_us: i64) -> Result<Self> {
        let options = Self {
            name: name.into(),
            hot_span_us,
            retention_us: None,
            aggregate_retention_us: None,
            retained_aggregate_resolutions: Vec::new(),
            write_mode: WriteMode::default(),
            max_lateness_us: None,
            max_future_skew_us: default_max_future_skew_us(),
            partition_span_us,
            hot_compression: CompressionProfile::hot_fast(),
            cold_compression: CompressionProfile::columnar(),
        };
        options.validate()?;
        Ok(options)
    }

    /// Задаёт срок хранения и повторно проверяет всю политику.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку, если retention не превышает hot span или конфликтует с lateness.
    pub fn with_retention_us(mut self, retention_us: Option<i64>) -> Result<Self> {
        self.retention_us = retention_us;
        self.validate()?;
        Ok(self)
    }

    /// Сохраняет выбранные точные сводки дольше исходных точек.
    ///
    /// Физическое удаление исходных данных округляется вниз до границы UTC-суток,
    /// чтобы ни одна минутная, часовая или суточная корзина не стала частичной.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку, если срок исходных точек не задан, срок сводок не больше
    /// него, список разрешений пуст или окно раздела не кратно UTC-суткам.
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

    /// Выбирает проверяемый договор записи для всех рядов политики.
    #[must_use]
    pub const fn with_write_mode(mut self, write_mode: WriteMode) -> Self {
        self.write_mode = write_mode;
        self
    }

    /// Задаёт максимально допустимое опоздание записи.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для отрицательного значения или значения больше retention.
    pub fn with_max_lateness_us(mut self, max_lateness_us: Option<i64>) -> Result<Self> {
        self.max_lateness_us = max_lateness_us;
        self.validate()?;
        Ok(self)
    }

    /// Ограничивает timestamp в будущем относительно часов ядра.
    ///
    /// `None` разрешает произвольное будущее, но отключает окончательный
    /// policy-wide seal: historical overlay в таком режиме остаётся обязательным.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для отрицательного значения.
    pub fn with_max_future_skew_us(mut self, max_future_skew_us: Option<i64>) -> Result<Self> {
        self.max_future_skew_us = max_future_skew_us;
        self.validate()?;
        Ok(self)
    }

    /// Задаёт горячий и холодный профили кодеков.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для неподдерживаемого codec или недопустимого уровня
    /// сжатия.
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

/// Долговечная неизменяемая политика хранения.
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
    #[serde(default)]
    pub write_mode: WriteMode,
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

/// Гарантированные временные границы политики на момент запроса.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionInfo {
    pub policy_id: u32,
    pub raw_retention_us: Option<i64>,
    pub aggregate_retention_us: Option<i64>,
    pub raw_available_from_us: Option<i64>,
    pub aggregate_available_from_us: Option<i64>,
    /// Исправления принимаются только с этой границы исходных данных.
    pub corrections_available_from_us: Option<i64>,
    pub available_to_us: Option<i64>,
    pub aggregate_resolutions: Vec<AggregateResolution>,
}

/// Те же границы, привязанные к разрешённому числовому ключу ряда.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SeriesStorageInfo {
    pub series: SeriesKey,
    pub retention: RetentionInfo,
}

/// Температура точки относительно policy и захваченного времени обслуживания.
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
            write_mode: self.write_mode,
            max_lateness_us: self.max_lateness_us,
            max_future_skew_us: self.max_future_skew_us,
            partition_span_us: self.partition_span_us,
            hot_compression: self.hot_compression,
            cold_compression: self.cold_compression,
        }
    }

    /// Классифицирует точку без чтения часов и изменения состояния.
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

/// Идентификатор политики при создании bucket.
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

/// Неизменяемый снимок каталога, безопасный для чтения без блокировок.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogSnapshot {
    spaces: Vec<SpaceDefinition>,
    policies: Vec<StoragePolicy>,
    buckets: Vec<BucketDefinition>,
}

impl CatalogSnapshot {
    pub(crate) fn from_storage_parts(
        spaces: Vec<SpaceDefinition>,
        policies: Vec<StoragePolicy>,
        buckets: Vec<BucketDefinition>,
        next_space_id: u32,
        next_policy_id: u32,
        next_bucket_id: u32,
    ) -> Result<Self> {
        let state = CatalogState {
            next_space_id,
            next_policy_id,
            next_bucket_id,
            spaces,
            policies,
            buckets,
        };
        state.validate_loaded()?;
        Ok(state.snapshot())
    }

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

    /// Разрешает имена ряда в стабильный `SeriesKey` и его политику.
    ///
    /// # Errors
    ///
    /// Возвращает `NotFound`, если пространство, bucket или policy отсутствует.
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

    /// Возвращает политику для стабильного ключа из этого каталога.
    ///
    /// # Errors
    ///
    /// Возвращает `NotFound`, если bucket не принадлежит указанному space.
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
            write_mode: options.write_mode,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_round_trip_and_resolution() -> Result<()> {
        let mut catalog = CatalogState::default();
        let space = catalog.create_space("production".into(), 10)?;
        let options = StoragePolicyOptions::new("default", 60, 3_600)?
            .with_retention_us(Some(7_200))?
            .with_max_lateness_us(Some(30))?;
        assert_eq!(options.write_mode, WriteMode::StrictlyIncreasing);
        assert_eq!(options.max_future_skew_us, Some(DAY_US));
        let policy = catalog.create_policy(options)?;
        let bucket =
            catalog.create_bucket("production", "latency".into(), &policy.id.into(), 11)?;
        catalog.validate_loaded()?;

        let snapshot = catalog.snapshot();
        let resolved = snapshot.resolve(&SeriesRef::new("production", "latency", 42))?;
        assert_eq!(resolved.0, SeriesKey::new(space.id, bucket.id, 42));
        assert_eq!(resolved.1, policy);
        Ok(())
    }

    #[test]
    fn policy_validation_rejects_inconsistent_spans_and_profiles() -> Result<()> {
        assert!(StoragePolicyOptions::new("bad", 0, 1).is_err());
        assert!(
            StoragePolicyOptions::new("bad", 10, 20)?
                .with_max_future_skew_us(Some(-1))
                .is_err()
        );
        assert!(
            StoragePolicyOptions::new("bad", 10, 20)?
                .with_retention_us(Some(10))
                .is_err()
        );
        assert!(
            StoragePolicyOptions::new("bad", 10, DAY_US)?
                .with_aggregate_retention_us(Some(100), vec![AggregateResolution::Day])
                .is_err()
        );
        assert!(
            StoragePolicyOptions::new("bad", 10, DAY_US)?
                .with_retention_us(Some(100))?
                .with_aggregate_retention_us(Some(100), vec![AggregateResolution::Day])
                .is_err()
        );
        assert!(
            StoragePolicyOptions::new("bad", 10, DAY_US - 1)?
                .with_retention_us(Some(100))?
                .with_aggregate_retention_us(Some(200), vec![AggregateResolution::Day])
                .is_err()
        );
        assert!(
            CompressionProfile::new(
                TimestampCodec::RawInt64,
                ValueCodec::RawFloat64,
                BlockCodec::None,
                Some(1)
            )
            .is_err()
        );
        assert!(matches!(
            CompressionProfile::new(
                TimestampCodec::DeltaOfDelta,
                ValueCodec::GorillaXor,
                BlockCodec::Zstd,
                Some(1),
            ),
            Err(Error::Invalid(message))
                if message.contains("zstd block level")
        ));

        let unsupported = CompressionProfile {
            timestamp_codec: TimestampCodec::DeltaOfDelta,
            value_codec: ValueCodec::GorillaXor,
            block_codec: BlockCodec::Zstd,
            block_level: Some(1),
        };
        assert!(matches!(
            StoragePolicyOptions::new("unsupported", 10, 20)?
                .with_compression(unsupported, CompressionProfile::columnar()),
            Err(Error::Invalid(message)) if message.contains("zstd block level")
        ));

        let mut catalog = CatalogState::default();
        catalog.create_policy(StoragePolicyOptions::new("loaded", 10, 20)?)?;
        catalog.policies[0].hot_compression = unsupported;
        assert!(matches!(
            catalog.validate_loaded(),
            Err(Error::Corruption(message))
                if message.contains("zstd block level") && message.contains("catalog policy")
        ));
        Ok(())
    }

    #[test]
    fn zstd_profile_accepts_only_level_three_and_roundtrips_catalog() -> Result<()> {
        for level in [None, Some(3)] {
            let profile = CompressionProfile::new(
                TimestampCodec::DeltaOfDelta,
                ValueCodec::GorillaXor,
                BlockCodec::Zstd,
                level,
            )?;
            assert_eq!(profile.storage_signature(), [2, 1, 2, 4]);
            assert_eq!(profile.effective_block_level(), Some(3));
            let mut catalog = CatalogState::default();
            catalog.create_policy(
                StoragePolicyOptions::new("zstd", 10, 20)?
                    .with_compression(CompressionProfile::columnar(), profile)?,
            )?;
            let loaded: CatalogState = serde_json::from_slice(&serde_json::to_vec(&catalog)?)?;
            loaded.validate_loaded()?;
            assert_eq!(loaded.policies[0].cold_compression, profile);
        }
        for level in 0..=u8::MAX {
            let mut profile = CompressionProfile::zstd();
            profile.block_level = Some(level);
            assert_eq!(profile.validate().is_ok(), level == 3);
        }
        Ok(())
    }
}
