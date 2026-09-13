use std::cmp::Ordering;
use std::ops::{BitOr, BitOrAssign};

use chrono::{Datelike, Days, Duration, LocalResult, Months, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, corrupt, invalid};
use crate::query::QueryOptions;

pub const DAY_US: i64 = 86_400_000_000;
pub const HOUR_US: i64 = 3_600_000_000;
pub const MINUTE_US: i64 = 60_000_000;

/// Полный ключ ряда формата Chimaera v1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SeriesKey {
    pub space_id: u32,
    pub bucket_id: u32,
    pub key: u64,
}

impl SeriesKey {
    #[must_use]
    pub const fn new(space_id: u32, bucket_id: u32, key: u64) -> Self {
        Self {
            space_id,
            bucket_id,
            key,
        }
    }
}

/// Входная точка пакетной записи.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Point {
    pub series: SeriesKey,
    pub timestamp_us: i64,
    pub value: f64,
}

/// Одна операция атомарного пакета записи.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WriteOperation {
    /// Создать или заменить точку.
    Put(Point),
    /// Удалить видимую версию точки, если она существует.
    Delete {
        series: SeriesKey,
        timestamp_us: i64,
    },
}

impl WriteOperation {
    /// Создаёт идемпотентное удаление точки.
    #[must_use]
    pub const fn delete(series: SeriesKey, timestamp_us: i64) -> Self {
        Self::Delete {
            series,
            timestamp_us,
        }
    }

    pub(crate) const fn series(self) -> SeriesKey {
        match self {
            Self::Put(point) => point.series,
            Self::Delete { series, .. } => series,
        }
    }

    pub(crate) const fn timestamp_us(self) -> i64 {
        match self {
            Self::Put(point) => point.timestamp_us,
            Self::Delete { timestamp_us, .. } => timestamp_us,
        }
    }
}

impl From<Point> for WriteOperation {
    fn from(point: Point) -> Self {
        Self::Put(point)
    }
}

impl Point {
    /// Создаёт точку и отклоняет NaN/Infinity до записи в WAL.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку, если `value` не является конечным числом.
    pub fn new(series: SeriesKey, timestamp_us: i64, value: f64) -> Result<Self> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PointKey {
    pub series: SeriesKey,
    pub timestamp_us: i64,
}

impl Ord for PointKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.series
            .cmp(&other.series)
            .then_with(|| self.timestamp_us.cmp(&other.timestamp_us))
    }
}

impl PartialOrd for PointKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VersionedPoint {
    pub key: PointKey,
    pub commit_id: u64,
    pub ordinal: u32,
    pub value_bits: u64,
    pub is_delete: bool,
}

impl VersionedPoint {
    pub fn put_value(self) -> Result<f64> {
        if self.is_delete {
            return Err(corrupt("delete operation has no value"));
        }
        Ok(f64::from_bits(self.value_bits))
    }
}

impl PartialEq for VersionedPoint {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.commit_id == other.commit_id
            && self.ordinal == other.ordinal
            && self.value_bits == other.value_bits
            && self.is_delete == other.is_delete
    }
}

impl Eq for VersionedPoint {}

impl Ord for VersionedPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.commit_id.cmp(&other.commit_id))
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

impl PartialOrd for VersionedPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    Sync,
    GroupCommit,
    Async,
}

impl Durability {
    pub(crate) const fn wal_flag(self) -> u16 {
        match self {
            Self::Sync => 1,
            Self::GroupCommit => 2,
            Self::Async => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteReceipt {
    pub commit_id: u64,
    pub operation_count: u32,
    pub point_count: u32,
    pub durable: bool,
}

/// Единица календарной группировки.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CalendarUnit {
    Day,
    Week,
    Month,
    Quarter,
    HalfYear,
    Year,
}

/// Первый день календарной недели.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WeekStart {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl WeekStart {
    const fn days_from_monday(self) -> u32 {
        match self {
            Self::Monday => 0,
            Self::Tuesday => 1,
            Self::Wednesday => 2,
            Self::Thursday => 3,
            Self::Friday => 4,
            Self::Saturday => 5,
            Self::Sunday => 6,
        }
    }
}

/// Календарная группировка с правилами встроенной базы IANA.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CalendarGrouping {
    unit: CalendarUnit,
    timezone: Tz,
    week_start: WeekStart,
}

impl CalendarGrouping {
    /// Проверяет имя IANA и создаёт календарную группировку.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для неизвестной timezone.
    pub fn new(unit: CalendarUnit, timezone: &str, week_start: WeekStart) -> Result<Self> {
        let timezone = timezone
            .parse::<Tz>()
            .map_err(|_| invalid(format!("invalid IANA timezone: {timezone}")))?;
        Ok(Self {
            unit,
            timezone,
            week_start,
        })
    }

    #[must_use]
    pub const fn unit(self) -> CalendarUnit {
        self.unit
    }

    #[must_use]
    pub fn timezone(self) -> &'static str {
        self.timezone.name()
    }

    #[must_use]
    pub const fn week_start(self) -> WeekStart {
        self.week_start
    }

    /// Возвращает календарную корзину, содержащую timestamp.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку вне диапазона календаря или `i64` микросекунд.
    pub fn bucket(self, timestamp_us: i64) -> Result<TimeBucket> {
        let utc = chrono::DateTime::<Utc>::from_timestamp_micros(timestamp_us)
            .ok_or_else(|| invalid("timestamp is outside the supported calendar range"))?;
        let local = utc.with_timezone(&self.timezone);
        let date = local.date_naive();
        let start_date = match self.unit {
            CalendarUnit::Day => date,
            CalendarUnit::Week => {
                let current = date.weekday().num_days_from_monday();
                let delta = (current + 7 - self.week_start.days_from_monday()) % 7;
                date.checked_sub_days(Days::new(u64::from(delta)))
                    .ok_or_else(|| invalid("calendar week start is outside supported range"))?
            }
            CalendarUnit::Month => calendar_date(date.year(), date.month(), 1)?,
            CalendarUnit::Quarter => {
                let month = (date.month0() / 3) * 3 + 1;
                calendar_date(date.year(), month, 1)?
            }
            CalendarUnit::HalfYear => {
                let month = (date.month0() / 6) * 6 + 1;
                calendar_date(date.year(), month, 1)?
            }
            CalendarUnit::Year => calendar_date(date.year(), 1, 1)?,
        };
        let end_date = match self.unit {
            CalendarUnit::Day => start_date.checked_add_days(Days::new(1)),
            CalendarUnit::Week => start_date.checked_add_days(Days::new(7)),
            CalendarUnit::Month => start_date.checked_add_months(Months::new(1)),
            CalendarUnit::Quarter => start_date.checked_add_months(Months::new(3)),
            CalendarUnit::HalfYear => start_date.checked_add_months(Months::new(6)),
            CalendarUnit::Year => start_date.checked_add_months(Months::new(12)),
        }
        .ok_or_else(|| invalid("calendar bucket end is outside supported range"))?;
        let start = resolve_local_boundary(self.timezone, start_date)?;
        let end = resolve_local_boundary(self.timezone, end_date)?;
        let bucket = TimeBucket {
            start_us: start.timestamp_micros(),
            end_us: end.timestamp_micros(),
        };
        if bucket.start_us > timestamp_us
            || bucket.end_us <= timestamp_us
            || bucket.start_us >= bucket.end_us
        {
            return Err(invalid("calendar timezone produced invalid bucket bounds"));
        }
        Ok(bucket)
    }

    fn is_utc_day(self) -> bool {
        self.unit == CalendarUnit::Day && matches!(self.timezone.name(), "UTC" | "Etc/UTC")
    }
}

fn calendar_date(year: i32, month: u32, day: u32) -> Result<NaiveDate> {
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| invalid("calendar boundary is outside supported range"))
}

fn resolve_local_boundary(timezone: Tz, date: NaiveDate) -> Result<chrono::DateTime<Tz>> {
    let mut local = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| invalid("invalid local calendar boundary"))?;
    // A few IANA zones have midnight gaps or have skipped a whole civil date.
    // The first representable instant at or after the boundary is the beginning
    // of that calendar interval. The 48-hour limit covers a skipped date.
    for _ in 0..=(48 * 60) {
        match timezone.from_local_datetime(&local) {
            LocalResult::Single(value) => return Ok(value),
            LocalResult::Ambiguous(first, second) => return Ok(first.min(second)),
            LocalResult::None => {
                local = local
                    .checked_add_signed(Duration::minutes(1))
                    .ok_or_else(|| invalid("local calendar boundary overflows"))?;
            }
        }
    }
    Err(invalid("IANA timezone gap exceeds two days"))
}

/// Проверенные границы одной временной корзины.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeBucket {
    pub start_us: i64,
    pub end_us: i64,
}

/// Способ разбиения времени в потоковом запросе.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Grouping {
    /// Календарные сутки UTC, совместимые с дневной aggregate-секцией v1.
    UtcDay,
    /// Равные интервалы в микросекундах относительно заданного origin.
    Fixed { duration_us: i64, origin_us: i64 },
    /// Календарная единица в timezone IANA.
    Calendar(CalendarGrouping),
}

/// Точное разрешение одной сохраняемой сводки.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateResolution {
    Minute,
    Hour,
    Day,
}

impl AggregateResolution {
    #[must_use]
    pub const fn duration_us(self) -> i64 {
        match self {
            Self::Minute => MINUTE_US,
            Self::Hour => HOUR_US,
            Self::Day => DAY_US,
        }
    }

    pub(crate) const fn id(self) -> u16 {
        match self {
            Self::Minute => 1,
            Self::Hour => 2,
            Self::Day => 3,
        }
    }

    pub(crate) const fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(Self::Minute),
            2 => Some(Self::Hour),
            3 => Some(Self::Day),
            _ => None,
        }
    }
}

/// Набор результатов, вычисляемых одним проходом по видимым точкам.
///
/// `count` и `sum` входят в любой групповой запрос и остаются обычными полями
/// [`GroupRow`]. Остальные флаги управляют дополнительными полями строки.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AggregateSet(u16);

impl AggregateSet {
    pub const COUNT_SUM: Self = Self(0);
    pub const MIN: Self = Self(1 << 0);
    pub const MAX: Self = Self(1 << 1);
    pub const MEAN: Self = Self(1 << 2);
    pub const POPULATION_VARIANCE: Self = Self(1 << 3);
    pub const SAMPLE_VARIANCE: Self = Self(1 << 4);
    pub const POPULATION_STANDARD_DEVIATION: Self = Self(1 << 5);
    pub const SAMPLE_STANDARD_DEVIATION: Self = Self(1 << 6);
    pub const FIRST: Self = Self(1 << 7);
    pub const LAST: Self = Self(1 << 8);
    pub const DELTA: Self = Self(1 << 9);
    pub const RATE: Self = Self(1 << 10);
    pub const ALL: Self = Self((1 << 11) - 1);

    #[must_use]
    pub const fn contains(self, aggregate: Self) -> bool {
        self.0 & aggregate.0 == aggregate.0
    }

    pub(crate) const fn bits(self) -> u16 {
        self.0
    }

    pub(crate) const fn from_bits(bits: u16) -> Option<Self> {
        if bits & !Self::ALL.0 == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }
}

impl Default for AggregateSet {
    fn default() -> Self {
        Self::COUNT_SUM
    }
}

impl BitOr for AggregateSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for AggregateSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Значение на определённой отметке времени для `first` и `last`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregatePoint {
    pub timestamp_us: i64,
    pub value: f64,
}

/// Источник данных для группового запроса.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupSource {
    /// Использовать точные сохранённые сводки, когда они полностью покрывают запрос.
    #[default]
    Auto,
    /// Всегда читать исходные MVCC-операции.
    Raw,
}

impl Grouping {
    pub(crate) fn validate(self) -> Result<()> {
        if let Self::Fixed { duration_us, .. } = self
            && duration_us <= 0
        {
            return Err(invalid("fixed grouping duration must be positive"));
        }
        Ok(())
    }

    /// Возвращает временную корзину, содержащую timestamp.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку при переполнении границ или календаря.
    pub fn bucket(self, timestamp_us: i64) -> Result<TimeBucket> {
        self.validate()?;
        if let Self::Calendar(calendar) = self {
            return calendar.bucket(timestamp_us);
        }
        let (start_us, end_us) = self.fixed_bucket_bounds(timestamp_us)?;
        Ok(TimeBucket { start_us, end_us })
    }

    pub(crate) fn bucket_bounds(self, timestamp_us: i64) -> Result<(i64, i64)> {
        let bucket = self.bucket(timestamp_us)?;
        Ok((bucket.start_us, bucket.end_us))
    }

    fn fixed_bucket_bounds(self, timestamp_us: i64) -> Result<(i64, i64)> {
        let (duration_us, origin_us) = match self {
            Self::UtcDay => (DAY_US, 0),
            Self::Fixed {
                duration_us,
                origin_us,
            } => (duration_us, origin_us),
            Self::Calendar(_) => unreachable!("calendar handled before fixed grouping"),
        };
        let duration = i128::from(duration_us);
        let origin = i128::from(origin_us);
        let timestamp = i128::from(timestamp_us);
        let start = origin + (timestamp - origin).div_euclid(duration) * duration;
        let end = start + duration;
        Ok((
            i64::try_from(start).map_err(|_| invalid("group bucket start overflows i64"))?,
            i64::try_from(end).map_err(|_| invalid("group bucket end overflows i64"))?,
        ))
    }

    pub(crate) fn is_utc_day(self) -> bool {
        match self {
            Self::UtcDay => true,
            Self::Calendar(calendar) => calendar.is_utc_day(),
            Self::Fixed { .. } => false,
        }
    }

    pub(crate) fn aggregate_resolution(self) -> Option<AggregateResolution> {
        match self {
            Self::UtcDay => Some(AggregateResolution::Day),
            Self::Calendar(calendar) if calendar.is_utc_day() => Some(AggregateResolution::Day),
            Self::Fixed {
                duration_us,
                origin_us,
            } if duration_us > 0 => {
                if duration_us % DAY_US == 0 && origin_us.rem_euclid(DAY_US) == 0 {
                    Some(AggregateResolution::Day)
                } else if duration_us % HOUR_US == 0 && origin_us.rem_euclid(HOUR_US) == 0 {
                    Some(AggregateResolution::Hour)
                } else if duration_us % MINUTE_US == 0 && origin_us.rem_euclid(MINUTE_US) == 0 {
                    Some(AggregateResolution::Minute)
                } else {
                    None
                }
            }
            Self::Calendar(_) | Self::Fixed { .. } => None,
        }
    }
}

/// Потоковый запрос с полуоткрытым диапазоном `[from_us, to_us)`.
#[derive(Clone, Debug)]
pub struct GroupRequest {
    pub series: Vec<SeriesKey>,
    pub from_us: i64,
    pub to_us: i64,
    pub scope: GroupScope,
    pub grouping: Grouping,
    pub aggregates: AggregateSet,
    pub source: GroupSource,
    pub options: QueryOptions,
}

/// Область агрегации нескольких рядов.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupScope {
    /// Возвращает независимый поток корзин для каждого ряда.
    #[default]
    PerKey,
    /// Объединяет значения всех рядов в один поток временных корзин.
    Combined,
}

impl GroupRequest {
    /// Проверяет границы и набор рядов.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для пустого набора, повторов или пустого диапазона.
    pub fn new(series: Vec<SeriesKey>, from_us: i64, to_us: i64) -> Result<Self> {
        let request = Self {
            series,
            from_us,
            to_us,
            scope: GroupScope::PerKey,
            grouping: Grouping::UtcDay,
            aggregates: AggregateSet::default(),
            source: GroupSource::Auto,
            options: QueryOptions::default(),
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.options.validate()?;
        if self.series.is_empty() {
            return Err(invalid("query needs at least one series"));
        }
        if self.from_us >= self.to_us {
            return Err(invalid("query range must be non-empty"));
        }
        if self.series.len() > self.options.max_series {
            return Err(Error::ResourceLimit("query series limit exceeded".into()));
        }
        let mut ordered = self.series.clone();
        ordered.sort_unstable();
        let first = ordered[0];
        if ordered
            .iter()
            .any(|series| series.space_id != first.space_id || series.bucket_id != first.bucket_id)
        {
            return Err(invalid("query series must belong to one space and bucket"));
        }
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("query contains duplicate series"));
        }
        self.grouping.validate()
    }

    /// Создаёт запрос с равными корзинами `duration_us` относительно `origin_us`.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для неположительной длительности и неверного диапазона.
    pub fn fixed(
        series: Vec<SeriesKey>,
        from_us: i64,
        to_us: i64,
        duration_us: i64,
        origin_us: i64,
    ) -> Result<Self> {
        let mut request = Self::new(series, from_us, to_us)?;
        request.grouping = Grouping::Fixed {
            duration_us,
            origin_us,
        };
        request.validate()?;
        Ok(request)
    }

    /// Создаёт календарный запрос в timezone IANA.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для неизвестной timezone или неверного запроса.
    pub fn calendar(
        series: Vec<SeriesKey>,
        from_us: i64,
        to_us: i64,
        unit: CalendarUnit,
        timezone: &str,
        week_start: WeekStart,
    ) -> Result<Self> {
        let mut request = Self::new(series, from_us, to_us)?;
        request.grouping = Grouping::Calendar(CalendarGrouping::new(unit, timezone, week_start)?);
        request.validate()?;
        Ok(request)
    }

    /// Создаёт точную агрегацию с числом строк порядка `target_rows`.
    ///
    /// Длительность округляется вверх до целой минуты, часа или суток. Если
    /// подходящая сохранённая сводка отсутствует, запрос остаётся точным и читает
    /// исходные точки.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку для нулевого числа строк или переполнения интервала.
    pub fn downsampled(
        series: Vec<SeriesKey>,
        from_us: i64,
        to_us: i64,
        target_rows: usize,
    ) -> Result<Self> {
        if target_rows == 0 {
            return Err(invalid("target row count must be positive"));
        }
        let span = i128::from(to_us) - i128::from(from_us);
        if span <= 0 {
            return Err(invalid("query range must be non-empty"));
        }
        let target =
            i128::try_from(target_rows).map_err(|_| invalid("target row count exceeds i128"))?;
        let desired = (span + target - 1) / target;
        let quantum = if desired <= i128::from(HOUR_US) {
            MINUTE_US
        } else if desired <= i128::from(DAY_US) {
            HOUR_US
        } else {
            DAY_US
        };
        let quantum_i128 = i128::from(quantum);
        let duration = ((desired + quantum_i128 - 1) / quantum_i128) * quantum_i128;
        let duration_us = i64::try_from(duration)
            .map_err(|_| invalid("downsampled bucket duration overflows i64"))?;
        Self::fixed(series, from_us, to_us, duration_us, 0)
    }

    /// Устанавливает область агрегации.
    #[must_use]
    pub const fn with_scope(mut self, scope: GroupScope) -> Self {
        self.scope = scope;
        self
    }

    /// Выбирает дополнительные результаты, вычисляемые в том же проходе.
    #[must_use]
    pub const fn with_aggregates(mut self, aggregates: AggregateSet) -> Self {
        self.aggregates = aggregates;
        self
    }

    /// Требует исходные точки либо разрешает планировщику выбрать сводку.
    #[must_use]
    pub const fn with_source(mut self, source: GroupSource) -> Self {
        self.source = source;
        self
    }

    /// Устанавливает deadline, cancellation token и пределы ресурсов.
    ///
    /// # Errors
    ///
    /// Возвращает ошибку, если пределы не положительны или уже превышены запросом.
    pub fn with_options(mut self, options: QueryOptions) -> Result<Self> {
        self.options = options;
        self.validate()?;
        Ok(self)
    }
}

/// Одна непустая временная корзина.
#[derive(Clone, Copy, Debug)]
pub struct GroupRow {
    /// Ключ ряда для `PerKey`; отсутствует для `Combined`.
    pub series: Option<SeriesKey>,
    pub bucket_start_us: i64,
    pub bucket_end_us: i64,
    /// Фактическая ширина этой строки; календарные строки могут различаться.
    pub resolution_us: i64,
    /// Разрешение сохранённой сводки, если строка получена без чтения исходных точек.
    pub source_resolution: Option<AggregateResolution>,
    pub sum: f64,
    pub count: u64,
    pub aggregates: AggregateSet,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub population_variance: Option<f64>,
    pub sample_variance: Option<f64>,
    pub population_standard_deviation: Option<f64>,
    pub sample_standard_deviation: Option<f64>,
    pub first: Option<AggregatePoint>,
    pub last: Option<AggregatePoint>,
    pub delta: Option<f64>,
    /// Изменение значения за секунду между первой и последней точкой.
    pub rate_per_second: Option<f64>,
    state: AggregateState,
}

impl PartialEq for GroupRow {
    fn eq(&self, other: &Self) -> bool {
        self.series == other.series
            && self.bucket_start_us == other.bucket_start_us
            && self.bucket_end_us == other.bucket_end_us
            && self.resolution_us == other.resolution_us
            && self.sum == other.sum
            && self.count == other.count
            && self.aggregates == other.aggregates
            && self.min == other.min
            && self.max == other.max
            && self.mean == other.mean
            && self.population_variance == other.population_variance
            && self.sample_variance == other.sample_variance
            && self.population_standard_deviation == other.population_standard_deviation
            && self.sample_standard_deviation == other.sample_standard_deviation
            && self.first == other.first
            && self.last == other.last
            && self.delta == other.delta
            && self.rate_per_second == other.rate_per_second
    }
}

impl GroupRow {
    pub(crate) fn from_state(
        series: Option<SeriesKey>,
        bucket_start_us: i64,
        bucket_end_us: i64,
        state: AggregateState,
        aggregates: AggregateSet,
    ) -> Result<Self> {
        state.validate()?;
        if !state.available.contains(aggregates) {
            return Err(corrupt("requested aggregate state is unavailable"));
        }
        let sum = state.total()?;
        let needs_population_variance = aggregates.contains(AggregateSet::POPULATION_VARIANCE)
            || aggregates.contains(AggregateSet::POPULATION_STANDARD_DEVIATION);
        let needs_sample_variance = aggregates.contains(AggregateSet::SAMPLE_VARIANCE)
            || aggregates.contains(AggregateSet::SAMPLE_STANDARD_DEVIATION);
        let population_variance = needs_population_variance
            .then(|| state.population_variance())
            .transpose()?;
        let sample_variance = needs_sample_variance
            .then(|| state.sample_variance())
            .transpose()?
            .flatten();
        let first = state.first;
        let last = state.last;
        let delta = first
            .zip(last)
            .map(|(first, last)| last.value - first.value);
        let rate_per_second = first.zip(last).and_then(|(first, last)| {
            let elapsed_us = i128::from(last.timestamp_us) - i128::from(first.timestamp_us);
            (elapsed_us > 0).then(|| (last.value - first.value) * 1_000_000.0 / elapsed_us as f64)
        });
        let resolution_us = bucket_end_us
            .checked_sub(bucket_start_us)
            .filter(|duration| *duration > 0)
            .ok_or_else(|| corrupt("group bucket width is not positive"))?;
        Ok(Self {
            series,
            bucket_start_us,
            bucket_end_us,
            resolution_us,
            source_resolution: None,
            sum,
            count: state.count,
            aggregates,
            min: aggregates.contains(AggregateSet::MIN).then_some(state.min),
            max: aggregates.contains(AggregateSet::MAX).then_some(state.max),
            mean: aggregates
                .contains(AggregateSet::MEAN)
                .then_some(state.mean),
            population_variance: aggregates
                .contains(AggregateSet::POPULATION_VARIANCE)
                .then_some(population_variance)
                .flatten(),
            sample_variance: aggregates
                .contains(AggregateSet::SAMPLE_VARIANCE)
                .then_some(sample_variance)
                .flatten(),
            population_standard_deviation: aggregates
                .contains(AggregateSet::POPULATION_STANDARD_DEVIATION)
                .then(|| population_variance.map(f64::sqrt))
                .flatten(),
            sample_standard_deviation: aggregates
                .contains(AggregateSet::SAMPLE_STANDARD_DEVIATION)
                .then_some(sample_variance.map(f64::sqrt))
                .flatten(),
            first: aggregates
                .contains(AggregateSet::FIRST)
                .then_some(first)
                .flatten(),
            last: aggregates
                .contains(AggregateSet::LAST)
                .then_some(last)
                .flatten(),
            delta: aggregates
                .contains(AggregateSet::DELTA)
                .then_some(delta)
                .flatten(),
            rate_per_second: aggregates
                .contains(AggregateSet::RATE)
                .then_some(rate_per_second)
                .flatten(),
            state,
        })
    }

    pub(crate) const fn state(self) -> AggregateState {
        self.state
    }

    pub(crate) const fn with_source_resolution(mut self, resolution: AggregateResolution) -> Self {
        self.source_resolution = Some(resolution);
        self
    }
}

pub(crate) fn utc_day_start(timestamp_us: i64) -> Result<i64> {
    Grouping::UtcDay
        .bucket_bounds(timestamp_us)
        .map(|(start, _)| start)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NeumaierSum {
    sum: f64,
    compensation: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateState {
    pub count: u64,
    pub sum: NeumaierSum,
    pub mean: f64,
    pub m2: f64,
    pub min: f64,
    pub max: f64,
    pub first: Option<AggregatePoint>,
    pub last: Option<AggregatePoint>,
    pub available: AggregateSet,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RemovalSafety {
    pub extrema: bool,
    pub edges: bool,
}

impl Default for AggregateState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: NeumaierSum::default(),
            mean: 0.0,
            m2: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
            available: AggregateSet::ALL,
        }
    }
}

impl AggregateState {
    pub(crate) fn for_aggregates(aggregates: AggregateSet) -> Self {
        Self {
            available: if aggregates == AggregateSet::COUNT_SUM {
                AggregateSet::COUNT_SUM
            } else {
                AggregateSet::ALL
            },
            ..Self::default()
        }
    }

    pub(crate) fn from_count_sum_parts(
        count: u64,
        raw_sum: f64,
        compensation: f64,
    ) -> Result<Self> {
        let state = Self {
            count,
            sum: NeumaierSum {
                sum: raw_sum,
                compensation,
            },
            available: AggregateSet::COUNT_SUM,
            ..Self::default()
        };
        if count == 0 {
            if raw_sum == 0.0 && compensation == 0.0 {
                return Ok(state);
            }
            return Err(corrupt("empty count/sum state has a non-zero sum"));
        }
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn count_sum_parts(self) -> (u64, f64, f64) {
        let (raw_sum, compensation) = self.sum.components();
        (self.count, raw_sum, compensation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_persisted(
        raw_sum: f64,
        compensation: f64,
        count: u64,
        mean: f64,
        m2: f64,
        min: f64,
        max: f64,
        first: AggregatePoint,
        last: AggregatePoint,
        available: AggregateSet,
    ) -> Result<Self> {
        let state = Self {
            count,
            sum: NeumaierSum {
                sum: raw_sum,
                compensation,
            },
            mean,
            m2,
            min,
            max,
            first: Some(first),
            last: Some(last),
            available,
        };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn add(&mut self, timestamp_us: i64, value: f64) -> Result<()> {
        if !value.is_finite() {
            return Err(corrupt("aggregate input is not finite"));
        }
        let next_count = self
            .count
            .checked_add(1)
            .ok_or_else(|| corrupt("aggregate count overflow"))?;
        self.sum.add(value);
        if self.available == AggregateSet::COUNT_SUM {
            self.count = next_count;
            self.total()?;
            return Ok(());
        }
        if self.count == 0 {
            self.mean = value;
            self.m2 = 0.0;
            self.min = value;
            self.max = value;
            let point = AggregatePoint {
                timestamp_us,
                value,
            };
            self.first = Some(point);
            self.last = Some(point);
        } else {
            let delta = value - self.mean;
            self.mean += delta / next_count as f64;
            let delta_after = value - self.mean;
            self.m2 += delta * delta_after;
            self.min = self.min.min(value);
            self.max = self.max.max(value);
            if self
                .first
                .is_none_or(|point| timestamp_us < point.timestamp_us)
            {
                self.first = Some(AggregatePoint {
                    timestamp_us,
                    value,
                });
            }
            if self
                .last
                .is_none_or(|point| timestamp_us > point.timestamp_us)
            {
                self.last = Some(AggregatePoint {
                    timestamp_us,
                    value,
                });
            }
        }
        self.count = next_count;
        if !self.mean.is_finite() || !self.m2.is_finite() {
            return Err(corrupt("aggregate numeric state is not finite"));
        }
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other;
            return Ok(());
        }
        let combined = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| corrupt("aggregate count overflow"))?;
        self.sum
            .merge_components(other.sum.sum, other.sum.compensation);
        let delta = other.mean - self.mean;
        self.m2 +=
            other.m2 + delta * delta * self.count as f64 * other.count as f64 / combined as f64;
        self.mean += delta * other.count as f64 / combined as f64;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        if other
            .first
            .zip(self.first)
            .is_some_and(|(other, current)| other.timestamp_us < current.timestamp_us)
        {
            self.first = other.first;
        }
        if other
            .last
            .zip(self.last)
            .is_some_and(|(other, current)| other.timestamp_us > current.timestamp_us)
        {
            self.last = other.last;
        }
        self.available = AggregateSet(self.available.0 & other.available.0);
        self.count = combined;
        self.validate()
    }

    pub(crate) fn remove(&mut self, timestamp_us: i64, value: f64) -> Result<RemovalSafety> {
        if self.count == 0 {
            return Err(corrupt("aggregate removal underflow"));
        }
        let safety = RemovalSafety {
            extrema: value != self.min && value != self.max,
            edges: self
                .first
                .is_some_and(|point| point.timestamp_us != timestamp_us)
                && self
                    .last
                    .is_some_and(|point| point.timestamp_us != timestamp_us),
        };
        self.sum.add(-value);
        if self.count == 1 {
            *self = Self::default();
            return Ok(safety);
        }
        let next_count = self.count - 1;
        let next_mean = (self.mean * self.count as f64 - value) / next_count as f64;
        self.m2 -= (value - self.mean) * (value - next_mean);
        if self.m2 < 0.0 && self.m2 > -f64::EPSILON * self.mean.abs().max(1.0).powi(2) {
            self.m2 = 0.0;
        }
        self.mean = next_mean;
        self.count = next_count;
        self.validate()?;
        Ok(safety)
    }

    pub(crate) fn total(self) -> Result<f64> {
        let (raw_sum, compensation) = self.sum.components();
        let total = raw_sum + compensation;
        if !total.is_finite() {
            return Err(corrupt("aggregate sum is not finite"));
        }
        Ok(total)
    }

    pub(crate) fn population_variance(self) -> Result<f64> {
        if self.count == 0 {
            return Err(corrupt("aggregate count must be positive"));
        }
        let value = (self.m2 / self.count as f64).max(0.0);
        if !value.is_finite() {
            return Err(corrupt("aggregate variance is not finite"));
        }
        Ok(value)
    }

    pub(crate) fn sample_variance(self) -> Result<Option<f64>> {
        if self.count < 2 {
            return Ok(None);
        }
        let value = (self.m2 / (self.count - 1) as f64).max(0.0);
        if !value.is_finite() {
            return Err(corrupt("aggregate sample variance is not finite"));
        }
        Ok(Some(value))
    }

    pub(crate) fn validate(self) -> Result<()> {
        if self.count == 0 {
            return Err(corrupt("aggregate count must be positive"));
        }
        self.total()?;
        if self.available != AggregateSet::COUNT_SUM
            && (!self.mean.is_finite()
                || !self.m2.is_finite()
                || self.m2 < 0.0
                || !self.min.is_finite()
                || !self.max.is_finite()
                || self.min > self.max
                || self.first.is_none()
                || self.last.is_none())
        {
            return Err(corrupt("invalid extended aggregate state"));
        }
        Ok(())
    }
}

impl NeumaierSum {
    pub fn add(&mut self, value: f64) {
        let next = self.sum + value;
        if self.sum.abs() >= value.abs() {
            self.compensation += (self.sum - next) + value;
        } else {
            self.compensation += (value - next) + self.sum;
        }
        self.sum = next;
    }

    pub fn merge_components(&mut self, raw_sum: f64, compensation: f64) {
        self.add(raw_sum);
        self.add(compensation);
    }

    #[must_use]
    pub const fn components(self) -> (f64, f64) {
        (self.sum, self.compensation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn fixed_grouping_rejects_non_positive_duration_in_public_bucket_api() {
        for duration_us in [0, -1] {
            let grouping = Grouping::Fixed {
                duration_us,
                origin_us: 0,
            };
            assert!(matches!(grouping.bucket(1), Err(Error::Invalid(_))));
        }
    }

    proptest! {
        #[test]
        fn streaming_state_matches_two_pass_reference(values in proptest::collection::vec(-1.0e6_f64..1.0e6_f64, 1..512)) {
            let mut state = AggregateState::default();
            for (index, value) in values.iter().copied().enumerate() {
                state.add(index as i64 - 256, value)?;
            }
            let count = values.len() as f64;
            let mean = values.iter().sum::<f64>() / count;
            let m2 = values.iter().map(|value| (value - mean).powi(2)).sum::<f64>();
            let tolerance = 1.0e-8_f64.max(m2.abs() * 1.0e-10);
            prop_assert_eq!(state.count, values.len() as u64);
            prop_assert_eq!(state.min, values.iter().copied().fold(f64::INFINITY, f64::min));
            prop_assert_eq!(state.max, values.iter().copied().fold(f64::NEG_INFINITY, f64::max));
            prop_assert!((state.m2 - m2).abs() <= tolerance);
        }
    }
}
