//! Typed catalogue records. Time and physical bounds are stored exactly.
use super::{Key, Record, Snapshot};
use crate::aggregate::archive::AggregateArchiveDescriptor;
use crate::error::{Result, corrupt};
use crate::persistence::layout::Domain;
use crate::query::QueryBudget;
use crate::segment::SegmentDescriptor;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

pub(crate) const SEGMENT: u8 = 1;
pub(crate) const DIRECTORY: u8 = 2;
pub(crate) const ARCHIVE: u8 = 3;
pub(crate) const DOMAIN: u8 = 4;
pub(crate) const PENDING: u8 = 5;
pub(crate) const ID: u8 = 6;
pub(crate) const PACK: u8 = 7;
pub(crate) const SINGLE: u8 = 8;
pub(crate) const RETIRED: u8 = 9;
pub(crate) const BLOCK: u8 = 10;
pub(crate) const REPACK: u8 = 11;
pub(crate) const PACK_MEMBER: u8 = 12;

pub(crate) const fn scope(kind: u8, policy: u32, shard: u16) -> u64 {
    ((kind as u64) << 56) | ((policy as u64) << 16) | shard as u64
}
pub(crate) const fn kind_scopes(kind: u8) -> (u64, u64) {
    ((kind as u64) << 56, (((kind as u64) + 1) << 56) - 1)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SegmentEntry {
    pub descriptor: SegmentDescriptor,
    /// Wall-clock milliseconds; remains meaningful after reopen.
    pub published_ms: u64,
    pub l1_eligible_since_ms: Option<u64>,
    pub directory_id: Option<u64>,
    /// Block addresses are in this same catalogue snapshot, without a file generation.
    pub catalog_indexed: bool,
}
impl SegmentEntry {
    pub fn key(&self) -> Key {
        segment_key(&self.descriptor)
    }
    pub fn end(&self) -> Result<i64> {
        self.descriptor
            .max_timestamp_us
            .checked_add(1)
            .ok_or_else(|| corrupt("catalog segment time bound overflows"))
    }
    pub fn record(&self) -> Result<Record> {
        let mut record = encode(self.key(), self.end()?, self)?;
        record.summary = Some(self.summary()?);
        Ok(record)
    }
    pub fn summary(&self) -> Result<super::Summary> {
        let d = &self.descriptor;
        let level = usize::from(d.level);
        if level > 2 {
            return Err(corrupt("catalog segment level"));
        }
        let mut value = super::Summary::default();
        value.counts[level] = 1;
        value.bytes[level] = d.file_size;
        value.minimum[level] = d.min_timestamp_us;
        value.maximum[level] = d.max_timestamp_us;
        value.rewrites[level] = d.rewrite_count;
        if level < 2 {
            value.oldest[level] = self.published_ms;
        } else {
            value.minimum_l2_bytes = d.file_size;
        }
        Ok(value)
    }
}
pub(crate) fn segment_key(d: &SegmentDescriptor) -> Key {
    Key {
        series: Default::default(),
        scope: scope(SEGMENT, d.policy_id, d.shard_id),
        start: d.min_timestamp_us,
        id: d.id,
    }
}

pub(crate) fn archive_key(d: &AggregateArchiveDescriptor) -> Key {
    Key {
        series: Default::default(),
        scope: scope(ARCHIVE, d.policy_id, d.shard_id),
        start: d.coverage_from_us,
        id: d.id,
    }
}
pub(crate) fn domain_key(kind: u8, d: Domain) -> Key {
    Key {
        series: Default::default(),
        scope: scope(kind, d.policy_id, d.shard_id),
        start: d.window_start_us,
        id: d.window_end_us as u64,
    }
}
pub(crate) fn id_key(id: u64) -> Key {
    Key {
        series: Default::default(),
        scope: scope(ID, 0, 0),
        start: 0,
        id,
    }
}
pub(crate) fn object_id_key(kind: u8, id: u64) -> Key {
    Key {
        series: Default::default(),
        scope: scope(ID, u32::from(kind), 0),
        start: 0,
        id,
    }
}
pub(crate) fn pack_key(d: &SegmentDescriptor) -> Key {
    Key {
        series: Default::default(),
        scope: scope(if d.pack_id.is_some() { PACK } else { SINGLE }, 0, 0),
        start: 0,
        id: d.pack_id.unwrap_or(d.id),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DomainEntry {
    pub counts: [u64; 3],
    pub bytes: [u64; 3],
    pub minimum_timestamp: i64,
    pub maximum_timestamp: i64,
    pub max_rewrites: [u32; 3],
    pub oldest_l0_ms: Option<u64>,
    pub oldest_l1_ms: Option<u64>,
    /// Bounds of L0/L1 only. L2 history must not force a debt scan.
    pub debt_bounds: [Option<DebtBounds>; 2],
    pub minimum_l2_bytes: Option<u64>,
    /// Pure L2 has a geometric candidate when all timestamps are sealed.
    /// False is independent of future seal-boundary advances.
    pub history_merge: bool,
    pub history_target_bytes: u64,
    pub history_trigger: usize,
    pub history_window_us: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DebtBounds {
    pub minimum_timestamp: i64,
    pub maximum_timestamp: i64,
}

impl DomainEntry {
    pub fn pending(&self) -> bool {
        self.counts[0] > 0 || self.counts[1] > 0 || self.history_merge
    }
    pub fn from_summary(s: &super::Summary, target: u64, trigger: usize) -> Self {
        Self {
            counts: s.counts,
            bytes: s.bytes,
            minimum_timestamp: *s.minimum.iter().min().expect("three levels"),
            maximum_timestamp: *s.maximum.iter().max().expect("three levels"),
            max_rewrites: s.rewrites,
            oldest_l0_ms: (s.counts[0] > 0).then_some(s.oldest[0]),
            oldest_l1_ms: (s.counts[1] > 0).then_some(s.oldest[1]),
            debt_bounds: std::array::from_fn(|i| {
                (s.counts[i] > 0).then_some(DebtBounds {
                    minimum_timestamp: s.minimum[i],
                    maximum_timestamp: s.maximum[i],
                })
            }),
            minimum_l2_bytes: (s.counts[2] > 0).then_some(s.minimum_l2_bytes),
            history_target_bytes: target,
            history_trigger: trigger,
            history_window_us: 0,
            history_merge: false,
        }
    }
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PackEntry {
    pub relative_path: String,
    pub references: u64,
    pub file_size: u64,
    pub live_bytes: u64,
    pub repack_since_ms: Option<u64>,
    /// A series deletion must not wait for the retention space-saving threshold.
    #[serde(default)]
    pub force_repack: bool,
}

impl PackEntry {
    pub fn needs_repack(&self) -> bool {
        self.references > 0
            && (self.force_repack
                || (self.live_bytes <= self.file_size / 4
                    && self.file_size.saturating_sub(self.live_bytes) >= 1024 * 1024))
    }
    pub fn candidate_key(&self, id: u64) -> Result<Option<Key>> {
        self.repack_since_ms
            .map(|since| {
                Ok(Key {
                    scope: scope(REPACK, 0, 0),
                    series: Default::default(),
                    start: i64::try_from(since).map_err(|_| corrupt("repack age overflow"))?,
                    id,
                })
            })
            .transpose()
    }
}

pub(crate) fn member_key(pack_id: u64, segment_id: u64) -> Key {
    Key {
        scope: scope(PACK_MEMBER, 0, 0),
        series: crate::types::SeriesKey::new(0, 0, pack_id),
        start: 0,
        id: segment_id,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RetiredEntry {
    pub retired_ms: u64,
    pub relative_path: String,
    pub file_size: u64,
    pub generation: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Totals {
    pub last_publication_ms: u64,
    pub segments: [u64; 3],
    pub payload_bytes: [u64; 3],
    pub physical_files: u64,
    pub physical_bytes: u64,
    pub directories: u64,
    pub directory_bytes: u64,
    pub directory_segments: u64,
    pub archives: u64,
    pub archive_bytes: u64,
    pub retired_files: u64,
    pub retired_bytes: u64,
    pub oldest_retired_ms: Option<u64>,
    pub repack_files: u64,
    pub repack_live_bytes: u64,
    pub repack_dead_bytes: u64,
}

pub(crate) fn encode(key: Key, end: i64, value: &impl Serialize) -> Result<Record> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > super::page::MAX_RECORD_BYTES || end <= key.start {
        return Err(corrupt("catalog record exceeds its size or time bounds"));
    }
    Ok(Record {
        key,
        end,
        bytes,
        summary: None,
    })
}

pub(crate) fn decode<T: DeserializeOwned>(record: &Record) -> Result<T> {
    Ok(serde_json::from_slice(&record.bytes)?)
}

pub(crate) fn segment(record: &Record) -> Result<SegmentEntry> {
    let entry: SegmentEntry = decode(record)?;
    if entry.key() != record.key
        || entry.end()? != record.end
        || record.summary != Some(entry.summary()?)
    {
        return Err(corrupt("catalog segment key/bounds/summary"));
    }
    Ok(entry)
}

impl Snapshot {
    pub(crate) fn segment(
        &self,
        id: u64,
        budget: Option<&Arc<QueryBudget>>,
    ) -> Result<Option<SegmentEntry>> {
        let Some(index) = self.get(id_key(id), budget)? else {
            return Ok(None);
        };
        let key: Key = decode(&index)?;
        if key.id != id || key.scope >> 56 != u64::from(SEGMENT) {
            return Err(corrupt("catalog segment id index"));
        }
        let row = self
            .get(key, budget)?
            .ok_or_else(|| corrupt("catalog dangling segment id"))?;
        let entry = segment(&row)?;
        Ok(Some(entry))
    }
}

pub(crate) fn domain_from_key(key: Key) -> Result<Domain> {
    let kind = key.scope >> 56;
    if kind != u64::from(DOMAIN) && kind != u64::from(PENDING) {
        return Err(corrupt("expected domain key"));
    }
    let domain = Domain {
        policy_id: ((key.scope >> 16) & u64::from(u32::MAX)) as u32,
        shard_id: key.scope as u16,
        window_start_us: key.start,
        window_end_us: key.id as i64,
    };
    if domain.window_start_us >= domain.window_end_us {
        return Err(corrupt("domain bounds"));
    }
    Ok(domain)
}
