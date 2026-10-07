pub(crate) mod archive;

use std::sync::Arc;

use crate::error::{Error, Result, corrupt, invalid};
use crate::memory::cache::MemoryReservation;
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use crate::persistence::layout::Domain;
use crate::persistence::wal::{
    put_i64, put_u16, put_u32, put_u64, read_i64, read_u16, read_u32, read_u64,
};
use crate::types::{
    AggregatePoint, AggregateResolution, AggregateSet, AggregateState, SeriesKey, VersionedPoint,
};

const LEGACY_MAGIC: &[u8; 8] = b"TSAGG002";
const MAGIC: &[u8; 8] = b"TSAGG003";
const VERSION: u16 = 3;
const LEGACY_VERSION: u16 = 2;
const STATE_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const LEGACY_DIRECTORY_SIZE: usize = 32;
const DIRECTORY_SIZE: usize = 40;
pub(crate) const RECORD_SIZE: usize = 112;
pub(crate) const MIN_SECTION_SIZE: usize = HEADER_SIZE + DIRECTORY_SIZE + RECORD_SIZE;
pub(crate) const MAX_BYTES: usize = 64 * 1024 * 1024;
const ESTIMATED_COMPRESSED_POINT_BYTES: usize = 8;
const MIN_AGGREGATE_SAVINGS_RATIO: usize = 2;
const RESOLUTIONS: [AggregateResolution; 3] = [
    AggregateResolution::Minute,
    AggregateResolution::Hour,
    AggregateResolution::Day,
];

/// Validated aggregate bytes kept together with their memory reservation.
#[derive(Debug)]
pub(crate) struct Section {
    bytes: Box<[u8]>,
    reservation: Option<Arc<MemoryReservation>>,
}

impl Section {
    pub(crate) fn new(bytes: Option<Vec<u8>>) -> Self {
        Self {
            bytes: bytes.unwrap_or_default().into_boxed_slice(),
            reservation: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn cache_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64).saturating_add(self.bytes.len() as u64)
    }

    pub(crate) fn set_reservation(&mut self, reservation: Arc<MemoryReservation>) {
        self.reservation = Some(reservation);
    }
}

pub(crate) fn checksum_bytes(bytes: &[u8]) -> Result<&[u8]> {
    let layout = counts(bytes)?;
    if layout.directory_size == DIRECTORY_SIZE {
        let root_length = HEADER_SIZE
            .checked_add(layout.entry_count.saturating_mul(DIRECTORY_SIZE))
            .ok_or_else(|| corrupt("aggregate root length overflow"))?;
        bytes
            .get(..root_length)
            .ok_or_else(|| corrupt("aggregate root is truncated"))
    } else {
        Ok(bytes)
    }
}

/// Copy surviving pages without recomputing floating-point aggregate states.
/// The caller validates the section and reserves both input and output bytes.
pub(crate) fn without_deleted_series(
    bytes: &[u8],
    deletions: &crate::deletion::Deletions,
    epoch: u64,
) -> Result<Option<EncodedChunk>> {
    let layout = counts(bytes)?;
    if layout.directory_size != DIRECTORY_SIZE {
        return Err(corrupt("unsupported archive page layout"));
    }
    let mut entries = 0;
    let mut records = 0;
    for index in 0..layout.entry_count {
        let (series, _, _, count, _) = directory(bytes, index)?;
        if deletions.cutoff(series) <= epoch {
            entries += 1;
            records += count;
        }
    }
    if entries == 0 {
        return Ok(None);
    }
    let mut output = vec![0; HEADER_SIZE + entries * DIRECTORY_SIZE + records * RECORD_SIZE];
    output[..HEADER_SIZE].copy_from_slice(&bytes[..HEADER_SIZE]);
    put_u32(&mut output, 16, entries as u32);
    put_u32(&mut output, 20, records as u32);
    let mut next_entry = 0;
    let mut next_record = 0;
    let mut minimum = None;
    let mut maximum = None;
    for index in 0..layout.entry_count {
        let (series, _, first, count, _) = directory(bytes, index)?;
        if deletions.cutoff(series) > epoch {
            continue;
        }
        minimum.get_or_insert(series);
        maximum = Some(series);
        let from = HEADER_SIZE + index * DIRECTORY_SIZE;
        let to = HEADER_SIZE + next_entry * DIRECTORY_SIZE;
        output[to..to + DIRECTORY_SIZE].copy_from_slice(&bytes[from..from + DIRECTORY_SIZE]);
        put_u32(&mut output, to + 24, next_record as u32);
        let from = HEADER_SIZE + layout.entry_count * DIRECTORY_SIZE + first * RECORD_SIZE;
        let to = HEADER_SIZE + entries * DIRECTORY_SIZE + next_record * RECORD_SIZE;
        output[to..to + count * RECORD_SIZE]
            .copy_from_slice(&bytes[from..from + count * RECORD_SIZE]);
        next_entry += 1;
        next_record += count;
    }
    Ok(Some(EncodedChunk {
        bytes: output,
        minimum_series: minimum.ok_or_else(|| corrupt("empty surviving archive"))?,
        maximum_series: maximum.ok_or_else(|| corrupt("empty surviving archive"))?,
    }))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregatePage {
    pub offset: u64,
    pub length: u32,
    pub crc32c: u32,
}

pub(crate) fn root_length_from_header(header: &[u8], section_length: usize) -> Result<usize> {
    if header.len() < HEADER_SIZE
        || !header.starts_with(MAGIC)
        || read_u16(header, 8) != VERSION
        || read_u16(header, 10) as usize != DIRECTORY_SIZE
        || read_u16(header, 12) as usize != RECORD_SIZE
        || read_u16(header, 14) as usize != RESOLUTIONS.len()
        || read_u32(header, 24) != 0
        || read_u32(header, 28) != 0
    {
        return Err(corrupt("invalid Chimaera aggregate page root header"));
    }
    let entry_count = read_u32(header, 16) as usize;
    let record_count = read_u32(header, 20) as usize;
    let root_length = HEADER_SIZE
        .checked_add(
            entry_count
                .checked_mul(DIRECTORY_SIZE)
                .ok_or_else(|| corrupt("aggregate page root length overflow"))?,
        )
        .ok_or_else(|| corrupt("aggregate page root length overflow"))?;
    if entry_count == 0
        || record_count == 0
        || root_length.checked_add(
            record_count
                .checked_mul(RECORD_SIZE)
                .ok_or_else(|| corrupt("aggregate page records length overflow"))?,
        ) != Some(section_length)
    {
        return Err(corrupt("aggregate page root does not match section length"));
    }
    Ok(root_length)
}

pub(crate) fn locate_page(
    root: &[u8],
    section_length: usize,
    requested_series: SeriesKey,
    requested_resolution: AggregateResolution,
) -> Result<Option<AggregatePage>> {
    Ok(locate_pages(
        root,
        section_length,
        &[requested_series],
        requested_resolution,
    )?
    .into_iter()
    .next()
    .map(|(_, page)| page))
}

pub(crate) fn locate_pages(
    root: &[u8],
    section_length: usize,
    requested_series: &[SeriesKey],
    requested_resolution: AggregateResolution,
) -> Result<Vec<(SeriesKey, AggregatePage)>> {
    let root_length = root_length_from_header(root, section_length)?;
    if root.len() != root_length {
        return Err(corrupt("aggregate page root length mismatch"));
    }
    let entry_count = read_u32(root, 16) as usize;
    let record_count = read_u32(root, 20) as usize;
    let mut expected_first = 0_usize;
    let mut previous_key = None;
    let mut requested = requested_series.to_vec();
    requested.sort_unstable();
    requested.dedup();
    let mut found = Vec::new();
    for index in 0..entry_count {
        let offset = HEADER_SIZE + index * DIRECTORY_SIZE;
        let entry = &root[offset..offset + DIRECTORY_SIZE];
        let series = SeriesKey::new(read_u32(entry, 0), read_u32(entry, 4), read_u64(entry, 8));
        let resolution = AggregateResolution::from_id(read_u16(entry, 16))
            .ok_or_else(|| corrupt("unknown aggregate page resolution"))?;
        let first = read_u32(entry, 24) as usize;
        let count = read_u32(entry, 28) as usize;
        let key = (series, resolution);
        if read_u16(entry, 18) != STATE_VERSION
            || read_u32(entry, 20) != u32::from(AggregateSet::ALL.bits())
            || read_u32(entry, 36) != 0
            || previous_key.is_some_and(|previous| previous >= key)
            || count == 0
            || first != expected_first
            || first.saturating_add(count) > record_count
        {
            return Err(corrupt("invalid aggregate page directory"));
        }
        if resolution == requested_resolution && requested.binary_search(&series).is_ok() {
            let length = count
                .checked_mul(RECORD_SIZE)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| corrupt("aggregate page length exceeds u32"))?;
            found.push((
                series,
                AggregatePage {
                    offset: u64::try_from(root_length + first * RECORD_SIZE)
                        .map_err(|_| corrupt("aggregate page offset exceeds u64"))?,
                    length,
                    crc32c: read_u32(entry, 32),
                },
            ));
        }
        expected_first += count;
        previous_key = Some(key);
    }
    if expected_first != record_count {
        return Err(corrupt("aggregate page directory record count mismatch"));
    }
    Ok(found)
}

pub(crate) fn section_from_page(
    series: SeriesKey,
    resolution: AggregateResolution,
    records: &[u8],
) -> Result<Vec<u8>> {
    section_from_pages(&[(series, records.to_vec())], resolution)
}

pub(crate) fn section_from_pages(
    pages: &[(SeriesKey, Vec<u8>)],
    resolution: AggregateResolution,
) -> Result<Vec<u8>> {
    if pages.is_empty()
        || pages.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || pages
            .iter()
            .any(|(_, records)| records.is_empty() || !records.len().is_multiple_of(RECORD_SIZE))
    {
        return Err(corrupt("aggregate page records have an invalid layout"));
    }
    let record_count = pages.iter().try_fold(0_usize, |total, (_, records)| {
        total
            .checked_add(records.len() / RECORD_SIZE)
            .ok_or_else(|| corrupt("aggregate page record count overflow"))
    })?;
    let records_length = pages.iter().try_fold(0_usize, |total, (_, records)| {
        total
            .checked_add(records.len())
            .ok_or_else(|| corrupt("aggregate page byte count overflow"))
    })?;
    let directory_length = pages
        .len()
        .checked_mul(DIRECTORY_SIZE)
        .ok_or_else(|| corrupt("aggregate page directory length overflow"))?;
    let length = HEADER_SIZE
        .checked_add(directory_length)
        .and_then(|value| value.checked_add(records_length))
        .ok_or_else(|| corrupt("aggregate page section length overflow"))?;
    if length > MAX_BYTES {
        return Err(corrupt("aggregate page section exceeds hard limit"));
    }
    let mut bytes = vec![0_u8; length];
    bytes[..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, DIRECTORY_SIZE as u16);
    put_u16(&mut bytes, 12, RECORD_SIZE as u16);
    put_u16(&mut bytes, 14, RESOLUTIONS.len() as u16);
    put_u32(
        &mut bytes,
        16,
        u32::try_from(pages.len()).map_err(|_| corrupt("too many aggregate pages"))?,
    );
    put_u32(
        &mut bytes,
        20,
        u32::try_from(record_count)
            .map_err(|_| corrupt("aggregate page record count exceeds u32"))?,
    );
    let mut first_record = 0_usize;
    let mut records_offset = HEADER_SIZE + directory_length;
    for (index, (series, records)) in pages.iter().enumerate() {
        let count = records.len() / RECORD_SIZE;
        let entry_offset = HEADER_SIZE + index * DIRECTORY_SIZE;
        let entry = &mut bytes[entry_offset..entry_offset + DIRECTORY_SIZE];
        put_u32(entry, 0, series.space_id);
        put_u32(entry, 4, series.bucket_id);
        put_u64(entry, 8, series.key);
        put_u16(entry, 16, resolution.id());
        put_u16(entry, 18, STATE_VERSION);
        put_u32(entry, 20, u32::from(AggregateSet::ALL.bits()));
        put_u32(
            entry,
            24,
            u32::try_from(first_record)
                .map_err(|_| corrupt("aggregate page offset exceeds u32"))?,
        );
        put_u32(
            entry,
            28,
            u32::try_from(count).map_err(|_| corrupt("aggregate page record count exceeds u32"))?,
        );
        put_u32(entry, 32, crc32c::crc32c(records));
        bytes[records_offset..records_offset + records.len()].copy_from_slice(records);
        records_offset += records.len();
        first_record += count;
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateRecord {
    pub resolution: AggregateResolution,
    pub bucket_start_us: i64,
    pub bucket_end_us: i64,
    pub state: AggregateState,
}

impl AggregateRecord {
    pub(crate) fn total(self) -> Result<f64> {
        self.state.total()
    }
}

struct SeriesAggregate {
    series: SeriesKey,
    resolution: AggregateResolution,
    records: Vec<AggregateRecord>,
}

struct ResolutionBuilder {
    resolution: AggregateResolution,
    records: Vec<AggregateRecord>,
    current_start_us: Option<i64>,
    state: AggregateState,
}

#[derive(Default)]
struct BuilderMemory {
    governor: Option<Arc<WorkMemoryGovernor>>,
    reservations: Vec<WorkMemoryReservation>,
    reserved_slack_bytes: u64,
}

impl BuilderMemory {
    const ENCODING_HEADROOM_BYTES: u64 = (MIN_SECTION_SIZE as u64) * 2;

    fn new(governor: Option<Arc<WorkMemoryGovernor>>) -> Self {
        Self {
            governor,
            ..Self::default()
        }
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        if bytes == 0 || self.governor.is_none() {
            return Ok(());
        }
        if bytes <= self.reserved_slack_bytes {
            self.reserved_slack_bytes -= bytes;
            return Ok(());
        }
        let needed = bytes.saturating_sub(self.reserved_slack_bytes);
        let chunk = needed.max(64 * 1024);
        let governor = self.governor.as_ref().expect("checked governor");
        let reservation = governor
            .try_reserve_background_with_headroom(
                WorkMemoryClass::AggregateBuilder,
                chunk,
                Self::ENCODING_HEADROOM_BYTES,
            )?
            .ok_or_else(|| {
                crate::error::Error::ResourceLimit(
                    "aggregate builder exceeds the shared work-memory limit".into(),
                )
            })?;
        self.reserved_slack_bytes = chunk.saturating_sub(needed);
        self.reservations.push(reservation);
        Ok(())
    }

    fn reserve_transient_up_to(
        &self,
        maximum_bytes: usize,
    ) -> Result<Option<WorkMemoryReservation>> {
        let maximum_bytes = u64::try_from(maximum_bytes).unwrap_or(u64::MAX);
        let Some(governor) = &self.governor else {
            return Ok(None);
        };
        governor
            .reserve_background_up_to(
                WorkMemoryClass::AggregateBuilder,
                maximum_bytes,
                Self::ENCODING_HEADROOM_BYTES,
            )
            .map(Some)
    }
}

impl ResolutionBuilder {
    fn new(resolution: AggregateResolution) -> Self {
        Self {
            resolution,
            records: Vec::new(),
            current_start_us: None,
            state: AggregateState::default(),
        }
    }

    fn add(
        &mut self,
        timestamp_us: i64,
        value: f64,
        domain: Domain,
        memory: &mut BuilderMemory,
    ) -> Result<()> {
        let duration_us = self.resolution.duration_us();
        let start_us = timestamp_us.div_euclid(duration_us) * duration_us;
        if self.current_start_us != Some(start_us) {
            if self
                .current_start_us
                .is_some_and(|current| start_us <= current)
            {
                return Err(corrupt("aggregate input timestamps are not sorted"));
            }
            self.finish_bucket(domain, memory)?;
            self.current_start_us = Some(start_us);
        }
        self.state.add(timestamp_us, value)?;
        Ok(())
    }

    fn finish_bucket(&mut self, domain: Domain, memory: &mut BuilderMemory) -> Result<()> {
        let Some(bucket_start_us) = self.current_start_us.take() else {
            return Ok(());
        };
        let bucket_end_us = bucket_start_us
            .checked_add(self.resolution.duration_us())
            .ok_or_else(|| invalid("aggregate boundary overflows i64"))?;
        if self.state.count > 0
            && bucket_start_us >= domain.window_start_us
            && bucket_end_us <= domain.window_end_us
        {
            self.state.validate()?;
            if self.records.len() == self.records.capacity() {
                let additional = self.records.capacity().max(4);
                memory
                    .reserve(additional.saturating_mul(std::mem::size_of::<AggregateRecord>()))?;
                self.records.reserve_exact(additional);
            }
            self.records.push(AggregateRecord {
                resolution: self.resolution,
                bucket_start_us,
                bucket_end_us,
                state: self.state,
            });
        }
        self.state = AggregateState::default();
        Ok(())
    }

    fn reset(&mut self) {
        self.records.clear();
        self.current_start_us = None;
        self.state = AggregateState::default();
    }
}

pub(crate) struct Builder {
    domain: Domain,
    series: Vec<SeriesAggregate>,
    stored_records: usize,
    current_series: Option<SeriesKey>,
    current_points: usize,
    current_safe: bool,
    resolutions: Vec<ResolutionBuilder>,
    required_resolutions: Vec<AggregateResolution>,
    memory: BuilderMemory,
    disabled: bool,
}

pub(crate) struct EncodedChunk {
    pub bytes: Vec<u8>,
    pub minimum_series: SeriesKey,
    pub maximum_series: SeriesKey,
}

impl Builder {
    pub(crate) fn new(domain: Domain) -> Self {
        Self::with_required_resolutions_and_memory(domain, Vec::new(), None)
    }

    pub(crate) fn with_work_memory(
        domain: Domain,
        required_resolutions: Vec<AggregateResolution>,
        governor: Arc<WorkMemoryGovernor>,
    ) -> Self {
        Self::with_required_resolutions_and_memory(domain, required_resolutions, Some(governor))
    }

    fn with_required_resolutions_and_memory(
        domain: Domain,
        mut required_resolutions: Vec<AggregateResolution>,
        governor: Option<Arc<WorkMemoryGovernor>>,
    ) -> Self {
        required_resolutions.sort_unstable();
        required_resolutions.dedup();
        Self {
            domain,
            series: Vec::new(),
            stored_records: 0,
            current_series: None,
            current_points: 0,
            current_safe: true,
            resolutions: RESOLUTIONS
                .into_iter()
                .map(ResolutionBuilder::new)
                .collect(),
            required_resolutions,
            memory: BuilderMemory::new(governor),
            disabled: false,
        }
    }

    pub(crate) fn add(&mut self, point: VersionedPoint) -> Result<()> {
        if self.disabled {
            return Ok(());
        }
        if self.current_series != Some(point.key.series) {
            if self
                .current_series
                .is_some_and(|series| point.key.series < series)
            {
                return Err(corrupt("aggregate input is not sorted by series"));
            }
            if let Err(error) = self.finish_series() {
                if self.required_resolutions.is_empty() && matches!(&error, Error::ResourceLimit(_))
                {
                    self.disable();
                    return Ok(());
                }
                return Err(error);
            }
            self.start_series(point.key.series);
        }
        if !self.current_safe {
            return Ok(());
        }
        let value = point.put_value()?;
        for builder in &mut self.resolutions {
            if let Err(error) =
                builder.add(point.key.timestamp_us, value, self.domain, &mut self.memory)
            {
                if !self.required_resolutions.is_empty() {
                    return Err(error);
                }
                if matches!(&error, Error::ResourceLimit(_)) {
                    self.disable();
                    return Ok(());
                }
                self.current_safe = false;
                for builder in &mut self.resolutions {
                    builder.reset();
                }
                return Ok(());
            }
        }
        self.current_points = self
            .current_points
            .checked_add(1)
            .ok_or_else(|| invalid("aggregate point count overflow"))?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Option<Vec<u8>>> {
        if self.disabled {
            return Ok(None);
        }
        if let Err(error) = self.finish_series() {
            if self.required_resolutions.is_empty() && matches!(&error, Error::ResourceLimit(_)) {
                return Ok(None);
            }
            return Err(error);
        }
        if self.series.is_empty() {
            return Ok(None);
        }
        let length = encoded_size(self.series.len(), self.stored_records);
        self.memory.reserve(length)?;
        encode(&self.series).map(Some)
    }

    pub(crate) fn finish_chunks(
        mut self,
        maximum_bytes: usize,
        mut emit: impl FnMut(usize, usize, EncodedChunk) -> Result<()>,
    ) -> Result<usize> {
        if !(MIN_SECTION_SIZE..=MAX_BYTES).contains(&maximum_bytes) {
            return Err(invalid("aggregate chunk size is outside supported bounds"));
        }
        self.finish_series()?;
        // The configured archive size is an upper bound. Reserve only the currently
        // available encoding workspace, then split more finely when the builder is
        // already using most of the shared budget.
        let chunk_memory = self
            .memory
            .reserve_transient_up_to(maximum_bytes.saturating_mul(2))?;
        let effective_maximum_bytes = chunk_memory
            .as_ref()
            .map(|reservation| {
                usize::try_from(reservation.bytes() / 2)
                    .unwrap_or(usize::MAX)
                    .min(maximum_bytes)
            })
            .unwrap_or(maximum_bytes);
        let chunk_count = encoded_chunk_count(&self.series, effective_maximum_bytes)?;
        if chunk_count == 0 {
            return Ok(0);
        }
        // Slicing may temporarily duplicate at most one chunk of records and encoding
        // creates one equally bounded byte buffer. Account both until every callback
        // has consumed its chunk.
        let _chunk_memory = chunk_memory;
        let maximum_records =
            effective_maximum_bytes.saturating_sub(HEADER_SIZE + DIRECTORY_SIZE) / RECORD_SIZE;
        if maximum_records == 0 {
            return Err(invalid("aggregate chunk cannot contain one record"));
        }
        let mut emitted = 0_usize;
        let mut pending = Vec::<SeriesAggregate>::new();
        let mut pending_records = 0_usize;
        for entry in self.series {
            let mut record_offset = 0_usize;
            while record_offset < entry.records.len() {
                let remaining_capacity = maximum_records.saturating_sub(pending_records);
                if remaining_capacity == 0
                    || (!pending.is_empty()
                        && encoded_size(pending.len() + 1, pending_records + 1)
                            > effective_maximum_bytes)
                {
                    emit(
                        emitted,
                        chunk_count,
                        encode_chunk(std::mem::take(&mut pending))?,
                    )?;
                    emitted += 1;
                    pending_records = 0;
                    continue;
                }
                let take = remaining_capacity.min(entry.records.len() - record_offset);
                let candidate_records = pending_records.saturating_add(take);
                if encoded_size(pending.len() + 1, candidate_records) > effective_maximum_bytes {
                    let adjusted = effective_maximum_bytes.saturating_sub(
                        HEADER_SIZE
                            + (pending.len() + 1) * DIRECTORY_SIZE
                            + pending_records * RECORD_SIZE,
                    ) / RECORD_SIZE;
                    if adjusted == 0 {
                        emit(
                            emitted,
                            chunk_count,
                            encode_chunk(std::mem::take(&mut pending))?,
                        )?;
                        emitted += 1;
                        pending_records = 0;
                        continue;
                    }
                    let end = record_offset + adjusted.min(entry.records.len() - record_offset);
                    pending.push(SeriesAggregate {
                        series: entry.series,
                        resolution: entry.resolution,
                        records: entry.records[record_offset..end].to_vec(),
                    });
                    pending_records += end - record_offset;
                    record_offset = end;
                } else {
                    let end = record_offset + take;
                    pending.push(SeriesAggregate {
                        series: entry.series,
                        resolution: entry.resolution,
                        records: entry.records[record_offset..end].to_vec(),
                    });
                    pending_records = candidate_records;
                    record_offset = end;
                }
            }
        }
        if !pending.is_empty() {
            emit(emitted, chunk_count, encode_chunk(pending)?)?;
            emitted += 1;
        }
        if emitted != chunk_count {
            return Err(corrupt("aggregate chunk plan changed during encoding"));
        }
        Ok(emitted)
    }

    fn start_series(&mut self, series: SeriesKey) {
        self.current_series = Some(series);
        self.current_points = 0;
        self.current_safe = true;
        for builder in &mut self.resolutions {
            builder.reset();
        }
    }

    fn disable(&mut self) {
        self.disabled = true;
        self.series = Vec::new();
        self.stored_records = 0;
        self.current_series = None;
        self.current_points = 0;
        self.current_safe = false;
        self.resolutions = RESOLUTIONS
            .into_iter()
            .map(ResolutionBuilder::new)
            .collect();
        self.memory.reservations.clear();
        self.memory.reserved_slack_bytes = 0;
    }

    fn finish_series(&mut self) -> Result<()> {
        let Some(series) = self.current_series.take() else {
            return Ok(());
        };
        for builder in &mut self.resolutions {
            builder.finish_bucket(self.domain, &mut self.memory)?;
        }
        if self.current_safe {
            let estimated_input_bytes = self
                .current_points
                .saturating_mul(ESTIMATED_COMPRESSED_POINT_BYTES);
            for builder in &mut self.resolutions {
                let aggregate_bytes = DIRECTORY_SIZE
                    .saturating_add(builder.records.len().saturating_mul(RECORD_SIZE));
                let beneficial = !builder.records.is_empty()
                    && if self.required_resolutions.is_empty() {
                        estimated_input_bytes
                            >= aggregate_bytes.saturating_mul(MIN_AGGREGATE_SAVINGS_RATIO)
                    } else {
                        self.required_resolutions.contains(&builder.resolution)
                    };
                if !beneficial {
                    continue;
                }
                let next_records = self
                    .stored_records
                    .checked_add(builder.records.len())
                    .ok_or_else(|| invalid("aggregate record count overflow"))?;
                if self.required_resolutions.is_empty()
                    && encoded_size(self.series.len() + 1, next_records) > MAX_BYTES
                {
                    continue;
                }
                if self.series.len() == self.series.capacity() {
                    let additional = self.series.capacity().max(4);
                    self.memory.reserve(
                        additional.saturating_mul(std::mem::size_of::<SeriesAggregate>()),
                    )?;
                    self.series.reserve_exact(additional);
                }
                self.series.push(SeriesAggregate {
                    series,
                    resolution: builder.resolution,
                    records: std::mem::take(&mut builder.records),
                });
                self.stored_records = next_records;
            }
        }
        for builder in &mut self.resolutions {
            builder.reset();
        }
        Ok(())
    }
}

fn encoded_chunk_count(series: &[SeriesAggregate], maximum_bytes: usize) -> Result<usize> {
    let maximum_records = maximum_bytes.saturating_sub(HEADER_SIZE + DIRECTORY_SIZE) / RECORD_SIZE;
    if maximum_records == 0 {
        return Err(invalid("aggregate chunk cannot contain one record"));
    }
    let mut chunks = 0_usize;
    let mut pending_entries = 0_usize;
    let mut pending_records = 0_usize;
    for entry in series {
        let mut record_offset = 0_usize;
        while record_offset < entry.records.len() {
            let remaining_capacity = maximum_records.saturating_sub(pending_records);
            if remaining_capacity == 0
                || (pending_entries != 0
                    && encoded_size(pending_entries + 1, pending_records + 1) > maximum_bytes)
            {
                chunks = chunks.saturating_add(1);
                pending_entries = 0;
                pending_records = 0;
                continue;
            }
            let take = remaining_capacity.min(entry.records.len() - record_offset);
            let candidate_records = pending_records.saturating_add(take);
            let selected = if encoded_size(pending_entries + 1, candidate_records) > maximum_bytes {
                let adjusted = maximum_bytes.saturating_sub(
                    HEADER_SIZE
                        + (pending_entries + 1) * DIRECTORY_SIZE
                        + pending_records * RECORD_SIZE,
                ) / RECORD_SIZE;
                if adjusted == 0 {
                    chunks = chunks.saturating_add(1);
                    pending_entries = 0;
                    pending_records = 0;
                    continue;
                }
                adjusted.min(entry.records.len() - record_offset)
            } else {
                take
            };
            pending_entries += 1;
            pending_records += selected;
            record_offset += selected;
        }
    }
    if pending_entries != 0 {
        chunks = chunks.saturating_add(1);
    }
    Ok(chunks)
}

fn encode_chunk(series: Vec<SeriesAggregate>) -> Result<EncodedChunk> {
    let minimum_series = series
        .first()
        .map(|entry| entry.series)
        .ok_or_else(|| invalid("cannot encode an empty aggregate chunk"))?;
    let maximum_series = series
        .last()
        .map(|entry| entry.series)
        .ok_or_else(|| invalid("cannot encode an empty aggregate chunk"))?;
    let bytes = encode(&series)?;
    Ok(EncodedChunk {
        bytes,
        minimum_series,
        maximum_series,
    })
}

fn encoded_size(series_count: usize, record_count: usize) -> usize {
    HEADER_SIZE
        .saturating_add(series_count.saturating_mul(DIRECTORY_SIZE))
        .saturating_add(record_count.saturating_mul(RECORD_SIZE))
}

fn encode(series: &[SeriesAggregate]) -> Result<Vec<u8>> {
    let record_count = series.iter().try_fold(0_usize, |total, entry| {
        total
            .checked_add(entry.records.len())
            .ok_or_else(|| invalid("aggregate record count overflow"))
    })?;
    let length = encoded_size(series.len(), record_count);
    if length > MAX_BYTES || series.is_empty() || record_count == 0 {
        return Err(invalid("aggregate section exceeds its bounded size"));
    }
    let mut bytes = vec![0_u8; length];
    bytes[..8].copy_from_slice(MAGIC);
    put_u16(&mut bytes, 8, VERSION);
    put_u16(&mut bytes, 10, DIRECTORY_SIZE as u16);
    put_u16(&mut bytes, 12, RECORD_SIZE as u16);
    put_u16(&mut bytes, 14, RESOLUTIONS.len() as u16);
    put_u32(
        &mut bytes,
        16,
        u32::try_from(series.len()).map_err(|_| invalid("too many aggregate entries"))?,
    );
    put_u32(
        &mut bytes,
        20,
        u32::try_from(record_count).map_err(|_| invalid("too many aggregate records"))?,
    );
    let mut first_record = 0_usize;
    for (index, entry) in series.iter().enumerate() {
        let offset = HEADER_SIZE + index * DIRECTORY_SIZE;
        put_u32(&mut bytes, offset, entry.series.space_id);
        put_u32(&mut bytes, offset + 4, entry.series.bucket_id);
        put_u64(&mut bytes, offset + 8, entry.series.key);
        put_u16(&mut bytes, offset + 16, entry.resolution.id());
        put_u16(&mut bytes, offset + 18, STATE_VERSION);
        put_u32(&mut bytes, offset + 20, u32::from(AggregateSet::ALL.bits()));
        put_u32(
            &mut bytes,
            offset + 24,
            u32::try_from(first_record).map_err(|_| invalid("aggregate offset exceeds u32"))?,
        );
        put_u32(
            &mut bytes,
            offset + 28,
            u32::try_from(entry.records.len())
                .map_err(|_| invalid("aggregate entry count exceeds u32"))?,
        );
        first_record += entry.records.len();
    }
    let records_offset = HEADER_SIZE + series.len() * DIRECTORY_SIZE;
    let mut record_index = 0_usize;
    for (entry_index, entry) in series.iter().enumerate() {
        let first_entry_record = record_index;
        for record in &entry.records {
            let offset = records_offset + record_index * RECORD_SIZE;
            let (raw_sum, compensation) = record.state.sum.components();
            let first = record
                .state
                .first
                .ok_or_else(|| corrupt("aggregate first point disappeared"))?;
            let last = record
                .state
                .last
                .ok_or_else(|| corrupt("aggregate last point disappeared"))?;
            put_i64(&mut bytes, offset, record.bucket_start_us);
            put_i64(&mut bytes, offset + 8, record.bucket_end_us);
            put_u64(&mut bytes, offset + 16, raw_sum.to_bits());
            put_u64(&mut bytes, offset + 24, compensation.to_bits());
            put_u64(&mut bytes, offset + 32, record.state.count);
            put_u64(&mut bytes, offset + 40, record.state.mean.to_bits());
            put_u64(&mut bytes, offset + 48, record.state.m2.to_bits());
            put_u64(&mut bytes, offset + 56, record.state.min.to_bits());
            put_u64(&mut bytes, offset + 64, record.state.max.to_bits());
            put_i64(&mut bytes, offset + 72, first.timestamp_us);
            put_u64(&mut bytes, offset + 80, first.value.to_bits());
            put_i64(&mut bytes, offset + 88, last.timestamp_us);
            put_u64(&mut bytes, offset + 96, last.value.to_bits());
            put_u16(&mut bytes, offset + 104, record.state.available.bits());
            record_index += 1;
        }
        let directory_offset = HEADER_SIZE + entry_index * DIRECTORY_SIZE;
        let record_from = records_offset + first_entry_record * RECORD_SIZE;
        let record_to = records_offset + record_index * RECORD_SIZE;
        let records_crc = crc32c::crc32c(&bytes[record_from..record_to]);
        put_u32(&mut bytes, directory_offset + 32, records_crc);
    }
    Ok(bytes)
}

pub(crate) fn validate(bytes: &[u8], domain: Domain) -> Result<()> {
    let layout = counts(bytes)?;
    let entry_count = layout.entry_count;
    let record_count = layout.record_count;
    let mut expected_first = 0_usize;
    let mut previous_key = None;
    for index in 0..entry_count {
        let (series, resolution, first, count, records_crc) = directory(bytes, index)?;
        let key = (series, resolution);
        if previous_key.is_some_and(|previous| previous >= key)
            || first != expected_first
            || count == 0
            || first.saturating_add(count) > record_count
        {
            return Err(corrupt("invalid aggregate directory"));
        }
        if let Some(expected_crc) = records_crc {
            let records_offset = HEADER_SIZE + entry_count * layout.directory_size;
            let from = records_offset + first * RECORD_SIZE;
            let to = from + count * RECORD_SIZE;
            if crc32c::crc32c(&bytes[from..to]) != expected_crc {
                return Err(corrupt("aggregate record page CRC32C mismatch"));
            }
        }
        let mut previous_bucket = None;
        for record_index in first..first + count {
            let record = record(bytes, entry_count, record_count, resolution, record_index)?;
            let duration_us = resolution.duration_us();
            let first_point = record
                .state
                .first
                .ok_or_else(|| corrupt("aggregate first point is absent"))?;
            let last_point = record
                .state
                .last
                .ok_or_else(|| corrupt("aggregate last point is absent"))?;
            if record.bucket_start_us < domain.window_start_us
                || record.bucket_end_us > domain.window_end_us
                || record.bucket_end_us.checked_sub(record.bucket_start_us) != Some(duration_us)
                || record.bucket_start_us.rem_euclid(duration_us) != 0
                || previous_bucket.is_some_and(|previous| previous >= record.bucket_start_us)
                || first_point.timestamp_us < record.bucket_start_us
                || last_point.timestamp_us >= record.bucket_end_us
                || first_point.timestamp_us > last_point.timestamp_us
            {
                return Err(corrupt("invalid aggregate record"));
            }
            record.state.validate()?;
            previous_bucket = Some(record.bucket_start_us);
        }
        expected_first += count;
        previous_key = Some(key);
    }
    if expected_first != record_count {
        return Err(corrupt("aggregate record count does not match directory"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct SectionLayout {
    entry_count: usize,
    record_count: usize,
    directory_size: usize,
}

fn counts(bytes: &[u8]) -> Result<SectionLayout> {
    let legacy = bytes.starts_with(LEGACY_MAGIC);
    let directory_size = if legacy {
        LEGACY_DIRECTORY_SIZE
    } else {
        DIRECTORY_SIZE
    };
    let expected_version = if legacy { LEGACY_VERSION } else { VERSION };
    if bytes.len() < HEADER_SIZE
        || bytes.len() > MAX_BYTES
        || (!legacy && !bytes.starts_with(MAGIC))
        || read_u16(bytes, 8) != expected_version
        || read_u16(bytes, 10) as usize != directory_size
        || read_u16(bytes, 12) as usize != RECORD_SIZE
        || read_u16(bytes, 14) as usize != RESOLUTIONS.len()
        || read_u32(bytes, 24) != 0
        || read_u32(bytes, 28) != 0
    {
        return Err(corrupt("invalid Chimaera aggregate v2 header"));
    }
    let entry_count = read_u32(bytes, 16) as usize;
    let record_count = read_u32(bytes, 20) as usize;
    if entry_count == 0
        || record_count == 0
        || HEADER_SIZE
            .saturating_add(entry_count.saturating_mul(directory_size))
            .saturating_add(record_count.saturating_mul(RECORD_SIZE))
            != bytes.len()
    {
        return Err(corrupt("aggregate counts do not match section length"));
    }
    Ok(SectionLayout {
        entry_count,
        record_count,
        directory_size,
    })
}

fn directory(
    bytes: &[u8],
    index: usize,
) -> Result<(SeriesKey, AggregateResolution, usize, usize, Option<u32>)> {
    let layout = counts(bytes)?;
    if index >= layout.entry_count {
        return Err(corrupt("aggregate directory index is outside bounds"));
    }
    let offset = HEADER_SIZE + index * layout.directory_size;
    let resolution = AggregateResolution::from_id(read_u16(bytes, offset + 16))
        .ok_or_else(|| corrupt("unknown aggregate resolution"))?;
    if read_u16(bytes, offset + 18) != STATE_VERSION
        || read_u32(bytes, offset + 20) != u32::from(AggregateSet::ALL.bits())
    {
        return Err(corrupt("unsupported aggregate state directory"));
    }
    if layout.directory_size == DIRECTORY_SIZE && read_u32(bytes, offset + 36) != 0 {
        return Err(corrupt("aggregate directory reserved bytes are not zero"));
    }
    Ok((
        SeriesKey::new(
            read_u32(bytes, offset),
            read_u32(bytes, offset + 4),
            read_u64(bytes, offset + 8),
        ),
        resolution,
        read_u32(bytes, offset + 24) as usize,
        read_u32(bytes, offset + 28) as usize,
        (layout.directory_size == DIRECTORY_SIZE).then(|| read_u32(bytes, offset + 32)),
    ))
}

fn record(
    bytes: &[u8],
    entry_count: usize,
    record_count: usize,
    resolution: AggregateResolution,
    index: usize,
) -> Result<AggregateRecord> {
    if index >= record_count {
        return Err(corrupt("aggregate record index is outside bounds"));
    }
    let directory_size = counts(bytes)?.directory_size;
    let offset = HEADER_SIZE + entry_count * directory_size + index * RECORD_SIZE;
    if read_u16(bytes, offset + 106) != 0 || read_u32(bytes, offset + 108) != 0 {
        return Err(corrupt("aggregate record reserved bytes are not zero"));
    }
    let available = AggregateSet::from_bits(read_u16(bytes, offset + 104))
        .ok_or_else(|| corrupt("unknown aggregate state flags"))?;
    let state = AggregateState::from_persisted(
        f64::from_bits(read_u64(bytes, offset + 16)),
        f64::from_bits(read_u64(bytes, offset + 24)),
        read_u64(bytes, offset + 32),
        f64::from_bits(read_u64(bytes, offset + 40)),
        f64::from_bits(read_u64(bytes, offset + 48)),
        f64::from_bits(read_u64(bytes, offset + 56)),
        f64::from_bits(read_u64(bytes, offset + 64)),
        AggregatePoint {
            timestamp_us: read_i64(bytes, offset + 72),
            value: f64::from_bits(read_u64(bytes, offset + 80)),
        },
        AggregatePoint {
            timestamp_us: read_i64(bytes, offset + 88),
            value: f64::from_bits(read_u64(bytes, offset + 96)),
        },
        available,
    )?;
    Ok(AggregateRecord {
        resolution,
        bucket_start_us: read_i64(bytes, offset),
        bucket_end_us: read_i64(bytes, offset + 8),
        state,
    })
}

/// Diagnostic lookup in bytes already generated by the pack worker. It does
/// not load blocks, clone the aggregate section, or affect publication errors.
#[cfg(feature = "pipeline-trace")]
pub(crate) fn trace_coverage(bytes: &[u8], key: crate::types::PointKey) -> Result<u8> {
    let layout = counts(bytes)?;
    let mut mask = 0;
    for (bit, resolution) in RESOLUTIONS.into_iter().enumerate() {
        let requested = (key.series, resolution);
        let (mut left, mut right) = (0, layout.entry_count);
        while left < right {
            let middle = left + (right - left) / 2;
            let (series, candidate, ..) = directory(bytes, middle)?;
            if (series, candidate) < requested {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        if left == layout.entry_count {
            continue;
        }
        let (series, candidate, first, count, _) = directory(bytes, left)?;
        if (series, candidate) != requested {
            continue;
        }
        let (mut left, mut right) = (first, first + count);
        while left < right {
            let middle = left + (right - left) / 2;
            let row = record(
                bytes,
                layout.entry_count,
                layout.record_count,
                resolution,
                middle,
            )?;
            if row.bucket_end_us <= key.timestamp_us {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        if left < first + count {
            let row = record(
                bytes,
                layout.entry_count,
                layout.record_count,
                resolution,
                left,
            )?;
            if row.bucket_start_us <= key.timestamp_us && key.timestamp_us < row.bucket_end_us {
                mask |= 1 << bit;
            }
        }
    }
    Ok(mask)
}

pub(crate) struct Cursor {
    section: Arc<Section>,
    entry_count: usize,
    record_count: usize,
    resolution: AggregateResolution,
    position: usize,
    limit: usize,
    from_us: i64,
    to_us: i64,
}

enum BoundaryMode {
    Part,
    Archive,
}

impl Cursor {
    pub(crate) fn for_series(
        section: Arc<Section>,
        series: SeriesKey,
        resolution: AggregateResolution,
        from_us: i64,
        to_us: i64,
    ) -> Result<Option<Self>> {
        let bytes = &section.bytes;
        if bytes.is_empty() {
            return Ok(None);
        }
        let layout = counts(bytes)?;
        let entry_count = layout.entry_count;
        let record_count = layout.record_count;
        let requested = (series, resolution);
        let mut left = 0_usize;
        let mut right = entry_count;
        while left < right {
            let middle = left + (right - left) / 2;
            let (candidate_series, candidate_resolution, _, _, _) = directory(bytes, middle)?;
            if (candidate_series, candidate_resolution) < requested {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        if left >= entry_count {
            return Ok(None);
        }
        let (candidate_series, candidate_resolution, first, count, _) = directory(bytes, left)?;
        if (candidate_series, candidate_resolution) != requested {
            return Ok(None);
        }
        Ok(Some(Self {
            section,
            entry_count,
            record_count,
            resolution,
            position: first,
            limit: first + count,
            from_us,
            to_us,
        }))
    }

    /// Immutable parts can contribute a partial day's summary when every
    /// represented point is selected.
    pub(crate) fn next_record_for_part(&mut self) -> Result<Option<AggregateRecord>> {
        self.next_record_inner(BoundaryMode::Part)
    }

    pub(crate) fn next_record_for_archive(&mut self) -> Result<Option<AggregateRecord>> {
        self.next_record_inner(BoundaryMode::Archive)
    }

    fn next_record_inner(&mut self, mode: BoundaryMode) -> Result<Option<AggregateRecord>> {
        while self.position < self.limit {
            let current = record(
                &self.section.bytes,
                self.entry_count,
                self.record_count,
                self.resolution,
                self.position,
            )?;
            self.position += 1;
            if current.bucket_end_us <= self.from_us {
                continue;
            }
            if current.bucket_start_us >= self.to_us {
                return Ok(None);
            }
            if current.bucket_start_us >= self.from_us && current.bucket_end_us <= self.to_us {
                return Ok(Some(current));
            }
            if matches!(mode, BoundaryMode::Archive) {
                let (first, last) = current
                    .state
                    .first
                    .zip(current.state.last)
                    .ok_or_else(|| corrupt("nonempty archive record has no endpoints"))?;
                if last.timestamp_us < self.from_us || first.timestamp_us >= self.to_us {
                    continue;
                }
                if first.timestamp_us >= self.from_us && last.timestamp_us < self.to_us {
                    return Ok(Some(current));
                }
                return Err(crate::Error::QueryUnavailable(
                    "query boundary splits a retained aggregate; raw points have expired".into(),
                ));
            }
            if matches!(mode, BoundaryMode::Part)
                && current.resolution == AggregateResolution::Day
                && current
                    .state
                    .first
                    .zip(current.state.last)
                    .is_some_and(|(first, last)| {
                        first.timestamp_us >= self.from_us && last.timestamp_us < self.to_us
                    })
            {
                return Ok(Some(current));
            }
        }
        Ok(None)
    }
}
