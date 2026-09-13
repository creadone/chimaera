use crate::error::{Result, invalid};
use crate::types::SeriesKey;

pub(crate) const DEFAULT_VIRTUAL_SHARDS: u16 = 64;
pub(crate) const DEFAULT_PARTITION_SPAN_US: i64 = 30 * 86_400_000_000;
const ROUTING_SEED: u64 = 0x5453_524f_5554_3031;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Domain {
    pub shard_id: u16,
    pub policy_id: u32,
    pub window_start_us: i64,
    pub window_end_us: i64,
}

impl Domain {
    pub(crate) fn for_point(
        series: SeriesKey,
        timestamp_us: i64,
        virtual_shards: u16,
        policy_id: u32,
        partition_span_us: i64,
    ) -> Result<Self> {
        validate_layout(virtual_shards, partition_span_us)?;
        if policy_id == 0 {
            return Err(invalid("policy ID must be positive"));
        }
        let shard_id = shard_for(series, virtual_shards);
        let timestamp = i128::from(timestamp_us);
        let span = i128::from(partition_span_us);
        let window_start = timestamp.div_euclid(span) * span;
        let window_end = window_start + span;
        let window_start_us =
            i64::try_from(window_start).map_err(|_| invalid("partition window overflows i64"))?;
        let window_end_us =
            i64::try_from(window_end).map_err(|_| invalid("partition window overflows i64"))?;
        Ok(Self {
            shard_id,
            policy_id,
            window_start_us,
            window_end_us,
        })
    }

    pub(crate) const fn overlaps(self, from_us: i64, to_us: i64) -> bool {
        self.window_start_us < to_us && self.window_end_us > from_us
    }
}

pub(crate) fn validate_layout(virtual_shards: u16, partition_span_us: i64) -> Result<()> {
    if virtual_shards == 0 || virtual_shards > 4096 || !virtual_shards.is_power_of_two() {
        return Err(invalid(
            "virtual shard count must be a power of two in 1..=4096",
        ));
    }
    if partition_span_us <= 0 {
        return Err(invalid("partition span must be positive"));
    }
    Ok(())
}

#[must_use]
pub(crate) fn shard_for(series: SeriesKey, virtual_shards: u16) -> u16 {
    debug_assert!(virtual_shards.is_power_of_two());
    let catalog_word = (u64::from(series.space_id) << 32) | u64::from(series.bucket_id);
    let hash = splitmix64(splitmix64(catalog_word ^ ROUTING_SEED) ^ series.key);
    (hash & u64::from(virtual_shards - 1)) as u16
}

fn splitmix64(input: u64) -> u64 {
    let mut value = input.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_matches_stable_v1_vectors() {
        let vectors = [
            (SeriesKey::new(1, 1, 1), 61),
            (SeriesKey::new(1, 1, 2), 3),
            (SeriesKey::new(7, 11, u64::MAX), 49),
            (SeriesKey::new(u32::MAX, u32::MAX, 0), 41),
        ];
        for (series, shard) in vectors {
            assert_eq!(shard_for(series, 64), shard);
        }
    }

    #[test]
    fn negative_timestamps_use_floor_partitioning() -> Result<()> {
        let domain = Domain::for_point(SeriesKey::new(1, 1, 1), -1, 64, 1, 100)?;
        assert_eq!(domain.window_start_us, -100);
        assert_eq!(domain.window_end_us, 0);
        Ok(())
    }
}
