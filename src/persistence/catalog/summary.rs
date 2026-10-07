//! Exact, mergeable segment metadata. Stored beside records and child links;
//! range folds read boundary pages and reuse summaries of enclosed subtrees.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(super) const BYTES: usize = 144;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Summary {
    pub counts: [u64; 3],
    pub bytes: [u64; 3],
    pub minimum: [i64; 3],
    pub maximum: [i64; 3],
    pub rewrites: [u32; 3],
    pub oldest: [u64; 2],
    pub minimum_l2_bytes: u64,
}

impl Default for Summary {
    fn default() -> Self {
        Self {
            counts: [0; 3],
            bytes: [0; 3],
            minimum: [i64::MAX; 3],
            maximum: [i64::MIN; 3],
            rewrites: [0; 3],
            oldest: [u64::MAX; 2],
            minimum_l2_bytes: u64::MAX,
        }
    }
}

impl Summary {
    pub fn merge(&mut self, rhs: &Self) -> Result<()> {
        for level in 0..3 {
            self.counts[level] = self.counts[level]
                .checked_add(rhs.counts[level])
                .ok_or_else(|| anyhow::anyhow!("catalog summary count overflow"))?;
            self.bytes[level] = self.bytes[level]
                .checked_add(rhs.bytes[level])
                .ok_or_else(|| anyhow::anyhow!("catalog summary bytes overflow"))?;
            self.minimum[level] = self.minimum[level].min(rhs.minimum[level]);
            self.maximum[level] = self.maximum[level].max(rhs.maximum[level]);
            self.rewrites[level] = self.rewrites[level].max(rhs.rewrites[level]);
        }
        for level in 0..2 {
            self.oldest[level] = self.oldest[level].min(rhs.oldest[level]);
        }
        self.minimum_l2_bytes = self.minimum_l2_bytes.min(rhs.minimum_l2_bytes);
        Ok(())
    }
    pub(super) fn matches(&self, levels: u8) -> bool {
        (0..3).any(|level| levels & (1 << level) != 0 && self.counts[level] != 0)
    }
    pub(super) fn encode(self, bytes: &mut [u8]) {
        let values = self
            .counts
            .into_iter()
            .chain(self.bytes)
            .chain(self.minimum.map(|v| v as u64))
            .chain(self.maximum.map(|v| v as u64))
            .chain(self.rewrites.map(u64::from))
            .chain(self.oldest)
            .chain([self.minimum_l2_bytes]);
        for (slot, value) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(values) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == BYTES, "catalog summary size");
        let mut at = 0;
        let mut next = || {
            let value = u64::from_le_bytes(bytes[at..at + 8].try_into().expect("bounded summary"));
            at += 8;
            value
        };
        let result = Self {
            counts: std::array::from_fn(|_| next()),
            bytes: std::array::from_fn(|_| next()),
            minimum: std::array::from_fn(|_| next() as i64),
            maximum: std::array::from_fn(|_| next() as i64),
            rewrites: [
                u32::try_from(next())?,
                u32::try_from(next())?,
                u32::try_from(next())?,
            ],
            oldest: std::array::from_fn(|_| next()),
            minimum_l2_bytes: next(),
        };
        for level in 0..3 {
            ensure!(
                if result.counts[level] == 0 {
                    result.bytes[level] == 0
                        && result.minimum[level] == i64::MAX
                        && result.maximum[level] == i64::MIN
                        && result.rewrites[level] == 0
                        && (level == 2 || result.oldest[level] == u64::MAX)
                } else {
                    result.minimum[level] <= result.maximum[level]
                },
                "catalog summary bounds"
            );
        }
        ensure!(
            result.counts[2] != 0 || result.minimum_l2_bytes == u64::MAX,
            "catalog L2 size summary"
        );
        Ok(result)
    }
}
