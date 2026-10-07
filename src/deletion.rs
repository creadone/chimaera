//! Bounded deletion barriers shared by immutable read snapshots.

use crate::error::{Error, Result, corrupt};
use crate::memory::cache::{MemoryClass, MemoryGovernor, MemoryReservation};
use crate::types::{SeriesKey, VersionedPoint};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const MAX_KEYS: usize = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Marker {
    pub series: SeriesKey,
    pub commit_id: u64,
}

/// State owns at most MAX_KEYS barriers. Old readers retain their own version,
/// charged to the same governor. The collector removes barriers only after all
/// pre-delete inputs have been materialized and swept. Exhaustion rejects a new
/// deletion before its WAL append; it never discards a live barrier.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Deletions {
    pub epoch: u64,
    pub entries: Vec<Marker>,
    #[serde(skip)]
    memory: Option<Arc<MemoryReservation>>,
}

impl std::fmt::Debug for Deletions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deletions")
            .field("epoch", &self.epoch)
            .field("entries", &self.entries.len())
            .finish()
    }
}

impl Deletions {
    pub fn is_initial(&self) -> bool {
        self.epoch == 0 && self.entries.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        if self.entries.len() > MAX_KEYS
            || self
                .entries
                .iter()
                .any(|m| m.commit_id == 0 || m.commit_id > self.epoch)
            || self.entries.windows(2).any(|p| p[0].series >= p[1].series)
        {
            return Err(corrupt("invalid series deletion barriers"));
        }
        Ok(())
    }

    pub fn charge(&mut self, governor: &Arc<MemoryGovernor>) -> Result<()> {
        self.validate()?;
        self.memory = Some(Self::reserve(governor, self.entries.capacity())?);
        Ok(())
    }

    fn reserve(governor: &Arc<MemoryGovernor>, count: usize) -> Result<Arc<MemoryReservation>> {
        governor
            .try_reserve_owned(
                MemoryClass::MetadataCatalog,
                (count as u64) * std::mem::size_of::<Marker>() as u64,
            )
            .ok_or_else(|| Error::ResourceLimit("series deletion memory limit".into()))
    }

    pub fn cutoff(&self, series: SeriesKey) -> u64 {
        self.entries
            .binary_search_by_key(&series, |m| m.series)
            .map_or(0, |i| self.entries[i].commit_id)
    }

    pub fn hides(&self, point: VersionedPoint) -> bool {
        !self.entries.is_empty() && point.commit_id <= self.cutoff(point.key.series)
    }

    pub fn updated(
        &self,
        keys: &[SeriesKey],
        commit_id: u64,
        governor: &Arc<MemoryGovernor>,
    ) -> Result<Arc<Self>> {
        debug_assert!(keys.windows(2).all(|p| p[0] < p[1]));
        let added = keys.iter().filter(|&&key| self.cutoff(key) == 0).count();
        let count = self.entries.len().saturating_add(added);
        if count > MAX_KEYS {
            return Err(Error::ResourceLimit(
                "too many pending series deletions".into(),
            ));
        }
        let memory = Self::reserve(governor, count)?;
        let mut entries = Vec::with_capacity(count);
        let mut old = self.entries.iter().copied().peekable();
        for &series in keys {
            while old.peek().is_some_and(|m| m.series < series) {
                entries.extend(old.next());
            }
            let previous = if old.peek().is_some_and(|m| m.series == series) {
                old.next().map_or(0, |m| m.commit_id)
            } else {
                0
            };
            entries.push(Marker {
                series,
                commit_id: previous.max(commit_id),
            });
        }
        entries.extend(old);
        Ok(Arc::new(Self {
            epoch: self.epoch.max(commit_id),
            entries,
            memory: Some(memory),
        }))
    }

    pub fn eligible(&self, materialized: u64, governor: &Arc<MemoryGovernor>) -> Result<Arc<Self>> {
        self.selected(self.epoch.min(materialized), governor, |m| {
            m.commit_id <= materialized
        })
    }

    pub fn collected(&self, swept: &Self, governor: &Arc<MemoryGovernor>) -> Result<Arc<Self>> {
        self.selected(self.epoch, governor, |m| {
            m.commit_id > swept.cutoff(m.series)
        })
    }

    fn selected(
        &self,
        epoch: u64,
        governor: &Arc<MemoryGovernor>,
        keep: impl Fn(&Marker) -> bool,
    ) -> Result<Arc<Self>> {
        let count = self.entries.iter().filter(|m| keep(m)).count();
        let memory = Self::reserve(governor, count)?;
        let mut entries = Vec::with_capacity(count);
        entries.extend(self.entries.iter().filter(|m| keep(m)).copied());
        Ok(Arc::new(Self {
            epoch,
            entries,
            memory: Some(memory),
        }))
    }
}
