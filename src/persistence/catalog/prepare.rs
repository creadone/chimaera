//! One private root, fed by bounded batches; only the manifest publishes it.
use super::writer::{Transaction, Writer};
use super::{Edit, Root, external, page, tree, work_busy};
use crate::error::{Result, corrupt};
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use anyhow::{Context, ensure};
use std::sync::Arc;

/// Owned by a single prepare call. At most MAX_EDITS records are buffered;
/// reservations are released on every error and payloads after each batch.
pub(crate) struct Preparation<'a> {
    writer: &'a mut Writer,
    work: &'a Arc<WorkMemoryGovernor>,
    pub(super) root: Root,
    edits: Vec<Edit>,
    slots: WorkMemoryReservation,
    payload: WorkMemoryReservation,
    base_generation: u64,
    pages: super::private::Pages,
}

impl<'a> Preparation<'a> {
    pub(super) fn new(
        writer: &'a mut Writer,
        work: &'a Arc<WorkMemoryGovernor>,
        root: Root,
    ) -> Result<Self> {
        Ok(Self {
            writer,
            work,
            root,
            edits: Vec::new(),
            slots: work.reserve_background(WorkMemoryClass::Compaction, 0)?,
            payload: work.reserve_background(WorkMemoryClass::Compaction, 0)?,
            base_generation: root.generation,
            pages: super::private::Pages::new(work).map_err(external)?,
        })
    }

    /// The producer owns key uniqueness across batches. Each batch is sorted
    /// here; repeated keys within a batch are rejected, never silently lost.
    pub(crate) fn push(&mut self, edit: Edit) -> Result<()> {
        if edit.value.as_ref().is_some_and(|r| {
            r.key != edit.key || r.end <= r.key.start || r.bytes.len() > page::MAX_RECORD_BYTES
        }) {
            return Err(corrupt("catalog edit value"));
        }
        if self.edits.len() == self.edits.capacity() {
            let capacity = (self.edits.capacity() * 2).clamp(4, tree::MAX_EDITS);
            let bytes = ((capacity - self.edits.capacity()) * std::mem::size_of::<Edit>()) as u64;
            if !self.slots.try_grow(bytes)? {
                self.pages.disable(self.writer).map_err(external)?;
                if !self.slots.try_grow(bytes)? {
                    return Err(work_busy(self.work, "edit buffer", bytes));
                }
            }
            self.edits.reserve_exact(capacity - self.edits.len());
        }
        let bytes = edit.value.as_ref().map_or(0, |r| r.bytes.capacity() as u64);
        if !self.payload.try_grow(bytes)? {
            self.pages.disable(self.writer).map_err(external)?;
            if !self.payload.try_grow(bytes)? {
                return Err(work_busy(self.work, "edit payload", bytes));
            }
        }
        self.edits.push(edit);
        if self.edits.len() == tree::MAX_EDITS {
            self.flush()?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        self.flush_inner().map_err(external)
    }

    pub(super) fn finish_pages(&mut self) -> Result<()> {
        self.pages.flush(self.writer).map_err(external)
    }

    fn flush_inner(&mut self) -> anyhow::Result<()> {
        if self.edits.is_empty() {
            return Ok(());
        }
        self.edits.sort_unstable_by_key(|edit| edit.key);
        ensure!(
            self.edits.windows(2).all(|pair| pair[0].key < pair[1].key),
            "duplicate catalog edit key"
        );
        let bytes = self
            .edits
            .iter()
            .filter_map(|edit| edit.value.as_ref())
            .map(|record| record.size() as u64)
            .sum::<u64>();
        let work_bytes =
            tree::scratch_bytes(self.root.height + 1, self.root.entries, self.edits.len())
                + bytes * 4;
        let mut scratch = self.work.try_reserve_background_with_headroom(
            WorkMemoryClass::Compaction,
            work_bytes,
            0,
        )?;
        if scratch.is_none() {
            self.pages.disable(self.writer)?;
            scratch = self.work.try_reserve_background_with_headroom(
                WorkMemoryClass::Compaction,
                work_bytes,
                0,
            )?;
        }
        let _work = scratch.ok_or_else(|| work_busy(self.work, "prepare", work_bytes))?;
        let generation = self
            .root
            .generation
            .checked_add(1)
            .context("catalog generation overflow")?;
        // From the first page mutation on, any failure requires recovery from
        // the old manifest. No intermediate root may escape this preparation.
        self.writer.needs_reopen = true;
        let mut tx = Transaction::new(generation, self.base_generation, &mut self.pages);
        let (link, height) = tree::apply(
            self.writer,
            &mut tx,
            self.root.link,
            self.root.height,
            &self.edits,
        )?;
        self.writer.record_retired(&tx)?;
        self.root = Root {
            generation,
            link,
            height,
            entries: self
                .root
                .entries
                .checked_add(tx.added)
                .and_then(|n| n.checked_sub(tx.removed))
                .context("catalog entry count overflow")?,
        };
        self.edits.clear();
        self.payload = self
            .work
            .reserve_background(WorkMemoryClass::Compaction, 0)?;

        Ok(())
    }
}
