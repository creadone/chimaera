//! One prepare call owns this bounded write buffer. Readers only see flushed roots.
use super::page::{Link, Node, PAGE, PageId};
use super::writer::Writer;
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use anyhow::{Result, ensure};
use std::sync::Arc;

const SLOTS: usize = 64;
const MAX_BYTES: u64 = 1 << 20;

struct Buffered {
    link: Link,
    node: Node,
    _memory: WorkMemoryReservation,
}

pub(super) struct Pages {
    slots: Vec<Option<Buffered>>,
    bytes: u64,
    memory: WorkMemoryReservation,
    work: Arc<WorkMemoryGovernor>,
}

impl Pages {
    pub(super) fn new(work: &Arc<WorkMemoryGovernor>) -> Result<Self> {
        let mut memory = work.reserve_background(WorkMemoryClass::Compaction, 0)?;
        // Optional optimization: a small work budget must still allow streaming.
        let slots = if memory.try_grow((SLOTS * std::mem::size_of::<Option<Buffered>>()) as u64)? {
            (0..SLOTS).map(|_| None).collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            slots,
            bytes: 0,
            memory,
            work: Arc::clone(work),
        })
    }

    fn index(&self, id: PageId) -> Option<usize> {
        (!self.slots.is_empty()).then(|| id.address as usize % self.slots.len())
    }

    pub(super) fn read(&self, link: Link) -> Result<Option<Node>> {
        let Some(page) = self
            .index(link.page)
            .and_then(|at| self.slots[at].as_ref())
            .filter(|page| page.link.page == link.page)
        else {
            return Ok(None);
        };
        ensure!(page.link == link, "private catalog page link mismatch");
        Ok(Some(page.node.clone()))
    }

    pub(super) fn remove(&mut self, id: PageId) -> bool {
        let Some(at) = self.index(id) else {
            return false;
        };
        if self.slots[at]
            .as_ref()
            .is_none_or(|page| page.link.page != id)
        {
            return false;
        }
        let page = self.slots[at].take().expect("matched private page");
        self.bytes -= charge(&page.node);
        true
    }

    pub(super) fn write(&mut self, writer: &mut Writer, link: Link, node: Node) -> Result<()> {
        let Some(at) = self.index(link.page) else {
            return write(writer, link, &node);
        };
        self.spill(writer, at)?;
        let bytes = charge(&node);
        if bytes > MAX_BYTES {
            return write(writer, link, &node);
        }
        if self.bytes + bytes > MAX_BYTES {
            self.flush(writer)?;
        }
        let mut memory = self.work.try_reserve_background_with_headroom(
            WorkMemoryClass::Compaction,
            bytes,
            0,
        )?;
        if memory.is_none() {
            self.flush(writer)?;
            memory = self.work.try_reserve_background_with_headroom(
                WorkMemoryClass::Compaction,
                bytes,
                0,
            )?;
        }
        if let Some(memory) = memory {
            self.slots[at] = Some(Buffered {
                link,
                node,
                _memory: memory,
            });
            self.bytes += bytes;
            Ok(())
        } else {
            write(writer, link, &node)
        }
    }

    fn spill(&mut self, writer: &mut Writer, at: usize) -> Result<()> {
        if let Some(page) = self.slots[at].take() {
            self.bytes -= charge(&page.node);
            write(writer, page.link, &page.node)?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self, writer: &mut Writer) -> Result<()> {
        for at in 0..self.slots.len() {
            self.spill(writer, at)?;
        }
        Ok(())
    }

    /// Release even the slot table before retrying a mandatory scratch reservation.
    pub(super) fn disable(&mut self, writer: &mut Writer) -> Result<()> {
        self.flush(writer)?;
        self.slots = Vec::new();
        self.memory = self
            .work
            .reserve_background(WorkMemoryClass::Compaction, 0)?;
        Ok(())
    }
}

fn charge(node: &Node) -> u64 {
    // Encoding during eviction/final flush also stays inside the work budget.
    (node.heap_bytes() + PAGE) as u64
}

fn write(writer: &mut Writer, link: Link, node: &Node) -> Result<()> {
    let (actual, bytes) = node.encode(link.page, writer.reader.uuid)?;
    ensure!(actual == link, "private catalog page changed");
    writer
        .arena
        .write(link.page.address, &bytes, &mut writer.counters)
}
