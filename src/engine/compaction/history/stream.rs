//! Same earliest-start/geometric rule as the reference, retaining at most two
//! fan-in windows. Oversized/unsafe overlap components keep only their end.
use super::*;
use crate::error::Result;
use crate::memory::work::{WorkMemoryClass, WorkMemoryGovernor, WorkMemoryReservation};
use std::collections::VecDeque;
use std::sync::Arc;

pub(in crate::engine) struct Selection {
    parts: Vec<SegmentDescriptor>,
    _memory: WorkMemoryReservation,
}
impl std::ops::Deref for Selection {
    type Target = [SegmentDescriptor];
    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

/// A key-ordered descriptor stream. Once a component cannot be selected,
/// catalogue sources may skip its remaining overlaps using subtree summaries.
pub(in crate::engine) trait Source {
    fn next(&mut self) -> Result<Option<SegmentDescriptor>>;

    /// Consume only descriptors connected to `end` and return their maximum end.
    /// Leaving the stream untouched is valid for sources without a time index.
    fn skip_blocked(&mut self, end: i64) -> Result<i64> {
        Ok(end)
    }
}

impl<F: FnMut() -> Result<Option<SegmentDescriptor>>> Source for F {
    fn next(&mut self) -> Result<Option<SegmentDescriptor>> {
        self()
    }
}

#[derive(Default)]
struct Component {
    parts: Vec<SegmentDescriptor>,
    end: i64,
    bytes: u64,
    largest: u64,
    blocked: bool,
    bucket: i64,
}
impl Component {
    fn add(&mut self, d: SegmentDescriptor, before: i64, limit: u64, window_us: i64) {
        let bucket = d.min_timestamp_us.div_euclid(window_us);
        let different_window = !self.parts.is_empty() && self.bucket != bucket;
        self.bucket = bucket;
        self.end = if self.parts.is_empty() && !self.blocked {
            d.max_timestamp_us
        } else {
            self.end.max(d.max_timestamp_us)
        };
        self.bytes = self.bytes.saturating_add(d.file_size);
        self.largest = self.largest.max(d.file_size);
        self.blocked |= d.level != 2
            || different_window
            || bucket != d.max_timestamp_us.div_euclid(window_us)
            || !is_sealed_descriptor(&d, before)
            || self.parts.len() == ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            || self.bytes > limit;
        if self.blocked {
            self.parts.clear();
        } else {
            self.parts.push(d);
        }
    }
}

#[derive(Default)]
struct Window {
    components: VecDeque<Component>,
    count: usize,
    bytes: u64,
}
impl Window {
    fn pop(&mut self) {
        if let Some(c) = self.components.pop_front() {
            self.count -= c.parts.len();
            self.bytes -= c.bytes;
        }
    }
    fn candidate(&self, trigger: usize) -> Option<usize> {
        let (mut count, mut bytes, mut largest, mut best) = (0, 0_u64, 0_u64, None);
        for (i, c) in self.components.iter().enumerate() {
            count += c.parts.len();
            bytes = bytes.saturating_add(c.bytes);
            largest = largest.max(c.largest);
            if count >= trigger && bytes.saturating_sub(largest) >= largest {
                best = Some(i + 1);
            }
        }
        best
    }
    fn take(&mut self, end: usize) -> Vec<SegmentDescriptor> {
        let mut out = self
            .components
            .drain(..end)
            .flat_map(|c| c.parts)
            .collect::<Vec<_>>();
        out.sort_unstable_by_key(|d| (d.min_timestamp_us, d.max_timestamp_us, d.id));
        out
    }
    fn finish(&mut self, trigger: usize) -> Vec<SegmentDescriptor> {
        while !self.components.is_empty() {
            if let Some(end) = self.candidate(trigger) {
                return self.take(end);
            }
            self.pop();
        }
        Vec::new()
    }
    fn push(
        &mut self,
        component: Component,
        trigger: usize,
        limit: u64,
    ) -> Option<Vec<SegmentDescriptor>> {
        if component.blocked {
            let found = self.finish(trigger);
            return (!found.is_empty()).then_some(found);
        }
        if self
            .components
            .back()
            .is_some_and(|last| last.bucket != component.bucket)
        {
            let found = self.finish(trigger);
            if !found.is_empty() {
                return Some(found);
            }
        }
        while self.count + component.parts.len() > ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            || self.bytes.saturating_add(component.bytes) > limit
        {
            if let Some(end) = self.candidate(trigger) {
                return Some(self.take(end));
            }
            self.pop();
        }
        self.count += component.parts.len();
        self.bytes += component.bytes;
        self.components.push_back(component);
        if self.count == ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS
            && let Some(end) = self.candidate(trigger)
        {
            return Some(self.take(end));
        }
        None
    }
}

pub(in crate::engine) fn select(
    mut source: impl Source,
    before: i64,
    target_bytes: u64,
    trigger: usize,
    window_us: i64,
    work: &Arc<WorkMemoryGovernor>,
) -> Result<Selection> {
    if window_us <= 0 {
        return Err(crate::error::corrupt("invalid historical time window"));
    }
    let trigger = trigger.max(2);
    // Keep the reservation alive while the caller opens the selected inputs.
    let memory = work.reserve_background(WorkMemoryClass::Compaction, 128 << 10)?;
    let selected = |parts| Selection {
        parts,
        _memory: memory,
    };
    if trigger > ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS {
        return Ok(selected(Vec::new()));
    }
    // Two windows, descriptor paths and deque slack; all are bounded by fan-in.
    let limit = target_bytes.min(ACTIVE_COMPACTION_MAX_INPUT_BYTES);
    let mut window = Window::default();
    let mut current: Option<Component> = None;
    while let Some(d) = source.next()? {
        if current.as_ref().is_some_and(|c| d.min_timestamp_us > c.end)
            && let Some(component) = current.take()
            && let Some(found) = window.push(component, trigger, limit)
        {
            return Ok(selected(found));
        }
        let component = current.get_or_insert_with(Component::default);
        component.add(d, before, limit, window_us);
        if component.blocked {
            let found = window.finish(trigger);
            if !found.is_empty() {
                return Ok(selected(found));
            }
            component.end = source.skip_blocked(component.end)?;
        }
    }
    if let Some(component) = current
        && let Some(found) = window.push(component, trigger, limit)
    {
        return Ok(selected(found));
    }
    Ok(selected(window.finish(trigger)))
}
