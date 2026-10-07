use super::*;
use crate::engine::compaction::history;
use crate::engine::files::Cursor;

fn bounds(domain: Domain) -> (Key, Key) {
    let scope = records::scope(records::SEGMENT, domain.policy_id, domain.shard_id);
    (
        Key {
            series: Default::default(),
            scope,
            start: domain.window_start_us,
            id: 0,
        },
        Key {
            series: Default::default(),
            scope,
            start: domain.window_end_us,
            id: 0,
        },
    )
}

impl Builder<'_> {
    fn summarize_range(
        &self,
        lo: Key,
        hi: Key,
        excluded: &[Key],
    ) -> Result<crate::persistence::catalog::Summary> {
        let mut value = self.base.snapshot.summarize(lo, hi, excluded)?;
        for row in self
            .changes
            .range(lo..hi)
            .filter_map(|(_, row)| row.as_ref())
        {
            value
                .merge(
                    &row.summary
                        .ok_or_else(|| corrupt("segment without summary"))?,
                )
                .map_err(|error| corrupt(error.to_string()))?;
        }
        Ok(value)
    }

    pub(super) fn summarize_domain(
        &self,
        domain: Domain,
        target: u64,
        trigger: usize,
        window_us: i64,
    ) -> Result<DomainEntry> {
        let (lo, hi) = bounds(domain);
        let count = self.changes.range(lo..hi).count();
        let _memory = self.work.reserve_background(
            WorkMemoryClass::Compaction,
            count as u64 * std::mem::size_of::<Key>() as u64,
        )?;
        let excluded = self
            .changes
            .range(lo..hi)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        let value = self.summarize_range(lo, hi, &excluded)?;
        let mut summary = DomainEntry::from_summary(&value, target, trigger);
        summary.history_window_us = window_us;
        summary.history_merge = summary.counts[2] >= trigger.max(2) as u64
            && summary.minimum_l2_bytes.is_some_and(|size| size < target);
        if summary.history_merge && summary.counts[0] == 0 && summary.counts[1] == 0 {
            // A refresh or an unrelated publication cannot change the proof.
            let previous = self.base.domain_metadata(domain)?;
            summary.history_merge = if excluded.is_empty()
                && let Some(previous) = previous
                && previous.history_target_bytes == target
                && previous.history_trigger == trigger
                && previous.history_window_us == window_us
            {
                previous.history_merge
            } else {
                let source = History {
                    builder: self,
                    lo,
                    hi,
                    excluded: &excluded,
                    changes: self.changes.range(lo..hi).peekable(),
                    cursor: Cursor::new(self.base, domain),
                    old: None,
                    exhausted: false,
                };
                !history::select(source, i64::MAX, target, trigger, window_us, &self.work)?
                    .is_empty()
            };
        }
        Ok(summary)
    }
}

/// Merge the published tree and this proposal. The changed-key slice already
/// belongs to summarize_domain's bounded work reservation; no extra index lives
/// beyond this call.
struct History<'a, 'base> {
    builder: &'a Builder<'base>,
    lo: Key,
    hi: Key,
    excluded: &'a [Key],
    changes: std::iter::Peekable<std::collections::btree_map::Range<'a, Key, Option<Record>>>,
    cursor: Cursor<'base>,
    old: Option<SegmentEntry>,
    exhausted: bool,
}

impl history::Source for History<'_, '_> {
    fn next(&mut self) -> Result<Option<SegmentDescriptor>> {
        while self.old.is_none() && !self.exhausted {
            match self.cursor.next()? {
                Some(entry) if !self.builder.changes.contains_key(&entry.key()) => {
                    diagnostics::add(Counter::PublicationHistoryDescriptors, 1);
                    self.old = Some(entry)
                }
                Some(_) => {}
                None => self.exhausted = true,
            }
        }
        while self.changes.peek().is_some_and(|(_, row)| row.is_none()) {
            self.changes.next();
        }
        if let Some((key, _)) = self.changes.peek()
            && self.old.as_ref().is_none_or(|entry| **key < entry.key())
        {
            let (_, row) = self.changes.next().expect("peeked change");
            let entry = records::segment(row.as_ref().expect("nonempty change"))?;
            diagnostics::add(Counter::PublicationHistoryDescriptors, 1);
            return Ok(Some(entry.descriptor));
        }
        Ok(self.old.take().map(|entry| entry.descriptor))
    }

    fn skip_blocked(&mut self, mut end: i64) -> Result<i64> {
        loop {
            // All starts <= end overlap the rejected component or precede it.
            // Previously consumed components end before this one, so their
            // summaries cannot extend this boundary. Include replacements and
            // exclude deletions before computing each extension.
            let hi = Key {
                start: end.saturating_add(1).min(self.hi.start),
                ..self.hi
            };
            let summary = self.builder.summarize_range(self.lo, hi, self.excluded)?;
            let next = summary.maximum.into_iter().fold(end, i64::max);
            if next == end {
                self.cursor.seek_after(end);
                self.old = None;
                self.exhausted = false;
                self.changes = self.builder.changes.range(hi..self.hi).peekable();
                return Ok(end);
            }
            end = next;
        }
    }
}
