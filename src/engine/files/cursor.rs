//! One bounded catalogue page, released before advancing to the next page.
use super::*;
use crate::persistence::catalog::{RecordPage, records::SegmentEntry};
use crate::persistence::layout::Domain;

pub(in crate::engine) struct Cursor<'a> {
    view: &'a View,
    domain: Domain,
    page: Option<RecordPage>,
    at: usize,
    after: Option<Key>,
    finished: bool,
}
impl<'a> Cursor<'a> {
    pub fn new(view: &'a View, domain: Domain) -> Self {
        Self {
            view,
            domain,
            page: None,
            at: 0,
            after: None,
            finished: false,
        }
    }
    pub fn next(&mut self) -> Result<Option<SegmentEntry>> {
        if self.finished {
            return Ok(None);
        }
        if self.page.as_ref().is_none_or(|p| self.at == p.len()) {
            self.page = None;
            let d = self.domain;
            let page = self.view.snapshot.page_range(
                records::scope(records::SEGMENT, d.policy_id, d.shard_id),
                d.window_start_us,
                d.window_end_us,
                self.after,
                32,
            )?;
            if page.is_empty() {
                self.finished = true;
                return Ok(None);
            }
            self.after = page.last().map(|r| r.key);
            self.page = Some(page);
            self.at = 0;
        }
        let row = &self.page.as_ref().expect("loaded cursor page")[self.at];
        self.at += 1;
        let entry = records::segment(row)?;
        if entry.descriptor.domain() != self.domain {
            return Err(corrupt("catalog cursor key/domain bounds"));
        }
        Ok(Some(entry))
    }

    /// The caller has already accounted for every start at or before `end`.
    pub(super) fn seek_after(&mut self, end: i64) {
        self.page = None;
        self.at = 0;
        self.after = Some(Key {
            scope: records::scope(
                records::SEGMENT,
                self.domain.policy_id,
                self.domain.shard_id,
            ),
            series: Default::default(),
            start: end,
            id: u64::MAX,
        });
        self.finished = end >= self.domain.window_end_us - 1;
    }
}
