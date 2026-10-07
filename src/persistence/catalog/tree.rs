//! Bounded COW edits and streaming interval selection.
use super::page::{Key, Link, MAX_DEPTH, Node, PageId, Record};
use super::reader::Reader;
use crate::diagnostics::publication::{self as diagnostics, Counter};
use crate::query::QueryBudget;
use anyhow::{Result, ensure};

pub(crate) const MAX_EDITS: usize = 512;
mod edit;
pub(super) use edit::{apply, scratch_bytes};

#[derive(Clone)]
pub(crate) struct Edit {
    pub key: Key,
    pub value: Option<Record>,
}

pub(super) struct Range {
    pub minimum_scope: u64,
    pub maximum_scope: u64,
    pub series: Option<crate::types::SeriesKey>,
    pub from: i64,
    pub to: i64,
    pub levels: u8,
}

impl Range {
    fn contains(&self, r: &Record) -> bool {
        (self.levels == 0 || r.summary.is_some_and(|s| s.matches(self.levels)))
            && r.key.scope >= self.minimum_scope
            && r.key.scope <= self.maximum_scope
            && self.series.is_none_or(|s| s == r.key.series)
            && r.key.start < self.to
            && r.end > self.from
    }
    fn overlaps(&self, l: Link) -> bool {
        (self.levels == 0 || l.summary.matches(self.levels))
            && l.first.scope <= self.maximum_scope
            && l.last.scope >= self.minimum_scope
            && self.series.is_none_or(|s| {
                (l.first.scope, l.first.series) <= (self.maximum_scope, s)
                    && (l.last.scope, l.last.series) >= (self.minimum_scope, s)
            })
            && l.max_end > self.from
            && ((l.first.scope, l.first.series) != (l.last.scope, l.last.series)
                || l.first.start < self.to)
    }
}

/// Ignore changed keys in the immutable tree; the caller folds their new
/// values separately. Fully enclosed, unchanged subtrees need no page read.
pub(super) fn summarize(
    reader: &Reader,
    link: Link,
    lo: Key,
    hi: Key,
    excluded: &[Key],
    depth: usize,
) -> Result<super::Summary> {
    ensure!(depth < MAX_DEPTH, "catalog summary depth limit");
    let mut output = super::Summary::default();
    if link.last < lo || link.first >= hi {
        return Ok(output);
    }
    diagnostics::add(Counter::CatalogSummaryNodes, 1);
    let first = excluded.partition_point(|k| *k < link.first);
    let last = excluded.partition_point(|k| *k <= link.last);
    let excluded = &excluded[first..last];
    if excluded.is_empty() && lo <= link.first && link.last < hi {
        return Ok(link.summary);
    }
    match &**reader.read(link, None)? {
        Node::Leaf(rows) => {
            for row in rows {
                if row.key >= lo
                    && row.key < hi
                    && excluded.binary_search(&row.key).is_err()
                    && let Some(summary) = row.summary
                {
                    diagnostics::add(Counter::CatalogSummaryRecords, 1);
                    output.merge(&summary)?;
                }
            }
        }
        Node::Branch(links) => {
            for child in links {
                output.merge(&summarize(reader, *child, lo, hi, excluded, depth + 1)?)?;
            }
        }
    }
    Ok(output)
}

pub(super) fn visit(
    reader: &Reader,
    root: Link,
    range: &Range,
    depth: usize,
    budget: Option<&QueryBudget>,
    consumer: &mut impl FnMut(&Record) -> Result<()>,
    page: &mut impl FnMut(PageId) -> Result<()>,
) -> Result<u64> {
    ensure!(depth < MAX_DEPTH, "catalog depth limit");
    if !range.overlaps(root) {
        return Ok(0);
    }
    page(root.page)?;
    match &**reader.read(root, budget)? {
        Node::Leaf(records) => {
            let mut count = 0;
            for record in records.iter().filter(|r| range.contains(r)) {
                if let Some(budget) = budget {
                    budget.check()?;
                }
                consumer(record)?;
                count += 1;
            }
            Ok(count)
        }
        Node::Branch(links) => {
            let mut count = 0;
            for link in links {
                count += visit(reader, *link, range, depth + 1, budget, consumer, page)?;
            }
            Ok(count)
        }
    }
}

pub(super) fn get(
    reader: &Reader,
    mut link: Link,
    key: Key,
    budget: Option<&QueryBudget>,
) -> Result<Option<Record>> {
    for _ in 0..MAX_DEPTH {
        if key < link.first || key > link.last {
            return Ok(None);
        }
        match &**reader.read(link, budget)? {
            Node::Leaf(records) => {
                return Ok(records
                    .binary_search_by_key(&key, |r| r.key)
                    .ok()
                    .map(|i| records[i].clone()));
            }
            Node::Branch(links) => {
                let at = links.partition_point(|l| l.first <= key);
                if at == 0 {
                    return Ok(None);
                }
                link = links[at - 1];
            }
        }
    }
    anyhow::bail!("catalog get depth limit")
}

/// Keyset pagination prunes the consumed prefix, including after slot reuse.
#[allow(clippy::too_many_arguments)]
pub(super) fn page(
    reader: &Reader,
    link: Link,
    range: &Range,
    after: Option<Key>,
    limit: usize,
    depth: usize,
    output: &mut Vec<Record>,
    budget: Option<&QueryBudget>,
    charge: &mut impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    ensure!(depth < MAX_DEPTH, "catalog page depth limit");
    if output.len() == limit || !range.overlaps(link) || after.is_some_and(|key| link.last <= key) {
        return Ok(());
    }
    if let Some(budget) = budget {
        budget.check()?;
    }
    match &**reader.read(link, budget)? {
        Node::Leaf(records) => {
            for record in records {
                if range.contains(record) && after.is_none_or(|key| record.key > key) {
                    charge(record.bytes.capacity() as u64)?;
                    output.push(record.clone());
                    if output.len() == limit {
                        break;
                    }
                }
            }
        }
        Node::Branch(links) => {
            for child in links {
                page(
                    reader,
                    *child,
                    range,
                    after,
                    limit,
                    depth + 1,
                    output,
                    budget,
                    charge,
                )?;
                if output.len() == limit {
                    break;
                }
            }
        }
    }
    Ok(())
}
