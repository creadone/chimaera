//! Balance bounded edits before publishing new pages. No state survives a chunk.
use super::{Edit, MAX_EDITS};
use crate::persistence::catalog::page::{
    BRANCH_CAP, HEADER, Link, MAX_DEPTH, MAX_LEAF_RECORDS, Node, PAGE, PageId, Record,
};
use crate::persistence::catalog::writer::{MAX_RETIRED, Transaction, Writer};
use anyhow::{Result, ensure};

const LEAF_BYTES: usize = PAGE - HEADER - 4;

pub(in crate::persistence::catalog) fn scratch_bytes(
    height: usize,
    entries: u64,
    edit_count: usize,
) -> u64 {
    // A small catalogue cannot have full-sized sibling vectors or retire the
    // maximum transaction's pages. Keep small-memory databases admissible.
    let records = entries.saturating_add(edit_count as u64).min(65_536) as usize;
    let edits = edit_count.min(MAX_EDITS);
    let links = (BRANCH_CAP * 2 + edits).min(records);
    let rows = (MAX_LEAF_RECORDS + edits).min(records);
    // Ancestors retain branch links; decoded leaf payloads coexist only at the
    // active depth. Inserted payloads are charged separately by prepare.
    let branches = height.saturating_sub(1) * (PAGE + 2 * links * std::mem::size_of::<Link>());
    let leaf = 3 * PAGE + 4 * rows * std::mem::size_of::<Record>();
    let retired = (3 * height * (edits + 1))
        .min(height * records)
        .min(MAX_RETIRED);
    // Retirement IDs and their encoded journal coexist until the chunk ends.
    (branches + leaf + 2 * retired.next_power_of_two() * std::mem::size_of::<PageId>()) as u64
}

pub(in crate::persistence::catalog) fn apply(
    writer: &mut Writer,
    tx: &mut Transaction,
    root: Option<Link>,
    height: usize,
    edits: &[Edit],
) -> Result<(Option<Link>, usize)> {
    let mut nodes = if let Some(root) = root {
        edit(writer, tx, root, edits, 0)?
    } else {
        let records = edits
            .iter()
            .filter_map(|e| e.value.clone())
            .collect::<Vec<_>>();
        tx.added = records.len() as u64;
        leaves(records)
    };
    let mut height = height.max(1);
    while nodes.len() > 1 {
        let links = nodes
            .into_iter()
            .map(|node| tx.write(writer, node))
            .collect::<Result<_>>()?;
        nodes = branches(links);
        height += 1;
        ensure!(height <= MAX_DEPTH, "catalog depth limit");
    }
    let Some(node) = nodes.pop() else {
        return Ok((None, 0));
    };
    if let Node::Branch(children) = &node
        && children.len() == 1
    {
        let mut link = children[0];
        height = height
            .checked_sub(1)
            .filter(|h| *h > 0)
            .ok_or_else(|| anyhow::anyhow!("catalog root height"))?;
        while height > 1 {
            let page = tx.read(writer, link)?;
            let Node::Branch(children) = &page else {
                anyhow::bail!("catalog root depth");
            };
            if children.len() != 1 {
                break;
            }
            let child = children[0];
            tx.discard(writer, link.page)?;
            link = child;
            height -= 1;
        }
        return Ok((Some(link), height));
    }
    Ok((Some(tx.write(writer, node)?), height))
}

fn edit(
    writer: &mut Writer,
    tx: &mut Transaction,
    link: Link,
    edits: &[Edit],
    depth: usize,
) -> Result<Vec<Node>> {
    ensure!(depth < MAX_DEPTH, "catalog depth limit");
    let node = tx.read(writer, link)?;
    tx.discard(writer, link.page)?;
    match node {
        Node::Leaf(mut records) => {
            for change in edits {
                match (
                    records.binary_search_by_key(&change.key, |r| r.key),
                    &change.value,
                ) {
                    (Ok(at), Some(record)) => records[at] = record.clone(),
                    (Ok(at), None) => {
                        records.remove(at);
                        tx.removed += 1;
                    }
                    (Err(at), Some(record)) => {
                        records.insert(at, record.clone());
                        tx.added += 1;
                    }
                    (Err(_), None) => {}
                }
            }
            Ok(leaves(records))
        }
        Node::Branch(links) => {
            let mut siblings = Siblings::default();
            let mut start = 0;
            for (i, child) in links.iter().enumerate() {
                let end = links.get(i + 1).map_or(edits.len(), |next| {
                    edits.partition_point(|e| e.key < next.first)
                });
                if start == end {
                    siblings.push(writer, tx, Child::Existing(*child, false), depth + 1)?;
                } else {
                    for node in edit(writer, tx, *child, &edits[start..end], depth + 1)? {
                        siblings.push(writer, tx, Child::Changed(node), depth + 1)?;
                    }
                }
                start = end;
            }
            Ok(branches(siblings.finish(writer, tx)?))
        }
    }
}

// One sibling remains unwritten so a following underflow can borrow from it.
// Unchanged pages are opened only when an adjacent changed page needs repair.
// Only one pending value per depth; boxing would allocate on every unchanged link.
#[allow(clippy::large_enum_variant)]
enum Child {
    Existing(Link, bool),
    Changed(Node),
}
impl Child {
    fn sparse(&self) -> bool {
        match self {
            Self::Existing(_, sparse) => *sparse,
            Self::Changed(node) => sparse(node),
        }
    }
    fn publish(self, writer: &mut Writer, tx: &mut Transaction) -> Result<Link> {
        match self {
            Self::Existing(link, _) => Ok(link),
            Self::Changed(node) => tx.write(writer, node),
        }
    }
    fn take(self, writer: &mut Writer, tx: &mut Transaction) -> Result<Node> {
        match self {
            Self::Existing(link, _) => {
                let node = tx.read(writer, link)?;
                tx.discard(writer, link.page)?;
                Ok(node)
            }
            Self::Changed(node) => Ok(node),
        }
    }
}

#[derive(Default)]
struct Siblings {
    output: Vec<Link>,
    pending: Option<Child>,
}
impl Siblings {
    fn push(
        &mut self,
        writer: &mut Writer,
        tx: &mut Transaction,
        child: Child,
        depth: usize,
    ) -> Result<()> {
        let Some(previous) = self.pending.take() else {
            self.pending = Some(child);
            return Ok(());
        };
        if previous.sparse() || child.sparse() {
            let left = previous.take(writer, tx)?;
            let right = child.take(writer, tx)?;
            let mut nodes = merge(writer, tx, left, right, depth)?;
            self.pending = nodes.pop().map(Child::Changed);
            for node in nodes {
                self.output.push(tx.write(writer, node)?);
            }
        } else {
            self.output.push(previous.publish(writer, tx)?);
            self.pending = Some(child);
        }
        Ok(())
    }
    fn finish(mut self, writer: &mut Writer, tx: &mut Transaction) -> Result<Vec<Link>> {
        if let Some(child) = self.pending {
            self.output.push(child.publish(writer, tx)?);
        }
        Ok(self.output)
    }
}

fn sparse(node: &Node) -> bool {
    match node {
        Node::Leaf(rows) => {
            rows.len() < MAX_LEAF_RECORDS / 3
                && rows.iter().map(Record::size).sum::<usize>() < LEAF_BYTES / 3
        }
        Node::Branch(links) => links.len() < BRANCH_CAP / 3,
    }
}

fn merge(
    writer: &mut Writer,
    tx: &mut Transaction,
    left: Node,
    right: Node,
    depth: usize,
) -> Result<Vec<Node>> {
    ensure!(depth < MAX_DEPTH, "catalog rebalance depth limit");
    match (left, right) {
        (Node::Leaf(mut left), Node::Leaf(right)) => {
            left.extend(right);
            Ok(leaves(left))
        }
        (Node::Branch(left), Node::Branch(right)) => {
            // A whole batch can leave a branch with one sparse descendant.
            // Joining its parent must also repair this newly adjacent boundary.
            let mut siblings = Siblings::default();
            for link in left.into_iter().chain(right) {
                let page = tx.read(writer, link)?;
                let underfull = sparse(&page);
                drop(page);
                siblings.push(writer, tx, Child::Existing(link, underfull), depth + 1)?;
            }
            Ok(branches(siblings.finish(writer, tx)?))
        }
        _ => anyhow::bail!("catalog sibling depth mismatch"),
    }
}

fn branches(links: Vec<Link>) -> Vec<Node> {
    if links.is_empty() {
        return Vec::new();
    }
    let count = links.len().div_ceil(BRANCH_CAP);
    let size = links.len().div_ceil(count);
    links
        .chunks(size)
        .map(|part| Node::Branch(part.to_vec()))
        .collect()
}

fn leaves(mut rows: Vec<Record>) -> Vec<Node> {
    if rows.is_empty() {
        return Vec::new();
    }
    let bytes = rows.iter().map(Record::size).sum::<usize>();
    if rows.len() <= MAX_LEAF_RECORDS && bytes <= LEAF_BYTES {
        return vec![Node::Leaf(rows)];
    }
    // Balance by bytes when space is exhausted; otherwise by record count.
    // Choosing the nearest boundary prevents one large record skewing a split.
    let split = if bytes > LEAF_BYTES {
        let mut prefix = 0;
        (1..rows.len())
            .min_by_key(|&i| {
                prefix += rows[i - 1].size();
                (2 * prefix).abs_diff(bytes)
            })
            .expect("validated records fit individually")
    } else {
        rows.len() / 2
    };
    let right = rows.split_off(split);
    let mut output = leaves(rows);
    output.extend(leaves(right));
    output
}
