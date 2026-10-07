//! CRC-protected interval pages. Scope groups one kind/policy/shard in the tree.
use super::summary::{self, Summary};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(super) const PAGE: usize = 16 * 1024;
pub(super) const HEADER: usize = 48;
pub(super) const BRANCH_CAP: usize = 63;
pub(super) const MAX_DEPTH: usize = 16;
pub(super) const MAX_RECORD_BYTES: usize = 4096;
pub(super) const MAX_LEAF_RECORDS: usize = 127;
pub(super) const LINK_BYTES: usize = 112 + summary::BYTES;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Key {
    pub scope: u64,
    pub series: crate::types::SeriesKey,
    pub start: i64,
    pub id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) struct PageId {
    pub generation: u64,
    pub slot: u64,
    pub address: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Link {
    pub page: PageId,
    pub first: Key,
    pub last: Key,
    pub max_end: i64,
    pub summary: Summary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub key: Key,
    pub end: i64,
    pub bytes: Vec<u8>,
    pub summary: Option<Summary>,
}

impl Record {
    pub(super) fn size(&self) -> usize {
        52 + self.bytes.len()
            + if self.summary.is_some() {
                summary::BYTES
            } else {
                0
            }
    }
}

#[derive(Clone)]
pub(super) enum Node {
    Leaf(Vec<Record>),
    Branch(Vec<Link>),
}

impl Node {
    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Leaf(rows) => {
                rows.capacity() * std::mem::size_of::<Record>()
                    + rows.iter().map(|r| r.bytes.capacity()).sum::<usize>()
            }
            Self::Branch(links) => links.capacity() * std::mem::size_of::<Link>(),
        }
    }

    pub(super) fn summary(&self, page: PageId) -> Result<Link> {
        let mut summary = Summary::default();
        let (first, last, max_end) = match self {
            Self::Leaf(records) => {
                for record in records {
                    if let Some(value) = &record.summary {
                        summary.merge(value)?;
                    }
                }
                ensure!(
                    !records.is_empty() && records.len() <= MAX_LEAF_RECORDS,
                    "catalog leaf count"
                );
                ensure!(
                    records.windows(2).all(|w| w[0].key < w[1].key),
                    "catalog leaf order"
                );
                ensure!(
                    records
                        .iter()
                        .all(|r| r.end > r.key.start && r.bytes.len() <= MAX_RECORD_BYTES),
                    "catalog record interval/size"
                );
                ensure!(
                    HEADER + records.iter().map(Record::size).sum::<usize>() <= PAGE - 4,
                    "catalog leaf size"
                );
                (
                    records[0].key,
                    records[records.len() - 1].key,
                    records.iter().map(|r| r.end).max(),
                )
            }
            Self::Branch(links) => {
                for link in links {
                    summary.merge(&link.summary)?;
                }
                ensure!(
                    !links.is_empty() && links.len() <= BRANCH_CAP,
                    "catalog branch count"
                );
                ensure!(
                    links.windows(2).all(|w| w[0].last < w[1].first),
                    "catalog branch order"
                );
                ensure!(
                    links.iter().all(|l| l.first <= l.last
                        && l.max_end > l.first.start
                        && (l.page.generation, l.page.slot) < (page.generation, page.slot)
                        && l.page.generation > 0
                        && l.page.slot > 0
                        && l.page.address > 0),
                    "catalog branch link"
                );
                (
                    links[0].first,
                    links[links.len() - 1].last,
                    links.iter().map(|l| l.max_end).max(),
                )
            }
        };
        Ok(Link {
            page,
            first,
            last,
            max_end: max_end.expect("nonempty catalog page"),
            summary,
        })
    }

    pub(super) fn encode(&self, id: PageId, uuid: [u8; 16]) -> Result<(Link, Box<[u8; PAGE]>)> {
        let link = self.summary(id)?;
        let mut b = Box::new([0; PAGE]);
        b[..8].copy_from_slice(b"CHMCAT05");
        put(&mut b[..], 8, id.generation);
        put(&mut b[..], 16, id.slot);
        b[24..40].copy_from_slice(&uuid);
        match self {
            Self::Leaf(records) => {
                put(&mut b[..], 40, records.len() as u64);
                let mut at = HEADER;
                for r in records {
                    key_put(&mut b[..], at, r.key);
                    put(&mut b[..], at + 40, r.end as u64);
                    let tag = r.bytes.len() as u32 | if r.summary.is_some() { 1 << 31 } else { 0 };
                    b[at + 48..at + 52].copy_from_slice(&tag.to_le_bytes());
                    let mut payload = at + 52;
                    if let Some(summary) = r.summary {
                        summary.encode(&mut b[payload..payload + summary::BYTES]);
                        payload += summary::BYTES;
                    }
                    b[payload..at + r.size()].copy_from_slice(&r.bytes);
                    at += r.size();
                }
            }
            Self::Branch(links) => {
                put(&mut b[..], 40, (1 << 32) | links.len() as u64);
                for (i, l) in links.iter().enumerate() {
                    let at = HEADER + i * LINK_BYTES;
                    put(&mut b[..], at, l.page.generation);
                    put(&mut b[..], at + 8, l.page.slot);
                    put(&mut b[..], at + 16, l.page.address);
                    key_put(&mut b[..], at + 24, l.first);
                    key_put(&mut b[..], at + 64, l.last);
                    put(&mut b[..], at + 104, l.max_end as u64);
                    l.summary.encode(&mut b[at + 112..at + LINK_BYTES]);
                }
            }
        }
        let crc = crc32c::crc32c(&b[..PAGE - 4]);
        b[PAGE - 4..].copy_from_slice(&crc.to_le_bytes());
        Ok((link, b))
    }

    pub(super) fn decode(b: &[u8; PAGE], expected: Link, uuid: [u8; 16]) -> Result<Self> {
        ensure!(
            &b[..8] == b"CHMCAT05" && b[24..40] == uuid,
            "catalog page identity/format"
        );
        ensure!(
            crc32c::crc32c(&b[..PAGE - 4]) == u32::from_le_bytes(b[PAGE - 4..].try_into()?),
            "catalog page CRC"
        );
        ensure!(
            get(b, 8) == expected.page.generation && get(b, 16) == expected.page.slot,
            "catalog page version"
        );
        let tag = get(b, 40);
        let count = (tag & 0xffff_ffff) as usize;
        let node = match tag >> 32 {
            0 => {
                ensure!(
                    (1..=MAX_LEAF_RECORDS).contains(&count),
                    "catalog leaf count"
                );
                let mut records = Vec::with_capacity(count);
                let mut at = HEADER;
                for _ in 0..count {
                    ensure!(at + 52 <= PAGE - 4, "catalog truncated record");
                    let tag = u32::from_le_bytes(b[at + 48..at + 52].try_into()?);
                    let len = (tag & !(1 << 31)) as usize;
                    let extra = if tag >> 31 != 0 { summary::BYTES } else { 0 };
                    ensure!(
                        len <= MAX_RECORD_BYTES && at + 52 + extra + len <= PAGE - 4,
                        "catalog record length"
                    );
                    records.push(Record {
                        key: key_get(b, at),
                        end: get(b, at + 40) as i64,
                        bytes: b[at + 52 + extra..at + 52 + extra + len].to_vec(),
                        summary: if extra != 0 {
                            Some(Summary::decode(&b[at + 52..at + 52 + extra])?)
                        } else {
                            None
                        },
                    });
                    at += 52 + extra + len;
                }
                Self::Leaf(records)
            }
            1 => {
                ensure!((1..=BRANCH_CAP).contains(&count), "catalog branch count");
                Self::Branch(
                    (0..count)
                        .map(|i| {
                            let at = HEADER + i * LINK_BYTES;
                            Ok(Link {
                                page: PageId {
                                    generation: get(b, at),
                                    slot: get(b, at + 8),
                                    address: get(b, at + 16),
                                },
                                first: key_get(b, at + 24),
                                last: key_get(b, at + 64),
                                max_end: get(b, at + 104) as i64,
                                summary: Summary::decode(&b[at + 112..at + LINK_BYTES])?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            _ => anyhow::bail!("catalog page kind"),
        };
        ensure!(
            node.summary(expected.page)? == expected,
            "catalog page summary mismatch"
        );
        Ok(node)
    }
}

fn put(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}
fn get(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("bounded catalog field"))
}
fn key_put(b: &mut [u8], at: usize, k: Key) {
    put(b, at, k.scope);
    put(
        b,
        at + 8,
        (u64::from(k.series.space_id) << 32) | u64::from(k.series.bucket_id),
    );
    put(b, at + 16, k.series.key);
    put(b, at + 24, k.start as u64);
    put(b, at + 32, k.id);
}
fn key_get(b: &[u8], at: usize) -> Key {
    Key {
        series: crate::types::SeriesKey::new(
            (get(b, at + 8) >> 32) as u32,
            get(b, at + 8) as u32,
            get(b, at + 16),
        ),
        scope: get(b, at),
        start: get(b, at + 24) as i64,
        id: get(b, at + 32),
    }
}
