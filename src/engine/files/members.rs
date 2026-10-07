//! Seek only members of one physical pack; direct keys avoid the ID index hop.
use super::*;

impl View {
    pub(crate) fn pack_members(
        &self,
        pack_id: u64,
        after: u64,
        limit: usize,
    ) -> Result<Vec<SegmentEntry>> {
        let rows = self.snapshot.page(
            records::PACK_MEMBER,
            Some(records::member_key(pack_id, after)),
            limit,
        )?;
        rows.iter()
            .take_while(|row| row.key.series.key == pack_id)
            .map(|row| {
                let key: Key = records::decode(row)?;
                let entry = records::segment(
                    &self
                        .snapshot
                        .get(key, None)?
                        .ok_or_else(|| corrupt("dangling pack member"))?,
                )?;
                if entry.descriptor.pack_id != Some(pack_id)
                    || row.key != records::member_key(pack_id, entry.descriptor.id)
                {
                    return Err(corrupt("pack member identity mismatch"));
                }
                Ok(entry)
            })
            .collect()
    }
}
