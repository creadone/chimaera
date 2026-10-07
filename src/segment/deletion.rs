//! Check deletion candidates through the exact index before decoding payloads.
use super::*;
use crate::deletion::Deletions;

impl Segment {
    pub(crate) fn has_deleted_points(
        self: &Arc<Self>,
        deletions: &Deletions,
        metrics: Option<Arc<SegmentReadMetrics>>,
    ) -> Result<bool> {
        let metadata = self.metadata()?;
        for marker in &deletions.entries {
            if marker.commit_id < self.descriptor.min_commit {
                continue;
            }
            let start = metadata.index.partition_point(|b| b.series < marker.series);
            for index in start..metadata.index.len() {
                if metadata.index[index].series != marker.series {
                    break;
                }
                // Keep one block alive, charged by the cleaner's existing decode
                // reservation. Do not populate the foreground block cache.
                let mut cursor = SegmentCursor::new_indexed(
                    Arc::clone(self),
                    Arc::clone(&metadata),
                    vec![index],
                    TimestampFilter::All,
                    false,
                    false,
                    None,
                    metrics.clone(),
                )?;
                while let Some(point) = cursor.next_operation()? {
                    if point.commit_id <= marker.commit_id {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
}
