//! L2 windows share an expiry horizon, including physical pack boundaries.
use super::*;
use crate::memory::work::WorkMemoryReservation;
use std::collections::BTreeSet;

/// A compaction owns this temporary set until its requests are published. Each
/// occupied window is charged before insertion; exhaustion aborts without publication.
pub(super) fn split(
    group: &CompactionGroup,
    ranges: Vec<TimeRange>,
    window_us: i64,
    reader: &mut CompactionReader,
    memory: &mut WorkMemoryReservation,
) -> Result<Vec<TimeRange>> {
    let Some(before) = group.sealed_before_us else {
        return Ok(ranges);
    };
    let mut output = Vec::new();
    for range in ranges {
        let to = range.to_us.min(before);
        if range.from_us < to {
            let first = range.from_us.div_euclid(window_us);
            let last = (to - 1).div_euclid(window_us);
            let mut occupied = BTreeSet::new();
            if i128::from(last) - i128::from(first) < 64 {
                for bucket in first..=last {
                    insert(&mut occupied, bucket, memory)?;
                }
            } else {
                // Rare sparse/wide inputs: enumerate actual points once rather
                // than walking arbitrarily many empty time windows. Normal
                // compaction uses descriptor bounds only, without this pass.
                for segment in &group.inputs {
                    if let Some(mut cursor) = reader.cursor(segment, range.from_us, to)? {
                        while let Some(point) = cursor.next_operation()? {
                            insert(
                                &mut occupied,
                                point.key.timestamp_us.div_euclid(window_us),
                                memory,
                            )?;
                        }
                    }
                }
            }
            for bucket in occupied {
                let start = i128::from(bucket) * i128::from(window_us);
                output.push(TimeRange {
                    from_us: i128::from(range.from_us).max(start) as i64,
                    to_us: i128::from(to).min(start + i128::from(window_us)) as i64,
                });
            }
        }
        if range.to_us > before {
            output.push(TimeRange {
                from_us: range.from_us.max(before),
                to_us: range.to_us,
            });
        }
    }
    Ok(output)
}

fn insert(
    windows: &mut BTreeSet<i64>,
    bucket: i64,
    memory: &mut WorkMemoryReservation,
) -> Result<()> {
    if !windows.contains(&bucket) {
        // Set node, range, request and pack-map slack. The caller charges
        // retained cursor buffers separately before creating requests.
        if !memory.try_grow(1024)? {
            return Err(Error::ResourceLimit(
                "L2 time windows exceed compaction work memory".into(),
            ));
        }
        windows.insert(bucket);
    }
    Ok(())
}

pub(super) fn pack_window(d: &crate::segment::SegmentDescriptor, window_us: i64) -> Option<i64> {
    (d.level == 2).then(|| d.min_timestamp_us.div_euclid(window_us))
}
