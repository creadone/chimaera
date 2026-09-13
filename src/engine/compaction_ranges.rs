//! Границы выходов слияния и деление по оценочному размеру.

use super::compaction_selection::{CompactionGroup, merge_time_ranges};
use super::{
    ACTIVE_COMPACTION_MAX_INPUT_BYTES, COMPACTION_OUTPUT_SPAN_US, MAX_COMPACTION_OUTPUT_RANGES,
    TimeRange,
};
use crate::error::{Result, corrupt};
use crate::layout::Domain;

fn build_compaction_output_ranges(
    domain: Domain,
    components: &[TimeRange],
    focus_ranges: &[TimeRange],
    span_us: i64,
    max_ranges: usize,
) -> Result<Option<Vec<TimeRange>>> {
    let origin = i128::from(domain.window_start_us);
    let span = i128::from(span_us);
    let mut outputs = Vec::new();
    for component in components {
        let mut cuts = vec![component.from_us, component.to_us];
        for focus in focus_ranges
            .iter()
            .filter(|focus| focus.overlaps(*component))
        {
            let from = focus.from_us.max(component.from_us);
            let to = focus.to_us.min(component.to_us);
            let from_delta = i128::from(from) - origin;
            let to_delta = i128::from(to) - origin;
            let first = origin + from_delta.div_euclid(span) * span;
            let last = origin + ((to_delta + span - 1) / span) * span;
            let mut boundary = first;
            while boundary <= last {
                let boundary_us = i64::try_from(boundary)
                    .map_err(|_| corrupt("compaction boundary overflows i64"))?;
                if boundary_us > component.from_us && boundary_us < component.to_us {
                    cuts.push(boundary_us);
                    if cuts.len() > max_ranges.saturating_add(1) {
                        return Ok(None);
                    }
                }
                boundary += span;
            }
        }
        cuts.sort_unstable();
        cuts.dedup();
        for pair in cuts.windows(2) {
            if outputs.len() == max_ranges {
                return Ok(None);
            }
            outputs.push(TimeRange {
                from_us: pair[0],
                to_us: pair[1],
            });
        }
    }
    Ok(Some(outputs))
}

pub(super) fn append_size_split_range(
    output: &mut Vec<TimeRange>,
    range: TimeRange,
    estimated_bytes: u128,
    target_bytes: u64,
    split_allowed: bool,
) {
    let span = i128::from(range.to_us) - i128::from(range.from_us);
    let pieces = if split_allowed {
        estimated_bytes
            .div_ceil(u128::from(target_bytes))
            .clamp(1, MAX_COMPACTION_OUTPUT_RANGES as u128)
    } else {
        1
    };
    let piece_count = i128::try_from(pieces).unwrap_or(i128::MAX);
    let piece_span = (span + piece_count - 1) / piece_count;
    let mut from = i128::from(range.from_us);
    while from < i128::from(range.to_us) {
        let to = (from + piece_span).min(i128::from(range.to_us));
        output.push(TimeRange {
            from_us: i64::try_from(from).expect("split stays within input range"),
            to_us: i64::try_from(to).expect("split stays within input range"),
        });
        from = to;
    }
}

fn split_compaction_ranges_by_estimated_size(
    group: &CompactionGroup,
    ranges: Vec<TimeRange>,
    target_bytes: u64,
) -> Vec<TimeRange> {
    if group.sealed_before_us.is_none() && !group.repair_overlap {
        return ranges;
    }
    let target_bytes = if group.repair_overlap {
        ACTIVE_COMPACTION_MAX_INPUT_BYTES
    } else {
        target_bytes
    };
    let input_bytes = group.inputs.iter().fold(0_u64, |sum, segment| {
        sum.saturating_add(segment.descriptor().file_size)
    });
    let total_span = ranges
        .iter()
        .map(|range| i128::from(range.to_us) - i128::from(range.from_us))
        .sum::<i128>()
        .max(1);
    let size_split_allowed = ranges.len() == 1;
    let mut split = Vec::with_capacity(ranges.len().saturating_add(1));
    for range in ranges {
        let boundary = group
            .sealed_before_us
            .unwrap_or(range.from_us)
            .clamp(range.from_us, range.to_us);
        for (from_us, to_us, sealed) in [
            (range.from_us, boundary, true),
            (boundary, range.to_us, false),
        ] {
            if from_us >= to_us {
                continue;
            }
            let span = i128::from(to_us) - i128::from(from_us);
            let estimated_bytes = u128::from(input_bytes)
                .saturating_mul(u128::try_from(span).unwrap_or(u128::MAX))
                .div_ceil(u128::try_from(total_span).unwrap_or(1));
            append_size_split_range(
                &mut split,
                TimeRange { from_us, to_us },
                estimated_bytes,
                target_bytes,
                size_split_allowed && (sealed || group.repair_overlap),
            );
        }
    }
    split
}

pub(super) fn compaction_output_ranges(
    group: &CompactionGroup,
    target_bytes: u64,
) -> Result<Vec<TimeRange>> {
    let components = merge_time_ranges(
        group
            .inputs
            .iter()
            .map(|segment| TimeRange::for_segment(segment))
            .collect::<Result<Vec<_>>>()?,
    );
    let outputs = if group.coalesce_disjoint {
        match (components.first(), components.last()) {
            (Some(first), Some(last)) => vec![TimeRange {
                from_us: first.from_us,
                to_us: last.to_us,
            }],
            _ => Vec::new(),
        }
    } else if group.focus_ranges.is_empty() {
        components
    } else {
        let domain_span = group
            .domain
            .window_end_us
            .checked_sub(group.domain.window_start_us)
            .ok_or_else(|| corrupt("compaction domain span overflows i64"))?;
        let mut span_us = COMPACTION_OUTPUT_SPAN_US.min(domain_span);
        loop {
            let outputs = build_compaction_output_ranges(
                group.domain,
                &components,
                &group.focus_ranges,
                span_us,
                MAX_COMPACTION_OUTPUT_RANGES,
            )?;
            if let Some(outputs) = outputs {
                break outputs;
            }
            if span_us >= domain_span {
                break components;
            }
            span_us = span_us.saturating_mul(2).min(domain_span);
        }
    };
    Ok(split_compaction_ranges_by_estimated_size(
        group,
        outputs,
        target_bytes,
    ))
}
