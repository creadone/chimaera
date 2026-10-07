//! Bounded consolidation of small, already sealed history fragments.

use super::selection::is_sealed_descriptor;
use crate::engine::{ACTIVE_COMPACTION_MAX_INPUT_BYTES, ACTIVE_COMPACTION_MAX_INPUT_SEGMENTS};
use crate::segment::SegmentDescriptor;
mod stream;
pub(in crate::engine) use stream::{Source, select};
