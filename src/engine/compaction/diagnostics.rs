//! Per-job attribution. No extra worker, input ownership or persistent state.
use super::*;
use crate::diagnostics::publication::{
    Collector,
    compaction::{Kind, Sample},
};

pub(super) struct Job {
    collector: Arc<Collector>,
    kind: Kind,
    started: Instant,
    pub sample: Sample,
    pub completed: bool,
}

impl Job {
    pub fn start(
        inner: &Inner,
        groups: &[CompactionGroup],
        cutoffs: &BTreeMap<u32, i64>,
        expired_archives: bool,
        started: Instant,
    ) -> Option<Self> {
        let collector = Arc::clone(inner.publication_diagnostics.get()?);
        let mut sample = Sample::default();
        let mut retention_groups = 0;
        for group in groups {
            let cutoff = cutoffs.get(&group.domain.policy_id).copied();
            let mut expires = false;
            for segment in &group.inputs {
                let d = segment.descriptor();
                sample.input_bytes = sample.input_bytes.saturating_add(d.file_size);
                if d.level == 0 {
                    sample.l0_input_bytes = sample.l0_input_bytes.saturating_add(d.file_size);
                }
                let bucket = if cutoff.is_some_and(|cutoff| d.max_timestamp_us < cutoff) {
                    expires = true;
                    &mut sample.expired_input_bytes
                } else if cutoff.is_some_and(|cutoff| d.min_timestamp_us < cutoff) {
                    expires = true;
                    &mut sample.crossing_input_bytes
                } else {
                    &mut sample.live_input_bytes
                };
                *bucket = bucket.saturating_add(d.file_size);
            }
            retention_groups += usize::from(expires);
        }
        sample.groups = groups.len() as u64;
        let kind = classify(groups.len(), retention_groups, expired_archives);
        Some(Self {
            collector,
            kind,
            started,
            sample,
            completed: false,
        })
    }
}

fn classify(groups: usize, retention: usize, expired_archives: bool) -> Kind {
    if retention == 0 && !expired_archives {
        Kind::Ordinary
    } else if retention == groups {
        Kind::Retention
    } else {
        Kind::Mixed
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        self.collector.record_compaction(
            self.kind,
            self.completed.then_some(&self.sample),
            self.started.elapsed(),
        );
    }
}
