use super::{Inner, Storage};
use crate::diagnostics::trace::{Collector, RegionTrace, Stage, Token, TraceConfig, TraceSession};
use crate::error::{Result, invalid};
use crate::persistence::layout::Domain;
use crate::segment::Segment;
use std::sync::Arc;

impl Storage {
    /// Start one bounded diagnostic session on this Storage. Install the
    /// application's tracing subscriber before calling this method.
    ///
    /// # Errors
    /// Rejects invalid limits or a second session on the same Storage.
    pub fn start_pipeline_trace(&self, config: TraceConfig) -> Result<TraceSession> {
        let collector = Collector::new(config)?;
        self.inner
            .pipeline_trace
            .set(Arc::clone(&collector))
            .map_err(|_| invalid("a pipeline trace session already exists"))?;
        Ok(TraceSession(collector))
    }
}

impl Inner {
    pub(super) fn trace_generation(&self, id: u64, stage: Stage) {
        if let Some(trace) = self.pipeline_trace.get() {
            trace.each(|sample| {
                if sample.in_generation(id) {
                    sample.mark(stage);
                }
            });
        }
    }

    pub(super) fn trace_inputs(&self, inputs: &[Arc<Segment>], stage: Stage) {
        if let Some(trace) = self.pipeline_trace.get() {
            let ids = inputs.iter().map(|s| s.descriptor().id).collect::<Vec<_>>();
            trace.each(|sample| {
                if sample.in_segments(&ids) {
                    sample.mark(stage);
                }
            });
        }
    }

    pub(super) fn trace_region(
        &self,
        domain: Domain,
        from: i64,
        to: i64,
        generation: Option<u64>,
        inputs: &[Arc<Segment>],
        level: u8,
    ) -> RegionTrace {
        let mut tokens = Vec::<Token>::new();
        if let Some(trace) = self.pipeline_trace.get() {
            let ids = inputs.iter().map(|s| s.descriptor().id).collect::<Vec<_>>();
            trace.each(|sample| {
                if sample.candidate(domain, from, to)
                    && generation
                        .map_or_else(|| sample.in_segments(&ids), |id| sample.in_generation(id))
                {
                    tokens.push(sample.clone());
                }
            });
        }
        RegionTrace::new(tokens, level)
    }

    pub(super) fn trace_outputs(&self, segments: &[Arc<Segment>], stage: Option<Stage>) {
        if let Some(trace) = self.pipeline_trace.get() {
            let ids = segments
                .iter()
                .map(|s| s.descriptor().id)
                .collect::<Vec<_>>();
            trace.each(|sample| {
                if let Some(stage) = stage {
                    sample.mark_pending(&ids, stage);
                } else {
                    sample.publish(&ids);
                }
            });
        }
    }
}
