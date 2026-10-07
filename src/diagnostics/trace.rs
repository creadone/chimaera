//! Bounded, opt-in tracing of a representative operation through the write path.
//!
//! Enable `pipeline-trace` and call `Storage::start_pipeline_trace` before the
//! workload. The application owns the `tracing` subscriber. Each selected batch
//! contributes its last operation; completion describes that operation, not all
//! shards of the batch. No point values or payloads are logged.

use crate::error::{Result, invalid};
use crate::persistence::layout::Domain;
use crate::types::{AggregateResolution, Point, PointKey, SeriesKey, VersionedPoint};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

/// Limits apply to the entire session, including completed samples.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct TraceConfig {
    pub every_batches: u64,
    pub max_samples: usize,
    /// Wait for this resolution, or the first available resolution when `None`.
    pub summary_resolution: Option<AggregateResolution>,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            every_batches: 1000,
            max_samples: 256,
            summary_resolution: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Stage {
    Entry,
    AdmissionReady,
    WriterReceived,
    PrepareStarted,
    WalAppendStarted,
    WalAppended,
    WalSyncStarted,
    WalDurable,
    Visible,
    ReplyReady,
    Frozen,
    FlushStarted,
    FlushPackQueued,
    FlushPackStarted,
    FlushPackFinished,
    FlushPublishQueued,
    FlushPublishStarted,
    L0Published,
    CompactionSelected,
    CompactionPackQueued,
    CompactionPackStarted,
    CompactionPackFinished,
    CompactionPublishQueued,
    CompactionPublishStarted,
    CompactionPublished,
    SummaryPublished,
}

const STAGES: [Stage; 26] = [
    Stage::Entry,
    Stage::AdmissionReady,
    Stage::WriterReceived,
    Stage::PrepareStarted,
    Stage::WalAppendStarted,
    Stage::WalAppended,
    Stage::WalSyncStarted,
    Stage::WalDurable,
    Stage::Visible,
    Stage::ReplyReady,
    Stage::Frozen,
    Stage::FlushStarted,
    Stage::FlushPackQueued,
    Stage::FlushPackStarted,
    Stage::FlushPackFinished,
    Stage::FlushPublishQueued,
    Stage::FlushPublishStarted,
    Stage::L0Published,
    Stage::CompactionSelected,
    Stage::CompactionPackQueued,
    Stage::CompactionPackStarted,
    Stage::CompactionPackFinished,
    Stage::CompactionPublishQueued,
    Stage::CompactionPublishStarted,
    Stage::CompactionPublished,
    Stage::SummaryPublished,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceOutcome {
    Pending,
    SummaryPublished,
    WriteFailed,
    WriteOutcomeUnknown,
}

/// All offsets use the same monotonic session clock. Missing milestones are
/// absent, never zero-filled or included as completed latency observations.
#[derive(Clone, Debug, Serialize)]
pub struct TraceSample {
    pub sample_id: u64,
    pub series: SeriesKey,
    pub timestamp_us: i64,
    pub commit_id: Option<u64>,
    pub generation_id: Option<u64>,
    pub segment_id: Option<u64>,
    pub outcome: TraceOutcome,
    pub summary_resolutions: Vec<AggregateResolution>,
    pub milestones: Vec<Milestone>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Milestone {
    pub name: String,
    pub offset_ns: u64,
}

#[derive(Debug, Serialize)]
pub struct TraceReport {
    pub schema_version: u32,
    pub config: TraceConfig,
    pub elapsed_ns: u64,
    pub batches_seen: u64,
    pub samples_omitted_at_capacity: u64,
    pub samples: Vec<TraceSample>,
}

/// Retain this handle through drain/close to include background completion.
/// `finish` stops new observations and releases all span trees. A snapshot does
/// neither, and is useful at the end of offered load before forced maintenance.
pub struct TraceSession(pub(crate) Arc<Collector>);

impl TraceSession {
    #[must_use]
    pub fn snapshot(&self) -> TraceReport {
        self.0.report()
    }

    #[must_use]
    pub fn finish(self) -> TraceReport {
        self.0.stopped.store(true, Ordering::Release);
        let report = self.0.report();
        let samples = lock(&self.0.samples).clone();
        for sample in &samples {
            // Drop the span outside the sample lock: subscribers execute on drop.
            let span = lock(&sample.0.as_ref().expect("registered sample").state)
                .span
                .take();
            drop(span);
        }
        report
    }
}

pub(crate) struct Collector {
    config: TraceConfig,
    started: Instant,
    seen: AtomicU64,
    omitted: AtomicU64,
    stopped: Arc<AtomicBool>,
    samples: Mutex<Vec<Token>>,
}

impl Collector {
    pub(crate) fn new(config: TraceConfig) -> Result<Arc<Self>> {
        if config.every_batches == 0 || !(1..=4096).contains(&config.max_samples) {
            return Err(invalid(
                "pipeline trace requires a positive stride and 1..=4096 samples",
            ));
        }
        Ok(Arc::new(Self {
            config,
            started: Instant::now(),
            seen: AtomicU64::new(0),
            omitted: AtomicU64::new(0),
            stopped: Arc::new(AtomicBool::new(false)),
            samples: Mutex::new(Vec::with_capacity(config.max_samples)),
        }))
    }

    pub(crate) fn begin(&self, operations: &[Point]) -> Token {
        let entry_ns = nanos(self.started.elapsed());
        if self.stopped.load(Ordering::Acquire) {
            return Token::default();
        }
        let sequence = self.seen.fetch_add(1, Ordering::Relaxed);
        if !sequence.is_multiple_of(self.config.every_batches) {
            return Token::default();
        }
        let Some(&operation) = operations.last() else {
            return Token::default();
        };
        let mut samples = lock(&self.samples);
        if self.stopped.load(Ordering::Acquire) {
            return Token::default();
        }
        if samples.len() == self.config.max_samples {
            self.omitted.fetch_add(1, Ordering::Relaxed);
            return Token::default();
        }
        let id = sequence + 1;
        let token = Token(Some(Arc::new(Sample {
            id,
            key: PointKey {
                series: operation.series,
                timestamp_us: operation.timestamp_us,
            },
            started: self.started,
            stopped: Arc::clone(&self.stopped),
            requested: self.config.summary_resolution,
            state: Mutex::new(SampleState {
                commit: None,
                domain: None,
                generation: None,
                segment: None,
                marks: [None; STAGES.len()],
                outcome: TraceOutcome::Pending,
                coverage: 0,
                pending: None,
                span: None,
            }),
        })));
        samples.push(token.clone());
        drop(samples);
        let span = tracing::info_span!(target: "chimaera::pipeline", parent: None,
            "write_path", sample_id = id, space_id = operation.series.space_id,
            bucket_id = operation.series.bucket_id, key = operation.series.key,
            timestamp_us = operation.timestamp_us);
        {
            let mut state = lock(&token.0.as_ref().expect("new sample").state);
            if !self.stopped.load(Ordering::Acquire) {
                state.span = Some(Arc::new(span));
            }
        }
        token.mark_at(Stage::Entry, entry_ns);
        token
    }

    pub(crate) fn each(&self, mut visit: impl FnMut(&Token)) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        // Never call tracing subscribers while holding the registry lock.
        let samples = lock(&self.samples).clone();
        for sample in samples {
            visit(&sample);
        }
    }

    fn report(&self) -> TraceReport {
        let mut samples = Vec::new();
        for token in lock(&self.samples).iter() {
            let sample = token.0.as_ref().expect("registered sample");
            let state = lock(&sample.state);
            samples.push(TraceSample {
                sample_id: sample.id,
                series: sample.key.series,
                timestamp_us: sample.key.timestamp_us,
                commit_id: state.commit,
                generation_id: state.generation,
                segment_id: state.segment,
                outcome: state.outcome,
                summary_resolutions: resolutions(state.coverage),
                milestones: STAGES
                    .iter()
                    .zip(state.marks)
                    .filter_map(|(stage, time)| {
                        time.map(|offset_ns| Milestone {
                            name: stage.name().into(),
                            offset_ns,
                        })
                    })
                    .collect(),
            });
        }
        TraceReport {
            schema_version: 1,
            config: self.config,
            elapsed_ns: nanos(self.started.elapsed()),
            batches_seen: self.seen.load(Ordering::Relaxed),
            samples_omitted_at_capacity: self.omitted.load(Ordering::Relaxed),
            samples,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Token(Option<Arc<Sample>>);

pub(crate) struct AdmissionTrace {
    token: Token,
    accepted: bool,
}

impl AdmissionTrace {
    pub(crate) fn new(token: Token) -> Self {
        Self {
            token,
            accepted: false,
        }
    }
    pub(crate) fn accepted(&mut self) {
        self.accepted = true;
    }
}

impl Drop for AdmissionTrace {
    fn drop(&mut self) {
        if !self.accepted {
            self.token.failed(false);
        }
    }
}

struct Sample {
    id: u64,
    key: PointKey,
    started: Instant,
    stopped: Arc<AtomicBool>,
    requested: Option<AggregateResolution>,
    state: Mutex<SampleState>,
}

struct SampleState {
    commit: Option<u64>,
    domain: Option<Domain>,
    generation: Option<u64>,
    segment: Option<u64>,
    marks: [Option<u64>; STAGES.len()],
    outcome: TraceOutcome,
    coverage: u8,
    pending: Option<Output>,
    span: Option<Arc<tracing::Span>>,
}

#[derive(Clone, Copy)]
struct Output {
    id: u64,
    level: u8,
    coverage: u8,
}

impl Token {
    pub(crate) fn is_sampled(&self) -> bool {
        self.0.is_some()
    }
    pub(crate) fn mark(&self, stage: Stage) {
        if let Some(sample) = &self.0 {
            self.mark_at(stage, nanos(sample.started.elapsed()));
        }
    }

    fn mark_at(&self, stage: Stage, offset_ns: u64) {
        let Some(sample) = &self.0 else {
            return;
        };
        if sample.stopped.load(Ordering::Acquire) {
            return;
        }
        let span = {
            let mut state = lock(&sample.state);
            if sample.stopped.load(Ordering::Acquire) {
                return;
            }
            let mark = &mut state.marks[stage as usize];
            if mark.is_some() {
                return;
            }
            *mark = Some(offset_ns);
            state.span.clone()
        };
        if let Some(span) = span {
            span.with_subscriber(|(_, dispatch)| {
                tracing::dispatcher::with_default(dispatch, || {
                    tracing::info!(target: "chimaera::pipeline", parent: span.as_ref(),
                    sample_id = sample.id, stage = stage.name(), offset_ns, "milestone");
                })
            });
        }
    }

    pub(crate) fn bind(&self, commit: u64, domain: Domain) {
        if let Some(sample) = &self.0 {
            let mut state = lock(&sample.state);
            if sample.stopped.load(Ordering::Acquire) {
                return;
            }
            state.commit = Some(commit);
            state.domain = Some(domain);
        }
    }

    pub(crate) fn failed(&self, unknown: bool) {
        if let Some(sample) = &self.0 {
            let mut state = lock(&sample.state);
            if sample.stopped.load(Ordering::Acquire) {
                return;
            }
            if state.marks[Stage::ReplyReady as usize].is_none() {
                state.outcome = if unknown {
                    TraceOutcome::WriteOutcomeUnknown
                } else {
                    TraceOutcome::WriteFailed
                };
            }
        }
    }

    pub(crate) fn freeze(&self, maximum_commit: u64, generation: u64) {
        let Some(sample) = &self.0 else {
            return;
        };
        let mut state = lock(&sample.state);
        if sample.stopped.load(Ordering::Acquire) {
            return;
        }
        if state.generation.is_none()
            && state.commit.is_some_and(|c| c <= maximum_commit)
            && state.marks[Stage::Visible as usize].is_some()
        {
            state.generation = Some(generation);
            drop(state);
            self.mark(Stage::Frozen);
        }
    }

    pub(crate) fn in_generation(&self, id: u64) -> bool {
        self.0
            .as_ref()
            .is_some_and(|s| lock(&s.state).generation == Some(id))
    }

    pub(crate) fn in_segments(&self, ids: &[u64]) -> bool {
        self.0.as_ref().is_some_and(|s| {
            let state = lock(&s.state);
            state.outcome == TraceOutcome::Pending
                && state.segment.is_some_and(|id| ids.contains(&id))
        })
    }

    pub(crate) fn candidate(&self, domain: Domain, from: i64, to: i64) -> bool {
        self.0.as_ref().is_some_and(|s| {
            let state = lock(&s.state);
            state.outcome == TraceOutcome::Pending
                && state.domain == Some(domain)
                && s.key.timestamp_us >= from
                && s.key.timestamp_us < to
        })
    }

    pub(crate) fn publish(&self, ids: &[u64]) {
        let Some(sample) = &self.0 else {
            return;
        };
        let mut state = lock(&sample.state);
        if sample.stopped.load(Ordering::Acquire) {
            return;
        }
        let Some(output) = state.pending.filter(|output| ids.contains(&output.id)) else {
            return;
        };
        state.pending = None;
        state.segment = Some(output.id);
        state.coverage = output.coverage;
        let summary = output.coverage != 0
            && sample
                .requested
                .is_none_or(|resolution| output.coverage & resolution_mask(resolution) != 0);
        if summary {
            state.outcome = TraceOutcome::SummaryPublished;
        }
        drop(state);
        self.mark(if output.level == 0 {
            Stage::L0Published
        } else {
            Stage::CompactionPublished
        });
        if summary {
            self.mark(Stage::SummaryPublished);
        }
    }

    pub(crate) fn mark_pending(&self, ids: &[u64], stage: Stage) {
        if self.0.as_ref().is_some_and(|sample| {
            lock(&sample.state)
                .pending
                .is_some_and(|output| ids.contains(&output.id))
        }) {
            self.mark(stage);
        }
    }
}

/// Observes already merged output, not input descriptors. Exact versions must
/// survive merge before summary coverage can count as completion.
#[derive(Default)]
pub(crate) struct RegionTrace {
    samples: Vec<(Token, u64)>,
    matched: Vec<Token>,
    position: usize,
    level: u8,
}

impl RegionTrace {
    pub(crate) fn new(tokens: Vec<Token>, level: u8) -> Self {
        let mut samples = tokens
            .into_iter()
            .filter_map(|token| {
                let commit = lock(&token.0.as_ref()?.state).commit?;
                Some((token, commit))
            })
            .collect::<Vec<_>>();
        samples.sort_by_key(|(token, _)| token.0.as_ref().expect("sample").key);
        Self {
            samples,
            matched: Vec::new(),
            position: 0,
            level,
        }
    }

    pub(crate) fn mark(&self, flush: Stage, compaction: Stage) {
        for (sample, _) in &self.samples {
            sample.mark(if self.level == 0 { flush } else { compaction });
        }
    }

    pub(crate) fn observe(&mut self, point: VersionedPoint) {
        while let Some((token, commit)) = self.samples.get(self.position) {
            let sample = token.0.as_ref().expect("sample");
            if sample.key > point.key {
                break;
            }
            if sample.key == point.key && point.commit_id == *commit {
                self.matched.push(token.clone());
            }
            self.position += 1;
        }
    }

    pub(crate) fn encoded(&self, id: u64, aggregate: Option<&[u8]>) {
        for token in &self.matched {
            let sample = token.0.as_ref().expect("sample");
            let coverage = aggregate
                .and_then(|bytes| crate::aggregate::trace_coverage(bytes, sample.key).ok())
                .unwrap_or(0);
            if sample.stopped.load(Ordering::Acquire) {
                continue;
            }
            let mut state = lock(&sample.state);
            if sample.stopped.load(Ordering::Acquire) {
                continue;
            }
            state.pending = Some(Output {
                id,
                level: self.level,
                coverage,
            });
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn resolution_mask(resolution: AggregateResolution) -> u8 {
    match resolution {
        AggregateResolution::Minute => 1,
        AggregateResolution::Hour => 2,
        AggregateResolution::Day => 4,
    }
}

fn resolutions(mask: u8) -> Vec<AggregateResolution> {
    [
        AggregateResolution::Minute,
        AggregateResolution::Hour,
        AggregateResolution::Day,
    ]
    .into_iter()
    .filter(|r| resolution_mask(*r) & mask != 0)
    .collect()
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::AdmissionReady => "admission_ready",
            Self::WriterReceived => "writer_received",
            Self::PrepareStarted => "prepare_started",
            Self::WalAppendStarted => "wal_append_started",
            Self::WalAppended => "wal_appended",
            Self::WalSyncStarted => "wal_sync_started",
            Self::WalDurable => "wal_durable",
            Self::Visible => "visible",
            Self::ReplyReady => "reply_ready",
            Self::Frozen => "frozen",
            Self::FlushStarted => "flush_started",
            Self::FlushPackQueued => "flush_pack_queued",
            Self::FlushPackStarted => "flush_pack_started",
            Self::FlushPackFinished => "flush_pack_finished",
            Self::FlushPublishQueued => "flush_publish_queued",
            Self::FlushPublishStarted => "flush_publish_started",
            Self::L0Published => "l0_published",
            Self::CompactionSelected => "compaction_selected",
            Self::CompactionPackQueued => "compaction_pack_queued",
            Self::CompactionPackStarted => "compaction_pack_started",
            Self::CompactionPackFinished => "compaction_pack_finished",
            Self::CompactionPublishQueued => "compaction_publish_queued",
            Self::CompactionPublishStarted => "compaction_publish_started",
            Self::CompactionPublished => "compaction_published",
            Self::SummaryPublished => "summary_published",
        }
    }
}
