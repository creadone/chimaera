//! Incremental deletion sweep owned by the existing compaction coordinator.
//! Only one source (or one atomic archive chunk set) is pinned at a time.
use super::*;
use crate::aggregate::archive::AggregateArchiveDescriptor;
use crate::deletion::Deletions;
use crate::persistence::catalog::{Key, records};
use crate::segment::Segment;

#[derive(Default)]
pub(super) struct Cleaner {
    swept: Option<Arc<Deletions>>,
    after: Option<Key>,
    phase: u8,
}

impl Cleaner {
    pub(super) fn step(&mut self, inner: &Inner) -> Result<bool> {
        let result = self.advance(inner);
        match result {
            Err(Error::ResourceLimit(_)) => Ok(false),
            other => other,
        }
    }

    fn advance(&mut self, inner: &Inner) -> Result<bool> {
        if self.swept.is_none() {
            let state = inner.state.read()?;
            if state.deletions.entries.is_empty() {
                return Ok(false);
            }
            let eligible = state
                .deletions
                .eligible(state.materialized_commit, &inner.memory_governor)?;
            if eligible.entries.is_empty() {
                return Ok(false);
            }
            self.swept = Some(eligible);
            self.after = None;
            self.phase = records::SEGMENT;
        }
        let swept = Arc::clone(self.swept.as_ref().expect("active deletion sweep"));
        let view = inner.state.read()?.files.clone();
        if self.phase == records::SEGMENT {
            let _memory = reserve(inner, 64 * 1024 + swept.entries.len() as u64 * 16)?;
            let keys = swept.entries.iter().map(|m| m.series).collect::<Vec<_>>();
            let page = view.snapshot.page(records::SEGMENT, self.after, 16)?;
            if page.is_empty() {
                self.after = None;
                self.phase = records::ARCHIVE;
            } else {
                for record in page.iter() {
                    let entry: records::SegmentEntry = records::decode(record)?;
                    if entry.descriptor.min_commit <= swept.epoch {
                        let segment = view.resources.open(entry.descriptor, None)?;
                        if segment.may_contain_any_series(&keys) {
                            rewrite_segment(inner, segment, Arc::clone(&swept))?;
                            self.after = Some(record.key);
                            return self.more(inner);
                        }
                    }
                    self.after = Some(record.key);
                }
            }
            return self.more(inner);
        }
        if self.phase == records::ARCHIVE {
            let page = view.snapshot.page(records::ARCHIVE, self.after, 1)?;
            if let Some(first) = page.first() {
                let descriptor: AggregateArchiveDescriptor = records::decode(first)?;
                if descriptor.chunk_index != 0 {
                    return Err(corrupt("deletion sweep found an incomplete archive group"));
                }
                // Atomic publication requires the complete chunk set. Descriptors
                // are charged before allocation; section buffers are bounded per file.
                let _memory = reserve(inner, u64::from(descriptor.chunk_count) * 4096)?;
                let mut group = Vec::with_capacity(descriptor.chunk_count as usize);
                let mut after = self.after;
                while group.len() < descriptor.chunk_count as usize {
                    let records = view.snapshot.page(
                        records::ARCHIVE,
                        after,
                        (descriptor.chunk_count as usize - group.len()).min(64),
                    )?;
                    if records.is_empty() {
                        return Err(corrupt("deletion sweep archive chunk missing"));
                    }
                    for record in records.iter() {
                        let next: AggregateArchiveDescriptor = records::decode(record)?;
                        if next.chunk_index != group.len() as u32
                            || next.chunk_count != descriptor.chunk_count
                            || next.domain() != descriptor.domain()
                        {
                            return Err(corrupt("deletion sweep archive chunk mismatch"));
                        }
                        group.push(view.resources.archive(next)?);
                        after = Some(record.key);
                    }
                }
                if group
                    .iter()
                    .any(|a| a.descriptor().deletion_epoch < swept.epoch)
                    && let Some(key) = rewrite_archives(inner, group, &swept)?
                {
                    after = Some(key);
                }
                self.after = after;
                return self.more(inner);
            }
            self.phase = records::REPACK;
        }
        if view.totals.repack_files != 0 {
            return Ok(false); // The repacker schedules its own next bounded step.
        }
        let (reply, response) = bounded(1);
        inner
            .publish_tx
            .send(PublishCommand::CollectDeletions { swept, reply })
            .map_err(|_| Error::WorkerStopped)?;
        response.recv().map_err(|_| Error::WorkerStopped)??;
        *self = Self::default();
        self.more(inner)
    }

    fn more(&self, inner: &Inner) -> Result<bool> {
        super::super::publication::wake_compaction(&inner.compaction_tx);
        Ok(true)
    }
}

fn rewrite_segment(inner: &Inner, segment: Arc<Segment>, swept: Arc<Deletions>) -> Result<()> {
    let descriptor = segment.descriptor();
    ensure_compaction_space(inner, descriptor.file_size)?;
    let memory = reserve(
        inner,
        4096 * std::mem::size_of::<VersionedPoint>() as u64 * 2,
    )?;
    // A range match is only a candidate. Rewriting an unchanged source would
    // create another candidate after our cursor and could prevent completion.
    if !segment.has_deleted_points(&swept, None)? {
        return Ok(());
    }
    let source = crate::segment::filter_deletions(segment.cursor()?, swept);
    let mut outputs = Vec::new();
    let mut unpublished = UnpublishedArtifacts::default();
    if let Some(source) = non_empty_cursor(source)? {
        let id = inner.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let catalog = inner.state.read()?.catalog.clone();
        let profile = if descriptor.level == 2 {
            sealed_compression_profile(&catalog, descriptor.policy_id)
        } else {
            compression_profile_for_domain(
                &catalog,
                descriptor.domain(),
                descriptor.max_timestamp_us,
                descriptor.level,
            )
        };
        let (reply, response) = bounded(1);
        inner
            .flush_pack_tx
            .send(FlushPackCommand::Write {
                pack_id: id,
                shard_id: descriptor.shard_id,
                requests: vec![SegmentWriteRequest {
                    #[cfg(feature = "pipeline-trace")]
                    trace: inner.trace_region(
                        descriptor.domain(),
                        descriptor.min_timestamp_us,
                        descriptor.max_timestamp_us.saturating_add(1),
                        None,
                        &[Arc::clone(&segment)],
                        descriptor.level,
                    ),
                    segment_id: id,
                    level: descriptor.level,
                    domain: descriptor.domain(),
                    profile,
                    rewrite_count: descriptor.rewrite_count.saturating_add(1),
                    sources: vec![source],
                }],
                reply,
            })
            .map_err(|_| Error::WorkerStopped)?;
        let mut written = response.recv().map_err(|_| Error::WorkerStopped)??;
        unpublished.absorb(&mut written.unpublished);
        outputs.extend(written.segments.into_iter().map(|s| s.segment));
        sync_segment_pack_group(&inner.root)?;
    }
    drop(memory);
    publish(
        inner,
        vec![segment],
        outputs,
        Vec::new(),
        Vec::new(),
        unpublished,
    )
}

fn rewrite_archives(
    inner: &Inner,
    group: Vec<Arc<AggregateArchive>>,
    swept: &Deletions,
) -> Result<Option<Key>> {
    let bytes = group.iter().map(|a| a.descriptor().file_size).sum();
    ensure_compaction_space(inner, bytes)?;
    let maximum = group
        .iter()
        .map(|a| a.descriptor().file_size)
        .max()
        .unwrap_or(0);
    let memory = reserve(inner, maximum * 3)?;
    let mut count = 0_u32;
    let mut changed = false;
    for archive in &group {
        let section = archive.read_section()?;
        let filtered = crate::aggregate::without_deleted_series(
            &section,
            swept,
            archive.descriptor().deletion_epoch,
        )?;
        changed |= filtered
            .as_ref()
            .is_none_or(|chunk| chunk.bytes.len() != section.len());
        count += u32::from(filtered.is_some());
    }
    if !changed {
        return Ok(None);
    }
    let first_id = inner
        .next_segment_id
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
            id.checked_add(u64::from(count))
        })
        .map_err(|_| corrupt("archive id overflow"))?;
    let mut outputs = Vec::with_capacity(count as usize);
    let mut unpublished = UnpublishedArtifacts::default();
    for archive in &group {
        let d = archive.descriptor();
        let section = archive.read_section()?;
        if let Some(chunk) =
            crate::aggregate::without_deleted_series(&section, swept, d.deletion_epoch)?
        {
            let output = AggregateArchive::write(
                &inner.root,
                inner.storage_uuid,
                AggregateArchiveWrite {
                    deletion_epoch: swept.epoch.max(d.deletion_epoch),
                    id: first_id + outputs.len() as u64,
                    domain: Domain {
                        shard_id: d.shard_id,
                        policy_id: d.policy_id,
                        window_start_us: d.window_start_us,
                        window_end_us: d.window_end_us,
                    },
                    coverage_from_us: d.coverage_from_us,
                    coverage_to_us: d.coverage_to_us,
                    resolutions: d.resolutions.clone(),
                    section: chunk.bytes,
                    minimum_series: chunk.minimum_series,
                    maximum_series: chunk.maximum_series,
                    chunk_index: outputs.len() as u32,
                    chunk_count: count,
                },
                inner.file_pool.clone(),
            )?;
            unpublished.push(output.path());
            outputs.push(output);
        }
    }
    let last = outputs.last().map(|a| records::archive_key(a.descriptor()));
    drop(memory);
    publish(inner, Vec::new(), Vec::new(), outputs, group, unpublished)?;
    Ok(last)
}

fn publish(
    inner: &Inner,
    inputs: Vec<Arc<Segment>>,
    outputs: Vec<Arc<Segment>>,
    new_archives: Vec<Arc<AggregateArchive>>,
    expired_archives: Vec<Arc<AggregateArchive>>,
    unpublished: UnpublishedArtifacts,
) -> Result<()> {
    let (reply, response) = bounded(1);
    inner
        .publish_tx
        .send(PublishCommand::Compaction {
            #[cfg(feature = "publication-diagnostics")]
            queued: Some(Instant::now()),
            inputs,
            outputs,
            new_archives,
            expired_archives,
            unpublished,
            reply,
        })
        .map_err(|_| Error::WorkerStopped)?;
    response.recv().map_err(|_| Error::WorkerStopped)??;
    Ok(())
}

fn reserve(inner: &Inner, bytes: u64) -> Result<crate::memory::work::WorkMemoryReservation> {
    inner
        .work_memory_governor
        .try_reserve_background_with_headroom(WorkMemoryClass::Compaction, bytes, 0)?
        .ok_or_else(|| Error::ResourceLimit("series deletion work memory is busy".into()))
}
