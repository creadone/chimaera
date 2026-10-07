//! One bounded relocation in flight, owned by the existing compaction coordinator.
use super::*;
use crate::engine::files::View;
use crate::memory::work::WorkMemoryReservation;
use crate::persistence::catalog::{
    Key,
    records::{self, PackEntry},
};
use crate::segment::{Relocation, Segment};

const MAX_MEMBERS: usize = 64;
const MAX_CANDIDATES: usize = 1;
const COPY_GROUP_BYTES: u64 = 32 * 1024 * 1024;
const WORK_BYTES: u64 = 64 * 1024 + MAX_MEMBERS as u64 * 8192 + MAX_CANDIDATES as u64 * 1024;

struct Pending {
    inputs: Vec<Arc<Segment>>,
    copy: Relocation,
    // Retain the root until publication/cancellation so GC cannot delete inputs.
    _view: View,
    _memory: WorkMemoryReservation,
}

#[derive(Default)]
pub(super) struct Repacker {
    after: Option<Key>,
    pending: Option<Pending>,
}

impl Repacker {
    pub(super) fn cancel(&mut self, inner: &Inner) {
        if self.pending.take().is_some() {
            inner
                .metrics
                .repack_canceled
                .fetch_add(1, Ordering::Relaxed);
        }
        inner
            .metrics
            .repack_remaining_bytes
            .store(0, Ordering::Relaxed);
    }

    pub(super) fn step(&mut self, inner: &Inner) -> Result<()> {
        let started = Instant::now();
        let result = self.advance(inner);
        inner.metrics.repack_nanos.fetch_add(
            crate::engine::support::duration_nanos(started.elapsed()),
            Ordering::Relaxed,
        );
        if result.is_err() {
            self.cancel(inner);
        }
        if matches!(result, Err(Error::ResourceLimit(_))) {
            // An optional cleanup must not degrade a healthy engine under load.
            // Its durable candidate is retried by maintenance after headroom returns.
            return Ok(());
        }
        result
    }

    fn advance(&mut self, inner: &Inner) -> Result<()> {
        if (!inner.options.retention_repack && inner.state.read()?.deletions.entries.is_empty())
            || inner.lifecycle.load(Ordering::Acquire) != super::super::OPEN
        {
            self.cancel(inner);
            return Ok(());
        }
        if let Some(pending) = &self.pending {
            let current = inner.state.read()?.files.clone();
            let mut valid = true;
            for input in &pending.inputs {
                valid &= current
                    .lookup(input.descriptor().id)?
                    .is_some_and(|e| e.descriptor == *input.descriptor());
            }
            if !valid {
                self.cancel(inner);
            }
        }
        if self.pending.is_none() {
            self.pending = self.select(inner)?;
        }
        let Some(pending) = self.pending.as_mut() else {
            return Ok(());
        };
        check_space(inner, pending.copy.remaining())?;
        let copied = pending.copy.step(
            &inner.root,
            &inner.file_pool,
            inner.options.repack_step_bytes,
        )?;
        inner.metrics.repack_steps.fetch_add(1, Ordering::Relaxed);
        inner
            .metrics
            .repack_bytes_copied
            .fetch_add(copied, Ordering::Relaxed);
        inner
            .metrics
            .compaction_physical_bytes_read
            .fetch_add(copied, Ordering::Relaxed);
        inner
            .metrics
            .compaction_physical_bytes_written
            .fetch_add(copied, Ordering::Relaxed);
        let remaining = pending.copy.remaining();
        inner
            .metrics
            .repack_remaining_bytes
            .store(remaining, Ordering::Relaxed);
        if remaining == 0 {
            let Pending {
                inputs,
                copy,
                _view,
                _memory,
            } = self.pending.take().expect("pending copy");
            let (descriptors, guard) = copy.finish(&inner.root, &inner.file_pool)?;
            let outputs = descriptors
                .into_iter()
                .map(|d| _view.resources.open(d, None))
                .collect::<Result<Vec<_>>>()?;
            let mut unpublished = UnpublishedArtifacts::default();
            unpublished.push(outputs[0].path().to_path_buf());
            guard.commit();
            let (reply, response) = bounded(1);
            inner
                .publish_tx
                .send(PublishCommand::Compaction {
                    #[cfg(feature = "publication-diagnostics")]
                    queued: Some(Instant::now()),
                    inputs,
                    outputs,
                    new_archives: Vec::new(),
                    expired_archives: Vec::new(),
                    unpublished,
                    reply,
                })
                .map_err(|_| Error::WorkerStopped)?;
            response.recv().map_err(|_| Error::WorkerStopped)??;
            inner.metrics.repack_jobs.fetch_add(1, Ordering::Relaxed);
        }
        // Yield between chunks/groups. Writes/ordinary compactions can proceed.
        super::super::publication::wake_compaction(&inner.compaction_tx);
        Ok(())
    }

    fn select(&mut self, inner: &Inner) -> Result<Option<Pending>> {
        let view = inner.state.read()?.files.clone();
        if view.totals.repack_files == 0 {
            self.after = None;
            return Ok(None);
        }
        let Some(memory) = inner
            .work_memory_governor
            .try_reserve_background_with_headroom(WorkMemoryClass::Compaction, WORK_BYTES, 0)?
        else {
            return Ok(None);
        };
        let mut candidates = view
            .snapshot
            .page(records::REPACK, self.after, MAX_CANDIDATES)?;
        if candidates.is_empty() && self.after.is_some() {
            self.after = None;
            candidates = view.snapshot.page(records::REPACK, None, MAX_CANDIDATES)?;
        }
        if let Some(candidate) = candidates.first() {
            self.after = Some(candidate.key);
            let key: Key = records::decode(candidate)?;
            let pack: PackEntry = records::decode(
                &view
                    .snapshot
                    .get(key, None)?
                    .ok_or_else(|| corrupt("repack candidate lost pack"))?,
            )?;
            if key.scope != records::scope(records::PACK, 0, 0)
                || key.id != candidate.key.id
                || !pack.needs_repack()
                || pack.candidate_key(key.id)? != Some(candidate.key)
            {
                return Err(corrupt("repack candidate mismatch"));
            }
            let mut inputs = Vec::new();
            let mut bytes = 0_u64;
            let mut window = None;
            for entry in view.pack_members(key.id, 0, MAX_MEMBERS)? {
                let next_window = temporal::pack_window(
                    &entry.descriptor,
                    inner.options.history_window.as_micros() as i64,
                );
                if !inputs.is_empty() && window != next_window {
                    break;
                }
                window = next_window;
                let next = bytes
                    .checked_add(entry.descriptor.file_size)
                    .ok_or_else(|| corrupt("repack input size overflow"))?;
                if !inputs.is_empty() && next > COPY_GROUP_BYTES {
                    break;
                }
                bytes = next;
                inputs.push(view.resources.open(entry.descriptor, None)?);
            }
            if inputs.is_empty() {
                return Err(corrupt("repack candidate has no members"));
            }
            check_space(inner, bytes)?;
            let first_id = inner
                .next_segment_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                    id.checked_add(inputs.len() as u64)
                })
                .map_err(|_| corrupt("segment id overflow"))?;
            let copy = Relocation::new(
                &inner.root,
                inner.storage_uuid,
                inputs.iter().map(|s| s.descriptor().clone()).collect(),
                first_id,
            )?;
            inner
                .metrics
                .repack_remaining_bytes
                .store(bytes, Ordering::Relaxed);
            return Ok(Some(Pending {
                inputs,
                copy,
                _view: view,
                _memory: memory,
            }));
        }
        Ok(None)
    }
}

fn check_space(inner: &Inner, bytes: u64) -> Result<()> {
    let required = inner
        .options
        .minimum_free_disk_bytes
        .checked_add(bytes)
        .ok_or_else(|| Error::ResourceLimit("repack disk requirement overflow".into()))?;
    let available = available_disk_bytes(&inner.root)?;
    if available < required {
        inner
            .metrics
            .compaction_space_rejections
            .fetch_add(1, Ordering::Relaxed);
        inner
            .metrics
            .last_compaction_required_bytes
            .store(required, Ordering::Relaxed);
        return Err(Error::InsufficientDiskSpace {
            available,
            required,
        });
    }
    Ok(())
}
