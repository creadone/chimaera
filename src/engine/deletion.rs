//! Series deletion admission and durable collection of completed barriers.

use super::publication::{
    capture_manifest_metadata, complete_manifest_publication, persist_manifest_with_commit,
};
use super::{Inner, Storage, WriteCommand};
use crate::deletion::{Deletions, MAX_KEYS};
use crate::engine::lifecycle::admission::{AdmissionOptions, WriteReply};
use crate::engine::ticket::WriteTicket;
use crate::error::{Result, invalid};
use crate::persistence::manifest::Manifest;
use crate::types::{SeriesKey, WriteReceipt};
use crossbeam_channel::bounded;
use std::sync::Arc;

impl Storage {
    /// Durably delete a series, including retained aggregates.
    /// Existing streams keep their snapshots; later writes start a new history.
    /// Disk reclamation is asynchronous. Retrying after a new write deletes that
    /// new data too; this is not an exactly-once request identifier.
    ///
    /// # Errors
    /// Returns admission, resource, WAL, or storage errors. A failed wait does
    /// not prove that a deletion was rolled back.
    pub fn delete_series(&self, series: SeriesKey) -> Result<WriteReceipt> {
        self.delete_series_batch(&[series])
    }

    /// Delete all selected series atomically at one commit position.
    /// Duplicate keys count once. Empty input is a no-op. The limit of 100,000
    /// applies before deduplication. Receipt operation_count counts unique
    /// requested keys, not the number of previously populated series.
    /// Each retry creates a new deletion barrier and can remove data written
    /// after the first attempt. Serialize those writes until the outcome is known.
    ///
    /// # Errors
    /// Returns validation, admission, resource, WAL, or storage errors.
    pub fn delete_series_batch(&self, series: &[SeriesKey]) -> Result<WriteReceipt> {
        self.enqueue_delete_series_batch(series)?.wait()
    }

    /// Enqueue a deletion; wait for the ticket before relying on durability.
    ///
    /// # Errors
    /// Returns validation, admission, resource, or closed-storage errors.
    pub fn enqueue_delete_series_batch(&self, series: &[SeriesKey]) -> Result<WriteTicket> {
        self.enqueue_delete_series_batch_with_options(series, AdmissionOptions::default())
    }

    /// Enqueue an atomic deletion using one admission deadline for memory and queue.
    /// An accepted ticket is not canceled by a wait timeout or by dropping it.
    ///
    /// # Errors
    /// Overloaded means the command was not accepted. Validation, resource and
    /// closed-storage errors are also possible.
    pub fn enqueue_delete_series_batch_with_options(
        &self,
        series: &[SeriesKey],
        options: AdmissionOptions,
    ) -> Result<WriteTicket> {
        self.ensure_writable()?;
        if series.len() > MAX_KEYS {
            return Err(invalid("deletion batch exceeds 100000 keys"));
        }
        let bytes = series.len() as u64 * std::mem::size_of::<SeriesKey>() as u64
            + crate::segment::PointBatch::METADATA_BYTES;
        let memory_reservation = self.reserve_write_queue_memory(bytes, options.deadline)?;
        let mut keys = series.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let count = keys.len() as u64;
        let (sender, receiver) = bounded(1);
        let (reply, accepted) = WriteReply::new(sender, Arc::clone(&self.inner.admission), count);
        self.admit_command(
            WriteCommand::Delete {
                keys,
                memory_reservation,
                reply,
            },
            &accepted,
            count,
            options.deadline,
        )?;
        Ok(WriteTicket::new(receiver).with_admission(Arc::clone(&self.inner.admission)))
    }
}

pub(super) fn collect(inner: &Inner, manifest: &mut Manifest, swept: &Deletions) -> Result<()> {
    // A delete cannot publish a new barrier between our durable manifest and
    // state update. Ordinary point writes do not take this mutex.
    let _guard = super::support::lock(&inner.deletion_lock)?;
    let current = Arc::clone(&inner.state.read()?.deletions);
    let collected = current.collected(swept, &inner.memory_governor)?;
    if collected.entries.len() == current.entries.len() {
        return Ok(());
    }
    let mut next = manifest.clone();
    capture_manifest_metadata(inner, &mut next)?;
    next.deletions = Arc::clone(&collected);

    let committed = std::cell::Cell::new(false);
    let persistence = persist_manifest_with_commit(inner, &next, || {
        *manifest = next.clone();
        committed.set(true);
    });
    complete_manifest_publication(inner, committed.get(), persistence, || {
        inner.state.write()?.deletions = collected;
        Ok(())
    })?;

    Ok(())
}
