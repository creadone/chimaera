//! Storage verification, checkpoint, flush, and explicit compaction.

use super::options::CompactionOptions;
use super::{CompactionCommand, Storage, WriteCommand};
use crate::engine::lifecycle::admission::{DEFAULT_WAIT_TIMEOUT, OverloadReason};
use crate::error::Result;
use crossbeam_channel::bounded;
use std::path::Path;
use std::time::Instant;

impl Storage {
    /// Read metadata under a shared directory lock without modifying files.
    /// Publication and file deletion wait for this diagnostic operation to finish.
    ///
    /// # Errors
    ///
    /// Returns missing or corrupt IDENTITY/manifest errors, or I/O errors.
    pub fn info(path: impl AsRef<Path>) -> Result<crate::admin::verify::StorageInfo> {
        crate::admin::verify::storage_info(path.as_ref())
    }

    /// Fully verify segment payloads and WAL without opening the database for writes.
    #[must_use]
    pub fn verify(path: impl AsRef<Path>) -> crate::admin::verify::VerificationReport {
        crate::admin::verify::verify_storage(path.as_ref())
    }
    #[must_use]
    pub fn verify_with_options(
        path: impl AsRef<Path>,
        options: crate::admin::verify::VerificationOptions,
    ) -> crate::admin::verify::VerificationReport {
        crate::admin::verify::verify_storage_with_options(path.as_ref(), options)
    }

    /// Recover a verifiable database and a permitted partial WAL tail into a new directory.
    ///
    /// The source is read-only; the result is published after normal recovery,
    /// checkpoint, and another full verification.
    ///
    /// # Errors
    ///
    /// Returns an error for corruption outside the permitted tail, an existing destination,
    /// or filesystem I/O failure.
    pub fn salvage(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<crate::admin::verify::SalvageResult> {
        crate::admin::verify::salvage_storage(source.as_ref(), destination.as_ref())
    }

    /// Flush the accepted write prefix and publish a checkpoint.
    /// Returns the checkpoint commit ID, not a point or byte count.
    ///
    /// # Errors
    ///
    /// Returns background-worker or filesystem errors.
    pub fn checkpoint(&self) -> Result<u64> {
        self.ensure_writable()?;
        let (reply, receiver) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Checkpoint { reply },
            &receiver,
            OverloadReason::WriteQueue,
            "checkpoint",
        )
    }

    /// Flush the current memtable to L0, retaining covered WAL until checkpoint.
    /// Returns the flushed barrier's commit ID.
    ///
    /// # Errors
    ///
    /// Returns writer, flush-worker, or atomic segment-publication errors.
    pub fn flush(&self) -> Result<u64> {
        self.ensure_writable()?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::Flush { reply: reply_tx },
            &reply_rx,
            OverloadReason::WriteQueue,
            "flush",
        )
    }

    /// Run full maintenance of active L0/L1, then historical L0/L2.
    /// Returns the greatest segment/archive ID reported by active/history compaction,
    /// or zero if neither reported an output ID. Cleanup/repack output IDs are excluded.
    /// This is not a commit ID or a debt count.
    ///
    /// # Errors
    ///
    /// Returns read, write, or atomic publication errors.
    pub fn compact(&self) -> Result<u64> {
        self.compact_with_options(CompactionOptions::default())
    }

    /// Run a full or threshold-driven compaction pass over published inputs.
    ///
    /// # Errors
    ///
    /// Returns read, write, or atomic publication errors.
    pub fn compact_with_options(&self, options: CompactionOptions) -> Result<u64> {
        self.compact_until(options, Instant::now() + DEFAULT_WAIT_TIMEOUT)
    }

    /// Wait for one compaction pass and then watermark pruning, using one deadline.
    /// An accepted operation continues after the wait expires.
    ///
    /// # Errors
    /// Each phase can return `Overloaded` before its admission or `OperationPending`
    /// after admission. A pruning-admission error can follow a completed compaction.
    /// Also returns compaction, watermark-pruning, and storage errors.
    pub fn compact_until(&self, options: CompactionOptions, deadline: Instant) -> Result<u64> {
        self.ensure_writable()?;
        let (reply_tx, reply_rx) = bounded(1);
        let compacted = self.request_command_until(
            &self.inner.compaction_tx,
            CompactionCommand::Trigger {
                force: options.full,
                reply: Some(reply_tx),
            },
            &reply_rx,
            OverloadReason::CompactionDebt,
            "compact",
            deadline,
        )?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command_until(
            &self.inner.write_tx,
            super::WriteCommand::PruneWatermarks { reply: reply_tx },
            &reply_rx,
            OverloadReason::CompactionDebt,
            "compact watermark cleanup",
            deadline,
        )?;
        Ok(compacted)
    }
}
