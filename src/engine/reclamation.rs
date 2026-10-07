//! Disk retirement records and catalogue leases protect lazy readers.
use super::Inner;
use super::files::publication::Builder;
use super::publication::{UnpublishedArtifacts, install_files};
use crate::error::{Error, Result};
use crate::persistence::catalog::records::{self, RetiredEntry};
use crate::persistence::manifest::Manifest;
use crate::persistence::wal::sync_directory;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

pub(super) fn collect_files(inner: &Inner, manifest: &mut Manifest) -> Result<()> {
    if super::support::lock(&inner.degraded)?.is_some() {
        return Ok(());
    }
    let base = inner.state.read()?.files.clone();
    if base.totals.retired_files == 0 {
        inner.file_catalog.collect()?;
        inner
            .metrics
            .retired_segment_groups
            .store(0, Ordering::Relaxed);
        inner
            .metrics
            .retired_segment_bytes
            .store(0, Ordering::Relaxed);
        inner
            .metrics
            .retired_backlog_nanos
            .store(0, Ordering::Relaxed);
        return Ok(());
    }
    let mut builder = Builder::new(&base, &inner.work_memory_governor)?;
    if !collect_into(inner, &mut builder)? {
        return Ok(());
    }
    let proposal = builder.finish(
        inner.options.l0_compaction_trigger,
        inner.options.history_rewrite_trigger,
        inner.options.history_segment_target_bytes,
        inner.options.history_window.as_micros() as i64,
    )?;
    install_files(
        inner,
        manifest,
        &base,
        proposal,
        &mut UnpublishedArtifacts::default(),
        None,
    )
}

// Still-pinned files remain in the durable retirement queue after close.
// Reopen reclaims them after complete catalogue/payload/WAL validation.

/// Add deletions to the next ordinary root commit. Call before adding new
/// retirements so oldest_retired_ms includes both the remainder and new rows.
/// Unlinking is safe even if this proposal fails: the durable retired record
/// survives and recovery retries the missing file idempotently.
pub(super) fn collect_into(inner: &Inner, builder: &mut Builder<'_>) -> Result<bool> {
    let _timer =
        crate::diagnostics::publication::stage(crate::diagnostics::publication::Stage::Reclaim);
    if super::support::lock(&inner.degraded)?.is_some() {
        return Ok(false);
    }
    inner.file_catalog.collect()?;
    let base = builder.base;
    inner
        .metrics
        .retired_segment_groups
        .store(base.totals.retired_files, Ordering::Relaxed);
    inner
        .metrics
        .retired_segment_bytes
        .store(base.totals.retired_bytes, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    inner.metrics.retired_backlog_nanos.store(
        base.totals
            .oldest_retired_ms
            .map_or(0, |ms| now.saturating_sub(ms).saturating_mul(1_000_000)),
        Ordering::Relaxed,
    );
    if base.totals.retired_files == 0 {
        inner
            .metrics
            .retired_backlog_nanos
            .store(0, Ordering::Relaxed);
        return Ok(false);
    }
    let oldest = inner
        .file_catalog
        .oldest_generation(base.root().generation)?;
    let mut changed = BTreeSet::new();
    let mut removed = 0;
    let mut next_oldest = None;
    // A bounded portion per publisher turn. Each kind is separate, so a held
    // prefix in one kind cannot starve deletions of another kind.
    for kind in [records::ARCHIVE, records::PACK, records::SINGLE] {
        let scope = records::scope(records::RETIRED, u32::from(kind), 0);
        let after = crate::persistence::catalog::Key {
            series: Default::default(),
            scope,
            start: i64::MIN,
            id: 0,
        };
        let page = base.snapshot.page(records::RETIRED, Some(after), 33)?;
        for (index, row) in page.iter().enumerate() {
            if row.key.scope != scope {
                break;
            }
            let retired: RetiredEntry = records::decode(row)?;
            if index == 32 || retired.generation > oldest {
                next_oldest = Some(
                    next_oldest.map_or(retired.retired_ms, |ms: u64| ms.min(retired.retired_ms)),
                );
                break;
            }
            let path = inner.root.join(&retired.relative_path);
            inner.open_file_cache.invalidate(&path)?;

            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::Io(error)),
            }
            if let Some(parent) = path.parent() {
                changed.insert(parent.to_path_buf());
            }
            builder.set(row.key, None)?;
            builder.totals.retired_files = builder
                .totals
                .retired_files
                .checked_sub(1)
                .ok_or_else(|| crate::error::corrupt("retired file counter underflow"))?;
            builder.totals.retired_bytes = builder
                .totals
                .retired_bytes
                .checked_sub(retired.file_size)
                .ok_or_else(|| crate::error::corrupt("retired byte counter underflow"))?;
            removed += 1;
        }
    }
    if removed == 0 {
        return Ok(false);
    }
    for directory in changed {
        sync_directory(&directory)?;
    }
    builder.totals.oldest_retired_ms = next_oldest;
    Ok(true)
}
