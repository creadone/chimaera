//! Ownership survives an abandoned pack reply, including a full reply channel.
use super::{FlushPackCommand, OpenOptions, Storage};
use crate::catalog::CompressionProfile;
use crate::error::Result;
use crate::layout::Domain;
use crate::segment::{SegmentWriteRequest, SliceCursor};
use crate::types::{PointKey, SeriesKey, VersionedPoint};
use crossbeam_channel::bounded;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn abandoned_pack_reply_cleans_files_and_releases_the_worker() -> Result<()> {
    let temporary = TempDir::new()?;
    let storage = Storage::open(
        temporary.path(),
        OpenOptions {
            flush_worker_count: 1,
            checkpoint_on_close: false,
            ..OpenOptions::default()
        },
    )?;
    let request = |id| {
        vec![SegmentWriteRequest {
            segment_id: id,
            level: 0,
            domain: Domain {
                shard_id: 0,
                policy_id: 1,
                window_start_us: 0,
                window_end_us: 100,
            },
            profile: CompressionProfile::columnar(),
            rewrite_count: 0,
            sources: vec![Box::new(SliceCursor::all(Arc::new(vec![VersionedPoint {
                key: PointKey {
                    series: SeriesKey::new(1, 1, 1),
                    timestamp_us: 1,
                },
                commit_id: 1,
                ordinal: 0,
                value_bits: 1.0_f64.to_bits(),
                is_delete: false,
            }])))],
        }]
    };
    let enqueue = |id, reply| {
        storage
            .inner
            .flush_pack_tx
            .send(FlushPackCommand::Write {
                pack_id: id,
                shard_id: 0,
                requests: request(id),
                reply,
            })
            .expect("pack worker is alive")
    };
    let (abandoned_tx, abandoned_rx) = bounded(1);
    // Fill the reply channel so the worker must wait. Dropping it must still
    // release both the queued reply and the newly prepared artifact.
    assert!(abandoned_tx.send(Err(crate::Error::WorkerStopped)).is_ok());
    enqueue(100, abandoned_tx);
    let (reply_tx, reply_rx) = bounded(1);
    enqueue(101, reply_tx);
    assert!(matches!(
        reply_rx.recv_timeout(Duration::from_millis(20)),
        Err(crossbeam_channel::RecvTimeoutError::Timeout)
    ));
    drop(abandoned_rx);
    let output = reply_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("worker did not resume")?;
    let path = output.segments[0].segment.path().to_path_buf();
    assert!(path.exists());
    assert!(
        !temporary
            .path()
            .join("segments/packs/PACK-S0000-00000000000000000100.tsp")
            .exists()
    );
    drop(output);
    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(temporary.path().join("tmp"))?.count(), 0);
    storage.close()?;
    assert!(Storage::verify(temporary.path()).valid);
    Ok(())
}
