//! Bounded file dispatch. Workers retain one pack's range set, not all descriptors.
use super::*;
use crate::admin::inventory::Inventory;
use crate::memory::work::WorkMemoryClass;
use crate::persistence::catalog::{
    Key,
    records::{self, PackEntry},
};
use crate::segment::SegmentDescriptor;
use crossbeam_channel::bounded;
use std::collections::BTreeMap;
use std::sync::Mutex;

enum Task {
    Pack(Key, PackEntry),
    Archive(crate::aggregate::archive::AggregateArchiveDescriptor),
}

#[derive(Default)]
struct Outcome {
    files: usize,
    operations: u64,
    error: Option<(usize, Error)>,
}

pub(super) fn verify(
    inventory: &Inventory,
    workers: usize,
    report: &mut VerificationReport,
) -> Result<()> {
    // The producer owns at most one decoded catalogue leaf. At most 2*workers
    // queued records and workers current tasks survive. No result list grows.
    let (send, receive) = bounded::<(usize, Task)>(workers * 2);
    let outcome = Mutex::new(Outcome::default());
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let receive = receive.clone();
            let outcome = &outcome;
            handles.push(scope.spawn(move || -> Result<()> {
                for (index, task) in receive {
                    let result = verify_task(inventory, task);
                    let mut outcome = outcome
                        .lock()
                        .map_err(|_| corrupt("verification result mutex is poisoned"))?;
                    match result {
                        Ok(count) => {
                            outcome.operations = outcome
                                .operations
                                .checked_add(count)
                                .ok_or_else(|| corrupt("verified operation count overflows"))?;
                            outcome.files += 1;
                        }
                        Err(error)
                            if outcome.error.as_ref().is_none_or(|(old, _)| index < *old) =>
                        {
                            outcome.error = Some((index, error));
                        }
                        Err(_) => {}
                    }
                }
                Ok(())
            }));
        }
        drop(receive);
        let mut index = 0;
        let mut submit = |task| -> Result<()> {
            send.send((index, task)).map_err(|_| Error::WorkerStopped)?;
            index += 1;
            Ok(())
        };
        let produced = (|| {
            for kind in [records::PACK, records::SINGLE] {
                inventory
                    .files
                    .visit_kind(kind, |key, pack| submit(Task::Pack(key, pack)))?;
            }
            inventory.files.visit_kind(records::ARCHIVE, |_, archive| {
                submit(Task::Archive(archive))
            })
        })();
        drop(send);
        for handle in handles {
            handle
                .join()
                .map_err(|_| corrupt("verification worker panicked"))??;
        }
        produced
    })?;
    let outcome = outcome
        .into_inner()
        .map_err(|_| corrupt("verification result mutex is poisoned"))?;
    report.verified_segment_operations = outcome.operations;
    report.verified_physical_files = outcome.files;
    if let Some((_, error)) = outcome.error {
        return Err(error);
    }
    Ok(())
}

fn verify_task(inventory: &Inventory, task: Task) -> Result<u64> {
    let view = &inventory.files;
    let (key, pack) = match task {
        Task::Pack(key, pack) => (key, pack),
        Task::Archive(archive) => {
            view.resources.archive(archive)?.verify()?;
            return Ok(0);
        }
    };
    let mut ranges = BTreeMap::<u64, u64>::new();
    let mut memory = view
        .snapshot
        .work_governor()
        .reserve_background(WorkMemoryClass::Recovery, 0)?;
    let mut operations = 0_u64;
    let mut members = 0_u64;
    let mut bytes = 0_u64;
    let mut check = |descriptor: SegmentDescriptor| -> Result<()> {
        if records::pack_key(&descriptor) != key || descriptor.relative_path != pack.relative_path {
            return Err(corrupt("physical file membership mismatch"));
        }
        let start = descriptor.file_offset;
        let end = start
            .checked_add(descriptor.file_size)
            .ok_or_else(|| corrupt("segment physical range overflows"))?;
        if end > pack.file_size
            || ranges
                .range(..=start)
                .next_back()
                .is_some_and(|(_, previous)| *previous > start)
            || ranges
                .range(start..)
                .next()
                .is_some_and(|(next, _)| *next < end)
        {
            return Err(corrupt(
                "segment physical ranges overlap or exceed their file",
            ));
        }
        if descriptor.pack_id.is_none() && (start != 0 || end != pack.file_size) {
            return Err(corrupt("standalone segment does not own its physical file"));
        }
        // Bounds are retained only for the current physical pack. Explicitly
        // reject a pack exceeding the work budget; the database's file count
        // never contributes to this reservation.
        if !memory.try_grow(128)? {
            return Err(Error::ResourceLimit(
                "verification pack ranges exceed work memory".into(),
            ));
        }
        ranges.insert(start, end);
        operations = operations
            .checked_add(verify_segment(inventory, descriptor)?)
            .ok_or_else(|| corrupt("verified operation count overflows"))?;
        members += 1;
        bytes = bytes
            .checked_add(end - start)
            .ok_or_else(|| corrupt("pack live bytes overflow"))?;
        Ok(())
    };
    if (key.scope >> 56) as u8 == records::SINGLE {
        check(
            view.lookup(key.id)?
                .ok_or_else(|| corrupt("missing standalone segment"))?
                .descriptor,
        )?;
    } else {
        let mut after = 0;
        loop {
            let page = view.pack_members(key.id, after, 64)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                after = entry.descriptor.id;
                check(entry.descriptor)?;
            }
        }
    }
    if members != pack.references || bytes != pack.live_bytes {
        return Err(corrupt("verified pack totals mismatch"));
    }
    Ok(operations)
}

fn verify_segment(inventory: &Inventory, descriptor: SegmentDescriptor) -> Result<u64> {
    let manifest = &inventory.manifest;
    let catalog = manifest.catalog.snapshot();
    let segment = inventory.files.resources.open(descriptor.clone(), None)?;
    segment.verify_operations(|operation| {
        let (policy_id, span) = catalog
            .storage_layout(operation.key.series)
            .unwrap_or((1, manifest.partition_span_us));
        let domain = Domain::for_point(
            operation.key.series,
            operation.key.timestamp_us,
            manifest.virtual_shards,
            policy_id,
            span,
        )
        .map_err(|_| corrupt("segment operation has an invalid storage domain"))?;
        if domain != descriptor.domain()
            || operation.commit_id < descriptor.min_commit
            || operation.commit_id > descriptor.max_commit
        {
            return Err(corrupt("segment operation does not match its descriptor"));
        }
        Ok(())
    })
}
