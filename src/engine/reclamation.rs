//! Удаление заменённых файлов после освобождения последнего снимка.

use super::support::{duration_nanos, lock, read};
use super::{Inner, degrade};
use crate::aggregate_archive::AggregateArchive;
use crate::error::Error;
use crate::segment::Segment;
use crate::wal::sync_directory;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::{fs, thread};

#[cfg(test)]
#[path = "reclamation_tests.rs"]
mod tests;

pub(super) fn reap_ready_read_directories(inner: &Inner) {
    let mut retired = match lock(&inner.retired_read_directories) {
        Ok(retired) => retired,
        Err(error) => {
            degrade(inner, &error);
            return;
        }
    };
    let mut retained = Vec::with_capacity(retired.len());
    for directory in retired.drain(..) {
        if Arc::strong_count(&directory) > 1 {
            retained.push(directory);
            continue;
        }
        let path = directory.path().to_path_buf();
        drop(directory);
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            degrade(inner, &Error::Io(error));
            continue;
        }
        if let Some(parent) = path.parent()
            && let Err(error) = sync_directory(parent)
        {
            degrade(inner, &error);
        }
    }
    *retired = retained;
}

pub(super) fn reap_ready_segments(inner: &Inner, retired: &mut Vec<Vec<Arc<Segment>>>) -> bool {
    // The publisher also polls on idle timeouts. There are no paths to protect
    // or remove when the retirement queue is empty.
    if retired.is_empty() {
        return false;
    }
    let active_paths = match read(&inner.state) {
        Ok(state) => state
            .segments
            .iter()
            .map(|segment| segment.path().to_path_buf())
            .collect::<HashSet<_>>(),
        Err(error) => {
            degrade(inner, &error);
            return false;
        }
    };
    let held_paths = retired
        .iter()
        .flatten()
        .filter(|segment| Arc::strong_count(segment) > 1)
        .map(|segment| segment.path().to_path_buf())
        .collect::<HashSet<_>>();
    let mut deleted_paths = HashSet::new();
    let mut changed_directories = HashSet::new();
    let mut removed = false;
    let mut index = 0;
    while index < retired.len() {
        if retired[index]
            .iter()
            .any(|segment| Arc::strong_count(segment) > 1)
        {
            index += 1;
            continue;
        }
        for segment in retired.swap_remove(index) {
            let path = segment.path();
            if active_paths.contains(path)
                || held_paths.contains(path)
                || !deleted_paths.insert(path.to_path_buf())
            {
                continue;
            }
            if let Err(error) = inner.open_file_cache.invalidate(path) {
                degrade(inner, &error);
                continue;
            }
            crate::failpoint::hit("reap_before_remove");
            match fs::remove_file(path) {
                Ok(()) => {
                    crate::failpoint::hit("reap_after_remove");
                    if let Some(parent) = path.parent() {
                        changed_directories.insert(parent.to_path_buf());
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => degrade(inner, &Error::Io(error)),
            }
        }
        removed = true;
    }
    for directory in changed_directories {
        if let Err(error) = sync_directory(&directory) {
            degrade(inner, &error);
        }
    }
    removed
}

pub(super) fn reap_ready_archives(
    inner: &Inner,
    retired: &mut Vec<Vec<Arc<AggregateArchive>>>,
) -> bool {
    let mut removed = false;
    let mut index = 0;
    while index < retired.len() {
        if retired[index]
            .iter()
            .any(|archive| Arc::strong_count(archive) > 1)
        {
            index += 1;
            continue;
        }
        let archives = retired.swap_remove(index);
        for archive in archives {
            if let Err(error) = fs::remove_file(archive.path())
                && error.kind() != std::io::ErrorKind::NotFound
            {
                degrade(inner, &Error::Io(error));
            } else {
                removed = true;
            }
        }
    }
    if removed && let Err(error) = sync_directory(&inner.root.join("aggregates")) {
        degrade(inner, &error);
    }
    removed
}

pub(super) fn record_retired_debt(
    inner: &Inner,
    retired: &[Vec<Arc<Segment>>],
    backlog_started: &mut Option<Instant>,
) {
    inner.metrics.retired_segment_groups.store(
        u64::try_from(retired.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let mut physical_ends = HashMap::<&Path, u64>::new();
    for segment in retired.iter().flatten() {
        let descriptor = segment.descriptor();
        let physical_end = descriptor.file_offset.saturating_add(descriptor.file_size);
        physical_ends
            .entry(segment.path())
            .and_modify(|end| *end = (*end).max(physical_end))
            .or_insert(physical_end);
    }
    inner.metrics.retired_segment_bytes.store(
        physical_ends
            .values()
            .copied()
            .fold(0_u64, u64::saturating_add),
        Ordering::Relaxed,
    );
    if retired.is_empty() {
        *backlog_started = None;
        inner
            .metrics
            .retired_backlog_nanos
            .store(0, Ordering::Relaxed);
    } else {
        let started = backlog_started.get_or_insert_with(Instant::now);
        inner
            .metrics
            .retired_backlog_nanos
            .store(duration_nanos(started.elapsed()), Ordering::Relaxed);
    }
}

pub(super) fn finish_retired_segments(inner: &Inner, retired: Vec<Vec<Arc<Segment>>>) {
    if retired.is_empty() {
        return;
    }
    let active_paths = match read(&inner.state) {
        Ok(state) => state
            .segments
            .iter()
            .map(|segment| segment.path().to_path_buf())
            .collect::<HashSet<_>>(),
        Err(error) => {
            degrade(inner, &error);
            return;
        }
    };
    let _ = thread::Builder::new()
        .name("chimaera-retired-reaper".into())
        .spawn(move || reap_retired_after_close(retired, &active_paths));
}

pub(super) fn finish_retired_archives(retired: Vec<Vec<Arc<AggregateArchive>>>) {
    if retired.is_empty() {
        return;
    }
    let _ = thread::Builder::new()
        .name("chimaera-aggregate-reaper".into())
        .spawn(move || {
            for group in retired {
                while group.iter().any(|archive| Arc::strong_count(archive) > 1) {
                    thread::sleep(Duration::from_millis(10));
                }
                for archive in group {
                    let _ = fs::remove_file(archive.path());
                }
            }
        });
}

fn reap_retired_after_close(mut retired: Vec<Vec<Arc<Segment>>>, active_paths: &HashSet<PathBuf>) {
    while !retired.is_empty() {
        let held_paths = retired
            .iter()
            .flatten()
            .filter(|segment| Arc::strong_count(segment) > 1)
            .map(|segment| segment.path().to_path_buf())
            .collect::<HashSet<_>>();
        let mut deleted_paths = HashSet::new();
        let mut changed_directories = HashSet::new();
        let mut removed = false;
        let mut index = 0;
        while index < retired.len() {
            if retired[index]
                .iter()
                .any(|segment| Arc::strong_count(segment) > 1)
            {
                index += 1;
                continue;
            }
            for segment in retired.swap_remove(index) {
                let path = segment.path();
                if active_paths.contains(path)
                    || held_paths.contains(path)
                    || !deleted_paths.insert(path.to_path_buf())
                {
                    continue;
                }
                match fs::remove_file(path) {
                    Ok(()) => {
                        if let Some(parent) = path.parent() {
                            changed_directories.insert(parent.to_path_buf());
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {}
                }
            }
            removed = true;
        }
        for directory in changed_directories {
            let _ = sync_directory(&directory);
        }
        if !removed {
            thread::sleep(Duration::from_millis(10));
        }
    }
}
