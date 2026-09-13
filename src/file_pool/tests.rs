use super::*;
use crate::admission::WorkMemoryGovernor;
use crate::{CancellationToken, QueryOptions};
use std::io::Write;
use std::sync::Barrier;
use tempfile::NamedTempFile;

fn budget(options: QueryOptions) -> Result<Arc<QueryBudget>> {
    QueryBudget::new_with_admission(options, WorkMemoryGovernor::new(1024 * 1024))
}

#[test]
fn concurrent_reads_keep_offsets_and_permit_after_cache_invalidation() -> Result<()> {
    let mut temporary = NamedTempFile::new()?;
    let data = (0..65536).map(|i| (i * 73 % 251) as u8).collect::<Vec<_>>();
    temporary.write_all(&data)?;
    temporary.flush()?;
    let pool = FilePermitPool::new(4)?;
    let cache = OpenFileCache::new(pool.clone(), 1);
    let query = budget(QueryOptions::default())?;
    let file = cache.get(temporary.path(), &query)?;
    cache.invalidate(temporary.path())?;
    assert_eq!(cache.stats()?.1, 0);
    assert_eq!(pool.stats().1, 1);
    let barrier = Barrier::new(4);
    std::thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();
        for worker in 0..4 {
            let (file, query, data, barrier) = (&file, &query, &data, &barrier);
            workers.push(scope.spawn(move || -> Result<()> {
                barrier.wait();
                let mut bytes = [0; 257];
                for number in 0..2000 {
                    let offset = (number * 173 + worker * 71) % (data.len() - bytes.len());
                    file.read_exact_at(offset as u64, &mut bytes, query)?;
                    assert_eq!(bytes, data[offset..offset + bytes.len()]);
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker.join().expect("reader panicked")?;
        }
        Ok(())
    })?;
    let error = file.read_exact_at(file.length() - 1, &mut [0; 2], &query);
    assert!(matches!(error, Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof));
    // An EOF on one offset must not affect the next caller's position.
    let mut bytes = [0; 17];
    file.read_exact_at(7, &mut bytes, &query)?;
    assert_eq!(bytes, data[7..24]);
    drop(file);
    assert_eq!(pool.stats().1, 0);
    Ok(())
}

#[test]
fn positional_read_observes_cancellation_and_deadline_before_io() -> Result<()> {
    let mut temporary = NamedTempFile::new()?;
    temporary.write_all(&[1, 2, 3])?;
    let cache = OpenFileCache::new(FilePermitPool::new(4)?, 1);
    let file = cache.get(temporary.path(), budget(QueryOptions::default())?.as_ref())?;
    let token = CancellationToken::new();
    let cancelled = budget(QueryOptions {
        cancellation: Some(token.clone()),
        ..QueryOptions::default()
    })?;
    token.cancel();
    let mut bytes = [9];
    assert!(matches!(
        file.read_exact_at(0, &mut bytes, &cancelled),
        Err(Error::QueryCancelled)
    ));
    assert_eq!(bytes, [9]);
    // Construct before expiry; the read must recheck the deadline.
    let expired = budget(QueryOptions {
        deadline: Some(Instant::now() + Duration::from_millis(20)),
        ..QueryOptions::default()
    })?;
    std::thread::sleep(Duration::from_millis(25));
    assert!(matches!(
        file.read_exact_at(0, &mut bytes, &expired),
        Err(Error::QueryDeadlineExceeded)
    ));
    assert_eq!(bytes, [9]);
    Ok(())
}
