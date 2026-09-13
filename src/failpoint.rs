#[cfg(debug_assertions)]
pub(crate) fn hit(name: &str) {
    if std::env::var("CHIMAERA_TEST_FAILPOINT").as_deref() == Ok(name) {
        eprintln!("[chimaera failpoint] {name}");
        if std::env::var("CHIMAERA_TEST_FAILPOINT_ACTION").as_deref() == Ok("pause") {
            let Some(marker) = std::env::var_os("CHIMAERA_TEST_FAILPOINT_MARKER") else {
                eprintln!("[chimaera failpoint] pause action requires a marker path");
                std::process::abort();
            };
            if let Err(error) = std::fs::write(marker, name) {
                eprintln!("[chimaera failpoint] cannot write marker: {error}");
                std::process::abort();
            }
            loop {
                std::thread::park_timeout(std::time::Duration::from_secs(3_600));
            }
        }
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub(crate) fn hit(_name: &str) {}

#[cfg(debug_assertions)]
pub(crate) fn wal_partial_write_bytes(commit_id: u64, frame_len: usize) -> Option<usize> {
    let target_commit = std::env::var("CHIMAERA_TEST_WAL_PARTIAL_COMMIT")
        .ok()?
        .parse::<u64>()
        .ok()?;
    if target_commit != commit_id {
        return None;
    }
    std::env::var("CHIMAERA_TEST_WAL_PARTIAL_BYTES")
        .ok()?
        .parse::<usize>()
        .ok()
        .map(|bytes| bytes.min(frame_len.saturating_sub(1)))
}

#[cfg(debug_assertions)]
pub(crate) fn wal_sync_error() -> bool {
    std::env::var("CHIMAERA_TEST_WAL_SYNC_ERROR").as_deref() == Ok("1")
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub(crate) fn wal_partial_write_bytes(_commit_id: u64, _frame_len: usize) -> Option<usize> {
    None
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub(crate) fn wal_sync_error() -> bool {
    false
}

#[cfg(debug_assertions)]
pub(crate) fn io_error(name: &str) -> std::io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static MATCHES: AtomicU64 = AtomicU64::new(0);
    if std::env::var("CHIMAERA_TEST_IO_ERROR").as_deref() == Ok(name) {
        let at = std::env::var("CHIMAERA_TEST_IO_ERROR_AT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        if MATCHES.fetch_add(1, Ordering::Relaxed) + 1 == at {
            let code = std::env::var("CHIMAERA_TEST_IO_ERROR_CODE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(libc::EIO);
            return Err(std::io::Error::from_raw_os_error(code));
        }
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub(crate) fn io_error(_name: &str) -> std::io::Result<()> {
    Ok(())
}
