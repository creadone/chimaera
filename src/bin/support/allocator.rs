//! Explicit allocator ownership for repository executables and test harnesses.
//! This module is never included in the shipping library.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Background workers start after allocator initialization, not before main.
#[cfg(all(feature = "jemalloc", target_os = "linux"))]
const OPTIONS: &[u8] = b"percpu_arena:percpu,dirty_decay_ms:5000,muzzy_decay_ms:0,oversize_threshold:67108864,lg_extent_max_active_fit:6,max_background_threads:4,background_thread:false,abort_conf:true\0";
#[cfg(all(feature = "jemalloc", not(target_os = "linux")))]
const OPTIONS: &[u8] = b"dirty_decay_ms:5000,muzzy_decay_ms:0,oversize_threshold:67108864,lg_extent_max_active_fit:6,background_thread:false,abort_conf:true\0";

// SAFETY: the documented configuration symbol points to immutable NUL-terminated
// bytes for the process lifetime. Each executable includes this module once.
#[cfg(feature = "jemalloc")]
#[used]
#[allow(unsafe_code)]
#[unsafe(export_name = "_rjem_malloc_conf")]
static CONFIGURATION: &u8 = &OPTIONS[0];

pub(crate) fn verify() -> chimaera::Result<serde_json::Value> {
    #[cfg(all(feature = "jemalloc", target_os = "linux"))]
    {
        static STARTED: std::sync::OnceLock<std::result::Result<(), String>> =
            std::sync::OnceLock::new();
        STARTED
            .get_or_init(|| {
                tikv_jemalloc_ctl::background_thread::write(true).map_err(|e| e.to_string())
            })
            .as_ref()
            .map_err(|e| chimaera::Error::Invalid(format!("jemalloc background workers: {e}")))?;
    }
    chimaera::allocator::verify()
}
