//! Optional statistics for the application's process allocator.
//!
//! The `jemalloc` feature enables observation, not allocator installation or
//! configuration. The application owns its global allocator and startup settings.
//! A linked jemalloc is reported only if it observes actual global allocations.

use crate::Result;

/// Jemalloc statistics refreshed through its epoch control, in bytes.
/// These describe jemalloc, not the entire process or its RSS.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Statistics {
    pub allocated_bytes: usize,
    pub active_bytes: usize,
    pub resident_bytes: usize,
    pub metadata_bytes: usize,
    pub mapped_bytes: usize,
    pub retained_virtual_bytes: usize,
}

/// Refresh process-wide jemalloc statistics, not just Chimaera's allocations.
/// Returns `None` if the feature is disabled or global allocations use another
/// allocator. The allocator probe is cached for the process lifetime; applications
/// that dynamically switch allocator backends are not supported by this observer.
///
/// # Errors
/// Returns an error if jemalloc cannot refresh or read its statistics.
pub fn statistics() -> Result<Option<Statistics>> {
    #[cfg(feature = "jemalloc")]
    {
        backend::statistics()
    }
    #[cfg(not(feature = "jemalloc"))]
    {
        Ok(None)
    }
}

/// Check the actual allocator and the accepted benchmark settings before work.
///
/// A standard `Vec` allocation and its release must change jemalloc's current
/// thread counters. Merely linking jemalloc or printing its name is insufficient.
/// Returns the effective settings for inclusion in the measurement report.
/// This check does not install or configure the allocator. Ordinary embedding
/// needs no benchmark preflight; use [`statistics`] for optional observation.
///
/// # Errors
/// Returns an error if jemalloc is disabled, a control cannot be read, the global
/// allocation probe fails, or the effective settings differ from the benchmark policy.
pub fn verify() -> Result<serde_json::Value> {
    #[cfg(feature = "jemalloc")]
    {
        backend::verify()
    }
    #[cfg(not(feature = "jemalloc"))]
    {
        Err(crate::error::invalid(
            "benchmark requires Chimaera's jemalloc feature; allocator is application-defined",
        ))
    }
}

#[cfg(feature = "jemalloc")]
mod backend {
    use super::*;
    use crate::error::invalid;
    use std::ffi::CStr;
    use tikv_jemalloc_ctl::{opt, raw, thread, version};

    fn control<T>(
        name: &str,
        value: std::result::Result<T, tikv_jemalloc_ctl::Error>,
    ) -> Result<T> {
        value.map_err(|e| invalid(format!("jemalloc {name}: {e}")))
    }

    #[derive(Clone, Copy)]
    struct GlobalProbe {
        allocated: u64,
        deallocated: u64,
    }

    fn probe_global() -> Result<Option<GlobalProbe>> {
        let allocated = control("thread.allocatedp", thread::allocatedp::read())?;
        let deallocated = control("thread.deallocatedp", thread::deallocatedp::read())?;
        let before = allocated.get();
        let bytes = std::hint::black_box(vec![0_u8; 1024 * 1024]);
        let allocation = allocated.get().wrapping_sub(before);
        let before = deallocated.get();
        drop(std::hint::black_box(bytes));
        let release = deallocated.get().wrapping_sub(before);
        Ok(
            (allocation >= 1024 * 1024 && release >= 1024 * 1024).then_some(GlobalProbe {
                allocated: allocation,
                deallocated: release,
            }),
        )
    }

    fn global_probe() -> Result<Option<GlobalProbe>> {
        // The global allocator cannot change after linking in supported apps.
        // One process-owned fixed result; no thread-local pointers escape.
        static PROBE: std::sync::OnceLock<std::result::Result<Option<GlobalProbe>, String>> =
            std::sync::OnceLock::new();
        PROBE
            .get_or_init(|| probe_global().map_err(|error| error.to_string()))
            .as_ref()
            .copied()
            .map_err(|error| invalid(error.clone()))
    }

    pub(super) fn statistics() -> Result<Option<Statistics>> {
        use tikv_jemalloc_ctl::{epoch, stats};
        if global_probe()?.is_none() {
            return Ok(None);
        }
        control("epoch", epoch::advance())?;
        Ok(Some(Statistics {
            allocated_bytes: control("stats.allocated", stats::allocated::read())?,
            active_bytes: control("stats.active", stats::active::read())?,
            resident_bytes: control("stats.resident", stats::resident::read())?,
            metadata_bytes: control("stats.metadata", stats::metadata::read())?,
            mapped_bytes: control("stats.mapped", stats::mapped::read())?,
            retained_virtual_bytes: control("stats.retained", stats::retained::read())?,
        }))
    }

    pub(super) fn verify() -> Result<serde_json::Value> {
        let probe = global_probe()?.ok_or_else(|| {
            invalid("jemalloc did not observe the global allocation and release probe")
        })?;
        // SAFETY: the documented mallctl types are ssize_t for decay, size_t for
        // thresholds, and bool for flags. percpu_arena is a jemalloc-owned static
        // NUL-terminated const char*. No pointers or thread-local counters escape.
        let (dirty, muzzy, oversize, fit, percpu, prof, active) = unsafe {
            (
                control(
                    "opt.dirty_decay_ms",
                    raw::read::<isize>(b"opt.dirty_decay_ms\0"),
                )?,
                control(
                    "opt.muzzy_decay_ms",
                    raw::read::<isize>(b"opt.muzzy_decay_ms\0"),
                )?,
                control(
                    "opt.oversize_threshold",
                    raw::read::<usize>(b"opt.oversize_threshold\0"),
                )?,
                control(
                    "opt.lg_extent_max_active_fit",
                    raw::read::<usize>(b"opt.lg_extent_max_active_fit\0"),
                )?,
                CStr::from_ptr(control(
                    "opt.percpu_arena",
                    raw::read::<*const libc::c_char>(b"opt.percpu_arena\0"),
                )?)
                .to_string_lossy()
                .into_owned(),
                control("opt.prof", raw::read::<bool>(b"opt.prof\0"))?,
                control("prof.active", raw::read::<bool>(b"prof.active\0"))?,
            )
        };
        #[cfg(target_os = "linux")]
        let (background, maximum) = (
            Some(control(
                "background_thread",
                tikv_jemalloc_ctl::background_thread::read(),
            )?),
            Some(control(
                "max_background_threads",
                tikv_jemalloc_ctl::max_background_threads::read(),
            )?),
        );
        // Darwin's jemalloc has no background-thread mallctl; null reports that
        // lack of support instead of presenting an invented effective value.
        #[cfg(not(target_os = "linux"))]
        let (background, maximum): (Option<bool>, Option<usize>) = (None, None);
        let tcache = control("opt.tcache", opt::tcache::read())?;
        let report = serde_json::json!({
            "name": "tikv-jemallocator", "crate_version": "0.7.0",
            "version": control("version", version::read())?.trim_end_matches('\0'),
            "global_probe_allocated_bytes": probe.allocated, "global_probe_deallocated_bytes": probe.deallocated,
            "background_thread": background, "max_background_threads": maximum,
            "percpu_arena": percpu, "narenas": control("opt.narenas", opt::narenas::read())?,
            "dirty_decay_ms": dirty, "muzzy_decay_ms": muzzy,
            "oversize_threshold": oversize, "lg_extent_max_active_fit": fit,
            "tcache": tcache, "prof": prof, "prof_active": active,
        });
        if dirty != 5000
            || muzzy != 0
            || oversize != 67_108_864
            || fit != 6
            || !tcache
            || (cfg!(target_os = "linux")
                && (background != Some(true) || maximum != Some(4) || percpu != "percpu"))
        {
            return Err(invalid(format!(
                "jemalloc benchmark configuration mismatch: {report}"
            )));
        }
        Ok(report)
    }
}
