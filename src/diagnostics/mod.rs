//! Opt-in runtime measurements.

#[cfg(feature = "publication-diagnostics")]
pub mod publication;
#[cfg(not(feature = "publication-diagnostics"))]
pub(crate) mod publication;
#[cfg(feature = "read-diagnostics")]
pub mod query;
#[cfg(feature = "pipeline-trace")]
pub mod trace;

#[cfg(feature = "state-diagnostics")]
pub mod state;
