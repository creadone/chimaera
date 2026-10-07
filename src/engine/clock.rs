use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

/// Clock used for retention and policy time validation.
pub trait Clock: Debug + Send + Sync + 'static {
    /// Return Unix time in microseconds.
    fn now_us(&self) -> i64;
}

/// Operating-system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_us(&self) -> i64 {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
    }
}
