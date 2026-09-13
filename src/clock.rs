use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

/// Источник wall-clock времени для retention и тестируемого обслуживания.
pub trait Clock: Debug + Send + Sync + 'static {
    /// Текущее время Unix epoch в микросекундах.
    fn now_us(&self) -> i64;
}

/// Системные часы в микросекундах Unix epoch.
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
