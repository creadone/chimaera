//! The existing state RwLock, with optional bounded diagnostic guards.

use crate::error::{Result, corrupt};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "state-diagnostics")]
use crate::diagnostics::state::{Collector, Guard, Session};
#[cfg(feature = "state-diagnostics")]
use std::sync::{Arc, OnceLock};

pub(super) struct Lock<T> {
    value: RwLock<T>,
    #[cfg(feature = "state-diagnostics")]
    collector: OnceLock<Arc<Collector>>,
}

#[cfg(feature = "state-diagnostics")]
pub(super) type Read<'a, T> = Guard<RwLockReadGuard<'a, T>>;
#[cfg(not(feature = "state-diagnostics"))]
pub(super) type Read<'a, T> = RwLockReadGuard<'a, T>;
#[cfg(feature = "state-diagnostics")]
pub(super) type Write<'a, T> = Guard<RwLockWriteGuard<'a, T>>;
#[cfg(not(feature = "state-diagnostics"))]
pub(super) type Write<'a, T> = RwLockWriteGuard<'a, T>;

impl<T> Lock<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            value: RwLock::new(value),
            #[cfg(feature = "state-diagnostics")]
            collector: OnceLock::new(),
        }
    }

    #[track_caller]
    pub(super) fn read(&self) -> Result<Read<'_, T>> {
        #[cfg(feature = "state-diagnostics")]
        let waiting = self.collector.get().map(|_| std::time::Instant::now());
        let guard = self
            .value
            .read()
            .map_err(|_| corrupt("state read lock is poisoned"))?;
        #[cfg(feature = "state-diagnostics")]
        return Ok(Guard::new(guard, self.collector.get(), waiting, false));
        #[cfg(not(feature = "state-diagnostics"))]
        Ok(guard)
    }

    #[track_caller]
    pub(super) fn write(&self) -> Result<Write<'_, T>> {
        #[cfg(feature = "state-diagnostics")]
        let waiting = self.collector.get().map(|_| std::time::Instant::now());
        let guard = self
            .value
            .write()
            .map_err(|_| corrupt("state write lock is poisoned"))?;
        #[cfg(feature = "state-diagnostics")]
        return Ok(Guard::new(guard, self.collector.get(), waiting, true));
        #[cfg(not(feature = "state-diagnostics"))]
        Ok(guard)
    }

    #[track_caller]
    pub(super) fn try_write(&self) -> Result<Option<Write<'_, T>>> {
        match self.value.try_write() {
            Ok(guard) => {
                #[cfg(feature = "state-diagnostics")]
                return Ok(Some(Guard::new(guard, self.collector.get(), None, true)));
                #[cfg(not(feature = "state-diagnostics"))]
                Ok(Some(guard))
            }
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(corrupt("query state lock is poisoned"))
            }
        }
    }

    #[cfg(feature = "state-diagnostics")]
    pub(super) fn start(&self) -> Result<Session> {
        let collector = Collector::new();
        self.collector
            .set(Arc::clone(&collector))
            .map_err(|_| crate::error::invalid("a state diagnostic session already exists"))?;
        Ok(Session(collector))
    }

    #[cfg(feature = "state-diagnostics")]
    pub(super) fn blockers(&self) -> Option<[u64; crate::diagnostics::state::SLOTS]> {
        self.collector.get().map(|collector| collector.blockers())
    }
}

#[cfg(feature = "state-diagnostics")]
impl super::Storage {
    /// Start bounded state-lock diagnostics for this Storage. Not a lock-owner oracle:
    /// holder observations can race with unlock and acquisition.
    ///
    /// # Errors
    /// A session has already been started for this Storage.
    pub fn start_state_diagnostics(&self) -> Result<Session> {
        self.inner.state.start()
    }
}
