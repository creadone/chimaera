use crate::diagnostics::publication::{Collector, Session};
use crate::engine::Storage;
use crate::error::{Result, invalid};
use std::sync::Arc;

impl Storage {
    /// Start fixed-size publication diagnostics on this Storage.
    ///
    /// # Errors
    /// Rejects a second session. Retain the handle through close to include
    /// completed background work in the final snapshot.
    pub fn start_publication_diagnostics(&self) -> Result<Session> {
        let collector = Collector::new();
        self.inner
            .publication_diagnostics
            .set(Arc::clone(&collector))
            .map_err(|_| invalid("a publication diagnostic session already exists"))?;
        Ok(Session(collector))
    }
}
