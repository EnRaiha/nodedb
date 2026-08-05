// SPDX-License-Identifier: BUSL-1.1

//! Per-connection state tracking pending COPY IN restore operations.
//!
//! pgwire's COPY protocol spans multiple wire messages:
//!   1. SimpleQuery: server returns CopyIn → records pending intent here.
//!   2. CopyHandler::on_copy_data (potentially many): byte accumulator grows.
//!   3. CopyHandler::on_copy_done: validate envelope and dispatch restore.
//!
//! State is keyed by a connection-level identifier minted in step 1
//! and looked up in step 2/3.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::control::security::identity::AuthenticatedIdentity;

#[derive(Debug)]
pub struct RestorePending {
    pub tenant_id: u64,
    pub dry_run: bool,
    /// Bypass the restore staleness gate (disaster-recovery restore-after-purge).
    pub force: bool,
    pub bytes: Vec<u8>,
    /// Hard cap on accumulated bytes. The handler aborts the COPY IN
    /// once the limit would be exceeded.
    pub max_bytes: u64,
    /// The identity that opened this COPY IN, carried from `intent_to_response`
    /// through to `on_copy_done` — `on_copy_done` runs on a per-connection
    /// `CopyHandler` with no identity of its own, and post-dispatch usage
    /// metering needs the caller who requested the restore to attribute the
    /// completed restore's usage to.
    pub identity: AuthenticatedIdentity,
}

impl RestorePending {
    pub fn new(
        tenant_id: u64,
        dry_run: bool,
        force: bool,
        max_bytes: u64,
        identity: AuthenticatedIdentity,
    ) -> Self {
        Self {
            tenant_id,
            dry_run,
            force,
            bytes: Vec::new(),
            max_bytes,
            identity,
        }
    }

    /// Returns Err once the cap would be exceeded.
    pub fn append(&mut self, chunk: &[u8]) -> Result<(), u64> {
        let new_total = self.bytes.len() as u64 + chunk.len() as u64;
        if new_total > self.max_bytes {
            return Err(self.max_bytes);
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

/// Connection-keyed map of in-flight restores.
#[derive(Default)]
pub struct RestoreState {
    inner: Mutex<HashMap<u64, RestorePending>>,
}

impl RestoreState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&self, conn_id: u64, pending: RestorePending) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.insert(conn_id, pending);
    }

    pub fn append(&self, conn_id: u64, chunk: &[u8]) -> Result<(), AppendError> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let entry = g.get_mut(&conn_id).ok_or(AppendError::NotPending)?;
        entry
            .append(chunk)
            .map_err(|cap| AppendError::OverCap { cap })
    }

    pub fn take(&self, conn_id: u64) -> Option<RestorePending> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.remove(&conn_id)
    }

    pub fn cancel(&self, conn_id: u64) {
        let _ = self.take(conn_id);
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AppendError {
    #[error("no restore pending on this connection")]
    NotPending,
    #[error("backup exceeds {cap}-byte size cap")]
    OverCap { cap: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::types::TenantId;

    fn test_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            1,
            "restore-user",
            TenantId::new(7),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    #[test]
    fn append_then_take() {
        let s = RestoreState::new();
        s.begin(
            1,
            RestorePending::new(7, false, false, 1024, test_identity()),
        );
        s.append(1, b"hello ").unwrap();
        s.append(1, b"world").unwrap();
        let p = s.take(1).unwrap();
        assert_eq!(p.tenant_id, 7);
        assert_eq!(p.bytes, b"hello world");
        assert!(s.take(1).is_none());
    }

    #[test]
    fn append_without_begin_fails() {
        let s = RestoreState::new();
        assert_eq!(s.append(99, b"x"), Err(AppendError::NotPending));
    }

    #[test]
    fn append_respects_cap() {
        let s = RestoreState::new();
        s.begin(1, RestorePending::new(7, false, false, 4, test_identity()));
        s.append(1, b"abc").unwrap();
        assert_eq!(s.append(1, b"de"), Err(AppendError::OverCap { cap: 4 }));
    }

    #[test]
    fn cancel_drops_state() {
        let s = RestoreState::new();
        s.begin(
            1,
            RestorePending::new(7, false, false, 1024, test_identity()),
        );
        s.cancel(1);
        assert_eq!(s.append(1, b"x"), Err(AppendError::NotPending));
    }
}
