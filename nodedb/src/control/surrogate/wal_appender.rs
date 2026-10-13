// SPDX-License-Identifier: BUSL-1.1

//! WAL appender for surrogate hwm flushes.
//!
//! Decouples the registry's flush path from a concrete `WalManager`
//! handle so tests can substitute a no-op (or in-memory) appender
//! without spinning up a real WAL. Production wires
//! [`WalSurrogateAppender`].
//!
//! The appender is invoked from [`crate::control::surrogate::assign`]
//! immediately after the catalog rows have been persisted. Order is
//! load-bearing for crash recovery: boot replays every `SurrogateAlloc`
//! and `SurrogateBind` record into the catalog and registry
//! (`crate::wal::replay_surrogate_records`). The replay only raises the
//! watermark and re-writes bindings, so a record the catalog already
//! holds changes nothing.

use std::sync::Arc;

use nodedb_types::{CollectionKey, TenantId};

use crate::wal::WalManager;
use crate::wal::manager::NO_APPLY_KEY;

/// Pluggable WAL appender. Tests substitute `NoopWalAppender`;
/// production wires [`WalSurrogateAppender`] (a thin wrapper over
/// `Arc<WalManager>`).
pub trait SurrogateWalAppender: Send + Sync {
    /// Append a `SurrogateAlloc` record carrying the new high-water
    /// surrogate value. Called by the surrogate-flush path after the
    /// catalog row has been updated.
    fn record_alloc_to_wal(&self, hi: u32) -> crate::Result<()>;

    /// Append one `SurrogateBind` record per `(surrogate, pk_bytes)` pair
    /// in `binds`, then force them to disk with one sync. Called by the
    /// assigner after the catalog transaction commits, so every binding
    /// is durable before the write-lock is released. Each record carries
    /// the key's bare name. The `(database_id, tenant_id)` scope is
    /// stamped into the record header so replay re-keys the binding under
    /// the same canonical key. An empty `binds` writes and syncs nothing.
    fn record_binds_to_wal(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        binds: &[(u32, &[u8])],
    ) -> crate::Result<()>;
}

/// Production appender — wraps `Arc<WalManager>` and forwards to
/// `WalAppender::append_surrogate_alloc`.
pub struct WalSurrogateAppender {
    wal: Arc<WalManager>,
}

impl WalSurrogateAppender {
    pub fn new(wal: Arc<WalManager>) -> Self {
        Self { wal }
    }
}

impl SurrogateWalAppender for WalSurrogateAppender {
    fn record_alloc_to_wal(&self, hi: u32) -> crate::Result<()> {
        self.wal
            .appender(NO_APPLY_KEY)
            .append_surrogate_alloc(hi)
            .map(|_| ())
    }

    fn record_binds_to_wal(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        binds: &[(u32, &[u8])],
    ) -> crate::Result<()> {
        if binds.is_empty() {
            return Ok(());
        }
        let appender = self.wal.appender(NO_APPLY_KEY);
        for &(surrogate, pk_bytes) in binds {
            appender.append_surrogate_bind(
                key.database_id(),
                tenant_id,
                surrogate,
                key.name(),
                pk_bytes,
            )?;
        }
        // One sync covers the whole batch and lands before the assigner
        // releases its write-lock. A crash after `assign` returns must
        // always see the bindings on replay. Group-commit batching alone
        // does not give that guarantee.
        self.wal.sync()
    }
}

/// No-op appender. Used by tests that exercise assign / flush logic
/// without a WAL (and by the bootstrap shim before the WAL is wired).
pub struct NoopWalAppender;

impl SurrogateWalAppender for NoopWalAppender {
    fn record_alloc_to_wal(&self, _hi: u32) -> crate::Result<()> {
        Ok(())
    }

    fn record_binds_to_wal(
        &self,
        _key: CollectionKey<'_>,
        _tenant_id: TenantId,
        _binds: &[(u32, &[u8])],
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_appender_succeeds() {
        let a = NoopWalAppender;
        a.record_alloc_to_wal(123).unwrap();
        a.record_alloc_to_wal(u32::MAX).unwrap();
    }
}
