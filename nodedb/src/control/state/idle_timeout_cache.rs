// SPDX-License-Identifier: BUSL-1.1

//! Per-database idle session timeout cache.
//!
//! Stores the `idle_session_timeout_secs` for each database in memory so the
//! idle-sweep loop can look up a session's per-database timeout without a
//! catalog round-trip on every sweep tick.
//! Updated synchronously in the `ALTER DATABASE SET IDLE_TIMEOUT` handler.

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_types::DatabaseId;

use crate::control::security::catalog::SystemCatalog;

/// In-memory cache of per-database idle session timeouts.
///
/// Control Plane only (`Send + Sync`). Backed by a `RwLock<HashMap>` for
/// concurrent reads with infrequent writes.
pub struct IdleTimeoutCache {
    inner: RwLock<HashMap<DatabaseId, u64>>,
}

impl IdleTimeoutCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Return the idle session timeout in seconds for `db_id`.
    ///
    /// Returns `0` (no per-database timeout) on cache miss.
    pub fn get(&self, db_id: DatabaseId) -> u64 {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&db_id)
            .copied()
            .unwrap_or(0)
    }

    /// Update the idle session timeout in seconds for `db_id`.
    /// Setting `secs = 0` removes the per-database override.
    pub fn set(&self, db_id: DatabaseId, secs: u64) {
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        if secs == 0 {
            map.remove(&db_id);
        } else {
            map.insert(db_id, secs);
        }
    }

    /// Replace the cache with every descriptor in the catalog.
    ///
    /// Called at startup. Only databases with a non-zero `idle_session_timeout_secs`
    /// are inserted; `get` returns `0` for unknown databases (same semantics as
    /// `AuditDmlCache`).
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let databases = catalog.list_databases()?;
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for descriptor in databases {
            if descriptor.idle_session_timeout_secs > 0 {
                map.insert(descriptor.id, descriptor.idle_session_timeout_secs);
            }
        }
        Ok(())
    }
}

impl Default for IdleTimeoutCache {
    fn default() -> Self {
        Self::new()
    }
}
