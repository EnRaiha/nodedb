// SPDX-License-Identifier: BUSL-1.1

//! Concurrent session store — keyed by socket address.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::state::{ConnSession, TransactionState};

/// Concurrent session store — keyed by socket address.
pub struct SessionStore {
    sessions: RwLock<HashMap<SocketAddr, ConnSession>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure a session exists for this address.
    pub fn ensure_session(&self, addr: SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.entry(addr).or_insert_with(ConnSession::new);
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(addr);
    }

    /// List all active sessions as (peer_address, transaction_state) pairs.
    pub fn all_sessions(&self) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(addr, session)| {
                let tx = match session.tx_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                (addr.to_string(), tx.to_string())
            })
            .collect()
    }

    /// Number of active sessions.
    pub fn count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    /// Look up cached physical tasks for a SQL string in the
    /// session's plan cache. `current_version` maps each
    /// recorded descriptor id to its current persisted version
    /// (or `None` if dropped). The cache returns a hit only
    /// when every recorded `(id, version)` pair still matches.
    ///
    /// On a hit returns both the cached tasks and the
    /// `DescriptorVersionSet` they were built against — the
    /// caller passes the set into
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    pub fn get_cached_plan<F>(
        &self,
        addr: &SocketAddr,
        sql: &str,
        current_version: F,
    ) -> Option<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
    )>
    where
        F: Fn(&nodedb_cluster::DescriptorId) -> Option<u64>,
    {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .get_mut(addr)
            .and_then(|s| s.plan_cache.get(sql, current_version))
    }

    /// Store compiled physical tasks in the session's plan
    /// cache along with the descriptor version set they were
    /// built against.
    pub fn put_cached_plan(
        &self,
        addr: &SocketAddr,
        sql: &str,
        tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
        versions: crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.plan_cache.put(sql, tasks, versions);
        }
    }

    /// Retrieve the `current_database` for a connection, or `None` if the session
    /// does not exist or has not had a database bound yet.
    pub fn get_current_database(&self, addr: &SocketAddr) -> Option<DatabaseId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr)?.current_database
    }

    /// Bind a database to a session.  Called at pgwire startup once the database
    /// name from the StartupMessage has been resolved to a `DatabaseId`.
    pub fn set_current_database(&self, addr: &SocketAddr, db_id: DatabaseId) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.current_database = Some(db_id);
        }
    }

    /// Read the session's superuser tenant override, if any. Returns `None`
    /// when the session has never run `SET TENANT` (the common case).
    pub fn get_effective_tenant_id(&self, addr: &SocketAddr) -> Option<TenantId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).and_then(|s| s.effective_tenant_id)
    }

    /// Install or clear the session's tenant override. Callers MUST have
    /// already verified the connection is a superuser and is not inside an
    /// active transaction — this method performs no policy checks.
    ///
    /// Invalidates the session's plan cache and SQL-level prepared statements
    /// so plans built against the prior tenant's catalog cannot be reused.
    pub fn set_effective_tenant_id(&self, addr: &SocketAddr, tenant: Option<TenantId>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.effective_tenant_id = tenant;
            session.plan_cache.clear();
            session.prepared_stmts.clear();
        }
    }

    /// Reset per-session state for a `USE DATABASE` switch:
    ///   1. Aborts any open transaction (discards tx_buffer, resets state to Idle).
    ///   2. Clears all SQL-level prepared statements.
    ///   3. Clears the wire-level plan cache.
    ///   4. Rebinds `current_database` to the new id.
    pub fn reset_for_database_switch(&self, addr: &SocketAddr, new_db: DatabaseId) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            // Abort open transaction.
            session.tx_state = TransactionState::Idle;
            session.tx_buffer.clear();
            session.tx_snapshot_lsn = None;
            session.tx_read_set.clear();
            session.savepoints.clear();
            session.pending_offset_commits.clear();
            session.pending_notifies.clear();
            // Invalidate prepared statements and plan cache.
            session.prepared_stmts.clear();
            session.plan_cache.clear();
            // A USE DATABASE switch crosses out of any tenant override — the
            // new database may not exist (or have the same id) in the override
            // tenant, so the safe contract is to drop the override on switch.
            session.effective_tenant_id = None;
            // Rebind database.
            session.current_database = Some(new_db);
        }
    }

    /// Access the session map with a read lock for use by other session submodules.
    pub(super) fn read_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&ConnSession) -> R,
    ) -> Option<R> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).map(f)
    }

    /// Access the session map with a write lock for use by other session submodules.
    pub(super) fn write_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&mut ConnSession) -> R,
    ) -> Option<R> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.get_mut(addr).map(f)
    }
}
