// SPDX-License-Identifier: BUSL-1.1

//! An owned transaction block for work the database initiates itself.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::server::shared::session::{DmlTxnCtx, SessionId, SessionStore};
use crate::control::state::SharedState;

/// A private session sitting inside a transaction block, owned by the caller.
///
/// Client transactions live on a connection; a trigger or event action has no
/// connection, so it brings its own session. The store is private to this
/// scope, so the fixed session identity never collides with another scope's.
pub struct SystemTxnScope {
    sessions: SessionStore,
    session_id: SessionId,
    /// Lease scopes retained by statement-serial planning (procedural trigger
    /// bodies). Each buffered task's `QueryLeaseScope` must stay alive until
    /// COMMIT finishes the version fence; the scope owns them so a per-statement
    /// `Arc` cannot drop early. `Mutex` because the scope is shared as an
    /// `Arc<SystemTxnScope>` between the executor and the commit path.
    lease_scopes: std::sync::Mutex<Vec<Arc<QueryLeaseScope>>>,
}

impl SystemTxnScope {
    /// Open a transaction block on a fresh private session.
    pub fn begin(state: &SharedState) -> Result<Self, crate::Error> {
        let sessions = SessionStore::new();
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
        // `begin` reports success without entering the block when the session
        // does not exist yet, so the session must be created first.
        sessions.ensure_session(addr);
        let session_id = SessionId::from(addr);

        let snapshot_lsn = {
            let next = state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        let snapshot_epoch = state
            .last_applied_calvin_epoch
            .load(std::sync::atomic::Ordering::Acquire);

        // Deliberately no `ddl_buffer::activate()`, unlike the client BEGIN
        // path. That buffer is thread-local and is only sound where the whole
        // transaction runs on one thread; a system transaction runs on the
        // Event Plane, where an await can move it between worker threads. A
        // system action carries DML, so DDL buffering has nothing to do here
        // and any DDL it did contain proposes through the normal path.
        sessions
            .begin(session_id, snapshot_lsn, snapshot_epoch)
            .map_err(|detail| crate::Error::BadRequest {
                detail: detail.to_owned(),
            })?;

        Ok(Self {
            sessions,
            session_id,
            lease_scopes: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Borrow the DML routing context for this scope's session.
    pub fn ctx(&self) -> DmlTxnCtx<'_> {
        DmlTxnCtx {
            sessions: &self.sessions,
            session_id: self.session_id,
        }
    }

    pub fn sessions(&self) -> &SessionStore {
        &self.sessions
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Retain a plan lease scope until the scope's COMMIT finishes the
    /// version fence. Statement-serial planning (procedural trigger bodies)
    /// acquires one `Arc<QueryLeaseScope>` per statement; without retention
    /// the Arc would drop when the statement returns and the COMMIT fence
    /// would lose the versions it must compare.
    pub fn retain_lease_scope(&self, lease_scope: Arc<QueryLeaseScope>) {
        self.lease_scopes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(lease_scope);
    }
}
