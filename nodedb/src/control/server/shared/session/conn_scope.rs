// SPDX-License-Identifier: BUSL-1.1

//! The single connection-scoped slot holding every per-connection session
//! value.
//!
//! Both the transactional DDL buffer and the DDL audit context live for
//! exactly one client connection and must follow its task across every
//! `.await` — tokio may poll a connection on a different worker after any of
//! them. They share one task-local so the connection future gains one layer.

use std::cell::RefCell;
use std::future::Future;

use super::audit_context::AuditCtx;
use super::ddl_buffer::DdlBuffer;
use super::ephemeral_sequence::EphemeralSequences;

/// Per-connection session slots. Each field is owned by its own module, which
/// exposes the accessors; nothing outside reaches through this struct.
pub(super) struct ConnScope {
    pub(super) ddl_buffer: RefCell<Option<DdlBuffer>>,
    pub(super) audit: RefCell<Option<AuditCtx>>,
    /// Sequences materialized from this connection's own buffered `CREATE
    /// SEQUENCE`, not yet visible in the shared registry. See
    /// `ephemeral_sequence` and `control::sequence::ddl_overlay`.
    pub(super) ephemeral_sequences: RefCell<EphemeralSequences>,
}

impl ConnScope {
    fn empty() -> Self {
        Self {
            ddl_buffer: RefCell::new(None),
            audit: RefCell::new(None),
            ephemeral_sequences: RefCell::new(EphemeralSequences::new()),
        }
    }
}

tokio::task_local! {
    static CONN_SCOPE: ConnScope;
}

/// Install the connection-scoped slots around `future`.
///
/// Every protocol entry point that can execute `BEGIN` wraps its whole
/// connection future in this exactly once. Returning the scope future
/// directly, rather than awaiting it inside an `async fn`, keeps one type
/// layer off the connection future's already deep state machine.
pub fn scoped<F: Future>(future: F) -> impl Future<Output = F::Output> {
    CONN_SCOPE.scope(ConnScope::empty(), future)
}

/// Run `f` against the current connection's slots, or return `default` when
/// the caller runs outside any connection scope (Event Plane system
/// transactions, bootstrap, background DDL).
pub(super) fn with_scope<T>(default: T, f: impl FnOnce(&ConnScope) -> T) -> T {
    CONN_SCOPE.try_with(f).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::super::{audit_context, ddl_buffer};
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn both_slots_are_installed_and_survive_migration() {
        scoped(async {
            ddl_buffer::activate();
            let _audit = audit_context::AuditScope::new(AuditCtx {
                auth_user_id: "1".into(),
                auth_user_name: "alice".into(),
                sql_text: "CREATE COLLECTION x".into(),
            });
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(ddl_buffer::is_active());
            assert!(audit_context::current().is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn slots_are_absent_outside_the_scope() {
        assert!(!ddl_buffer::is_active());
        assert!(audit_context::current().is_none());
    }
}
