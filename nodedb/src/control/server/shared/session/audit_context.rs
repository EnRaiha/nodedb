// SPDX-License-Identifier: BUSL-1.1

//! Per-connection DDL audit context.
//!
//! The pgwire `do_query` entry point installs a snapshot of the
//! authenticated identity + raw statement text on the connection slot
//! before dispatching the statement. Every `propose_catalog_entry`
//! call that fires inside that scope picks up the context and
//! attaches it to the replicated [`MetadataEntry::CatalogDdlAudited`]
//! so the applier can emit the J.4 audit record on every replica —
//! including followers that never saw the raw SQL.
//!
//! The slot is connection-scoped, not thread-scoped: the statement
//! handler awaits between install and consumption, and tokio may move
//! the task to another worker at any of those awaits.

use std::cell::RefCell;

/// Minimal identity/SQL snapshot captured at pgwire statement entry.
///
/// Cloned into every `CatalogDdlAudited` entry proposed while it's
/// active; cleared on scope exit via [`AuditScope::drop`].
#[derive(Debug, Clone)]
pub struct AuditCtx {
    pub auth_user_id: String,
    pub auth_user_name: String,
    pub sql_text: String,
}

/// Run `f` against this connection's audit slot, or return `default` when the
/// caller runs outside any connection scope.
fn with_slot<T>(default: T, f: impl FnOnce(&RefCell<Option<AuditCtx>>) -> T) -> T {
    super::conn_scope::with_scope(default, |scope| f(&scope.audit))
}

/// RAII guard that installs `ctx` in the connection's audit slot on
/// construction and clears it on drop. Use at the top of a pgwire
/// statement handler so nested DDL proposers inherit the context.
pub struct AuditScope {
    _private: (),
}

impl AuditScope {
    pub fn new(ctx: AuditCtx) -> Self {
        with_slot((), |c| *c.borrow_mut() = Some(ctx));
        Self { _private: () }
    }
}

impl Drop for AuditScope {
    fn drop(&mut self) {
        with_slot((), |c| {
            let _ = c.borrow_mut().take();
        });
    }
}

/// Return a clone of the currently-installed audit context, if any.
pub fn current() -> Option<AuditCtx> {
    with_slot(None, |c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    fn ctx(user: &str, sql: &str) -> AuditCtx {
        AuditCtx {
            auth_user_id: user.into(),
            auth_user_name: user.into(),
            sql_text: sql.into(),
        }
    }

    #[tokio::test]
    async fn no_context_by_default() {
        conn_scope::scoped(async {
            assert!(current().is_none());
        })
        .await;
    }

    #[test]
    fn outside_any_scope_is_inert() {
        assert!(current().is_none());
        let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn scope_installs_and_clears() {
        conn_scope::scoped(async {
            {
                let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
                let seen = current().expect("scope sets context");
                assert_eq!(seen.auth_user_name, "alice");
                assert_eq!(seen.sql_text, "CREATE COLLECTION x");
            }
            assert!(current().is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn inner_scope_shadows_outer() {
        conn_scope::scoped(async {
            let _outer = AuditScope::new(ctx("root", "outer"));
            {
                let _inner = AuditScope::new(ctx("bob", "inner"));
                assert_eq!(
                    current().expect("inner scope sets context").auth_user_name,
                    "bob"
                );
            }
            // After inner drops the slot is cleared entirely — outer scope is
            // not restored. Pgwire installs exactly one scope per `do_query`,
            // so this is fine; the test pins it so a future caller does not
            // assume restoration behaviour.
            assert!(current().is_none());
        })
        .await;
    }

    /// The context must survive a worker-thread hop: the statement handler
    /// awaits `execute_sql` between installing the scope and proposing DDL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_survives_worker_thread_migration() {
        conn_scope::scoped(async {
            let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                current().expect("context survives the hop").auth_user_name,
                "alice"
            );
        })
        .await;
    }
}
