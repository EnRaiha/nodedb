// SPDX-License-Identifier: BUSL-1.1

//! Per-session DDL audit context.
//!
//! The pgwire `do_query` entry point installs a snapshot of the
//! authenticated identity + raw statement text on a thread-local
//! before dispatching the statement. Every `propose_catalog_entry`
//! call that fires inside that scope picks up the context and
//! attaches it to the replicated [`MetadataEntry::CatalogDdlAudited`]
//! so the applier can emit the J.4 audit record on every replica —
//! including followers that never saw the raw SQL.
//!
//! Thread-local is sound here because pgwire DDL runs synchronously
//! on a single OS thread (the handler wraps the raft wait in
//! `block_in_place`); installation and consumption happen on the
//! same thread.

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

thread_local! {
    static CURRENT: RefCell<Option<AuditCtx>> = const { RefCell::new(None) };
}

/// RAII guard that installs `ctx` for the current thread on
/// construction and clears it on drop. Use at the top of a pgwire
/// statement handler so nested DDL proposers inherit the context.
pub struct AuditScope {
    _private: (),
}

impl AuditScope {
    pub fn new(ctx: AuditCtx) -> Self {
        CURRENT.with(|c| *c.borrow_mut() = Some(ctx));
        Self { _private: () }
    }
}

impl Drop for AuditScope {
    fn drop(&mut self) {
        CURRENT.with(|c| c.borrow_mut().take());
    }
}

/// Return a clone of the currently-installed audit context, if any.
pub fn current() -> Option<AuditCtx> {
    CURRENT.with(|c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(user: &str, sql: &str) -> AuditCtx {
        AuditCtx {
            auth_user_id: user.into(),
            auth_user_name: user.into(),
            sql_text: sql.into(),
        }
    }

    #[test]
    fn no_context_by_default() {
        assert!(current().is_none());
    }

    #[test]
    fn scope_installs_and_clears() {
        {
            let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
            let seen = current().expect("scope sets context");
            assert_eq!(seen.auth_user_name, "alice");
            assert_eq!(seen.sql_text, "CREATE COLLECTION x");
        }
        assert!(current().is_none());
    }

    #[test]
    fn inner_scope_shadows_outer() {
        let _outer = AuditScope::new(ctx("root", "outer"));
        {
            let _inner = AuditScope::new(ctx("bob", "inner"));
            assert_eq!(current().unwrap().auth_user_name, "bob");
        }
        // After inner drops the thread-local is cleared entirely —
        // outer scope is not restored. Pgwire installs exactly one
        // scope per `do_query`, so this is fine; document it
        // clearly via the test so a future caller doesn't assume
        // restoration behaviour.
        assert!(current().is_none());
    }
}
