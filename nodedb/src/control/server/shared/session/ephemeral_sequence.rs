// SPDX-License-Identifier: BUSL-1.1

//! Per-connection ephemeral sequence handles.
//!
//! `CREATE SEQUENCE` inside an open transaction is buffered
//! ([`super::ddl_buffer`]) and never reaches `SequenceRegistry`'s shared map
//! until COMMIT applies it (`post_apply` only runs for
//! `ProposeOutcome::needs_local_apply()`, which `Buffered` never satisfies).
//! This slot holds handles the same connection has materialized from its own
//! buffered `CREATE SEQUENCE` so `NEXTVAL` / `CURRVAL` / `SETVAL` see them
//! before COMMIT. See `control::sequence::ddl_overlay` for the fallback that
//! populates it.
//!
//! Cleared by [`super::ddl_buffer::take`], [`super::ddl_buffer::discard`],
//! and [`super::ddl_buffer::truncate`] — the exact three sites that mutate
//! the DDL buffer's contents — so this map can never disagree with it.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::control::sequence::types::SequenceHandle;

/// Ephemeral handles for one connection, keyed by
/// `"{database_id}:{tenant_id}:{name}"` (the same key `SequenceRegistry` uses
/// for its shared map).
pub type EphemeralSequences = HashMap<String, SequenceHandle>;

/// Run `f` against this connection's ephemeral-sequence slot, or return
/// `default` outside any connection scope.
fn with_slot<T>(default: T, f: impl FnOnce(&RefCell<EphemeralSequences>) -> T) -> T {
    super::conn_scope::with_scope(default, |scope| f(&scope.ephemeral_sequences))
}

/// Run `f` against the already-materialized handle at `key`. `None` when no
/// connection scope is active, or nothing has been materialized under `key`
/// yet.
pub fn with_handle<T>(key: &str, f: impl Fn(&SequenceHandle) -> T) -> Option<T> {
    with_slot(None, |slot| slot.borrow().get(key).map(&f))
}

/// Materialize a handle under `key` via `make` if absent, then run `f`
/// against it. `None` outside any connection scope.
pub fn materialize_and_with<T>(
    key: &str,
    make: impl FnOnce() -> SequenceHandle,
    f: impl Fn(&SequenceHandle) -> T,
) -> Option<T> {
    with_slot(None, |slot| {
        let mut map = slot.borrow_mut();
        let handle = map.entry(key.to_owned()).or_insert_with(make);
        Some(f(handle))
    })
}

/// Drop every ephemeral handle. Called anywhere the DDL buffer itself is
/// cleared or rewound, so a sequence whose defining `CREATE SEQUENCE` is no
/// longer buffered cannot keep serving from a stale handle.
pub fn clear() {
    with_slot((), |slot| slot.borrow_mut().clear());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::server::shared::session::conn_scope;

    fn def(name: &str) -> StoredSequence {
        StoredSequence::new(4, 1, name.to_owned(), "alice".into())
    }

    #[tokio::test]
    async fn miss_outside_materialization_is_none() {
        conn_scope::scoped(async {
            assert!(with_handle("4:1:orders_seq", |h| h.def.name.clone()).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn materialize_then_reuse_the_same_handle() {
        conn_scope::scoped(async {
            let first = materialize_and_with(
                "4:1:orders_seq",
                || SequenceHandle::new(def("orders_seq"), None),
                |h| h.nextval().unwrap(),
            )
            .expect("connection scope is active");
            assert_eq!(first, 1);

            // A second materialize call reuses the existing handle rather
            // than resetting the counter — `make` must not run again.
            let second = materialize_and_with(
                "4:1:orders_seq",
                || panic!("must not re-materialize an existing handle"),
                |h| h.nextval().unwrap(),
            )
            .expect("connection scope is active");
            assert_eq!(second, 2);
        })
        .await;
    }

    #[tokio::test]
    async fn clear_drops_every_handle() {
        conn_scope::scoped(async {
            materialize_and_with(
                "4:1:orders_seq",
                || SequenceHandle::new(def("orders_seq"), None),
                |_| (),
            );
            clear();
            assert!(with_handle("4:1:orders_seq", |h| h.def.name.clone()).is_none());
        })
        .await;
    }

    #[test]
    fn outside_any_scope_is_inert() {
        assert!(with_handle("4:1:orders_seq", |h| h.def.name.clone()).is_none());
        clear();
    }
}
