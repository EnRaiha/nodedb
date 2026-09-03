// SPDX-License-Identifier: BUSL-1.1

//! Connection-scoped fallback for a sequence not yet in the shared
//! [`super::registry::SequenceRegistry`] map.
//!
//! `CREATE SEQUENCE` inside an open transaction is buffered and only reaches
//! the shared registry at COMMIT (`post_apply` runs only for
//! `ProposeOutcome::needs_local_apply()`, which `Buffered` never satisfies).
//! Without this fallback, `NEXTVAL` / `CURRVAL` / `SETVAL` on a sequence
//! created earlier in the same transaction resolve as missing.
//!
//! This is deliberately not the shared map: another connection must never
//! observe, or consume values from, a sequence this transaction has not
//! committed.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::server::shared::session::{ddl_buffer, ephemeral_sequence};

use super::types::SequenceHandle;

fn registry_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

/// Replay this connection's buffered DDL to find the definition currently in
/// effect for `(database_id, tenant_id, name)`. `None` when nothing buffered
/// targets it, or the last targeting entry is a `DROP SEQUENCE`.
fn buffered_def(database_id: u64, tenant_id: u64, name: &str) -> Option<StoredSequence> {
    ddl_buffer::with_buffered(|buffered| {
        let mut current = None;
        for item in buffered {
            match &item.entry {
                CatalogEntry::PutSequence(stored)
                    if stored.database_id == database_id
                        && stored.tenant_id == tenant_id
                        && stored.name == name =>
                {
                    current = Some((**stored).clone());
                }
                CatalogEntry::DeleteSequence {
                    database_id: entry_database,
                    tenant_id: entry_tenant,
                    name: entry_name,
                } if *entry_database == database_id
                    && *entry_tenant == tenant_id
                    && entry_name == name =>
                {
                    current = None;
                }
                _ => {}
            }
        }
        current
    })
    .flatten()
}

/// Resolve `(database_id, tenant_id, name)` through this connection's
/// ephemeral overlay: an already-materialized handle first, else a fresh one
/// built from the still-buffered `CREATE SEQUENCE`. `None` when neither source
/// has it — the caller's own not-found error applies.
pub(super) fn resolve<T>(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    f: impl Fn(&SequenceHandle) -> T,
) -> Option<T> {
    let key = registry_key(database_id, tenant_id, name);
    if let Some(result) = ephemeral_sequence::with_handle(&key, &f) {
        return Some(result);
    }
    let def = buffered_def(database_id, tenant_id, name)?;
    ephemeral_sequence::materialize_and_with(&key, || SequenceHandle::new(def, None), f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn put(database_id: u64, tenant_id: u64, name: &str) -> CatalogEntry {
        CatalogEntry::PutSequence(Box::new(StoredSequence::new(
            database_id,
            tenant_id,
            name.to_owned(),
            "alice".into(),
        )))
    }

    fn delete(database_id: u64, tenant_id: u64, name: &str) -> CatalogEntry {
        CatalogEntry::DeleteSequence {
            database_id,
            tenant_id,
            name: name.to_owned(),
        }
    }

    #[tokio::test]
    async fn resolves_a_buffered_create_by_materializing_it() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put(4, 1, "orders_seq")));
            let value = resolve(4, 1, "orders_seq", |h| h.nextval().unwrap());
            assert_eq!(value, Some(1));
        })
        .await;
    }

    #[tokio::test]
    async fn reuses_the_same_materialized_handle_across_calls() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put(4, 1, "orders_seq")));
            assert_eq!(
                resolve(4, 1, "orders_seq", |h| h.nextval().unwrap()),
                Some(1)
            );
            assert_eq!(
                resolve(4, 1, "orders_seq", |h| h.nextval().unwrap()),
                Some(2)
            );
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_create_then_drop_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put(4, 1, "orders_seq"));
            ddl_buffer::try_buffer(delete(4, 1, "orders_seq"));
            assert!(resolve(4, 1, "orders_seq", |h| h.nextval().unwrap()).is_none());
        })
        .await;
    }

    /// A buffered create in one database must not resolve for another.
    #[tokio::test]
    async fn another_database_does_not_resolve_the_same_name() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put(4, 1, "orders_seq"));
            assert!(resolve(5, 1, "orders_seq", |h| h.nextval().unwrap()).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn an_unrelated_name_does_not_resolve() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put(4, 1, "orders_seq"));
            assert!(resolve(4, 1, "invoices_seq", |h| h.nextval().unwrap()).is_none());
        })
        .await;
    }
}
