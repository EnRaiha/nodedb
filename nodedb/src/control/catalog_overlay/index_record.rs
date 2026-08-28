// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for index registry records.
//!
//! See [`super::collection`] for the mechanism this mirrors: a `CREATE
//! INDEX` buffered inside an open transaction must be visible to a later
//! `DROP INDEX` or read in that same transaction, before COMMIT ever writes
//! the row to redb.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::index_record::StoredIndexRecord;

/// True when `entry` mutates the index `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: u64, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutIndexRecord(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteIndexRecord {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
            ..
        } => *entry_db == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredIndexRecord>, entry: &CatalogEntry) -> Option<StoredIndexRecord> {
    match entry {
        CatalogEntry::PutIndexRecord(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteIndexRecord { .. } => None,
        _ => current,
    }
}

/// Resolve one index record through this connection's uncommitted DDL.
pub fn resolve_index_record(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredIndexRecord>,
) -> Option<StoredIndexRecord> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::index_record::IndexKind;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_owned(),
            kind: IndexKind::Vector,
            collection: "docs".to_owned(),
            fields: vec!["embedding".to_owned()],
            is_active: true,
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutIndexRecord(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_owned(),
            collection: "docs".to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredIndexRecord>) -> Option<StoredIndexRecord> {
        resolve_index_record(0, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("idx_a")));
            let resolved = resolve("idx_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "idx_a");
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("idx_a"));
            ddl_buffer::try_buffer(delete("idx_a"));
            assert!(resolve("idx_a", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn outside_a_transaction_the_committed_row_wins() {
        conn_scope::scoped(async {
            assert!(resolve("idx_a", None).is_none());
            assert!(resolve("idx_a", Some(stored("idx_a"))).is_some());
        })
        .await;
    }
}
