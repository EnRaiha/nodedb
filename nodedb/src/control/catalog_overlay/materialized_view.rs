// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for materialized views.
//!
//! See [`super::collection`] for the mechanism this mirrors. The target
//! identity is `(database_id, tenant_id, name)`, matching the catalog key.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::types::StoredMaterializedView;

/// True when `entry` mutates the materialized view
/// `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: u64, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutMaterializedView(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteMaterializedView {
            database_id: entry_database,
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_database == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(
    current: Option<StoredMaterializedView>,
    entry: &CatalogEntry,
) -> Option<StoredMaterializedView> {
    match entry {
        CatalogEntry::PutMaterializedView(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteMaterializedView { .. } => None,
        _ => current,
    }
}

/// Resolve one materialized view through this connection's uncommitted DDL.
pub fn resolve_materialized_view(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredMaterializedView>,
) -> Option<StoredMaterializedView> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredMaterializedView {
        StoredMaterializedView {
            database_id: 2,
            tenant_id: 1,
            name: name.to_owned(),
            source: "orders".into(),
            query_sql: "SELECT * FROM orders".into(),
            refresh_mode: "auto".into(),
            owner: "alice".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutMaterializedView(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteMaterializedView {
            database_id: 2,
            tenant_id: 1,
            name: name.to_owned(),
        }
    }

    fn resolve(
        name: &str,
        committed: Option<StoredMaterializedView>,
    ) -> Option<StoredMaterializedView> {
        resolve_materialized_view(2, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("mv_a")));
            let resolved = resolve("mv_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "mv_a");
        })
        .await;
    }

    /// The overlay keys on the database too, so a buffered create in one
    /// database never answers a lookup in another.
    #[tokio::test]
    async fn a_buffered_create_of_another_database_is_invisible() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("mv_a")));
            assert!(resolve_materialized_view(3, 1, "mv_a", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("mv_a"));
            ddl_buffer::try_buffer(delete("mv_a"));
            assert!(resolve("mv_a", None).is_none());
        })
        .await;
    }
}
