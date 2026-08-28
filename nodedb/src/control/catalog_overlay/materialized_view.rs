// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for materialized views.
//!
//! See [`super::collection`] for the mechanism this mirrors. Materialized
//! views are tenant-scoped only — `StoredMaterializedView` carries no
//! `database_id`, so the target identity here is `(tenant_id, name)`.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::types::StoredMaterializedView;

/// True when `entry` mutates the materialized view `(tenant_id, name)`.
fn targets(entry: &CatalogEntry, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutMaterializedView(stored) => {
            stored.tenant_id == tenant_id && stored.name == name
        }
        CatalogEntry::DeleteMaterializedView {
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_tenant == tenant_id && entry_name == name,
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
    tenant_id: u64,
    name: &str,
    committed: Option<StoredMaterializedView>,
) -> Option<StoredMaterializedView> {
    super::core::resolve(committed, |entry| targets(entry, tenant_id, name), step)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredMaterializedView {
        StoredMaterializedView {
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
            tenant_id: 1,
            name: name.to_owned(),
        }
    }

    fn resolve(
        name: &str,
        committed: Option<StoredMaterializedView>,
    ) -> Option<StoredMaterializedView> {
        resolve_materialized_view(1, name, committed)
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
