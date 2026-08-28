// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for triggers.
//!
//! See [`super::collection`] for the mechanism this mirrors.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::trigger_types::StoredTrigger;

/// True when `entry` mutates the trigger `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutTrigger(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteTrigger {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_db == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredTrigger>, entry: &CatalogEntry) -> Option<StoredTrigger> {
    match entry {
        CatalogEntry::PutTrigger(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteTrigger { .. } => None,
        _ => current,
    }
}

/// Resolve one trigger through this connection's uncommitted DDL.
pub fn resolve_trigger(
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredTrigger>,
) -> Option<StoredTrigger> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 1,
            database_id: DatabaseId::DEFAULT,
            name: name.to_owned(),
            collection: "items".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN SELECT 1; END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "alice".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutTrigger(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteTrigger {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: name.to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredTrigger>) -> Option<StoredTrigger> {
        resolve_trigger(DatabaseId::DEFAULT, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("trg_a")));
            let resolved = resolve("trg_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "trg_a");
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("trg_a"));
            ddl_buffer::try_buffer(delete("trg_a"));
            assert!(resolve("trg_a", None).is_none());
        })
        .await;
    }
}
