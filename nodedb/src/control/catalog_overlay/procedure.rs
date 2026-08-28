// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for stored procedures.
//!
//! See [`super::collection`] for the mechanism this mirrors.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::procedure_types::StoredProcedure;

/// True when `entry` mutates the procedure `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutProcedure(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteProcedure {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_db == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredProcedure>, entry: &CatalogEntry) -> Option<StoredProcedure> {
    match entry {
        CatalogEntry::PutProcedure(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteProcedure { .. } => None,
        _ => current,
    }
}

/// Resolve one procedure through this connection's uncommitted DDL.
pub fn resolve_procedure(
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredProcedure>,
) -> Option<StoredProcedure> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::{
        ParamDirection, ProcedureParam, ProcedureRoutability,
    };
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredProcedure {
        StoredProcedure {
            tenant_id: 1,
            database_id: DatabaseId::DEFAULT,
            name: name.to_owned(),
            parameters: vec![ProcedureParam {
                name: "x".into(),
                data_type: "INT".into(),
                direction: ParamDirection::In,
            }],
            body_sql: "BEGIN SELECT 1; END".into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            routability: ProcedureRoutability::MultiCollection,
            owner: "alice".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutProcedure(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteProcedure {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: name.to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredProcedure>) -> Option<StoredProcedure> {
        resolve_procedure(DatabaseId::DEFAULT, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("proc_a")));
            let resolved = resolve("proc_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "proc_a");
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("proc_a"));
            ddl_buffer::try_buffer(delete("proc_a"));
            assert!(resolve("proc_a", None).is_none());
        })
        .await;
    }
}
