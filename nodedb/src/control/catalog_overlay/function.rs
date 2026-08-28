// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for user-defined functions.
//!
//! See [`super::collection`] for the mechanism this mirrors.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::function_types::StoredFunction;

/// True when `entry` mutates the function `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutFunction(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteFunction {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_db == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredFunction>, entry: &CatalogEntry) -> Option<StoredFunction> {
    match entry {
        CatalogEntry::PutFunction(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteFunction { .. } => None,
        _ => current,
    }
}

/// Resolve one function through this connection's uncommitted DDL.
pub fn resolve_function(
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredFunction>,
) -> Option<StoredFunction> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::function_types::{
        FunctionLanguage, FunctionSecurity, FunctionVolatility,
    };
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredFunction {
        StoredFunction {
            tenant_id: 1,
            database_id: DatabaseId::DEFAULT,
            name: name.to_owned(),
            parameters: vec![],
            return_type: "INT".into(),
            body_sql: "SELECT 1".into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Sql,
            wasm_hash: None,
            wasm_module: None,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "alice".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutFunction(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteFunction {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: name.to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredFunction>) -> Option<StoredFunction> {
        resolve_function(DatabaseId::DEFAULT, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("fn_a")));
            let resolved = resolve("fn_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "fn_a");
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("fn_a"));
            ddl_buffer::try_buffer(delete("fn_a"));
            assert!(resolve("fn_a", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn a_different_database_is_untouched() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("fn_a"));
            assert!(resolve_function(DatabaseId::new(9), 1, "fn_a", None).is_none());
        })
        .await;
    }
}
