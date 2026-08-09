// SPDX-License-Identifier: BUSL-1.1

//! redb table definitions for the sparse engine's non-versioned tables, plus
//! the shared redb-error mapping helper every sparse module reports through.

use redb::TableDefinition;

/// Table definition for the primary document store.
/// Key: "{database_id}:{tenant_id}:{collection}:{document_id}" → Value: document bytes.
pub(crate) const DOCUMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("documents");

/// Table definition for secondary indexes.
/// Key: "{database_id}:{tenant_id}:{collection}:{field}:{value}:{document_id}" → Value: empty (existence index).
pub(in crate::engine::sparse) const INDEXES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("indexes");

/// Map a redb error into our crate error with context.
pub(in crate::engine::sparse) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("{ctx}: {e}"),
    }
}
