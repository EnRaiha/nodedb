// SPDX-License-Identifier: BUSL-1.1

//! In-memory registry of all arrays, mirrored to the system catalog.
//!
//! The registry is keyed by the full [`ArrayId`].  A name is only unique
//! within its tenant/database scope; never use a bare name to address catalog
//! state outside the legacy default-database compatibility methods. All public methods return
//! cloned [`ArrayCatalogEntry`] values — the handle is an `Arc<RwLock>`
//! shared between Control and the Data Plane, and callers must not
//! hold the internal lock across engine calls.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_array::types::ArrayId;
use nodedb_types::NodeDbError;

use super::entry::ArrayCatalogEntry;

/// Shared-ownership handle. `RwLock` (not `Mutex`) because the Data
/// Plane does read-mostly lookups during scans while DDL is rare.
pub type ArrayCatalogHandle = Arc<RwLock<ArrayCatalog>>;

/// Purely in-memory index; persistence is the caller's concern (see
/// [`super::persist`]).
#[derive(Debug, Default)]
pub struct ArrayCatalog {
    entries: HashMap<ArrayId, ArrayCatalogEntry>,
}

impl ArrayCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle() -> ArrayCatalogHandle {
        Arc::new(RwLock::new(Self::new()))
    }

    /// Insert a new entry. Duplicate name or id is rejected — DDL must
    /// drop the existing array first.
    pub fn register(&mut self, entry: ArrayCatalogEntry) -> Result<(), NodeDbError> {
        if self.entries.contains_key(&entry.array_id) {
            return Err(NodeDbError::array(
                entry.name.clone(),
                "array id already registered",
            ));
        }
        self.entries.insert(entry.array_id.clone(), entry);
        Ok(())
    }

    /// Look up a name in its explicit tenant/database namespace.
    pub fn lookup_by_name_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Option<ArrayCatalogEntry> {
        self.lookup_by_id(&ArrayId::in_database(tenant_id, database_id, name))
    }

    /// Legacy decoder/lookup for pre-database catalog callers. It is exactly
    /// the DEFAULT database namespace and must not be used by production paths.
    pub fn lookup_by_name(&self, name: &str) -> Option<ArrayCatalogEntry> {
        self.entries
            .iter()
            .find(|(id, _)| id.database_id == nodedb_types::DatabaseId::DEFAULT && id.name == name)
            .map(|(_, entry)| entry.clone())
    }

    pub fn lookup_by_id(&self, id: &ArrayId) -> Option<ArrayCatalogEntry> {
        self.entries.get(id).cloned()
    }

    pub fn unregister_in_database(
        &mut self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Option<ArrayCatalogEntry> {
        self.entries
            .remove(&ArrayId::in_database(tenant_id, database_id, name))
    }

    /// Legacy DEFAULT-database removal retained for old persisted callers.
    pub fn unregister(&mut self, name: &str) -> Option<ArrayCatalogEntry> {
        let id = self
            .entries
            .keys()
            .find(|id| id.database_id == nodedb_types::DatabaseId::DEFAULT && id.name == name)
            .cloned()?;
        self.entries.remove(&id)
    }

    pub fn all_entries(&self) -> Vec<ArrayCatalogEntry> {
        self.entries.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::TenantId;

    fn entry(name: &str) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::new(TenantId::new(1), name),
            name: name.to_string(),
            schema_msgpack: vec![0x80], // empty msgpack map
            schema_hash: 0xDEAD_BEEF,
            created_at_ms: 1_700_000_000_000,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    #[test]
    fn register_lookup_roundtrip() {
        let mut cat = ArrayCatalog::new();
        let e = entry("genomes");
        cat.register(e.clone()).unwrap();

        assert_eq!(cat.lookup_by_name("genomes"), Some(e.clone()));
        assert_eq!(cat.lookup_by_id(&e.array_id), Some(e.clone()));
        assert_eq!(cat.all_entries(), vec![e]);
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn same_name_in_different_databases_is_isolated() {
        let mut cat = ArrayCatalog::new();
        let mut db1 = entry("same");
        db1.array_id =
            ArrayId::in_database(TenantId::new(1), nodedb_types::DatabaseId::new(1), "same");
        let mut db2 = entry("same");
        db2.array_id =
            ArrayId::in_database(TenantId::new(1), nodedb_types::DatabaseId::new(2), "same");
        cat.register(db1.clone()).unwrap();
        cat.register(db2.clone()).unwrap();
        assert_eq!(cat.lookup_by_id(&db1.array_id), Some(db1));
        assert_eq!(cat.lookup_by_id(&db2.array_id), Some(db2));
        assert_eq!(cat.len(), 2);
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let mut cat = ArrayCatalog::new();
        cat.register(entry("a")).unwrap();
        let err = cat.register(entry("a")).unwrap_err();
        assert!(err.to_string().contains("already registered"));
    }

    #[test]
    fn unregister_removes_both_sides() {
        let mut cat = ArrayCatalog::new();
        let e = entry("x");
        cat.register(e.clone()).unwrap();
        let removed = cat.unregister("x").expect("existed");
        assert_eq!(removed.array_id, e.array_id);
        assert!(cat.lookup_by_name("x").is_none());
        assert!(cat.lookup_by_id(&e.array_id).is_none());
    }
}
