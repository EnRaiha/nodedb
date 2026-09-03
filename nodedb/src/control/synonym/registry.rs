// SPDX-License-Identifier: BUSL-1.1

//! In-memory registry of synonym groups.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! The registry is the single source of truth for duplicate detection and
//! SHOW SYNONYM GROUPS queries in the Control Plane.
//!
//! Entries key on `(database_id, tenant_id, group_name)`, matching the catalog
//! row and the Data Plane FTS backend. One tenant can hold the same group name
//! in two databases.
//!
//! Query-time synonym expansion happens in the Data Plane via the FTS backend.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::control::security::catalog::StoredSynonymGroup;

/// In-memory synonym group registry (Control Plane, `Send + Sync`).
pub struct SynonymRegistry {
    /// `(database_id, tenant_id, group_name)` → `StoredSynonymGroup`.
    by_name: RwLock<HashMap<(u64, u64, String), StoredSynonymGroup>>,
}

impl SynonymRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    /// Insert or replace a synonym group in the registry.
    pub fn register(&self, def: StoredSynonymGroup) {
        let key = (def.database_id, def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// Remove a synonym group. Returns `true` if it existed.
    pub fn unregister(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        let key = (database_id, tenant_id, name.to_string());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    /// Check whether a synonym group exists in one database.
    pub fn exists(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        let key = (database_id, tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.contains_key(&key)
    }

    /// Get a synonym group by name.
    pub fn get(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<StoredSynonymGroup> {
        let key = (database_id, tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    /// List every synonym group of one tenant in one database.
    pub fn list_for_tenant(&self, database_id: u64, tenant_id: u64) -> Vec<StoredSynonymGroup> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|g| g.database_id == database_id && g.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Reload from catalog. Used at startup and by recovery verifier.
    pub fn reload_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_synonym_groups()?;
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for g in fresh {
            let key = (g.database_id, g.tenant_id, g.name.clone());
            map.insert(key, g);
        }
        Ok(())
    }
}

impl Default for SynonymRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(database_id: u64, tenant_id: u64, name: &str, term: &str) -> StoredSynonymGroup {
        StoredSynonymGroup {
            database_id,
            tenant_id,
            name: name.into(),
            terms: vec![term.into()],
            created_at: 0,
        }
    }

    /// `exists` answers per database, so a CREATE of the same name in a second
    /// database is not refused as a duplicate. Drop the `database_id` segment
    /// from the key and this fails.
    #[test]
    fn a_group_in_one_database_does_not_block_the_same_name_in_another() {
        let reg = SynonymRegistry::new();
        reg.register(group(1, 7, "colours", "red"));

        assert!(reg.exists(1, 7, "colours"));
        assert!(
            !reg.exists(2, 7, "colours"),
            "the second database has no such group, so CREATE must be accepted"
        );

        reg.register(group(2, 7, "colours", "blue"));
        assert_eq!(reg.get(1, 7, "colours").unwrap().terms, vec!["red"]);
        assert_eq!(reg.get(2, 7, "colours").unwrap().terms, vec!["blue"]);

        assert!(reg.unregister(1, 7, "colours"));
        assert!(!reg.exists(1, 7, "colours"));
        assert!(
            reg.exists(2, 7, "colours"),
            "the other database keeps its group"
        );
    }

    #[test]
    fn list_for_tenant_is_scoped_to_one_database() {
        let reg = SynonymRegistry::new();
        reg.register(group(1, 7, "a", "x"));
        reg.register(group(1, 7, "b", "y"));
        reg.register(group(2, 7, "a", "z"));

        assert_eq!(reg.list_for_tenant(1, 7).len(), 2);
        assert_eq!(reg.list_for_tenant(2, 7).len(), 1);
    }
}
