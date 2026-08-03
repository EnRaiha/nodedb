// SPDX-License-Identifier: BUSL-1.1

//! Synonym group metadata operations for the system catalog.

use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};

use super::types::{SYNONYM_GROUPS, SystemCatalog, catalog_err};

/// Persisted synonym group definition.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct StoredSynonymGroup {
    pub tenant_id: u64,
    pub name: String,
    pub terms: Vec<String>,
    pub created_at: u64,
}

impl SystemCatalog {
    /// Store a synonym group. Overwrites any existing group with the same name.
    pub fn put_synonym_group(&self, def: &StoredSynonymGroup) -> crate::Result<()> {
        let key = synonym_group_key(def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize synonym group", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SYNONYM_GROUPS)
                .map_err(|e| catalog_err("open synonym_groups", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert synonym group", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a synonym group. Returns `true` if it existed.
    pub fn delete_synonym_group(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        let key = synonym_group_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(SYNONYM_GROUPS)
                .map_err(|e| catalog_err("open synonym_groups", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete synonym group", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all synonym groups for a tenant.
    pub fn load_synonym_groups_for_tenant(
        &self,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredSynonymGroup>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SYNONYM_GROUPS)
            .map_err(|e| catalog_err("open synonym_groups", e))?;

        let mut groups = Vec::new();
        let mut range = table
            .range::<&str>(prefix.as_str()..)
            .map_err(|e| catalog_err("range synonym_groups", e))?;
        while let Some(Ok((key, value))) = range.next() {
            if !key.value().starts_with(&prefix) {
                break;
            }
            if let Ok(def) = zerompk::from_msgpack::<StoredSynonymGroup>(value.value()) {
                groups.push(def);
            }
        }
        Ok(groups)
    }

    /// Load all synonym groups (all tenants). Used on startup to hydrate the registry.
    pub fn load_all_synonym_groups(&self) -> crate::Result<Vec<StoredSynonymGroup>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SYNONYM_GROUPS)
            .map_err(|e| catalog_err("open synonym_groups", e))?;

        let mut groups = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range synonym_groups", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<StoredSynonymGroup>(value.value()) {
                groups.push(def);
            }
        }
        Ok(groups)
    }
}

fn synonym_group_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_and_load() {
        let cat = make_catalog();
        let def = StoredSynonymGroup {
            tenant_id: 1,
            name: "db_terms".into(),
            terms: vec!["database".into(), "db".into(), "datastore".into()],
            created_at: 1000,
        };
        cat.put_synonym_group(&def).unwrap();

        let all = cat.load_all_synonym_groups().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "db_terms");
        assert_eq!(all[0].terms.len(), 3);
    }

    #[test]
    fn delete_synonym_group() {
        let cat = make_catalog();
        let def = StoredSynonymGroup {
            tenant_id: 1,
            name: "g1".into(),
            terms: vec!["a".into(), "b".into()],
            created_at: 0,
        };
        cat.put_synonym_group(&def).unwrap();
        assert!(cat.delete_synonym_group(1, "g1").unwrap());
        assert!(!cat.delete_synonym_group(1, "g1").unwrap());
    }

    #[test]
    fn tenant_isolation() {
        let cat = make_catalog();
        cat.put_synonym_group(&StoredSynonymGroup {
            tenant_id: 1,
            name: "g".into(),
            terms: vec!["a".into()],
            created_at: 0,
        })
        .unwrap();
        cat.put_synonym_group(&StoredSynonymGroup {
            tenant_id: 2,
            name: "g".into(),
            terms: vec!["b".into()],
            created_at: 0,
        })
        .unwrap();

        let t1 = cat.load_synonym_groups_for_tenant(1).unwrap();
        assert_eq!(t1.len(), 1);
        assert_eq!(t1[0].terms, vec!["a"]);

        let t2 = cat.load_synonym_groups_for_tenant(2).unwrap();
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].terms, vec!["b"]);
    }
}
