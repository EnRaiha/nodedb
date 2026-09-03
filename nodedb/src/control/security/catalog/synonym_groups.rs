// SPDX-License-Identifier: BUSL-1.1

//! Synonym group metadata operations for the system catalog.
//!
//! `_system.synonym_groups` keys on `"{database_id}:{tenant_id}:{name}"`.
//!
//! The database segment scopes the row. The Data Plane FTS backend already
//! stores a group per database, so a tenant-only key lets one catalog row
//! stand for groups that live in two databases.

use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use super::types::{SYNONYM_GROUPS, SystemCatalog, catalog_err};

/// Persisted synonym group definition.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct StoredSynonymGroup {
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
    pub terms: Vec<String>,
    pub created_at: u64,
}

impl SystemCatalog {
    /// Store a synonym group. Overwrites any existing group with the same name.
    ///
    /// The key comes from the entry, so the row can never land under a
    /// database the entry does not name.
    pub fn put_synonym_group(&self, def: &StoredSynonymGroup) -> crate::Result<()> {
        let key = synonym_group_key(def.database_id, def.tenant_id, &def.name);
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

    /// Get a synonym group by database, tenant, and name.
    pub fn get_synonym_group(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredSynonymGroup>> {
        let key = synonym_group_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SYNONYM_GROUPS)
            .map_err(|e| catalog_err("open synonym_groups", e))?;
        let opt = table
            .get(key.as_str())
            .map_err(|e| catalog_err("get synonym group", e))?;
        Ok(opt.and_then(|v| zerompk::from_msgpack::<StoredSynonymGroup>(v.value()).ok()))
    }

    /// Delete a synonym group. Returns `true` if it existed.
    pub fn delete_synonym_group(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = synonym_group_key(database_id, tenant_id, name);
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

    /// Load every synonym group of one tenant in one database.
    ///
    /// The scan is bounded to the tenant's key range, so a node holding many
    /// tenants reads only the rows it returns.
    pub fn load_synonym_groups_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredSynonymGroup>> {
        let lower = format!("{database_id}:{tenant_id}:");
        let upper = tenant_upper_bound(database_id, tenant_id);
        self.range_synonym_groups(&lower, &upper)
    }

    /// Load every synonym group of one database, across every tenant.
    pub fn load_synonym_groups_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredSynonymGroup>> {
        let lower = format!("{database_id}:");
        let upper = database_upper_bound(database_id);
        self.range_synonym_groups(&lower, &upper)
    }

    /// Load every synonym group across all databases and tenants.
    ///
    /// The registry loads every database on startup, so this stays a full-table
    /// scan.
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
            .range(..)
            .map_err(|e| catalog_err("range synonym_groups", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<StoredSynonymGroup>(value.value()) {
                groups.push(def);
            }
        }
        Ok(groups)
    }

    /// Decode every synonym group in one key range.
    fn range_synonym_groups(
        &self,
        lower: &str,
        upper: &str,
    ) -> crate::Result<Vec<StoredSynonymGroup>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SYNONYM_GROUPS)
            .map_err(|e| catalog_err("open synonym_groups", e))?;

        let mut groups = Vec::new();
        for item in table
            .range(lower..upper)
            .map_err(|e| catalog_err("range synonym_groups", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read synonym group", e))?;
            let def: StoredSynonymGroup = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize synonym group", e))?;
            groups.push(def);
        }
        Ok(groups)
    }
}

fn synonym_group_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

/// Exclusive upper bound for one database's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every tenant of the database.
fn database_upper_bound(database_id: u64) -> String {
    format!("{database_id};")
}

/// Exclusive upper bound for one tenant's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every group of the tenant.
fn tenant_upper_bound(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn group(database_id: u64, tenant_id: u64, name: &str, terms: &[&str]) -> StoredSynonymGroup {
        StoredSynonymGroup {
            database_id,
            tenant_id,
            name: name.into(),
            terms: terms.iter().map(|t| (*t).to_string()).collect(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_load() {
        let (_dir, cat) = make_catalog();
        cat.put_synonym_group(&group(2, 1, "db_terms", &["database", "db", "datastore"]))
            .unwrap();

        let all = cat.load_all_synonym_groups().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "db_terms");
        assert_eq!(all[0].terms.len(), 3);
    }

    #[test]
    fn delete_synonym_group() {
        let (_dir, cat) = make_catalog();
        cat.put_synonym_group(&group(2, 1, "g1", &["a", "b"]))
            .unwrap();
        assert!(cat.delete_synonym_group(2, 1, "g1").unwrap());
        assert!(!cat.delete_synonym_group(2, 1, "g1").unwrap());
        assert!(cat.get_synonym_group(2, 1, "g1").unwrap().is_none());
    }

    #[test]
    fn tenant_isolation() {
        let (_dir, cat) = make_catalog();
        cat.put_synonym_group(&group(2, 1, "g", &["a"])).unwrap();
        cat.put_synonym_group(&group(2, 2, "g", &["b"])).unwrap();

        let t1 = cat.load_synonym_groups_for_tenant(2, 1).unwrap();
        assert_eq!(t1.len(), 1);
        assert_eq!(t1[0].terms, vec!["a"]);

        let t2 = cat.load_synonym_groups_for_tenant(2, 2).unwrap();
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].terms, vec!["b"]);
    }

    #[test]
    fn load_in_database_excludes_another_database() {
        let (_dir, cat) = make_catalog();
        cat.put_synonym_group(&group(2, 1, "g", &["a"])).unwrap();
        cat.put_synonym_group(&group(2, 5, "g", &["b"])).unwrap();
        cat.put_synonym_group(&group(3, 1, "g", &["c"])).unwrap();

        assert_eq!(cat.load_synonym_groups_in_database(2).unwrap().len(), 2);
        assert_eq!(cat.load_synonym_groups_in_database(3).unwrap().len(), 1);
    }

    /// One name, two databases, one tenant: each database keeps its own group,
    /// and a delete in one leaves the other whole. Drop the `database_id`
    /// segment from the key and this fails.
    #[test]
    fn synonym_groups_of_one_database_survive_a_delete_in_another() {
        let (_dir, cat) = make_catalog();

        cat.put_synonym_group(&group(1, 7, "colours", &["red", "crimson"]))
            .unwrap();
        cat.put_synonym_group(&group(2, 7, "colours", &["blue", "azure"]))
            .unwrap();

        assert_eq!(
            cat.get_synonym_group(1, 7, "colours")
                .unwrap()
                .unwrap()
                .terms,
            vec!["red", "crimson"]
        );
        assert_eq!(
            cat.get_synonym_group(2, 7, "colours")
                .unwrap()
                .unwrap()
                .terms,
            vec!["blue", "azure"]
        );

        assert!(cat.delete_synonym_group(1, 7, "colours").unwrap());

        assert!(cat.get_synonym_group(1, 7, "colours").unwrap().is_none());
        assert_eq!(
            cat.get_synonym_group(2, 7, "colours")
                .unwrap()
                .unwrap()
                .terms,
            vec!["blue", "azure"],
            "the other database keeps its group"
        );
    }
}
