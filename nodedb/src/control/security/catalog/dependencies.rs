// SPDX-License-Identifier: BUSL-1.1

//! Object dependency tracking for the system catalog.
//!
//! Stores edges: source (function/trigger/procedure/view) → targets (functions, collections).
//! Used to block DROP when dependents exist.

use super::types::{DEPENDENCIES, SystemCatalog, catalog_err};
use nodedb_types::id::DatabaseId;
use redb::ReadableDatabase;
use std::collections::HashMap;

/// A single dependency edge: the source object references the target.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack, PartialEq, Eq)]
pub struct Dependency {
    /// Type of referenced object: "function", "collection".
    pub target_type: String,
    /// Name of referenced object.
    pub target_name: String,
}

/// All dependencies for a source object.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DependencyList {
    pub deps: Vec<Dependency>,
}

impl SystemCatalog {
    /// Store the dependency list for a source object.
    ///
    /// Key format: `"v2:{source_type}:{tenant_id}:{database_id}:{source_name}"`.
    ///
    /// A write in the default database removes the legacy unscoped row,
    /// completing its migration to the v2 key. Overwrites any previous list.
    pub fn put_dependencies(
        &self,
        database_id: DatabaseId,
        source_type: &str,
        tenant_id: u64,
        source_name: &str,
        deps: &[Dependency],
    ) -> crate::Result<()> {
        let key = dep_key(database_id, source_type, tenant_id, source_name);
        let list = DependencyList {
            deps: deps.to_vec(),
        };
        let bytes = zerompk::to_msgpack_vec(&list).map_err(|e| catalog_err("serialize deps", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("open dependencies", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert deps", e))?;
            if database_id == DatabaseId::DEFAULT {
                table
                    .remove(legacy_dep_key(source_type, tenant_id, source_name).as_str())
                    .map_err(|e| catalog_err("remove legacy deps", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete the dependency list for a source object.
    pub fn delete_dependencies(
        &self,
        database_id: DatabaseId,
        source_type: &str,
        tenant_id: u64,
        source_name: &str,
    ) -> crate::Result<()> {
        let key = dep_key(database_id, source_type, tenant_id, source_name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("open dependencies", e))?;
            let _ = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove deps", e))?;
            if database_id == DatabaseId::DEFAULT {
                let _ = table
                    .remove(legacy_dep_key(source_type, tenant_id, source_name).as_str())
                    .map_err(|e| catalog_err("remove legacy deps", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Find all source objects that depend on a given target.
    ///
    /// Scans dependency lists in the selected database and returns source
    /// names that reference `(target_type, target_name)`. Legacy unscoped
    /// rows are readable only in the default database; a v2 row wins when
    /// both versions exist for the same source object.
    pub fn find_dependents(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        target_type: &str,
        target_name: &str,
    ) -> crate::Result<Vec<(String, String)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(DEPENDENCIES)
            .map_err(|e| catalog_err("open dependencies", e))?;

        let mut lists = HashMap::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range deps", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read dep", e))?;
            let Some((is_v2, source_type, entry_tid, entry_db, source_name)) =
                parse_dep_key(key.value())
            else {
                continue;
            };
            if entry_tid != tenant_id || entry_db != database_id {
                continue;
            }

            let list: DependencyList = match zerompk::from_msgpack(value.value()) {
                Ok(l) => l,
                Err(_) => continue,
            };
            let source = (source_type.to_string(), source_name.to_string());
            // Preserve a v2 record if the legacy record sorts after it.
            if is_v2 || !lists.contains_key(&source) {
                lists.insert(source, list);
            }
        }

        Ok(lists
            .into_iter()
            .filter_map(|((source_type, source_name), list)| {
                list.deps
                    .iter()
                    .any(|dep| dep.target_type == target_type && dep.target_name == target_name)
                    .then_some((source_type, source_name))
            })
            .collect())
    }
}

pub(crate) fn dep_key(
    database_id: DatabaseId,
    source_type: &str,
    tenant_id: u64,
    source_name: &str,
) -> String {
    format!(
        "v2:{source_type}:{tenant_id}:{}:{source_name}",
        database_id.as_u64()
    )
}

pub(crate) fn legacy_dep_key(source_type: &str, tenant_id: u64, source_name: &str) -> String {
    format!("{source_type}:{tenant_id}:{source_name}")
}

fn parse_dep_key(key: &str) -> Option<(bool, &str, u64, DatabaseId, &str)> {
    let parts: Vec<&str> = key.splitn(5, ':').collect();
    match parts.as_slice() {
        ["v2", source_type, tenant_id, database_id, source_name] => Some((
            true,
            source_type,
            tenant_id.parse().ok()?,
            DatabaseId::new(database_id.parse().ok()?),
            source_name,
        )),
        [source_type, tenant_id, source_name] => Some((
            false,
            source_type,
            tenant_id.parse().ok()?,
            DatabaseId::DEFAULT,
            source_name,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn collection_dependency(name: &str) -> Dependency {
        Dependency {
            target_type: "collection".into(),
            target_name: name.into(),
        }
    }

    fn write_dependency_row(catalog: &SystemCatalog, key: &str, deps: Vec<Dependency>) {
        let bytes = zerompk::to_msgpack_vec(&DependencyList { deps }).unwrap();
        let txn = catalog.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(DEPENDENCIES).unwrap();
            table.insert(key, bytes.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn store_and_find_dependents() {
        let catalog = make_catalog();
        catalog
            .put_dependencies(
                DatabaseId::DEFAULT,
                "function",
                1,
                "f",
                &[collection_dependency("users")],
            )
            .unwrap();

        let deps = catalog
            .find_dependents(DatabaseId::DEFAULT, 1, "collection", "users")
            .unwrap();
        assert_eq!(deps, vec![("function".into(), "f".into())]);
    }

    #[test]
    fn no_dependents() {
        let catalog = make_catalog();
        let deps = catalog
            .find_dependents(DatabaseId::DEFAULT, 1, "collection", "orders")
            .unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn tenant_isolation() {
        let catalog = make_catalog();
        catalog
            .put_dependencies(
                DatabaseId::DEFAULT,
                "function",
                1,
                "f",
                &[collection_dependency("users")],
            )
            .unwrap();

        let deps = catalog
            .find_dependents(DatabaseId::DEFAULT, 2, "collection", "users")
            .unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn same_tenant_and_name_are_isolated_by_database() {
        let catalog = make_catalog();
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        catalog
            .put_dependencies(db1, "function", 1, "f", &[collection_dependency("users")])
            .unwrap();
        catalog
            .put_dependencies(db2, "function", 1, "f", &[collection_dependency("orders")])
            .unwrap();

        assert_eq!(
            catalog
                .find_dependents(db1, 1, "collection", "users")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
        assert!(
            catalog
                .find_dependents(db1, 1, "collection", "orders")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .find_dependents(db2, 1, "collection", "orders")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
    }

    #[test]
    fn delete_dependencies_is_scoped_to_database() {
        let catalog = make_catalog();
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        for database_id in [db1, db2] {
            catalog
                .put_dependencies(
                    database_id,
                    "function",
                    1,
                    "f",
                    &[collection_dependency("users")],
                )
                .unwrap();
        }

        catalog
            .delete_dependencies(db1, "function", 1, "f")
            .unwrap();

        assert!(
            catalog
                .find_dependents(db1, 1, "collection", "users")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .find_dependents(db2, 1, "collection", "users")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
    }

    #[test]
    fn legacy_rows_are_confined_to_default_database() {
        let catalog = make_catalog();
        write_dependency_row(
            &catalog,
            &legacy_dep_key("function", 1, "f"),
            vec![collection_dependency("users")],
        );

        assert_eq!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "users")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
        assert!(
            catalog
                .find_dependents(DatabaseId::new(1), 1, "collection", "users")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn default_write_migrates_legacy_row_and_v2_wins() {
        let catalog = make_catalog();
        let legacy_key = legacy_dep_key("function", 1, "f");
        write_dependency_row(&catalog, &legacy_key, vec![collection_dependency("legacy")]);

        catalog
            .put_dependencies(
                DatabaseId::DEFAULT,
                "function",
                1,
                "f",
                &[collection_dependency("v2")],
            )
            .unwrap();

        assert!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "legacy")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "v2")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
    }

    #[test]
    fn v2_row_takes_precedence_over_coexisting_legacy_row() {
        let catalog = make_catalog();
        write_dependency_row(
            &catalog,
            &legacy_dep_key("function", 1, "f"),
            vec![collection_dependency("legacy")],
        );
        write_dependency_row(
            &catalog,
            &dep_key(DatabaseId::DEFAULT, "function", 1, "f"),
            vec![collection_dependency("v2")],
        );

        assert!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "legacy")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "v2")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
    }
}
