// SPDX-License-Identifier: BUSL-1.1

//! Object dependency tracking for the system catalog.
//!
//! Stores edges: source (function/trigger/procedure/view) → targets (functions, collections).
//! Used to block DROP when dependents exist.

use super::types::{DEPENDENCIES, SystemCatalog, catalog_err};
use nodedb_types::id::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};
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
    /// Overwrites any previous list.
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
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Find all source objects that depend on a given target.
    ///
    /// Scans dependency lists in the selected database and returns source
    /// names that reference `(target_type, target_name)`.
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
        for entry in table.range(..).map_err(|e| catalog_err("range deps", e))? {
            let (key, value) = entry.map_err(|e| catalog_err("read dep", e))?;
            let Some((source_type, entry_tid, entry_db, source_name)) = parse_dep_key(key.value())
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
            lists.insert(source, list);
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

fn parse_dep_key(key: &str) -> Option<(&str, u64, DatabaseId, &str)> {
    let parts: Vec<&str> = key.splitn(5, ':').collect();
    match parts.as_slice() {
        ["v2", source_type, tenant_id, database_id, source_name] => Some((
            source_type,
            tenant_id.parse().ok()?,
            DatabaseId::new(database_id.parse().ok()?),
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
    fn a_replacing_write_supersedes_the_previous_dependency_list() {
        let catalog = make_catalog();
        catalog
            .put_dependencies(
                DatabaseId::DEFAULT,
                "function",
                1,
                "f",
                &[collection_dependency("first")],
            )
            .unwrap();
        catalog
            .put_dependencies(
                DatabaseId::DEFAULT,
                "function",
                1,
                "f",
                &[collection_dependency("second")],
            )
            .unwrap();

        assert!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "first")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "collection", "second")
                .unwrap(),
            vec![("function".into(), "f".into())]
        );
    }
}
