// SPDX-License-Identifier: BUSL-1.1

//! Custom type metadata operations for the system catalog.
//!
//! Persists `CREATE TYPE` definitions (enum and composite) via the
//! `_system.custom_types` redb table. Key: `"{database_id}:{tenant_id}:{name}"`.
//!
//! The database segment scopes the row. Two databases in one tenant can hold a
//! same-named type, and a shared key makes a column in one resolve against the
//! other's definition.

use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use super::types::{CUSTOM_TYPES, SystemCatalog, catalog_err};

/// A named field in a composite type.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct CompositeField {
    pub name: String,
    pub type_name: String,
}

/// The kind of a custom type.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum CustomTypeDef {
    /// `CREATE TYPE <n> AS ENUM ('a', 'b', ...)`
    Enum { labels: Vec<String> },
    /// `CREATE TYPE <n> AS (<f1> <t1>, <f2> <t2>, ...)`
    Composite { fields: Vec<CompositeField> },
}

/// Persisted custom type record.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct StoredCustomType {
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
    pub def: CustomTypeDef,
    /// Stable u32 OID assigned at creation time. Persisted so the same OID
    /// is always returned to pgwire clients, even after restart.
    pub oid: u32,
    pub created_at: u64,
}

impl SystemCatalog {
    /// Store a custom type. Overwrites any existing type with the same name.
    ///
    /// The key comes from the entry, so the row can never land under a
    /// database the entry does not name.
    pub fn put_custom_type(&self, def: &StoredCustomType) -> crate::Result<()> {
        let key = custom_type_key(def.database_id, def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize custom type", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CUSTOM_TYPES)
                .map_err(|e| catalog_err("open custom_types", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert custom type", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a custom type. Returns `true` if it existed.
    pub fn delete_custom_type(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = custom_type_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(CUSTOM_TYPES)
                .map_err(|e| catalog_err("open custom_types", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete custom type", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Get a single custom type by database, tenant, and name.
    pub fn get_custom_type(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredCustomType>> {
        let key = custom_type_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CUSTOM_TYPES)
            .map_err(|e| catalog_err("open custom_types", e))?;
        let opt = table
            .get(key.as_str())
            .map_err(|e| catalog_err("get custom type", e))?;
        Ok(opt.and_then(|v| zerompk::from_msgpack::<StoredCustomType>(v.value()).ok()))
    }

    /// Load every custom type of one tenant in one database.
    ///
    /// The scan is bounded to the tenant's key range, so a node holding many
    /// tenants reads only the rows it returns.
    pub fn load_custom_types_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCustomType>> {
        let lower = format!("{database_id}:{tenant_id}:");
        let upper = tenant_upper_bound(database_id, tenant_id);
        self.range_custom_types(&lower, &upper)
    }

    /// Load every custom type of one database, across every tenant.
    pub fn load_custom_types_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredCustomType>> {
        let lower = format!("{database_id}:");
        let upper = database_upper_bound(database_id);
        self.range_custom_types(&lower, &upper)
    }

    /// Load every custom type across all databases and tenants.
    ///
    /// The registry loads every database on startup, so this stays a full-table
    /// scan.
    pub fn load_all_custom_types(&self) -> crate::Result<Vec<StoredCustomType>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CUSTOM_TYPES)
            .map_err(|e| catalog_err("open custom_types", e))?;

        let mut types = Vec::new();
        let mut range = table
            .range(..)
            .map_err(|e| catalog_err("range custom_types", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<StoredCustomType>(value.value()) {
                types.push(def);
            }
        }
        Ok(types)
    }

    /// Decode every custom type in one key range.
    fn range_custom_types(&self, lower: &str, upper: &str) -> crate::Result<Vec<StoredCustomType>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CUSTOM_TYPES)
            .map_err(|e| catalog_err("open custom_types", e))?;

        let mut types = Vec::new();
        for item in table
            .range(lower..upper)
            .map_err(|e| catalog_err("range custom_types", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read custom type", e))?;
            let def: StoredCustomType = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize custom type", e))?;
            types.push(def);
        }
        Ok(types)
    }
}

fn custom_type_key(database_id: u64, tenant_id: u64, name: &str) -> String {
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
/// immediately past every type of the tenant.
fn tenant_upper_bound(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> (SystemCatalog, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (cat, dir)
    }

    fn make_enum(name: &str, database_id: u64, tenant_id: u64, oid: u32) -> StoredCustomType {
        StoredCustomType {
            database_id,
            tenant_id,
            name: name.to_string(),
            def: CustomTypeDef::Enum {
                labels: vec!["active".into(), "inactive".into()],
            },
            oid,
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_load() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&make_enum("status", 2, 1, 70001))
            .unwrap();

        let all = cat.load_all_custom_types().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "status");
        assert_eq!(all[0].oid, 70001);
    }

    #[test]
    fn get_single() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&make_enum("mood", 2, 1, 70001))
            .unwrap();

        assert!(cat.get_custom_type(2, 1, "mood").unwrap().is_some());
        assert!(cat.get_custom_type(2, 1, "nonexistent").unwrap().is_none());
    }

    #[test]
    fn delete_custom_type() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&make_enum("color", 2, 1, 70001))
            .unwrap();
        assert!(cat.delete_custom_type(2, 1, "color").unwrap());
        assert!(!cat.delete_custom_type(2, 1, "color").unwrap());
    }

    #[test]
    fn tenant_isolation() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&make_enum("x", 2, 1, 70001)).unwrap();
        cat.put_custom_type(&make_enum("x", 2, 2, 70002)).unwrap();

        let t1 = cat.load_custom_types_for_tenant(2, 1).unwrap();
        assert_eq!(t1.len(), 1);
        assert_eq!(t1[0].oid, 70001);

        let t2 = cat.load_custom_types_for_tenant(2, 2).unwrap();
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].oid, 70002);
    }

    #[test]
    fn load_in_database_excludes_another_database() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&make_enum("x", 2, 1, 70001)).unwrap();
        cat.put_custom_type(&make_enum("x", 2, 5, 70002)).unwrap();
        cat.put_custom_type(&make_enum("x", 3, 1, 70003)).unwrap();

        assert_eq!(cat.load_custom_types_in_database(2).unwrap().len(), 2);
        assert_eq!(cat.load_custom_types_in_database(3).unwrap().len(), 1);
    }

    #[test]
    fn composite_roundtrip() {
        let (cat, _dir) = make_catalog();
        let def = StoredCustomType {
            database_id: 2,
            tenant_id: 1,
            name: "address".into(),
            def: CustomTypeDef::Composite {
                fields: vec![
                    CompositeField {
                        name: "street".into(),
                        type_name: "TEXT".into(),
                    },
                    CompositeField {
                        name: "city".into(),
                        type_name: "TEXT".into(),
                    },
                ],
            },
            oid: 70100,
            created_at: 0,
        };
        cat.put_custom_type(&def).unwrap();
        let got = cat.get_custom_type(2, 1, "address").unwrap().unwrap();
        match got.def {
            CustomTypeDef::Composite { fields } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "street");
            }
            _ => panic!("expected composite"),
        }
    }

    /// One name, two databases, one tenant: each database keeps its own type
    /// and its own OID, and a delete in one leaves the other whole. Drop the
    /// `database_id` segment from the key and this fails.
    #[test]
    fn custom_types_of_one_database_survive_a_delete_in_another() {
        let (cat, _dir) = make_catalog();

        cat.put_custom_type(&make_enum("addr", 1, 7, 70011))
            .unwrap();
        cat.put_custom_type(&make_enum("addr", 2, 7, 70012))
            .unwrap();

        let first = cat.get_custom_type(1, 7, "addr").unwrap().unwrap();
        let second = cat.get_custom_type(2, 7, "addr").unwrap().unwrap();
        assert_eq!(first.oid, 70011);
        assert_eq!(second.oid, 70012);
        assert_ne!(
            first.oid, second.oid,
            "pgwire clients see the OID, so the two types must not share one"
        );

        assert!(cat.delete_custom_type(1, 7, "addr").unwrap());

        assert!(cat.get_custom_type(1, 7, "addr").unwrap().is_none());
        assert_eq!(
            cat.get_custom_type(2, 7, "addr").unwrap().unwrap().oid,
            70012,
            "the other database keeps its type"
        );
    }
}
