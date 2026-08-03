// SPDX-License-Identifier: BUSL-1.1

//! Custom type metadata operations for the system catalog.
//!
//! Persists `CREATE TYPE` definitions (enum and composite) via the
//! `_system.custom_types` redb table. Key: `"{tenant_id}:{name}"`.

use redb::ReadableDatabase;
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
    pub fn put_custom_type(&self, def: &StoredCustomType) -> crate::Result<()> {
        let key = custom_type_key(def.tenant_id, &def.name);
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
    pub fn delete_custom_type(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        let key = custom_type_key(tenant_id, name);
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

    /// Get a single custom type by `(tenant_id, name)`.
    pub fn get_custom_type(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredCustomType>> {
        let key = custom_type_key(tenant_id, name);
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

    /// Load all custom types for a tenant.
    pub fn load_custom_types_for_tenant(
        &self,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCustomType>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CUSTOM_TYPES)
            .map_err(|e| catalog_err("open custom_types", e))?;

        let mut types = Vec::new();
        let mut range = table
            .range::<&str>(prefix.as_str()..)
            .map_err(|e| catalog_err("range custom_types", e))?;
        while let Some(Ok((key, value))) = range.next() {
            if !key.value().starts_with(&prefix) {
                break;
            }
            if let Ok(def) = zerompk::from_msgpack::<StoredCustomType>(value.value()) {
                types.push(def);
            }
        }
        Ok(types)
    }

    /// Load all custom types (all tenants). Used on startup to hydrate the registry.
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
            .range::<&str>(..)
            .map_err(|e| catalog_err("range custom_types", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<StoredCustomType>(value.value()) {
                types.push(def);
            }
        }
        Ok(types)
    }
}

fn custom_type_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
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

    fn make_enum(name: &str, tenant_id: u64) -> StoredCustomType {
        StoredCustomType {
            tenant_id,
            name: name.to_string(),
            def: CustomTypeDef::Enum {
                labels: vec!["active".into(), "inactive".into()],
            },
            oid: 70001,
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_load() {
        let (cat, _dir) = make_catalog();
        let def = make_enum("status", 1);
        cat.put_custom_type(&def).unwrap();

        let all = cat.load_all_custom_types().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "status");
        assert_eq!(all[0].oid, 70001);
    }

    #[test]
    fn get_single() {
        let (cat, _dir) = make_catalog();
        let def = make_enum("mood", 1);
        cat.put_custom_type(&def).unwrap();

        let got = cat.get_custom_type(1, "mood").unwrap();
        assert!(got.is_some());
        let got = cat.get_custom_type(1, "nonexistent").unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn delete_custom_type() {
        let (cat, _dir) = make_catalog();
        let def = make_enum("color", 1);
        cat.put_custom_type(&def).unwrap();
        assert!(cat.delete_custom_type(1, "color").unwrap());
        assert!(!cat.delete_custom_type(1, "color").unwrap());
    }

    #[test]
    fn tenant_isolation() {
        let (cat, _dir) = make_catalog();
        cat.put_custom_type(&StoredCustomType {
            tenant_id: 1,
            name: "x".into(),
            def: CustomTypeDef::Enum {
                labels: vec!["a".into()],
            },
            oid: 70001,
            created_at: 0,
        })
        .unwrap();
        cat.put_custom_type(&StoredCustomType {
            tenant_id: 2,
            name: "x".into(),
            def: CustomTypeDef::Enum {
                labels: vec!["b".into()],
            },
            oid: 70002,
            created_at: 0,
        })
        .unwrap();

        let t1 = cat.load_custom_types_for_tenant(1).unwrap();
        assert_eq!(t1.len(), 1);
        assert_eq!(t1[0].oid, 70001);

        let t2 = cat.load_custom_types_for_tenant(2).unwrap();
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].oid, 70002);
    }

    #[test]
    fn composite_roundtrip() {
        let (cat, _dir) = make_catalog();
        let def = StoredCustomType {
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
        let got = cat.get_custom_type(1, "address").unwrap().unwrap();
        match got.def {
            CustomTypeDef::Composite { fields } => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "street");
            }
            _ => panic!("expected composite"),
        }
    }
}
