// SPDX-License-Identifier: BUSL-1.1

//! In-memory custom type registry (Control Plane, `Send + Sync`).
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! The registry is the single source of truth for duplicate detection,
//! SHOW TYPES queries, OID assignment, and drop-protection checks.
//!
//! Entries key on `(database_id, tenant_id, type_name)`, matching the catalog
//! row. One tenant can hold the same type name in two databases.

use std::collections::HashMap;
use std::sync::{
    RwLock,
    atomic::{AtomicU32, Ordering},
};

use crate::control::security::catalog::{CustomTypeDef, StoredCustomType};

/// Base OID for user-defined types. PG built-in OIDs end well below 10000;
/// extension OIDs typically start at 16384. We use 70000+ to leave room.
const USER_TYPE_OID_BASE: u32 = 70_000;

/// In-memory custom type registry.
pub struct CustomTypeRegistry {
    /// `(database_id, tenant_id, type_name)` → `StoredCustomType`.
    by_name: RwLock<HashMap<(u64, u64, String), StoredCustomType>>,
    /// Next OID to assign. Starts at `USER_TYPE_OID_BASE + 1` and increments.
    next_oid: AtomicU32,
}

impl CustomTypeRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
            next_oid: AtomicU32::new(USER_TYPE_OID_BASE + 1),
        }
    }

    /// Allocate the next available OID. The value is stable for the lifetime
    /// of this process but is NOT persisted here — the DDL handler persists
    /// the chosen OID inside `StoredCustomType` before writing to the catalog.
    ///
    /// The counter spans every database. A pgwire client reads an OID as the
    /// identity of one type, so two distinct types must never share one.
    pub fn alloc_oid(&self) -> u32 {
        self.next_oid.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert or replace a type in the registry. Also advances `next_oid`
    /// past the stored OID to avoid collisions after restart-reload.
    pub fn register(&self, def: StoredCustomType) {
        let next = def.oid.saturating_add(1);
        self.next_oid.fetch_max(next, Ordering::Relaxed);
        let key = (def.database_id, def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// Remove a custom type. Returns `true` if it existed.
    pub fn unregister(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        let key = (database_id, tenant_id, name.to_string());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    /// Check whether a type exists in one database.
    pub fn exists(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        let key = (database_id, tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.contains_key(&key)
    }

    /// Get a type by name.
    pub fn get(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<StoredCustomType> {
        let key = (database_id, tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    /// List every type of one tenant in one database.
    pub fn list_for_tenant(&self, database_id: u64, tenant_id: u64) -> Vec<StoredCustomType> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|t| t.database_id == database_id && t.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Return the pg OID for a named type, or `None` if unknown.
    pub fn oid_for(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<u32> {
        self.get(database_id, tenant_id, name).map(|t| t.oid)
    }

    /// Validate that `value` is a legal label for the enum type `name`.
    /// Returns `Ok(())` if valid or if the type is not an enum.
    /// Returns `Err(invalid_label)` if the type exists but the label is not in it.
    pub fn validate_enum_label(
        &self,
        database_id: u64,
        tenant_id: u64,
        type_name: &str,
        value: &str,
    ) -> Result<(), String> {
        match self.get(database_id, tenant_id, type_name) {
            Some(StoredCustomType {
                def: CustomTypeDef::Enum { labels },
                ..
            }) => {
                if labels.iter().any(|l| l == value) {
                    Ok(())
                } else {
                    Err(format!(
                        "invalid input value for enum \"{type_name}\": \"{value}\""
                    ))
                }
            }
            _ => Ok(()),
        }
    }

    /// Reload from catalog. Used at startup and by recovery verifier.
    pub fn reload_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_custom_types()?;
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for t in fresh {
            let next = t.oid.saturating_add(1);
            self.next_oid.fetch_max(next, Ordering::Relaxed);
            let key = (t.database_id, t.tenant_id, t.name.clone());
            map.insert(key, t);
        }
        Ok(())
    }
}

impl Default for CustomTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enum_type(
        database_id: u64,
        tenant_id: u64,
        name: &str,
        label: &str,
        oid: u32,
    ) -> StoredCustomType {
        StoredCustomType {
            database_id,
            tenant_id,
            name: name.into(),
            def: CustomTypeDef::Enum {
                labels: vec![label.into()],
            },
            oid,
            created_at: 0,
        }
    }

    /// `exists` answers per database, so a CREATE TYPE of the same name in a
    /// second database is not refused as a duplicate. Drop the `database_id`
    /// segment from the key and this fails.
    #[test]
    fn a_type_in_one_database_does_not_block_the_same_name_in_another() {
        let reg = CustomTypeRegistry::new();
        reg.register(enum_type(1, 7, "addr", "home", 70011));

        assert!(reg.exists(1, 7, "addr"));
        assert!(
            !reg.exists(2, 7, "addr"),
            "the second database has no such type, so CREATE must be accepted"
        );

        reg.register(enum_type(2, 7, "addr", "work", 70012));
        assert_eq!(reg.oid_for(1, 7, "addr"), Some(70011));
        assert_eq!(reg.oid_for(2, 7, "addr"), Some(70012));

        assert!(reg.unregister(1, 7, "addr"));
        assert!(!reg.exists(1, 7, "addr"));
        assert!(
            reg.exists(2, 7, "addr"),
            "the other database keeps its type"
        );
    }

    /// A label legal in one database is rejected in another that defines the
    /// same type name differently.
    #[test]
    fn enum_labels_resolve_against_the_types_own_database() {
        let reg = CustomTypeRegistry::new();
        reg.register(enum_type(1, 7, "state", "draft", 70011));
        reg.register(enum_type(2, 7, "state", "live", 70012));

        assert!(reg.validate_enum_label(1, 7, "state", "draft").is_ok());
        assert!(reg.validate_enum_label(1, 7, "state", "live").is_err());
        assert!(reg.validate_enum_label(2, 7, "state", "live").is_ok());
    }

    #[test]
    fn alloc_oid_never_repeats_across_databases() {
        let reg = CustomTypeRegistry::new();
        let first = reg.alloc_oid();
        let second = reg.alloc_oid();
        assert_ne!(first, second);
    }
}
