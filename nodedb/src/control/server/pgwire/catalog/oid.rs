// SPDX-License-Identifier: BUSL-1.1

//! Stable, deterministic OID helpers for catalog relations.
//!
//! OIDs must be stable across server restarts, collection drop+recreate, and
//! across different nodes in a cluster. They are derived from a FNV-1a hash of
//! the collection or index identity so that the same logical object always
//! produces the same OID, regardless of creation order.
//!
//! The low 31 bits of the FNV-1a hash are used (so the value fits in a
//! positive i64), then 16384 is added to keep user OIDs above the reserved
//! range (0–16383) used by built-in Postgres system objects.

use crate::util::fnv1a_hash;

/// Well-known PostgreSQL OIDs for the catalog relations themselves, so
/// `'pg_class'::regclass` resolves the way clients expect.
pub const SYSTEM_REL_OIDS: &[(&str, i64)] = &[
    ("pg_class", 1259),
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_namespace", 2615),
    ("pg_index", 2610),
    ("pg_authid", 1260),
    ("pg_database", 1262),
    ("pg_proc", 1255),
];

/// Stable OID for a collection, for use in `pg_class` and `pg_attribute`.
///
/// Key: `"{tenant_id}:{collection_name}"`
pub fn stable_collection_oid(tenant_id: u64, collection_name: &str) -> i64 {
    let key = format!("{tenant_id}:{collection_name}");
    16384 + (fnv1a_hash(key.as_bytes()) & 0x7FFF_FFFF) as i64
}

/// Stable OID for a secondary index, for use in `pg_index`.
///
/// Key: `"idx:{tenant_id}:{collection_name}:{index_name}"`
pub fn stable_index_oid(tenant_id: u64, collection_name: &str, index_name: &str) -> i64 {
    let key = format!("idx:{tenant_id}:{collection_name}:{index_name}");
    16384 + (fnv1a_hash(key.as_bytes()) & 0x7FFF_FFFF) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_oid_is_deterministic() {
        let a = stable_collection_oid(1, "users");
        let b = stable_collection_oid(1, "users");
        assert_eq!(a, b);
    }

    #[test]
    fn collection_oid_is_positive() {
        let oid = stable_collection_oid(1, "users");
        assert!(oid > 0, "OID must be positive i64");
    }

    #[test]
    fn collection_oid_above_reserved_range() {
        let oid = stable_collection_oid(1, "users");
        assert!(oid >= 16384, "OID must be above the reserved range");
    }

    #[test]
    fn different_collection_names_produce_different_oids() {
        let a = stable_collection_oid(1, "users");
        let b = stable_collection_oid(1, "orders");
        assert_ne!(a, b);
    }

    #[test]
    fn same_name_different_tenants_produce_different_oids() {
        let a = stable_collection_oid(1, "users");
        let b = stable_collection_oid(2, "users");
        assert_ne!(a, b);
    }

    #[test]
    fn index_oid_is_deterministic() {
        let a = stable_index_oid(1, "users", "idx_email");
        let b = stable_index_oid(1, "users", "idx_email");
        assert_eq!(a, b);
    }

    #[test]
    fn index_oid_differs_from_collection_oid() {
        let coll = stable_collection_oid(1, "users");
        let idx = stable_index_oid(1, "users", "idx_email");
        assert_ne!(coll, idx);
    }

    #[test]
    fn different_index_names_produce_different_oids() {
        let a = stable_index_oid(1, "users", "idx_email");
        let b = stable_index_oid(1, "users", "idx_name");
        assert_ne!(a, b);
    }
}
