// SPDX-License-Identifier: BUSL-1.1

//! Apply retention policy catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader parses the policy body and checks the collection,
//! the duplicate name, and the per-collection uniqueness before proposing, so
//! apply carries no policy: a rejection here would leave followers without a
//! row the leader accepted.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;

/// Apply a `PutRetentionPolicy` entry. CREATE and ALTER both land here.
pub fn put(def: &RetentionPolicyDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_retention_policy(def)
        .map_err(|e| catalog_err(&format!("put_retention_policy '{}'", def.name), e))
}

/// Apply a `DeleteRetentionPolicy` entry. A missing row is not an error: the
/// entry is idempotent under replay.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_retention_policy(database_id, tenant_id, name)
        .map_err(|e| catalog_err(&format!("delete_retention_policy '{name}'"), e))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};
    use crate::engine::timeseries::retention_policy::TierDef;

    const DB: u64 = 0;
    const TENANT: u64 = 7;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> RetentionPolicyDef {
        RetentionPolicyDef {
            database_id: DB,
            tenant_id: TENANT,
            name: "sensor_policy".to_string(),
            collection: "sensor_data".to_string(),
            tiers: vec![TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms: 604_800_000,
                archive: None,
            }],
            auto_tier: true,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".to_string(),
            created_at: 1_000,
        }
    }

    fn delete_entry() -> CatalogEntry {
        CatalogEntry::DeleteRetentionPolicy {
            database_id: DB,
            tenant_id: TENANT,
            name: "sensor_policy".to_string(),
            collection: "sensor_data".to_string(),
        }
    }

    #[test]
    fn put_retention_policy_roundtrips_through_codec() {
        let entry = CatalogEntry::PutRetentionPolicy(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutRetentionPolicy(def) => {
                assert_eq!(def.name, sample().name);
                assert_eq!(def.collection, sample().collection);
                assert_eq!(def.database_id, DB);
                assert_eq!(def.tenant_id, TENANT);
                assert!(def.auto_tier);
                assert_eq!(def.tiers.len(), 1);
                assert_eq!(def.tiers[0].retain_ms, 604_800_000);
                assert_eq!(def.eval_interval_ms, sample().eval_interval_ms);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_retention_policy_roundtrips_through_codec() {
        let decoded = decode(&encode(&delete_entry()).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteRetentionPolicy {
                database_id,
                tenant_id,
                name,
                collection,
            } => {
                assert_eq!(database_id, DB);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(name, "sensor_policy");
                assert_eq!(collection, "sensor_data");
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_retention_policy_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(
            &CatalogEntry::PutRetentionPolicy(Box::new(sample())),
            &catalog,
        )
        .unwrap();
        let stored = catalog.load_all_retention_policies().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, "sensor_policy");

        apply::apply_to(&delete_entry(), &catalog).unwrap();
        assert!(catalog.load_all_retention_policies().unwrap().is_empty());
    }

    #[test]
    fn apply_put_overwrites_the_row_the_way_alter_needs() {
        let (_dir, catalog) = open_catalog();
        put(&sample(), &catalog).expect("apply initial put");

        let disabled = RetentionPolicyDef {
            enabled: false,
            ..sample()
        };
        put(&disabled, &catalog).expect("apply re-put");

        let stored = catalog.load_all_retention_policies().unwrap();
        assert_eq!(stored.len(), 1, "ALTER re-puts one row: {stored:?}");
        assert!(!stored[0].enabled);
    }

    #[test]
    fn deleting_an_absent_retention_policy_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete(DB, TENANT, "never-defined", &catalog).expect("delete absent retention policy");
    }
}
