// SPDX-License-Identifier: BUSL-1.1

//! Retention policy metadata operations for the system catalog.

use super::types::{RETENTION_POLICIES, SystemCatalog, catalog_err};
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Store a retention policy definition.
    ///
    /// Key format: `(database_id, "{tenant_id}:{policy_name}")`.
    pub fn put_retention_policy(&self, def: &RetentionPolicyDef) -> crate::Result<()> {
        let inner_key = retention_policy_key(def.tenant_id, &def.name);
        let bytes = zerompk::to_msgpack_vec(def)
            .map_err(|e| catalog_err("serialize retention policy", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(RETENTION_POLICIES)
                .map_err(|e| catalog_err("open retention_policies", e))?;
            table
                .insert((def.database_id, inner_key.as_str()), bytes.as_slice())
                .map_err(|e| catalog_err("insert retention policy", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a retention policy. Returns true if it existed.
    pub fn delete_retention_policy(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let inner_key = retention_policy_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(RETENTION_POLICIES)
                .map_err(|e| catalog_err("open retention_policies", e))?;
            existed = table
                .remove((database_id, inner_key.as_str()))
                .map_err(|e| catalog_err("delete retention policy", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all retention policies (all databases, all tenants). Each
    /// returned def carries its own `database_id`.
    pub fn load_all_retention_policies(&self) -> crate::Result<Vec<RetentionPolicyDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(RETENTION_POLICIES)
            .map_err(|e| catalog_err("open retention_policies", e))?;

        let mut policies = Vec::new();
        let mut range = table
            .range::<(u64, &str)>(..)
            .map_err(|e| catalog_err("range retention_policies", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<RetentionPolicyDef>(value.value()) {
                policies.push(def);
            }
        }
        Ok(policies)
    }
}

fn retention_policy_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::engine::timeseries::retention_policy::types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_and_load() {
        let cat = make_catalog();
        let def = RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name: "sensor_policy".into(),
            collection: "sensor_data".into(),
            tiers: vec![
                TierDef {
                    tier_index: 0,
                    resolution_ms: 0,
                    aggregates: Vec::new(),
                    retain_ms: 604_800_000,
                    archive: None,
                },
                TierDef {
                    tier_index: 1,
                    resolution_ms: 60_000,
                    aggregates: Vec::new(),
                    retain_ms: 7_776_000_000,
                    archive: None,
                },
            ],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".into(),
            created_at: 1000,
        };
        cat.put_retention_policy(&def).unwrap();

        let all = cat.load_all_retention_policies().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "sensor_policy");
        assert_eq!(all[0].collection, "sensor_data");
        assert_eq!(all[0].tiers.len(), 2);
        assert!(all[0].tiers[0].is_raw());
        assert_eq!(all[0].tiers[1].resolution_ms, 60_000);
    }

    #[test]
    fn delete_retention_policy() {
        let cat = make_catalog();
        let def = RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name: "p1".into(),
            collection: "c1".into(),
            tiers: vec![TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms: 86_400_000,
                archive: None,
            }],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".into(),
            created_at: 0,
        };
        cat.put_retention_policy(&def).unwrap();
        assert!(cat.delete_retention_policy(0, 1, "p1").unwrap());
        assert!(!cat.delete_retention_policy(0, 1, "p1").unwrap());
        assert!(cat.load_all_retention_policies().unwrap().is_empty());
    }

    #[test]
    fn overwrite_policy() {
        let cat = make_catalog();
        let mut def = RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name: "p1".into(),
            collection: "c1".into(),
            tiers: vec![TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms: 86_400_000,
                archive: None,
            }],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".into(),
            created_at: 0,
        };
        cat.put_retention_policy(&def).unwrap();

        def.enabled = false;
        cat.put_retention_policy(&def).unwrap();

        let all = cat.load_all_retention_policies().unwrap();
        assert_eq!(all.len(), 1);
        assert!(!all[0].enabled);
    }
}
