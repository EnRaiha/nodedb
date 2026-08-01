// SPDX-License-Identifier: BUSL-1.1

//! Trigger enumeration for `PurgeCollection` cascade.
//!
//! A trigger fires on DML against a specific collection. When that
//! collection is hard-deleted, every trigger bound to it must be
//! dropped in the same metadata-raft commit — leaving a trigger that
//! references a missing collection would make every subsequent write
//! path evaluate the orphan at trigger-dispatch time.

use crate::control::security::catalog::SystemCatalog;

/// Enumerate triggers bound to `(database_id, tenant_id, collection)`.
/// Returns trigger names only.
pub fn find_triggers_on(
    catalog: &SystemCatalog,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_triggers_in_database(database_id, tenant_id)?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|t| t.collection == collection)
        .map(|t| t.name)
        .collect();
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredTrigger;
    use tempfile::TempDir;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    fn trig(tenant: u64, name: &str, collection: &str) -> StoredTrigger {
        use crate::control::security::catalog::trigger_types::{
            TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity,
            TriggerSecurity, TriggerTiming,
        };
        StoredTrigger {
            tenant_id: tenant,
            database_id: nodedb_types::id::DatabaseId::DEFAULT,
            name: name.to_string(),
            collection: collection.to_string(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: String::new(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::default(),
            security: TriggerSecurity::default(),
            batch_mode: TriggerBatchMode::default(),
            owner: "o".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn filters_by_collection() {
        let (c, _t) = cat();
        c.put_trigger(&trig(1, "t_users_insert", "users")).unwrap();
        c.put_trigger(&trig(1, "t_users_update", "users")).unwrap();
        c.put_trigger(&trig(1, "t_orders", "orders")).unwrap();
        let found = find_triggers_on(&c, nodedb_types::DatabaseId::DEFAULT, 1, "users").unwrap();
        assert_eq!(found, vec!["t_users_insert", "t_users_update"]);
    }

    #[test]
    fn skips_cross_tenant() {
        let (c, _t) = cat();
        c.put_trigger(&trig(1, "t_a", "users")).unwrap();
        c.put_trigger(&trig(2, "t_b", "users")).unwrap();
        assert_eq!(
            find_triggers_on(&c, nodedb_types::DatabaseId::DEFAULT, 1, "users").unwrap(),
            vec!["t_a"]
        );
    }

    #[test]
    fn skips_same_tenant_trigger_in_another_database() {
        let (c, _t) = cat();
        c.put_trigger(&trig(1, "t_default", "users")).unwrap();
        let mut other = trig(1, "t_other", "users");
        other.database_id = nodedb_types::DatabaseId::new(2);
        c.put_trigger(&other).unwrap();

        assert_eq!(
            find_triggers_on(&c, nodedb_types::DatabaseId::DEFAULT, 1, "users").unwrap(),
            vec!["t_default"]
        );
        assert_eq!(
            find_triggers_on(&c, nodedb_types::DatabaseId::new(2), 1, "users").unwrap(),
            vec!["t_other"]
        );
    }
}
