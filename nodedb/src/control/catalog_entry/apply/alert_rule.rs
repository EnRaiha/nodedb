// SPDX-License-Identifier: BUSL-1.1

//! Apply alert rule catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader parses the condition, the window, and the notify
//! targets, and checks the collection and the duplicate name before proposing.
//! A rejection here would leave followers without a row the leader accepted.

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::event::alert::types::AlertDef;

/// Apply a `PutAlertRule` entry. CREATE and ALTER both land here.
pub fn put(def: &AlertDef, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_alert_rule(def)
        .map_err(|e| catalog_err(&format!("put_alert_rule '{}'", def.name), e))
}

/// Apply a `DeleteAlertRule` entry. A missing row is not an error: the entry
/// is idempotent under replay.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_alert_rule(database_id, tenant_id, name)
        .map_err(|e| catalog_err(&format!("delete_alert_rule '{name}'"), e))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};
    use crate::event::alert::types::{AlertCondition, CompareOp, NotifyTarget};

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const NAME: &str = "high_temp";

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> AlertDef {
        AlertDef {
            database_id: DB,
            tenant_id: TENANT,
            name: NAME.to_string(),
            collection: "metrics".to_string(),
            where_filter: Some("device_type = 'compressor'".to_string()),
            condition: AlertCondition {
                agg_func: "avg".to_string(),
                column: "temperature".to_string(),
                op: CompareOp::Gt,
                threshold: 90.0,
            },
            group_by: vec!["device_id".to_string()],
            window_ms: 300_000,
            fire_after: 3,
            recover_after: 2,
            severity: "critical".to_string(),
            notify_targets: vec![NotifyTarget::Topic {
                name: "alerts".to_string(),
            }],
            enabled: true,
            owner: "admin".to_string(),
            created_at: 1_000,
        }
    }

    fn delete_entry() -> CatalogEntry {
        CatalogEntry::DeleteAlertRule {
            database_id: DB,
            tenant_id: TENANT,
            name: NAME.to_string(),
        }
    }

    #[test]
    fn put_alert_rule_roundtrips_through_codec() {
        let entry = CatalogEntry::PutAlertRule(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutAlertRule(def) => {
                assert_eq!(def.database_id, DB);
                assert_eq!(def.tenant_id, TENANT);
                assert_eq!(def.name, NAME);
                assert_eq!(def.collection, "metrics");
                assert_eq!(def.where_filter, sample().where_filter);
                assert_eq!(def.condition.agg_func, "avg");
                assert_eq!(def.condition.threshold, 90.0);
                assert_eq!(def.group_by, vec!["device_id".to_string()]);
                assert_eq!(def.window_ms, 300_000);
                assert_eq!(def.fire_after, 3);
                assert_eq!(def.recover_after, 2);
                assert_eq!(def.severity, "critical");
                assert_eq!(def.notify_targets.len(), 1);
                assert!(def.enabled);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_alert_rule_roundtrips_through_codec() {
        let decoded = decode(&encode(&delete_entry()).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteAlertRule {
                database_id,
                tenant_id,
                name,
            } => {
                assert_eq!(database_id, DB);
                assert_eq!(tenant_id, TENANT);
                assert_eq!(name, NAME);
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_alert_rule_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(&CatalogEntry::PutAlertRule(Box::new(sample())), &catalog).unwrap();
        let stored = catalog.load_all_alert_rules().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, NAME);

        apply::apply_to(&delete_entry(), &catalog).unwrap();
        assert!(catalog.load_all_alert_rules().unwrap().is_empty());
    }

    #[test]
    fn apply_put_overwrites_the_row_the_way_alter_needs() {
        let (_dir, catalog) = open_catalog();
        put(&sample(), &catalog).expect("apply initial put");

        let disabled = AlertDef {
            enabled: false,
            ..sample()
        };
        put(&disabled, &catalog).expect("apply re-put");

        let stored = catalog.load_all_alert_rules().unwrap();
        assert_eq!(stored.len(), 1, "ALTER re-puts one row: {stored:?}");
        assert!(!stored[0].enabled);
    }

    #[test]
    fn deleting_an_absent_alert_rule_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete(DB, TENANT, "never-defined", &catalog).expect("delete absent alert rule");
    }
}
