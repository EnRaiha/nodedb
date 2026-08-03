// SPDX-License-Identifier: BUSL-1.1

//! Alert rule metadata operations for the system catalog.

use super::types::{ALERT_RULES, SystemCatalog, catalog_err};
use crate::event::alert::types::AlertDef;
use redb::ReadableDatabase;

impl SystemCatalog {
    /// Store an alert rule definition.
    ///
    /// Key format: `(database_id, "{tenant_id}:{alert_name}")`.
    pub fn put_alert_rule(&self, def: &AlertDef) -> crate::Result<()> {
        let inner_key = alert_key(def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize alert rule", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ALERT_RULES)
                .map_err(|e| catalog_err("open alert_rules", e))?;
            table
                .insert((def.database_id, inner_key.as_str()), bytes.as_slice())
                .map_err(|e| catalog_err("insert alert rule", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete an alert rule. Returns true if it existed.
    pub fn delete_alert_rule(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let inner_key = alert_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(ALERT_RULES)
                .map_err(|e| catalog_err("open alert_rules", e))?;
            existed = table
                .remove((database_id, inner_key.as_str()))
                .map_err(|e| catalog_err("delete alert rule", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all alert rules (all databases, all tenants). Each returned
    /// def carries its own `database_id`.
    pub fn load_all_alert_rules(&self) -> crate::Result<Vec<AlertDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ALERT_RULES)
            .map_err(|e| catalog_err("open alert_rules", e))?;

        let mut rules = Vec::new();
        let mut range = table
            .range::<(u64, &str)>(..)
            .map_err(|e| catalog_err("range alert_rules", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<AlertDef>(value.value()) {
                rules.push(def);
            }
        }
        Ok(rules)
    }
}

fn alert_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::event::alert::types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn make_alert(name: &str) -> AlertDef {
        AlertDef {
            database_id: 0,
            tenant_id: 1,
            name: name.into(),
            collection: "metrics".into(),
            where_filter: None,
            condition: AlertCondition {
                agg_func: "avg".into(),
                column: "temperature".into(),
                op: CompareOp::Gt,
                threshold: 90.0,
            },
            group_by: vec!["device_id".into()],
            window_ms: 300_000,
            fire_after: 3,
            recover_after: 2,
            severity: "critical".into(),
            notify_targets: vec![NotifyTarget::Topic {
                name: "alerts".into(),
            }],
            enabled: true,
            owner: "admin".into(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_load() {
        let cat = make_catalog();
        cat.put_alert_rule(&make_alert("high_temp")).unwrap();

        let all = cat.load_all_alert_rules().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "high_temp");
        assert_eq!(all[0].severity, "critical");
        assert_eq!(all[0].fire_after, 3);
    }

    #[test]
    fn delete_alert_rule() {
        let cat = make_catalog();
        cat.put_alert_rule(&make_alert("a1")).unwrap();
        assert!(cat.delete_alert_rule(0, 1, "a1").unwrap());
        assert!(!cat.delete_alert_rule(0, 1, "a1").unwrap());
        assert!(cat.load_all_alert_rules().unwrap().is_empty());
    }
}
