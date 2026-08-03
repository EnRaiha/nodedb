// SPDX-License-Identifier: BUSL-1.1
//! Trigger metadata operations for the system catalog.

use super::trigger_types::StoredTrigger;
use super::types::{SystemCatalog, TRIGGERS, catalog_err};
use nodedb_types::id::DatabaseId;
use redb::ReadableDatabase;
use std::collections::HashMap;

impl SystemCatalog {
    pub fn put_trigger(&self, trigger: &StoredTrigger) -> crate::Result<()> {
        let key = trigger_key(trigger.tenant_id, trigger.database_id, &trigger.name);
        let bytes =
            zerompk::to_msgpack_vec(trigger).map_err(|e| catalog_err("serialize trigger", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("open triggers", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert trigger", e))?;
            if trigger.database_id == DatabaseId::DEFAULT {
                table
                    .remove(legacy_trigger_key(trigger.tenant_id, &trigger.name).as_str())
                    .map_err(|e| catalog_err("remove legacy trigger", e))?;
            }
        }
        txn.commit().map_err(|e| catalog_err("commit", e))
    }
    pub fn get_trigger(&self, tenant_id: u64, name: &str) -> crate::Result<Option<StoredTrigger>> {
        self.get_trigger_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }
    pub fn get_trigger_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredTrigger>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(TRIGGERS)
            .map_err(|e| catalog_err("open triggers", e))?;
        for key in [
            Some(trigger_key(tenant_id, database_id, name)),
            (database_id == DatabaseId::DEFAULT).then(|| legacy_trigger_key(tenant_id, name)),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(value) = table
                .get(key.as_str())
                .map_err(|e| catalog_err("get trigger", e))?
            {
                return zerompk::from_msgpack(value.value())
                    .map(Some)
                    .map_err(|e| catalog_err("deser trigger", e));
            }
        }
        Ok(None)
    }
    pub fn delete_trigger(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        self.delete_trigger_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }
    pub fn delete_trigger_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("open triggers", e))?;
            let v2 = table
                .remove(trigger_key(tenant_id, database_id, name).as_str())
                .map_err(|e| catalog_err("remove trigger", e))?
                .is_some();
            let legacy = if database_id == DatabaseId::DEFAULT {
                table
                    .remove(legacy_trigger_key(tenant_id, name).as_str())
                    .map_err(|e| catalog_err("remove legacy trigger", e))?
                    .is_some()
            } else {
                false
            };
            existed = v2 || legacy;
        }
        txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }
    pub fn load_all_triggers(&self) -> crate::Result<Vec<StoredTrigger>> {
        self.load_triggers_matching(|_| true)
    }
    pub fn load_triggers_for_tenant(&self, tenant_id: u64) -> crate::Result<Vec<StoredTrigger>> {
        self.load_triggers_matching(|t| t.tenant_id == tenant_id)
    }
    pub fn load_triggers_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredTrigger>> {
        self.load_triggers_matching(|t| t.database_id == database_id && t.tenant_id == tenant_id)
    }
    fn load_triggers_matching(
        &self,
        include: impl Fn(&StoredTrigger) -> bool,
    ) -> crate::Result<Vec<StoredTrigger>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(TRIGGERS)
            .map_err(|e| catalog_err("open triggers", e))?;
        let mut rows = HashMap::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range triggers", e))?
        {
            let (_key, value) = entry.map_err(|e| catalog_err("read trigger", e))?;
            let trigger: StoredTrigger = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser trigger", e))?;
            if include(&trigger) {
                rows.insert(
                    (trigger.tenant_id, trigger.database_id, trigger.name.clone()),
                    trigger,
                );
            }
        }
        Ok(rows.into_values().collect())
    }
}
fn trigger_key(tenant_id: u64, database_id: DatabaseId, name: &str) -> String {
    format!("v2:{tenant_id}:{}:{name}", database_id.as_u64())
}
fn legacy_trigger_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn trigger(database_id: DatabaseId, body_sql: &str) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            collection: "items".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: body_sql.into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "admin".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    #[test]
    fn triggers_are_isolated_by_database() {
        let catalog = catalog();
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        catalog
            .put_trigger(&trigger(db1, "BEGIN SELECT 1; END"))
            .unwrap();
        catalog
            .put_trigger(&trigger(db2, "BEGIN SELECT 2; END"))
            .unwrap();

        assert_eq!(
            catalog
                .get_trigger_in_database(db1, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "BEGIN SELECT 1; END"
        );
        assert_eq!(
            catalog
                .get_trigger_in_database(db2, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "BEGIN SELECT 2; END"
        );
        assert_eq!(catalog.load_triggers_in_database(db1, 1).unwrap().len(), 1);
        assert_eq!(catalog.load_triggers_in_database(db2, 1).unwrap().len(), 1);
        assert!(
            catalog
                .delete_trigger_in_database(db1, 1, "same_name")
                .unwrap()
        );
        assert!(
            catalog
                .get_trigger_in_database(db1, 1, "same_name")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_trigger_in_database(db2, 1, "same_name")
                .unwrap()
                .is_some()
        );
    }

    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct LegacyTrigger {
        tenant_id: u64,
        name: String,
        collection: String,
        timing: TriggerTiming,
        events: TriggerEvents,
        granularity: TriggerGranularity,
        body_sql: String,
        owner: String,
        created_at: u64,
    }

    #[test]
    fn legacy_default_trigger_is_migrated_and_v2_wins_deduplication() {
        let catalog = catalog();
        let legacy = LegacyTrigger {
            tenant_id: 1,
            name: "same_name".into(),
            collection: "items".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            body_sql: "legacy".into(),
            owner: "admin".into(),
            created_at: 0,
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).unwrap();
        let decoded: StoredTrigger = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.database_id, DatabaseId::DEFAULT);
        let txn = catalog.db.begin_write().unwrap();
        {
            txn.open_table(TRIGGERS)
                .unwrap()
                .insert("1:same_name", bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            catalog
                .get_trigger(1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "legacy"
        );

        catalog
            .put_trigger(&trigger(DatabaseId::DEFAULT, "v2"))
            .unwrap();
        let txn = catalog.db.begin_read().unwrap();
        assert!(
            txn.open_table(TRIGGERS)
                .unwrap()
                .get("1:same_name")
                .unwrap()
                .is_none()
        );
        drop(txn);

        let txn = catalog.db.begin_write().unwrap();
        {
            txn.open_table(TRIGGERS)
                .unwrap()
                .insert("1:same_name", bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        let loaded = catalog
            .load_triggers_in_database(DatabaseId::DEFAULT, 1)
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].body_sql, "v2");
    }
}
