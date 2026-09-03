// SPDX-License-Identifier: BUSL-1.1
//! Trigger metadata operations for the system catalog.

use super::trigger_types::StoredTrigger;
use super::types::{SystemCatalog, TRIGGERS, catalog_err};
use nodedb_types::id::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};
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
        }
        txn.commit().map_err(|e| catalog_err("commit", e))
    }
    pub fn get_trigger(&self, tenant_id: u64, name: &str) -> crate::Result<Option<StoredTrigger>> {
        self.get_trigger_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }
    /// Get a trigger in an exact database scope, with the calling
    /// connection's buffered transactional DDL merged in — a `CREATE
    /// TRIGGER` this same transaction has buffered but not yet committed
    /// resolves here too.
    pub fn get_trigger_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredTrigger>> {
        let committed = self.get_committed_trigger_in_database(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_trigger(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// Committed-only read, bypassing the transaction DDL overlay. The
    /// descriptor stamper reads through this — see
    /// `get_committed_collection` for why.
    pub fn get_committed_trigger_in_database(
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
        let key = trigger_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get trigger", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser trigger", e))
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
            existed = table
                .remove(trigger_key(tenant_id, database_id, name).as_str())
                .map_err(|e| catalog_err("remove trigger", e))?
                .is_some();
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
    /// Every trigger of one database, across every tenant.
    ///
    /// The key leads with `tenant_id`, not `database_id`, so this scans
    /// every trigger row and filters in memory.
    pub fn load_triggers_for_database(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StoredTrigger>> {
        self.load_triggers_matching(|t| t.database_id == database_id)
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
            .range(..)
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
}
