// SPDX-License-Identifier: BUSL-1.1

//! Schedule metadata operations for the system catalog.

use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;

use nodedb_types::id::DatabaseId;

use super::types::{SCHEDULES, SystemCatalog, catalog_err};
use crate::event::scheduler::ScheduleDef;

impl SystemCatalog {
    /// Store a schedule definition under its database-scoped key.
    pub fn put_schedule(&self, def: &ScheduleDef) -> crate::Result<()> {
        let key = schedule_key(def.tenant_id, DatabaseId::new(def.database_id), &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize schedule", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(SCHEDULES)
                .map_err(|e| catalog_err("open schedules", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert schedule", e))?;
        }
        txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn get_schedule(&self, tenant_id: u64, name: &str) -> crate::Result<Option<ScheduleDef>> {
        self.get_schedule_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }

    pub fn get_schedule_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<ScheduleDef>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(SCHEDULES)
            .map_err(|e| catalog_err("open schedules", e))?;
        let key = schedule_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get schedule", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser schedule", e))
    }

    /// Delete a schedule in the default database.
    pub fn delete_schedule(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        self.delete_schedule_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }

    pub fn delete_schedule_in_database(
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
                .open_table(SCHEDULES)
                .map_err(|e| catalog_err("open schedules", e))?;
            existed = table
                .remove(schedule_key(tenant_id, database_id, name).as_str())
                .map_err(|e| catalog_err("remove schedule", e))?
                .is_some();
        }
        txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    pub fn load_all_schedules(&self) -> crate::Result<Vec<ScheduleDef>> {
        self.load_schedules_matching(|_| true)
    }

    pub fn load_schedules_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<ScheduleDef>> {
        self.load_schedules_matching(|s| {
            s.database_id == database_id.as_u64() && s.tenant_id == tenant_id
        })
    }

    fn load_schedules_matching(
        &self,
        include: impl Fn(&ScheduleDef) -> bool,
    ) -> crate::Result<Vec<ScheduleDef>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = txn
            .open_table(SCHEDULES)
            .map_err(|e| catalog_err("open schedules", e))?;
        // Keep one definition per logical key regardless of iteration order.
        let mut rows: HashMap<(u64, u64, String), ScheduleDef> = HashMap::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range schedules", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read schedule", e))?;
            let def: ScheduleDef = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser schedule", e))?;
            if include(&def) {
                let logical = (def.database_id, def.tenant_id, def.name.clone());
                rows.insert(logical, def);
            }
        }
        Ok(rows.into_values().collect())
    }
}

fn schedule_key(tenant_id: u64, database_id: DatabaseId, name: &str) -> String {
    format!("v2:{tenant_id}:{}:{name}", database_id.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::scheduler::types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }
    fn schedule(database_id: u64, name: &str) -> ScheduleDef {
        ScheduleDef {
            database_id,
            tenant_id: 1,
            name: name.into(),
            cron_expr: "* * * * *".into(),
            body_sql: "BEGIN RETURN; END".into(),
            scope: ScheduleScope::Normal,
            missed_policy: MissedPolicy::Skip,
            allow_overlap: true,
            enabled: true,
            target_collection: None,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn v2_schedules_are_scoped_by_database() {
        let cat = make_catalog();
        cat.put_schedule(&schedule(1, "cleanup")).unwrap();
        cat.put_schedule(&schedule(2, "cleanup")).unwrap();
        assert_eq!(cat.load_all_schedules().unwrap().len(), 2);
        assert_eq!(
            cat.get_schedule_in_database(DatabaseId::new(1), 1, "cleanup")
                .unwrap()
                .unwrap()
                .database_id,
            1
        );
        assert!(
            cat.delete_schedule_in_database(DatabaseId::new(1), 1, "cleanup")
                .unwrap()
        );
        assert!(
            cat.get_schedule_in_database(DatabaseId::new(2), 1, "cleanup")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn default_database_schedule_round_trips_and_deletes() {
        let cat = make_catalog();
        cat.put_schedule(&schedule(0, "cleanup")).unwrap();
        assert_eq!(
            cat.get_schedule(1, "cleanup").unwrap().unwrap().database_id,
            0
        );
        assert_eq!(cat.load_all_schedules().unwrap().len(), 1);
        assert!(cat.delete_schedule(1, "cleanup").unwrap());
        assert!(cat.get_schedule(1, "cleanup").unwrap().is_none());
    }
}
