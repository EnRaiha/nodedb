// SPDX-License-Identifier: BUSL-1.1

//! Schedule metadata operations for the system catalog.

use redb::ReadableDatabase;
use std::collections::HashMap;

use nodedb_types::id::DatabaseId;

use super::types::{SCHEDULES, SystemCatalog, catalog_err};
use crate::event::scheduler::ScheduleDef;

impl SystemCatalog {
    /// Store a schedule definition under its database-scoped v2 key.
    ///
    /// Default-database writes also remove the pre-database legacy key, so the
    /// v2 row is authoritative immediately after migration.
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
            if def.database_id == DatabaseId::DEFAULT.as_u64() {
                table
                    .remove(legacy_schedule_key(def.tenant_id, &def.name).as_str())
                    .map_err(|e| catalog_err("remove legacy schedule", e))?;
            }
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
        for key in [
            Some(schedule_key(tenant_id, database_id, name)),
            (database_id == DatabaseId::DEFAULT).then(|| legacy_schedule_key(tenant_id, name)),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(value) = table
                .get(key.as_str())
                .map_err(|e| catalog_err("get schedule", e))?
            {
                return zerompk::from_msgpack(value.value())
                    .map(Some)
                    .map_err(|e| catalog_err("deser schedule", e));
            }
        }
        Ok(None)
    }

    /// Delete a schedule. The default database also removes a legacy row.
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
            let v2 = table
                .remove(schedule_key(tenant_id, database_id, name).as_str())
                .map_err(|e| catalog_err("remove schedule", e))?
                .is_some();
            let legacy = if database_id == DatabaseId::DEFAULT {
                table
                    .remove(legacy_schedule_key(tenant_id, name).as_str())
                    .map_err(|e| catalog_err("remove legacy schedule", e))?
                    .is_some()
            } else {
                false
            };
            existed = v2 || legacy;
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
        // v2 keys and legacy keys can coexist during an upgrade. Keep just one
        // definition per logical key, with v2 taking precedence regardless of
        // iteration order.
        let mut rows: HashMap<(u64, u64, String), (bool, ScheduleDef)> = HashMap::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range schedules", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read schedule", e))?;
            let mut def: ScheduleDef = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser schedule", e))?;
            let is_v2 = key.value().starts_with("v2:");
            if !is_v2 {
                def.database_id = DatabaseId::DEFAULT.as_u64();
            }
            if include(&def) {
                let logical = (def.database_id, def.tenant_id, def.name.clone());
                if !rows
                    .get(&logical)
                    .is_some_and(|(existing_v2, _)| *existing_v2)
                    || is_v2
                {
                    rows.insert(logical, (is_v2, def));
                }
            }
        }
        Ok(rows.into_values().map(|(_, def)| def).collect())
    }
}

fn schedule_key(tenant_id: u64, database_id: DatabaseId, name: &str) -> String {
    format!("v2:{tenant_id}:{}:{name}", database_id.as_u64())
}
fn legacy_schedule_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
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

    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct LegacyScheduleDef {
        tenant_id: u64,
        name: String,
        cron_expr: String,
        body_sql: String,
        scope: ScheduleScope,
        missed_policy: MissedPolicy,
        allow_overlap: bool,
        enabled: bool,
        target_collection: Option<String>,
        owner: String,
        created_at: u64,
    }

    #[test]
    fn legacy_default_schedule_falls_back_and_v2_wins() {
        let cat = make_catalog();
        let legacy = LegacyScheduleDef {
            tenant_id: 1,
            name: "cleanup".into(),
            cron_expr: "* * * * *".into(),
            body_sql: "BEGIN RETURN; END".into(),
            scope: ScheduleScope::Normal,
            missed_policy: MissedPolicy::Skip,
            allow_overlap: true,
            enabled: true,
            target_collection: None,
            owner: "admin".into(),
            created_at: 0,
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).unwrap();
        let decoded: ScheduleDef = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.database_id, DatabaseId::DEFAULT.as_u64());
        let txn = cat.db.begin_write().unwrap();
        {
            txn.open_table(SCHEDULES)
                .unwrap()
                .insert("1:cleanup", bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            cat.get_schedule(1, "cleanup").unwrap().unwrap().database_id,
            0
        );
        let v2 = schedule(0, "cleanup");
        cat.put_schedule(&v2).unwrap();
        assert_eq!(cat.load_all_schedules().unwrap().len(), 1);
        assert!(cat.delete_schedule(1, "cleanup").unwrap());
        assert!(cat.get_schedule(1, "cleanup").unwrap().is_none());
    }
}
