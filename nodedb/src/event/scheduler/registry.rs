// SPDX-License-Identifier: BUSL-1.1

//! In-memory registry of scheduled jobs.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_types::id::DatabaseId;

use super::types::ScheduleDef;

/// In-memory schedule registry.
pub struct ScheduleRegistry {
    /// (database_id, tenant_id, schedule_name) → ScheduleDef.
    by_name: RwLock<HashMap<(u64, u64, String), ScheduleDef>>,
}

impl ScheduleRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, def: ScheduleDef) {
        let key = (def.database_id, def.tenant_id, def.name.clone());
        self.by_name
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, def);
    }

    pub fn unregister(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
        self.by_name
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(database_id.as_u64(), tenant_id, name.to_string()))
            .is_some()
    }

    pub fn get(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> Option<ScheduleDef> {
        self.by_name
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(database_id.as_u64(), tenant_id, name.to_string()))
            .cloned()
    }

    /// Update an existing schedule definition (ALTER SCHEDULE).
    pub fn update(&self, def: ScheduleDef) {
        self.register(def);
    }

    /// List all enabled schedules (all tenants). Used by the scheduler loop.
    pub fn list_all_enabled(&self) -> Vec<ScheduleDef> {
        self.by_name
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|s| s.enabled)
            .cloned()
            .collect()
    }
    /// List all schedules (all tenants, enabled and disabled).
    pub fn list_all(&self) -> Vec<ScheduleDef> {
        self.by_name
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Clear and reload from catalog. Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_schedules()?;
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for sched in fresh {
            map.insert(
                (sched.database_id, sched.tenant_id, sched.name.clone()),
                sched,
            );
        }
        Ok(())
    }

    /// List all schedules for a tenant in its selected database.
    pub fn list_for_tenant(&self, database_id: DatabaseId, tenant_id: u64) -> Vec<ScheduleDef> {
        self.by_name
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|s| s.database_id == database_id.as_u64() && s.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Load from catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let schedules = match catalog.load_all_schedules() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load schedules from catalog");
                return;
            }
        };
        if schedules.is_empty() {
            return;
        }
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        for sched in schedules {
            map.insert(
                (sched.database_id, sched.tenant_id, sched.name.clone()),
                sched,
            );
        }
        tracing::info!(count = map.len(), "loaded schedules from catalog");
    }
}
impl Default for ScheduleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::scheduler::types::{MissedPolicy, ScheduleScope};
    fn schedule(database_id: u64) -> ScheduleDef {
        ScheduleDef {
            database_id,
            tenant_id: 1,
            name: "same".into(),
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
    fn same_name_in_different_databases_coexists() {
        let registry = ScheduleRegistry::new();
        registry.register(schedule(1));
        registry.register(schedule(2));
        assert_eq!(registry.list_all().len(), 2);
        assert_eq!(
            registry
                .get(DatabaseId::new(1), 1, "same")
                .unwrap()
                .database_id,
            1
        );
        assert!(registry.unregister(DatabaseId::new(1), 1, "same"));
        assert!(registry.get(DatabaseId::new(2), 1, "same").is_some());
    }
}
