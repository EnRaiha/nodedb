// SPDX-License-Identifier: BUSL-1.1

//! In-memory alert rule registry.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::AlertDef;

/// Thread-safe in-memory registry of alert rules.
///
/// Keyed by `(database_id, tenant_id, alert_name)`. Lives on the Control
/// Plane (`Send + Sync`).
pub struct AlertRegistry {
    by_name: RwLock<HashMap<(u64, u64, String), AlertDef>>,
}

impl AlertRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    fn read_map(&self) -> std::sync::RwLockReadGuard<'_, HashMap<(u64, u64, String), AlertDef>> {
        self.by_name.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write_map(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<(u64, u64, String), AlertDef>> {
        self.by_name.write().unwrap_or_else(|p| p.into_inner())
    }

    pub fn register(&self, def: AlertDef) {
        let key = (def.database_id, def.tenant_id, def.name.clone());
        self.write_map().insert(key, def);
    }

    pub fn unregister(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        self.write_map()
            .remove(&(database_id, tenant_id, name.to_string()))
            .is_some()
    }

    pub fn get(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<AlertDef> {
        self.read_map()
            .get(&(database_id, tenant_id, name.to_string()))
            .cloned()
    }

    pub fn update(&self, def: AlertDef) {
        self.register(def);
    }

    /// List all enabled alerts (all tenants). Used by the eval loop.
    pub fn list_all_enabled(&self) -> Vec<AlertDef> {
        self.read_map()
            .values()
            .filter(|a| a.enabled)
            .cloned()
            .collect()
    }

    /// List all alerts (all tenants, enabled and disabled).
    /// Used by the recovery verifier.
    pub fn list_all(&self) -> Vec<AlertDef> {
        self.read_map().values().cloned().collect()
    }

    /// Clear and reload from catalog. Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_alert_rules()?;
        let mut map = self.write_map();
        map.clear();
        for alert in fresh {
            let key = (alert.database_id, alert.tenant_id, alert.name.clone());
            map.insert(key, alert);
        }
        Ok(())
    }

    /// List all alerts for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u64) -> Vec<AlertDef> {
        self.read_map()
            .values()
            .filter(|a| a.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// List all alert rules for a tenant within a single database.
    ///
    /// Used by `SHOW ALERTS`, which must not leak rules from other databases
    /// the tenant exists in into the current session.
    pub fn list_for_tenant_in_database(&self, database_id: u64, tenant_id: u64) -> Vec<AlertDef> {
        self.read_map()
            .values()
            .filter(|a| a.database_id == database_id && a.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Load from catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let alerts = match catalog.load_all_alert_rules() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load alert rules from catalog");
                return;
            }
        };
        if alerts.is_empty() {
            return;
        }
        let mut map = self.write_map();
        for alert in alerts {
            let key = (alert.database_id, alert.tenant_id, alert.name.clone());
            map.insert(key, alert);
        }
        tracing::info!(count = map.len(), "loaded alert rules from catalog");
    }
}

impl Default for AlertRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::alert::types::{AlertCondition, CompareOp};

    fn make_alert(tenant_id: u64, name: &str) -> AlertDef {
        AlertDef {
            database_id: 0,
            tenant_id,
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
            notify_targets: Vec::new(),
            enabled: true,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn register_and_get() {
        let reg = AlertRegistry::new();
        reg.register(make_alert(1, "high_temp"));
        assert!(reg.get(0, 1, "high_temp").is_some());
        assert!(reg.get(0, 1, "other").is_none());
        assert!(reg.get(0, 2, "high_temp").is_none());
    }

    #[test]
    fn unregister() {
        let reg = AlertRegistry::new();
        reg.register(make_alert(1, "a1"));
        assert!(reg.unregister(0, 1, "a1"));
        assert!(!reg.unregister(0, 1, "a1"));
        assert!(reg.get(0, 1, "a1").is_none());
    }

    #[test]
    fn list_enabled() {
        let reg = AlertRegistry::new();
        reg.register(make_alert(1, "a1"));
        let mut disabled = make_alert(1, "a2");
        disabled.enabled = false;
        reg.register(disabled);
        let enabled = reg.list_all_enabled();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "a1");
    }
}
