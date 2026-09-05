// SPDX-License-Identifier: Apache-2.0

//! Database and tenant budget quotas: set, clear, and scope teardown.
//!
//! ## Lock-poisoning policy
//!
//! The maps guarded by `RwLock` here (`database_budgets`, `tenant_budgets`)
//! contain only `Arc<Budget>` handles — never partially-mutated invariants.
//! `Budget` itself is built from atomics and is consistent at every byte
//! boundary. A panic in another thread therefore cannot leave the *contents*
//! of these maps in an inconsistent state; only the `RwLock`'s poison flag
//! is set. The governor recovers via `unwrap_or_else(|p| p.into_inner())`
//! so a one-off panic in a quota helper does not poison the entire memory
//! subsystem and stall every future reservation. If a Budget's atomics ever
//! grow into a multi-step protocol that can be partially updated, this
//! policy must be revisited.

use nodedb_types::{DatabaseId, TenantId};

use super::core::MemoryGovernor;
use crate::scoped_budget::{clear_scoped_limit, set_scoped_limit};

impl MemoryGovernor {
    // ── Database budget setters ───────────────────────────────────────────────

    /// Install or replace the memory ceiling for a database.
    ///
    /// Called by the catalog apply path when `ALTER DATABASE … SET QUOTA` is
    /// executed. Takes effect for all subsequent `try_reserve` calls; in-flight
    /// tokens already issued are not recalled.
    pub fn set_database_budget(&self, db: DatabaseId, max_bytes: usize) {
        let mut map = self
            .database_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_scoped_limit(&mut map, db, max_bytes);
    }

    /// Remove the per-database budget ceiling, making that database uncapped.
    pub fn clear_database_budget(&self, db: DatabaseId) {
        let mut map = self
            .database_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        clear_scoped_limit(&mut map, &db);
    }

    // ── Tenant budget setters ─────────────────────────────────────────────────

    /// Install or replace the memory ceiling for a tenant within a database.
    pub fn set_tenant_budget(&self, db: DatabaseId, tenant: TenantId, max_bytes: usize) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_scoped_limit(&mut map, (db, tenant), max_bytes);
    }

    /// Remove the per-tenant budget ceiling.
    pub fn clear_tenant_budget(&self, db: DatabaseId, tenant: TenantId) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        clear_scoped_limit(&mut map, &(db, tenant));
    }

    // ── Scope teardown ────────────────────────────────────────────────────────

    /// Remove every memory budget of a dropped database, tenants included.
    /// A dropped scope is never re-capped, so live tokens keep no entry alive.
    pub fn clear_database_scope(&self, db: DatabaseId) {
        {
            let mut map = self
                .database_budgets
                .write()
                .unwrap_or_else(|p| p.into_inner());
            map.remove(&db);
        }
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(entry_db, _), _| *entry_db != db);
    }

    /// Remove a dropped tenant's memory budgets in every database.
    /// A dropped scope is never re-capped, so live tokens keep no entry alive.
    pub fn clear_tenant_scope(&self, tenant: TenantId) {
        let mut map = self
            .tenant_budgets
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(_, entry_tenant), _| *entry_tenant != tenant);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineId;
    use crate::governor::test_support::{db, tenant, test_config};

    // ── Quota changes must not orphan a live counter ─────────────────────────

    #[test]
    fn raising_a_live_quota_keeps_the_allocated_bytes_tracked() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.set_database_budget(db(), 1000);

        // 400 held + 700 requested exceeds the new 1000 cap.
        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, crate::error::MemError::DatabaseBudgetExhausted { .. }),
            "raising the quota must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn raising_a_live_tenant_quota_keeps_the_allocated_bytes_tracked() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_tenant_budget(db(), tenant(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.set_tenant_budget(db(), tenant(), 1000);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, crate::error::MemError::TenantBudgetExhausted { .. }),
            "raising the quota must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn clearing_a_quota_with_live_tokens_keeps_the_counter() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        let _held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();
        gov.clear_database_budget(db());
        gov.set_database_budget(db(), 1000);

        let err = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 700)
            .unwrap_err();
        assert!(
            matches!(err, crate::error::MemError::DatabaseBudgetExhausted { .. }),
            "clear-then-set must keep the held 400 bytes counted, got {err:?}"
        );
    }

    #[test]
    fn clearing_a_quota_with_no_live_tokens_drops_the_entry() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);
        gov.set_tenant_budget(db(), tenant(), 500);

        {
            let _tok = gov
                .try_reserve(db(), tenant(), EngineId::Vector, 400)
                .unwrap();
        }
        gov.clear_database_budget(db());
        gov.clear_tenant_budget(db(), tenant());

        assert!(
            !gov.database_budgets.read().unwrap().contains_key(&db()),
            "database entry must be removed once no token holds its counter"
        );
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant())),
            "tenant entry must be removed once no token holds its counter"
        );
    }

    #[test]
    fn released_bytes_do_not_count_against_a_later_quota() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        gov.set_database_budget(db(), 500);

        {
            let _tok = gov
                .try_reserve(db(), tenant(), EngineId::Vector, 400)
                .unwrap();
        }
        gov.set_database_budget(db(), 500);

        // The released 400 bytes are gone, so the full 500 is available.
        let _tok = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 500)
            .expect("released bytes must not be double-counted");
    }

    #[test]
    fn clear_database_scope_drops_the_database_and_its_tenants() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let other = DatabaseId::new(9);
        gov.set_database_budget(db(), 500);
        gov.set_tenant_budget(db(), tenant(), 500);
        gov.set_database_budget(other, 500);
        gov.set_tenant_budget(other, tenant(), 500);
        let held = gov
            .try_reserve(db(), tenant(), EngineId::Vector, 400)
            .unwrap();

        gov.clear_database_scope(db());

        assert!(!gov.database_budgets.read().unwrap().contains_key(&db()));
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant())),
            "a live token must not keep a dropped scope's entry alive"
        );
        assert!(gov.database_budgets.read().unwrap().contains_key(&other));
        assert!(
            gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(other, tenant()))
        );
        drop(held);
    }

    #[test]
    fn clear_tenant_scope_drops_that_tenant_in_every_database() {
        let gov = MemoryGovernor::new(test_config()).unwrap();
        let other_db = DatabaseId::new(9);
        let other_tenant = TenantId::new(2);
        gov.set_tenant_budget(db(), tenant(), 500);
        gov.set_tenant_budget(other_db, tenant(), 500);
        gov.set_tenant_budget(db(), other_tenant, 500);

        gov.clear_tenant_scope(tenant());

        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), tenant()))
        );
        assert!(
            !gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(other_db, tenant()))
        );
        assert!(
            gov.tenant_budgets
                .read()
                .unwrap()
                .contains_key(&(db(), other_tenant))
        );
    }
}
