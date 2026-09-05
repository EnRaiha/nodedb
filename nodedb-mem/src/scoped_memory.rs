// SPDX-License-Identifier: Apache-2.0

//! Governor handle pre-bound to a database, tenant, and engine.
//!
//! A call site holding one cannot omit a budget layer. The identity is set
//! at construction, never per call.

use std::sync::Arc;

use nodedb_types::{DatabaseId, TenantId};

use crate::engine::EngineId;
use crate::error::Result;
use crate::governor::MemoryGovernor;
use crate::reservation_token::ReservationToken;

/// A [`MemoryGovernor`] handle bound to one database, tenant, and engine.
#[derive(Clone, Debug)]
pub struct ScopedMemory {
    governor: Arc<MemoryGovernor>,
    database_id: DatabaseId,
    tenant_id: TenantId,
    engine: EngineId,
}

impl ScopedMemory {
    pub fn new(
        governor: Arc<MemoryGovernor>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        engine: EngineId,
    ) -> Self {
        Self {
            governor,
            database_id,
            tenant_id,
            engine,
        }
    }

    /// Reserve `bytes`, checking global, database, tenant, then engine.
    ///
    /// # Errors
    ///
    /// Names the first exhausted layer.
    pub fn reserve(&self, bytes: usize) -> Result<ReservationToken> {
        self.governor
            .try_reserve(self.database_id, self.tenant_id, self.engine, bytes)
    }

    /// Account `bytes` already resident. Never fails.
    ///
    /// Denying a charge hides allocated memory, it never frees it.
    pub fn charge(&self, bytes: usize) -> ReservationToken {
        self.governor
            .charge(self.database_id, self.tenant_id, self.engine, bytes)
    }

    pub fn engine(&self) -> EngineId {
        self.engine
    }

    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn governor(&self) -> &Arc<MemoryGovernor> {
        &self.governor
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::engine_limits::EngineLimits;
    use crate::error::MemError;
    use crate::governor::GovernorConfig;

    fn make_governor(limits: &[(EngineId, usize)], ceiling: usize) -> Arc<MemoryGovernor> {
        let engine_limits: EngineLimits = limits.iter().copied().collect();
        Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: ceiling,
                engine_limits,
            })
            .expect("valid config"),
        )
    }

    fn scoped(gov: &Arc<MemoryGovernor>, engine: EngineId) -> ScopedMemory {
        ScopedMemory::new(
            Arc::clone(gov),
            DatabaseId::DEFAULT,
            TenantId::new(1),
            engine,
        )
    }

    #[test]
    fn reservation_moves_the_global_counter_symmetrically() {
        let gov = make_governor(&[(EngineId::Vector, 4096)], 8192);
        let mem = scoped(&gov, EngineId::Vector);

        {
            let _token = mem.reserve(1000).expect("within budget");
            assert_eq!(
                gov.global_counter.allocated.load(Ordering::Relaxed),
                1000,
                "a reservation must increment the global counter it releases on drop"
            );
        }

        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "dropping the token must return the global counter to zero"
        );
    }

    #[test]
    fn reserve_over_engine_budget_returns_err_and_charges_nothing() {
        let gov = make_governor(&[(EngineId::Fts, 512)], 1024);
        let mem = scoped(&gov, EngineId::Fts);

        let err = mem.reserve(1000).unwrap_err();
        assert!(
            matches!(err, MemError::BudgetExhausted { .. }),
            "expected BudgetExhausted, got {err:?}"
        );
        assert_eq!(gov.budget(EngineId::Fts).allocated(), 0);
    }

    #[test]
    fn independent_engines_accumulate_and_release_independently() {
        let gov = make_governor(
            &[
                (EngineId::Vector, 4096),
                (EngineId::Columnar, 4096),
                (EngineId::Graph, 4096),
            ],
            16384,
        );
        let vector = scoped(&gov, EngineId::Vector);
        let columnar = scoped(&gov, EngineId::Columnar);
        let graph = scoped(&gov, EngineId::Graph);

        let t1 = vector.reserve(1000).unwrap();
        let t2 = columnar.reserve(2000).unwrap();
        let t3 = graph.reserve(3000).unwrap();

        assert_eq!(gov.budget(EngineId::Vector).allocated(), 1000);
        assert_eq!(gov.budget(EngineId::Columnar).allocated(), 2000);
        assert_eq!(gov.budget(EngineId::Graph).allocated(), 3000);

        drop(t2);
        assert_eq!(gov.budget(EngineId::Vector).allocated(), 1000);
        assert_eq!(gov.budget(EngineId::Columnar).allocated(), 0);
        assert_eq!(gov.budget(EngineId::Graph).allocated(), 3000);

        drop(t1);
        drop(t3);
        assert_eq!(gov.budget(EngineId::Vector).allocated(), 0);
        assert_eq!(gov.budget(EngineId::Graph).allocated(), 0);
    }

    #[test]
    fn freed_reservation_can_be_reserved_again() {
        let gov = make_governor(&[(EngineId::Timeseries, 1024)], 2048);
        let mem = scoped(&gov, EngineId::Timeseries);

        {
            let _t = mem.reserve(1024).unwrap();
            assert!(mem.reserve(1).is_err(), "budget fully consumed");
        } // token dropped -> budget freed

        let _t2 = mem
            .reserve(1024)
            .expect("budget freed by previous token drop");
    }

    /// `mem::forget` on the returned token prevents release across all
    /// four layers `try_reserve` charges.
    #[test]
    fn mem_forget_does_not_release() {
        let gov = make_governor(&[(EngineId::Kv, 4096)], 8192);
        let mem = scoped(&gov, EngineId::Kv);

        let token = mem.reserve(500).unwrap();
        assert_eq!(gov.budget(EngineId::Kv).allocated(), 500);

        std::mem::forget(token);

        assert_eq!(
            gov.budget(EngineId::Kv).allocated(),
            500,
            "mem::forget intentionally skips drop; bytes remain charged"
        );
    }

    #[test]
    fn reserve_zero_bytes_is_allowed() {
        let gov = make_governor(&[(EngineId::Query, 1024)], 2048);
        let mem = scoped(&gov, EngineId::Query);

        let token = mem.reserve(0).expect("zero bytes always fits");
        assert_eq!(token.size(), 0);
        drop(token);
        assert_eq!(gov.budget(EngineId::Query).allocated(), 0);
    }

    // ── `charge`: infallible accounting for already-resident memory ─────────

    #[test]
    fn charge_past_the_engine_limit_still_accounts_the_bytes() {
        let gov = make_governor(&[(EngineId::Timeseries, 100)], 1000);
        let mem = scoped(&gov, EngineId::Timeseries);

        let token = mem.charge(500);
        assert_eq!(token.size(), 500);
        assert_eq!(
            gov.budget(EngineId::Timeseries).allocated(),
            500,
            "a charge past the engine limit must still be visible to the governor"
        );
    }

    #[test]
    fn charge_past_the_global_ceiling_still_accounts_and_clamps_utilization() {
        let gov = make_governor(&[(EngineId::Timeseries, 50)], 100);
        let mem = scoped(&gov, EngineId::Timeseries);

        let token = mem.charge(500);
        assert_eq!(token.size(), 500);
        assert_eq!(
            gov.global_utilization_percent(),
            100,
            "utilization must clamp at 100%, not wrap, when a charge exceeds the ceiling"
        );
    }

    #[test]
    fn dropping_a_charged_token_releases_every_layer() {
        let gov = make_governor(&[(EngineId::Kv, 1024)], 2048);
        let mem = scoped(&gov, EngineId::Kv);

        {
            let _token = mem.charge(2000);
            assert_eq!(gov.budget(EngineId::Kv).allocated(), 2000);
            assert_eq!(gov.global_counter.allocated.load(Ordering::Relaxed), 2000);
        }

        assert_eq!(
            gov.budget(EngineId::Kv).allocated(),
            0,
            "dropping a charged token must return the engine counter to baseline"
        );
        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "dropping a charged token must return the global counter to baseline"
        );
    }

    #[test]
    fn charge_does_not_loosen_reserve() {
        let gov = make_governor(&[(EngineId::Graph, 100)], 1000);
        let mem = scoped(&gov, EngineId::Graph);

        let _held = mem.charge(500);
        let err = mem
            .reserve(1)
            .expect_err("reserve must still refuse past the limit after an unrelated charge");
        assert!(matches!(err, MemError::BudgetExhausted { .. }));
    }
}
