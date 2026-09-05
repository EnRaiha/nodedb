// SPDX-License-Identifier: Apache-2.0

//! RAII reservation token for the four-level memory hierarchy.
//!
//! A [`ReservationToken`] is produced by
//! [`MemoryGovernor::try_reserve`](crate::governor::MemoryGovernor::try_reserve)
//! and holds references to all four budget layers:
//! global counter, optional per-database counter, optional per-tenant counter,
//! and the engine identifier for engine-budget release.
//!
//! Dropping the token releases all four layers atomically.
//!
//! # Panic safety
//!
//! `Drop` uses atomic operations and a `tracing::warn!` on over-release.
//! Neither path panics.
//!
//! # `mem::forget`
//!
//! Calling `mem::forget` on a token prevents release. This is intentional:
//! the token represents live allocations that must not be double-freed.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use nodedb_types::{DatabaseId, TenantId};

use crate::engine::EngineId;
use crate::governor::GlobalCounter;
use crate::over_release::{Layer, ReleaseIdentity, release_layer};

/// Holds a memory reservation across the four budget layers.
///
/// Releasing happens in reverse order (engine → tenant → database → global)
/// on drop.
#[must_use = "dropping a ReservationToken immediately releases the reservation; bind it to a variable"]
pub struct ReservationToken {
    /// Shared global-ceiling atomic. Drop decrements this.
    pub(crate) global_counter: Arc<GlobalCounter>,
    /// Per-database allocated counter. `None` if no database budget.
    pub(crate) database_counter: Option<Arc<AtomicUsize>>,
    /// Per-tenant allocated counter. `None` if no tenant budget.
    pub(crate) tenant_counter: Option<Arc<AtomicUsize>>,
    /// Per-engine allocated counter. `None` if no engine budget (unusual —
    /// `try_reserve` always requires a registered engine).
    pub(crate) engine_counter: Option<Arc<AtomicUsize>>,
    /// Bytes reserved at every layer.
    pub(crate) size: usize,
    /// Identity carried for `Debug` and metrics.
    db: DatabaseId,
    tenant: TenantId,
    engine: EngineId,
}

/// Parameters for constructing a [`ReservationToken`].
///
/// Used by [`MemoryGovernor::try_reserve`] to avoid a too-many-arguments
/// constructor.
pub(crate) struct ReservationParams {
    pub global_counter: Arc<GlobalCounter>,
    pub database_counter: Option<Arc<AtomicUsize>>,
    pub tenant_counter: Option<Arc<AtomicUsize>>,
    pub engine_counter: Option<Arc<AtomicUsize>>,
    pub size: usize,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub engine: EngineId,
}

impl ReservationToken {
    /// Construct a new token. Called only by [`MemoryGovernor::try_reserve`].
    pub(crate) fn new(params: ReservationParams) -> Self {
        Self {
            global_counter: params.global_counter,
            database_counter: params.database_counter,
            tenant_counter: params.tenant_counter,
            engine_counter: params.engine_counter,
            size: params.size,
            db: params.db,
            tenant: params.tenant,
            engine: params.engine,
        }
    }

    /// Number of bytes reserved by this token.
    pub fn size(&self) -> usize {
        self.size
    }

    /// The database this reservation is scoped to.
    pub fn database_id(&self) -> DatabaseId {
        self.db
    }

    /// The tenant this reservation is scoped to.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant
    }

    /// The engine this reservation is scoped to.
    pub fn engine(&self) -> EngineId {
        self.engine
    }
}

impl Drop for ReservationToken {
    fn drop(&mut self) {
        let size = self.size;
        if size == 0 {
            return;
        }

        // Release in reverse order: engine → tenant → database → global.
        //
        // Each decrement saturates at zero. A concurrent token on the same
        // engine counter can legitimately drive it below this token's
        // `size` by the time this token drops (e.g. a timeseries flush's
        // token released the memtable footprint while a per-batch token
        // was still in scope). A plain `fetch_sub` would wrap such a
        // counter to ~usize::MAX, which every utilization reader treats
        // as 100 % → permanent Emergency pressure → suspended SPSC reads
        // → schema-register barrier deadlock. Clamping keeps an
        // over-release a harmless zero instead, and the shortfall the
        // clamp would otherwise hide is counted against the matching
        // layer on `self.global_counter.over_release`.
        let identity = ReleaseIdentity {
            db: self.db,
            tenant: self.tenant,
            engine: self.engine,
        };
        let over_release = &self.global_counter.over_release;
        if let Some(ref counter) = self.engine_counter {
            release_layer(counter, size, Layer::Engine, over_release, identity);
        }
        if let Some(ref counter) = self.tenant_counter {
            release_layer(counter, size, Layer::Tenant, over_release, identity);
        }
        if let Some(ref counter) = self.database_counter {
            release_layer(counter, size, Layer::Database, over_release, identity);
        }
        release_layer(
            &self.global_counter.allocated,
            size,
            Layer::Global,
            over_release,
            identity,
        );
    }
}

impl std::fmt::Debug for ReservationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservationToken")
            .field("size", &self.size)
            .field("db", &self.db)
            .field("tenant", &self.tenant)
            .field("engine", &self.engine)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use nodedb_types::{DatabaseId, TenantId};

    use super::{ReservationParams, ReservationToken};
    use crate::engine::EngineId;
    use crate::governor::GlobalCounter;

    fn make_counter(val: usize) -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(val))
    }

    fn make_global(val: usize) -> Arc<GlobalCounter> {
        let global = GlobalCounter::new(1024 * 1024);
        global
            .allocated
            .store(val, std::sync::atomic::Ordering::Relaxed);
        Arc::new(global)
    }

    #[test]
    fn drop_releases_all_four_levels() {
        let global = make_global(100);
        let db_ctr = make_counter(100);
        let tenant_ctr = make_counter(100);
        let engine_ctr = make_counter(100);

        let token = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: Some(Arc::clone(&db_ctr)),
            tenant_counter: Some(Arc::clone(&tenant_ctr)),
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 100,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        });

        assert_eq!(
            global.allocated.load(std::sync::atomic::Ordering::Relaxed),
            100
        );

        drop(token);

        assert_eq!(
            global.allocated.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(db_ctr.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(tenant_ctr.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(engine_ctr.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn drop_with_no_scoped_counters_releases_global() {
        let global = make_global(200);
        let token = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: None,
            size: 200,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Query,
        });
        drop(token);
        assert_eq!(
            global.allocated.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn drop_does_not_underflow_a_counter_released_below_size() {
        // Two live tokens can credit the same engine budget. When one
        // drops first — e.g. a timeseries flush's token releasing the
        // full memtable footprint while a live per-batch token still
        // holds a small reservation — the budget can be driven to zero
        // before the second token drops. The second token's `fetch_sub`
        // on drop must NOT wrap that counter into the multi-exabyte
        // range: a wrapped engine or tenant counter reads as 100%
        // utilization (Emergency) forever, suspends the core's SPSC
        // reads, and deadlocks every subsequent DDL on the
        // schema-register barrier — the exact "healthy /healthz, every
        // query fails" failure mode. Drop must saturate at zero.
        let global = make_global(40);
        let engine_ctr = make_counter(40);
        let tenant_ctr = make_counter(40);

        let token = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: Some(Arc::clone(&tenant_ctr)),
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 40,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Timeseries,
        });

        // A concurrent token's drop drains the engine + global counters
        // past what this token reserved (a flush releasing the full
        // memtable footprint while the small per-batch token is alive).
        engine_ctr.store(0, std::sync::atomic::Ordering::Relaxed);
        global
            .allocated
            .store(0, std::sync::atomic::Ordering::Relaxed);

        drop(token);

        let engine = engine_ctr.load(std::sync::atomic::Ordering::Relaxed);
        let glob = global.allocated.load(std::sync::atomic::Ordering::Relaxed);
        let tenant = tenant_ctr.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            engine, 0,
            "engine counter underflowed to {engine} on token drop — a wrapped \
             counter reads as 100% utilization (Emergency) forever"
        );
        assert_eq!(
            glob, 0,
            "global counter underflowed to {glob} on token drop"
        );
        // The tenant layer was not touched by the concurrent drop, so it
        // returns to zero normally — proving the drop still works where
        // the counter is consistent.
        assert_eq!(tenant, 0, "tenant counter should release normally to 0");
    }

    /// Build a governor whose global ceiling covers every engine's uniform
    /// per-engine limit — the shape the governor's config check demands
    /// (`EngineLimits::total() <= global_ceiling`).
    fn generous_governor(per_engine: usize) -> crate::governor::MemoryGovernor {
        crate::governor::MemoryGovernor::new(crate::governor::GovernorConfig {
            global_ceiling: per_engine * EngineId::ALL.len(),
            engine_limits: crate::engine_limits::EngineLimits::uniform(per_engine),
        })
        .expect("uniform limits always satisfy the global-ceiling check")
    }

    #[test]
    fn second_drop_over_the_same_engine_counter_is_counted() {
        // Two tokens share one engine counter that was only ever credited
        // once (100 bytes) — the shape of a double-release: the first
        // token's drop is the legitimate release, so by the time the
        // second token drops, the bytes it asks to release are already
        // gone. The live release path must count that, not silently clamp
        // it away.
        let global = make_global(100);
        let engine_ctr = make_counter(100);

        let first = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 100,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        });
        let second = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 100,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        });

        drop(first);
        assert_eq!(
            global.over_release.total(),
            0,
            "the first drop is a clean release of the only 100 bytes credited"
        );

        drop(second);
        assert!(
            global.over_release.engine() > 0,
            "the second drop releases 100 bytes the engine counter no \
             longer holds — the live-path over-release the detector exists \
             to catch"
        );
        assert_eq!(
            global.over_release.global(),
            1,
            "the global counter is credited once and released twice, so it \
             over-releases alongside the engine layer"
        );
        assert_eq!(
            global.over_release.total(),
            2,
            "total() sums the layers, and this drop drifts both the engine \
             and the global counter"
        );
        assert_eq!(
            global.over_release.database(),
            0,
            "a token with no database counter cannot drift that layer"
        );
        assert_eq!(global.over_release.tenant(), 0);
    }

    #[test]
    fn clean_reserve_and_drop_leaves_every_over_release_counter_at_zero() {
        let gov = generous_governor(1024);
        let token = gov
            .try_reserve(DatabaseId::DEFAULT, TenantId::new(1), EngineId::Vector, 100)
            .unwrap();
        drop(token);

        assert_eq!(gov.total_over_release_count(), 0);
        assert_eq!(gov.global_over_release_count(), 0);
        assert_eq!(gov.database_over_release_count(), 0);
        assert_eq!(gov.tenant_over_release_count(), 0);
        assert_eq!(gov.engine_over_release_count(), 0);
    }

    #[test]
    fn over_release_still_clamps_the_counter_to_zero_never_wrapping() {
        // The detector counts the event; it must not change the clamp
        // behaviour the counter itself relies on to avoid reading as a
        // wrapped-to-usize::MAX 100% utilization.
        let global = make_global(100);
        let engine_ctr = make_counter(100);

        let first = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 100,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        });
        let second = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: Some(Arc::clone(&engine_ctr)),
            size: 100,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        });
        drop(first);
        drop(second);

        assert_eq!(
            engine_ctr.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "an over-release must saturate at zero, never wrap toward usize::MAX"
        );
    }

    #[test]
    fn zero_size_drop_is_noop() {
        let global = make_global(0);
        let token = ReservationToken::new(ReservationParams {
            global_counter: Arc::clone(&global),
            database_counter: None,
            tenant_counter: None,
            engine_counter: None,
            size: 0,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Query,
        });
        drop(token);
        assert_eq!(
            global.allocated.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }
}
