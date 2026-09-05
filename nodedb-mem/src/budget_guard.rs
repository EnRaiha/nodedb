// SPDX-License-Identifier: Apache-2.0

//! RAII budget guard for the memory governor.
//!
//! [`BudgetGuard`] acquires a byte reservation from the [`MemoryGovernor`]
//! on construction and releases it automatically when dropped.  This prevents
//! budget leaks if the caller returns early or propagates an error between
//! reserving and freeing memory.
//!
//! # Usage
//!
//! ```ignore
//! let _g = governor.reserve(EngineId::Vector, n * size_of::<f32>())?;
//! let v: Vec<f32> = Vec::with_capacity(n); // budget already reserved
//! // _g dropped at end of scope → bytes returned to engine budget
//! ```
//!
//! # `mem::forget` note
//!
//! If a `BudgetGuard` is forgotten via [`std::mem::forget`] the reservation
//! is never released.  This is intentional: the guard owns accounting for
//! bytes that a live allocation is using.  Callers must not forget guards
//! that are the sole record of outstanding reservations.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::budget::atomic_saturating_sub;
use crate::engine::EngineId;
use crate::error::{MemError, Result};
use crate::governor::{GlobalCounter, MemoryGovernor};
use crate::reserve_scope::ReserveScope;

/// RAII guard that holds a byte reservation from the [`MemoryGovernor`].
///
/// Dropping the guard releases the reserved bytes back to the engine budget.
/// The guard is `!Send` by default because it is normally used on Data-Plane
/// TPC cores (`!Send` enforced by the executor).  If you genuinely need to
/// move a guard across threads (e.g. from a background compaction task) you
/// can wrap it in an explicit `Arc<Mutex<...>>` — but that pattern is rare
/// and typically wrong on the Data Plane.
#[must_use = "dropping a BudgetGuard immediately releases the reservation; bind it to a variable"]
#[derive(Debug)]
pub struct BudgetGuard {
    global_counter: Arc<GlobalCounter>,
    engine_counter: Arc<AtomicUsize>,
    engine: EngineId,
    bytes: usize,
}

impl BudgetGuard {
    /// Internal constructor — called only by [`MemoryGovernor::reserve`] with
    /// the counters a committed [`ReserveScope`] credited.
    pub(crate) fn new(
        global_counter: Arc<GlobalCounter>,
        engine_counter: Arc<AtomicUsize>,
        engine: EngineId,
        bytes: usize,
    ) -> Self {
        Self {
            global_counter,
            engine_counter,
            engine,
            bytes,
        }
    }

    /// The engine this guard is accounting against.
    pub fn engine(&self) -> EngineId {
        self.engine
    }

    /// The number of bytes reserved by this guard.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        atomic_saturating_sub(&self.engine_counter, self.bytes);
        atomic_saturating_sub(&self.global_counter.allocated, self.bytes);
    }
}

impl MemoryGovernor {
    /// Reserve `bytes` for `engine` and return a [`BudgetGuard`] that releases
    /// them on drop.
    ///
    /// Credits the global counter and the engine budget only — the same two
    /// layers the guard releases on drop, so credit and release stay
    /// symmetric. Callers that also need the database/tenant layers must use
    /// [`MemoryGovernor::try_reserve`] and hold a `ReservationToken`.
    ///
    /// # Errors
    ///
    /// Returns [`MemError::BudgetExhausted`] or [`MemError::GlobalCeilingExceeded`]
    /// if the reservation would exceed any configured limit.  Returns
    /// [`MemError::UnknownEngine`] if `engine` is not registered.
    pub fn reserve(self: &Arc<Self>, engine: EngineId, bytes: usize) -> Result<BudgetGuard> {
        let budget = self.budget(engine).ok_or(MemError::UnknownEngine(engine))?;

        let mut scope = ReserveScope::new(Arc::clone(&self.global_counter), bytes);
        scope.try_credit_global()?;

        let engine_counter = match budget.try_reserve_arc(bytes) {
            Some(arc) => arc,
            None => {
                return Err(MemError::BudgetExhausted {
                    engine,
                    requested: bytes,
                    available: budget.available(),
                    limit: budget.limit(),
                });
            }
        };
        scope.credit_engine(Arc::clone(&engine_counter));

        let layers = scope.commit();
        Ok(BudgetGuard::new(
            layers.global,
            engine_counter,
            engine,
            bytes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::error::MemError;
    use crate::governor::GovernorConfig;

    fn make_governor(limits: &[(EngineId, usize)], ceiling: usize) -> Arc<MemoryGovernor> {
        let engine_limits: HashMap<EngineId, usize> = limits.iter().copied().collect();
        Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: ceiling,
                engine_limits,
            })
            .expect("valid config"),
        )
    }

    #[test]
    fn guard_reserve_and_drop_move_the_global_counter_symmetrically() {
        let gov = make_governor(&[(EngineId::Vector, 4096)], 8192);

        {
            let _guard = gov.reserve(EngineId::Vector, 1000).expect("within budget");
            assert_eq!(
                gov.global_counter.allocated.load(Ordering::Relaxed),
                1000,
                "a guard reservation must increment the global counter it releases on drop"
            );
        }

        assert_eq!(
            gov.global_counter.allocated.load(Ordering::Relaxed),
            0,
            "dropping the guard must return the global counter to zero"
        );
    }

    #[test]
    fn reserve_within_budget_releases_on_drop() {
        let gov = make_governor(&[(EngineId::Vector, 4096)], 8192);

        {
            let guard = gov.reserve(EngineId::Vector, 1000).expect("within budget");
            assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
            assert_eq!(guard.bytes(), 1000);
            assert_eq!(guard.engine(), EngineId::Vector);
            // guard dropped here
        }

        assert_eq!(
            gov.budget(EngineId::Vector).unwrap().allocated(),
            0,
            "bytes must be returned on drop"
        );
    }

    #[test]
    fn reserve_over_budget_returns_err() {
        let gov = make_governor(&[(EngineId::Fts, 512)], 1024);

        let err = gov.reserve(EngineId::Fts, 1000).unwrap_err();
        assert!(
            matches!(err, MemError::BudgetExhausted { .. }),
            "expected BudgetExhausted, got {err:?}"
        );
        // No bytes charged.
        assert_eq!(gov.budget(EngineId::Fts).unwrap().allocated(), 0);
    }

    #[test]
    fn multiple_guards_accumulate_and_release_independently() {
        let gov = make_governor(
            &[
                (EngineId::Vector, 4096),
                (EngineId::Columnar, 4096),
                (EngineId::Graph, 4096),
            ],
            16384,
        );

        let g1 = gov.reserve(EngineId::Vector, 1000).unwrap();
        let g2 = gov.reserve(EngineId::Columnar, 2000).unwrap();
        let g3 = gov.reserve(EngineId::Graph, 3000).unwrap();

        assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
        assert_eq!(gov.budget(EngineId::Columnar).unwrap().allocated(), 2000);
        assert_eq!(gov.budget(EngineId::Graph).unwrap().allocated(), 3000);

        drop(g2); // release only Columnar
        assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 1000);
        assert_eq!(gov.budget(EngineId::Columnar).unwrap().allocated(), 0);
        assert_eq!(gov.budget(EngineId::Graph).unwrap().allocated(), 3000);

        drop(g1);
        drop(g3);
        assert_eq!(gov.budget(EngineId::Vector).unwrap().allocated(), 0);
        assert_eq!(gov.budget(EngineId::Graph).unwrap().allocated(), 0);
    }

    /// Demonstrates that `mem::forget` prevents the release.
    /// This is documented behaviour — callers must not forget guards.
    #[test]
    fn mem_forget_does_not_release() {
        let gov = make_governor(&[(EngineId::Kv, 4096)], 8192);

        let guard = gov.reserve(EngineId::Kv, 500).unwrap();
        assert_eq!(gov.budget(EngineId::Kv).unwrap().allocated(), 500);

        std::mem::forget(guard);

        // Bytes are NOT released — accounting drift matches the allocation.
        assert_eq!(
            gov.budget(EngineId::Kv).unwrap().allocated(),
            500,
            "mem::forget intentionally skips drop; bytes remain charged"
        );
    }

    #[test]
    fn reserve_zero_bytes_is_allowed() {
        let gov = make_governor(&[(EngineId::Query, 1024)], 2048);
        let guard = gov
            .reserve(EngineId::Query, 0)
            .expect("zero bytes always fits");
        assert_eq!(guard.bytes(), 0);
        drop(guard);
        assert_eq!(gov.budget(EngineId::Query).unwrap().allocated(), 0);
    }

    #[test]
    fn second_reserve_after_drop_succeeds() {
        let gov = make_governor(&[(EngineId::Timeseries, 1024)], 2048);

        {
            let _g = gov.reserve(EngineId::Timeseries, 1024).unwrap();
            // Budget fully consumed — a second reserve must fail.
            assert!(gov.reserve(EngineId::Timeseries, 1).is_err());
        } // _g dropped → budget freed

        // Now the same reservation must succeed again.
        let _g2 = gov
            .reserve(EngineId::Timeseries, 1024)
            .expect("budget freed by previous guard drop");
    }
}
