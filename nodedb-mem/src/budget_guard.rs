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

use crate::engine::EngineId;
use crate::error::{MemError, Result};
use crate::governor::MemoryGovernor;

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
    governor: Arc<MemoryGovernor>,
    engine: EngineId,
    bytes: usize,
}

impl BudgetGuard {
    /// Internal constructor — called only by [`MemoryGovernor::reserve`].
    pub(crate) fn new(governor: Arc<MemoryGovernor>, engine: EngineId, bytes: usize) -> Self {
        Self {
            governor,
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
        self.governor.release(self.engine, self.bytes);
    }
}

impl MemoryGovernor {
    /// Reserve `bytes` for `engine` and return a [`BudgetGuard`] that releases
    /// them on drop.
    ///
    /// # Errors
    ///
    /// Returns [`MemError::BudgetExhausted`] or [`MemError::GlobalCeilingExceeded`]
    /// if the reservation would exceed any configured limit.  Returns
    /// [`MemError::UnknownEngine`] if `engine` is not registered.
    pub fn reserve(self: &Arc<Self>, engine: EngineId, bytes: usize) -> Result<BudgetGuard> {
        let budget = self.budget(engine).ok_or(MemError::UnknownEngine(engine))?;

        // Global ceiling check.
        let total_allocated = self.total_allocated();
        let ceiling = self.global_ceiling();
        if total_allocated + bytes > ceiling {
            return Err(MemError::GlobalCeilingExceeded {
                allocated: total_allocated,
                ceiling,
                requested: bytes,
            });
        }

        // Per-engine check.
        if !budget.try_reserve(bytes) {
            return Err(MemError::BudgetExhausted {
                engine,
                requested: bytes,
                available: budget.available(),
                limit: budget.limit(),
            });
        }

        Ok(BudgetGuard::new(Arc::clone(self), engine, bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

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
