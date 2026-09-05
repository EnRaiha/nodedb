// SPDX-License-Identifier: Apache-2.0

//! [`MemoryGovernor`] struct definition, construction, and basic accessors.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_types::{DatabaseId, TenantId};

use super::config::GovernorConfig;
use super::global_counter::GlobalCounter;
use crate::budget::Budget;
use crate::engine::EngineId;
use crate::error::Result;
use crate::pressure::PressureThresholds;
use crate::scoped_budget::ScopedBudget;

/// The central memory governor.
///
/// Thread-safe: global, database, and tenant counters use atomics.
/// The budget map itself is behind an `RwLock`; reads (common) take a shared
/// lock, writes (rare — only when quotas change) take an exclusive lock.
#[derive(Debug)]
pub struct MemoryGovernor {
    /// Per-engine budgets, one for every `EngineId`, indexed by `EngineId::index()`.
    pub(super) budgets: [Budget; EngineId::COUNT],

    /// Shared global counter. Held by both the governor and every live token.
    pub(crate) global_counter: Arc<GlobalCounter>,

    /// Global ceiling in bytes.
    pub(super) global_ceiling: usize,

    /// Pressure thresholds for graduated backpressure.
    pub(super) thresholds: PressureThresholds,

    /// Per-database budget map. Keyed by `DatabaseId`. Populated lazily via
    /// `set_database_budget`; databases without an entry are uncapped.
    pub(super) database_budgets: RwLock<HashMap<DatabaseId, ScopedBudget>>,

    /// Per-tenant budget map. Keyed by `(DatabaseId, TenantId)`. Populated
    /// lazily via `set_tenant_budget`.
    pub(super) tenant_budgets: RwLock<HashMap<(DatabaseId, TenantId), ScopedBudget>>,
}

impl MemoryGovernor {
    /// Create a new governor with the given configuration.
    pub fn new(config: GovernorConfig) -> Result<Self> {
        config.validate()?;

        let limits = config.engine_limits.as_array();
        let budgets: [Budget; EngineId::COUNT] = std::array::from_fn(|i| Budget::new(limits[i]));

        let global_counter = Arc::new(GlobalCounter::new(config.global_ceiling));

        Ok(Self {
            budgets,
            global_counter,
            global_ceiling: config.global_ceiling,
            thresholds: PressureThresholds::default(),
            database_budgets: RwLock::new(HashMap::new()),
            tenant_budgets: RwLock::new(HashMap::new()),
        })
    }

    /// Get the budget for a specific engine.
    pub fn budget(&self, engine: EngineId) -> &Budget {
        &self.budgets[engine.index()]
    }

    /// Get the global ceiling.
    pub fn global_ceiling(&self) -> usize {
        self.global_ceiling
    }
}
