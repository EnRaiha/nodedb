// SPDX-License-Identifier: Apache-2.0

//! [`GovernorConfig`], the input to [`MemoryGovernor::new`](super::core::MemoryGovernor::new).

use crate::engine_limits::EngineLimits;
use crate::error::{MemError, Result};

/// Configuration for the memory governor.
#[derive(Debug, Clone)]
pub struct GovernorConfig {
    /// Global memory ceiling in bytes. The sum of all engine budgets
    /// must not exceed this.
    pub global_ceiling: usize,

    /// Per-engine budget limits, one entry for every `EngineId`.
    pub engine_limits: EngineLimits,
}

impl GovernorConfig {
    /// Validate that the sum of engine limits does not exceed the global ceiling.
    pub fn validate(&self) -> Result<()> {
        let total = self.engine_limits.total();
        if total > self.global_ceiling {
            return Err(MemError::GlobalCeilingExceeded {
                allocated: total,
                ceiling: self.global_ceiling,
                requested: 0,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::governor::core::MemoryGovernor;
    use crate::governor::test_support::test_config;

    #[test]
    fn invalid_config_rejected() {
        let mut config = test_config();
        config.global_ceiling = 100;
        assert!(MemoryGovernor::new(config).is_err());
    }
}
