// SPDX-License-Identifier: Apache-2.0

//! Background maintenance tuning — auto-ANALYZE triggering.
//!
//! Covers the Control-Plane maintenance work that runs off the write path.
//! Per-database CPU budgets come from the quota record, not from here.

use serde::{Deserialize, Serialize};

fn default_auto_analyze_min_mutations() -> u64 {
    // A collection re-analyzes once mutations reach 10% of its last row
    // count. This floor keeps a small collection from re-scanning on a
    // handful of writes.
    1_000
}

/// Tuning knobs for background maintenance triggered by user writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceTuning {
    /// Smallest mutation count that can trigger an automatic ANALYZE.
    ///
    /// The trigger fires at `max(last_row_count / 10, this)`, so lowering it
    /// makes a small collection refresh its statistics sooner, and raising it
    /// trades planner accuracy for fewer background scans.
    #[serde(default = "default_auto_analyze_min_mutations")]
    pub auto_analyze_min_mutations: u64,
}

impl Default for MaintenanceTuning {
    fn default() -> Self {
        Self {
            auto_analyze_min_mutations: default_auto_analyze_min_mutations(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_floor_is_one_thousand() {
        assert_eq!(
            MaintenanceTuning::default().auto_analyze_min_mutations,
            1000
        );
    }

    #[test]
    fn override_via_toml() {
        let parsed: MaintenanceTuning =
            toml::from_str("auto_analyze_min_mutations = 20").expect("deserialize");
        assert_eq!(parsed.auto_analyze_min_mutations, 20);
    }

    #[test]
    fn empty_table_keeps_the_default() {
        let parsed: MaintenanceTuning = toml::from_str("").expect("deserialize");
        assert_eq!(parsed.auto_analyze_min_mutations, 1000);
    }
}
