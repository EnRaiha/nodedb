// SPDX-License-Identifier: Apache-2.0

//! PromQL tuning — limits and defaults for the `/obsv/api/v1/*` read
//! endpoints.

use serde::{Deserialize, Serialize};

fn default_max_samples() -> usize {
    5_000_000
}

fn default_lookback_delta_ms() -> i64 {
    // Prometheus' default lookback delta: 5 minutes.
    300_000
}

fn default_max_points_per_series() -> usize {
    // Prometheus' range-query resolution cap.
    11_000
}

/// Tuning knobs for PromQL reads over stored timeseries data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromqlTuning {
    /// Stored rows one PromQL request can load. A request that needs more
    /// fails with an error. It never returns a truncated result.
    #[serde(default = "default_max_samples")]
    pub max_samples: usize,
    /// How far back an instant selector looks for a sample, in milliseconds.
    #[serde(default = "default_lookback_delta_ms")]
    pub lookback_delta_ms: i64,
    /// Points one series of a range query can hold. A finer step fails.
    #[serde(default = "default_max_points_per_series")]
    pub max_points_per_series: usize,
}

impl Default for PromqlTuning {
    fn default() -> Self {
        Self {
            max_samples: default_max_samples(),
            lookback_delta_ms: default_lookback_delta_ms(),
            max_points_per_series: default_max_points_per_series(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_prometheus_where_it_has_one() {
        let t = PromqlTuning::default();
        assert_eq!(t.max_samples, 5_000_000);
        assert_eq!(t.lookback_delta_ms, 300_000);
        assert_eq!(t.max_points_per_series, 11_000);
    }

    #[test]
    fn override_max_samples() {
        let t: PromqlTuning = toml::from_str("max_samples = 2").unwrap();
        assert_eq!(t.max_samples, 2);
        assert_eq!(t.lookback_delta_ms, 300_000);
    }
}
