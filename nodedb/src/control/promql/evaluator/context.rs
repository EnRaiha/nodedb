//! Evaluation context: pre-fetched series plus query timing parameters.

use super::super::types::*;

/// Context for evaluation: pre-fetched series + query parameters.
pub struct EvalContext {
    /// All available time series (pre-fetched from storage).
    pub series: Vec<Series>,
    /// Evaluation timestamp for instant queries (milliseconds).
    pub timestamp_ms: i64,
    /// Lookback delta: how far back to search for a sample.
    pub lookback_ms: i64,
}

impl Default for EvalContext {
    fn default() -> Self {
        Self {
            series: vec![],
            timestamp_ms: 0,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        }
    }
}
