//! Evaluation context: pre-fetched series plus query timing parameters.

use std::sync::Arc;

use super::super::types::*;

/// Context for evaluation: pre-fetched series + query parameters.
///
/// `series` is shared, so the per-step contexts of a range query or a
/// subquery reuse one copy of the data.
pub struct EvalContext {
    /// All available time series. Each series holds its samples ascending
    /// by timestamp, with no two samples at the same timestamp.
    pub series: Arc<[Series]>,
    /// Evaluation timestamp for instant queries (milliseconds).
    pub timestamp_ms: i64,
    /// Lookback delta: how far back to search for a sample.
    pub lookback_ms: i64,
}

impl Default for EvalContext {
    fn default() -> Self {
        Self {
            series: Arc::from(Vec::new()),
            timestamp_ms: 0,
            lookback_ms: DEFAULT_LOOKBACK_MS,
        }
    }
}
