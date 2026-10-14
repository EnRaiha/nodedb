// SPDX-License-Identifier: BUSL-1.1

//! Per-aggregate watermark tracking.
//!
//! The watermark is the highest source timestamp folded into the aggregate.
//! It only moves forward. A late row below it needs no recompute: every
//! partial part merges without loss, so folding the partition that holds
//! the late row gives the same buckets as one pass over all rows.

use serde::{Deserialize, Serialize};

/// Per-aggregate watermark state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    /// Highest source timestamp folded into the aggregate.
    pub watermark_ts: i64,
    /// Total rows folded into the aggregate.
    pub rows_aggregated: u64,
    /// Wall-clock time of the last fold.
    pub last_refresh_ms: i64,
}

impl Default for WatermarkState {
    fn default() -> Self {
        Self {
            watermark_ts: i64::MIN,
            rows_aggregated: 0,
            last_refresh_ms: 0,
        }
    }
}

impl WatermarkState {
    /// Record one fold of `rows` rows whose highest timestamp is `max_ts`.
    pub fn advance(&mut self, max_ts: i64, rows: u64, now_ms: i64) {
        if max_ts > self.watermark_ts {
            self.watermark_ts = max_ts;
        }
        self.rows_aggregated += rows;
        self.last_refresh_ms = now_ms;
    }
}
