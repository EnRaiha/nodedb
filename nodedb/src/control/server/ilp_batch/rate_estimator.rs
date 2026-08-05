// SPDX-License-Identifier: BUSL-1.1

//! EWMA-based rate estimator for adaptive ILP batch sizing.

pub(crate) struct IlpRateEstimator {
    /// Smoothed rate in lines/second.
    rate: f64,
    /// EWMA smoothing factor (0.2 = responsive to recent changes).
    alpha: f64,
    /// Last measurement timestamp.
    last_ts: std::time::Instant,
}

impl IlpRateEstimator {
    pub(crate) fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.2,
            last_ts: std::time::Instant::now(),
        }
    }

    /// Record that `lines` were flushed since the last call.
    pub(crate) fn record(&mut self, lines: u64) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_ts).as_secs_f64();
        self.last_ts = now;

        if elapsed > 0.0 {
            let instant_rate = lines as f64 / elapsed;
            if self.rate == 0.0 {
                self.rate = instant_rate;
            } else {
                self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            }
        }
    }

    /// Suggest (batch_size, window_ms) based on current rate.
    pub(crate) fn suggest_batch_params(&self) -> (u64, u64) {
        if self.rate > 100_000.0 {
            // High rate: large batches, short window.
            (10_000, 10)
        } else if self.rate > 1_000.0 {
            // Medium rate: moderate batches.
            (1_000, 50)
        } else {
            // Low rate: small batches, long window.
            (100, 100)
        }
    }
}
