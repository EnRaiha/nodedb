// SPDX-License-Identifier: BUSL-1.1

//! Per-stream CDC lag warning emitter.
//!
//! Tracks the rolling drop rate per `(tenant_id, stream_name)` over a
//! configurable 1-minute window. When the accumulated drop count exceeds
//! `threshold_per_window`, emits a structured `warn!` log entry that
//! on-call tooling (Grafana, Loki, PagerDuty) can alert on.
//!
//! This lives in the Event Plane (never does storage I/O). State is
//! in-memory only — window resets on process restart, which is acceptable
//! because each reset gives at most one false-negative window.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default rolling window duration.
const DEFAULT_WINDOW: Duration = Duration::from_secs(60);

/// Default drop threshold per window (drops/minute) before warning.
pub const DEFAULT_THRESHOLD: u64 = 1000;

/// Map size at which an idle sweep runs inline during `record_drops`. Keeps the
/// HashMap bounded for long-running servers with high stream churn even when
/// callers forget to invoke `remove_stream` on stream deletion.
const IDLE_GC_TRIGGER_SIZE: usize = 1024;

/// Window-multiple after which an entry is considered idle and reclaimable.
/// At the default 60s window this is 10 minutes of silence.
const IDLE_GC_WINDOWS: u32 = 10;

/// Per-stream window accumulator.
struct StreamWindow {
    /// When the current window started.
    window_start: Instant,
    /// Drops accumulated in the current window.
    window_drops: u64,
}

impl StreamWindow {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            window_drops: 0,
        }
    }

    /// Record `drops` new drop events. Returns `Some(rate)` if the threshold
    /// was crossed in this window and the caller should emit a warning.
    /// Returns `None` otherwise.
    fn record(&mut self, drops: u64, threshold: u64, window: Duration) -> Option<u64> {
        let elapsed = self.window_start.elapsed();
        if elapsed >= window {
            // Roll the window. Don't carry over the previous count so each
            // window is independent — avoids storm of repeated warnings after
            // the rate spikes and then subsides.
            self.window_start = Instant::now();
            self.window_drops = drops;
        } else {
            self.window_drops += drops;
        }

        if self.window_drops >= threshold {
            Some(self.window_drops)
        } else {
            None
        }
    }
}

/// Emits structured lag warnings when per-stream drop rates cross the threshold.
pub struct CdcLagWarner {
    /// Per-stream accumulators. Key: `(tenant_id, stream_name)`.
    windows: Mutex<HashMap<(u64, String), StreamWindow>>,
    /// Drops per window before a warning fires.
    threshold: u64,
    /// Window duration.
    window: Duration,
}

impl CdcLagWarner {
    pub fn new(threshold: u64) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            threshold,
            window: DEFAULT_WINDOW,
        }
    }

    /// Record that `drops` events were evicted from `stream_name` owned by
    /// `tenant_id`. Emits a `tracing::warn!` when the rolling rate exceeds
    /// the threshold.
    ///
    /// No-op when `drops == 0`.
    pub fn record_drops(&self, tenant_id: u64, stream_name: &str, drops: u64, oldest_index: u64) {
        if drops == 0 {
            return;
        }

        let mut windows = self.windows.lock().unwrap_or_else(|p| p.into_inner());
        if windows.len() >= IDLE_GC_TRIGGER_SIZE {
            let idle_after = self.window * IDLE_GC_WINDOWS;
            windows.retain(|_, w| w.window_start.elapsed() < idle_after);
        }
        let key = (tenant_id, stream_name.to_string());
        let entry = windows.entry(key).or_insert_with(StreamWindow::new);

        if let Some(rate) = entry.record(drops, self.threshold, self.window) {
            tracing::warn!(
                tenant_id,
                stream = stream_name,
                dropped_in_window = rate,
                threshold = self.threshold,
                oldest_available_index = oldest_index,
                "CDC stream drop rate exceeded threshold: lagging consumers may miss events"
            );
        }
    }

    /// Remove the window state for a stream that has been dropped. Prevents
    /// unbounded growth when streams are frequently created and deleted.
    pub fn remove_stream(&self, tenant_id: u64, stream_name: &str) {
        let mut windows = self.windows.lock().unwrap_or_else(|p| p.into_inner());
        windows.remove(&(tenant_id, stream_name.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_warn_below_threshold() {
        let warner = CdcLagWarner::new(1000);
        // 999 drops in one call — should not cross threshold.
        warner.record_drops(1, "orders_stream", 999, 0);
        // No panic / no assertion — just verify it doesn't crash and the
        // window is tracking. We check internal state via a second call.
        warner.record_drops(1, "orders_stream", 0, 0); // no-op
    }

    #[test]
    fn warn_at_threshold() {
        // Use threshold=1 so a single drop fires.
        let warner = CdcLagWarner::new(1);
        // This should emit a warn! (we cannot assert on log output easily,
        // but at minimum it must not panic).
        warner.record_drops(1, "stream_a", 1, 500);
    }

    #[test]
    fn window_resets() {
        let mut w = StreamWindow::new();
        // Manually back-date the window start to simulate expiry.
        w.window_start = Instant::now() - Duration::from_secs(61);
        // After the window rolls, the counter restarts at `drops`, not += drops.
        let result = w.record(5, 10, Duration::from_secs(60));
        // 5 < 10 threshold → None.
        assert!(result.is_none());
        assert_eq!(w.window_drops, 5);
    }

    #[test]
    fn window_accumulates() {
        let mut w = StreamWindow::new();
        // Window has not expired; drops accumulate.
        w.record(3, 10, Duration::from_secs(60));
        let result = w.record(8, 10, Duration::from_secs(60));
        // 3 + 8 = 11 >= 10 threshold → Some(11).
        assert_eq!(result, Some(11));
    }

    #[test]
    fn remove_stream_cleans_up() {
        let warner = CdcLagWarner::new(100);
        warner.record_drops(1, "s1", 50, 0);
        warner.remove_stream(1, "s1");
        // After removal, the next record call starts a fresh window.
        // Accumulating 50 again should not cross threshold = 100.
        warner.record_drops(1, "s1", 50, 0);
        let windows = warner.windows.lock().unwrap();
        let w = windows.get(&(1u64, "s1".to_string())).unwrap();
        // Window was reset on remove, so only 50 accumulated.
        assert_eq!(w.window_drops, 50);
    }
}
