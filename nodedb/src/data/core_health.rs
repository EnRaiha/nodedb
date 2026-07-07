// SPDX-License-Identifier: BUSL-1.1

//! Per-core panic-tracking watchdog used by the Data Plane TPC runtime.
//!
//! Isolated from `runtime.rs` so the core event loop stays under the file
//! size limit; this module owns only the panic-window bookkeeping and
//! degraded-mode transition, no eventfd or dispatch logic.

use std::time::Instant;

use tracing::info;

/// Maximum consecutive panics before the core enters degraded mode.
///
/// Degraded mode drains and rejects all incoming requests with
/// `ErrorCode::CoreDegraded` instead of executing them. This prevents
/// a poison-pill request from hot-looping through catch_unwind.
pub(super) const MAX_CONSECUTIVE_PANICS: u32 = 3;

/// Window in which consecutive panics are counted. If more than
/// `MAX_CONSECUTIVE_PANICS` occur within this window, the core degrades.
/// Panics separated by more than this duration reset the counter.
pub(super) const PANIC_WINDOW_SECS: u64 = 60;

/// How long a degraded core stays in degraded mode before attempting
/// recovery. After this cool-down, the core resets its panic counter
/// and resumes accepting requests. If the poison-pill request is still
/// in the queue, it will panic again and re-enter degraded mode — but
/// by then the offending request has been drained and rejected.
const DEGRADED_COOLDOWN_SECS: u64 = 30;

/// Tracks core health across panics for the watchdog.
pub(super) struct CoreHealthWatchdog {
    /// Number of panics in the current window.
    pub(super) consecutive_panics: u32,
    /// Timestamp of the first panic in the current window.
    window_start: Option<Instant>,
    /// Whether this core has been marked degraded.
    degraded: bool,
    /// When the core entered degraded mode (for cool-down recovery).
    degraded_at: Option<Instant>,
}

impl CoreHealthWatchdog {
    pub(super) fn new() -> Self {
        Self {
            consecutive_panics: 0,
            window_start: None,
            degraded: false,
            degraded_at: None,
        }
    }

    /// Record a panic. Returns `true` if the core should enter degraded mode.
    pub(super) fn record_panic(&mut self) -> bool {
        let now = Instant::now();

        // Reset window if the previous panic was outside the window.
        if let Some(start) = self.window_start {
            if now.duration_since(start).as_secs() > PANIC_WINDOW_SECS {
                self.consecutive_panics = 0;
                self.window_start = Some(now);
            }
        } else {
            self.window_start = Some(now);
        }

        self.consecutive_panics += 1;

        if self.consecutive_panics >= MAX_CONSECUTIVE_PANICS {
            self.degraded = true;
            self.degraded_at = Some(Instant::now());
        }

        self.degraded
    }

    /// Record a successful tick (no panic). Resets the consecutive counter.
    pub(super) fn record_success(&mut self) {
        if self.consecutive_panics > 0 {
            self.consecutive_panics = 0;
            self.window_start = None;
        }
    }

    /// Check if the core is degraded. If the cool-down period has elapsed,
    /// auto-recover: reset panic counters and exit degraded mode.
    pub(super) fn is_degraded(&mut self) -> bool {
        if self.degraded
            && let Some(degraded_at) = self.degraded_at
            && degraded_at.elapsed().as_secs() >= DEGRADED_COOLDOWN_SECS
        {
            info!(
                cooldown_secs = DEGRADED_COOLDOWN_SECS,
                "core recovered from degraded mode after cool-down"
            );
            self.degraded = false;
            self.degraded_at = None;
            self.consecutive_panics = 0;
            self.window_start = None;
            return false;
        }
        self.degraded
    }
}
