// SPDX-License-Identifier: BUSL-1.1

//! Periodic driver that ships buffered SIEM events to the configured webhook.
//!
//! Pure Control-Plane async work — an HTTP POST, no storage I/O and no TPC
//! tasks — registered on the same shutdown-aware `spawn_loop` used by the
//! audit-log flush timer, so it is cancelled and joined with every other
//! background loop.
//!
//! The loop is not spawned at all when no destination is configured: an idle
//! timer waking forever for a disabled feature is pure waste.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::delivery::DeliveryOutcome;
use crate::control::state::SharedState;

/// Spawn the SIEM webhook delivery loop.
///
/// Delivery cadence is `flush_interval_secs`. A failed attempt keeps the
/// batch buffered and doubles the wait up to `max_backoff_secs`, so a webhook
/// outage neither spins nor loses events; the first success resets the wait.
pub fn spawn_export_loop(shared: &Arc<SharedState>) {
    if !shared.siem.is_configured() {
        return;
    }

    if !shared.siem.has_webhook() {
        // Destinations are configured but no webhook URL: the buffers fill for
        // CDC drain only, and overflow shows up in the drop counters. Say so
        // once at startup rather than letting events pile up unexplained.
        warn!(
            "SIEM destinations configured without webhook_url — events buffer \
             for CDC drain only and are dropped once the buffer fills"
        );
        return;
    }

    let base = Duration::from_secs(shared.siem.flush_interval_secs().max(1));
    let max_backoff = Duration::from_secs(shared.siem.max_backoff_secs().max(1)).max(base);
    let shared_siem = Arc::clone(shared);

    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "siem_export",
        move |mut shutdown| async move {
            let mut delay = base;
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                match shared_siem.siem.flush_webhook().await {
                    DeliveryOutcome::Idle | DeliveryOutcome::Delivered(_) => delay = base,
                    DeliveryOutcome::Failed(pending) => {
                        delay = delay.saturating_mul(2).min(max_backoff).max(base);
                        warn!(
                            pending,
                            retry_in_secs = delay.as_secs(),
                            buffered = shared_siem.siem.buffered_count(),
                            "SIEM webhook delivery failed; batch requeued for retry"
                        );
                    }
                }
            }
        },
    );

    info!(
        interval_secs = base.as_secs(),
        max_backoff_secs = max_backoff.as_secs(),
        "SIEM export loop running"
    );
}
