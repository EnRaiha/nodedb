// SPDX-License-Identifier: BUSL-1.1

//! Rate limiting for the warnings a withheld CDC event raises.
//!
//! Both reasons a delivery is withheld are properties of the STREAM, not of the
//! individual event: a subscription that captured no roles stays roleless, and a
//! payload shape a rule cannot reach repeats for every row written the same way.
//! Warning per event would therefore emit one line per row for as long as the
//! condition holds, burying the very message an operator needs to act on.
//!
//! A subscriber scope lives only for one delivery cycle, so per-scope
//! deduplication would still warn once per batch. The window is kept here
//! instead, keyed by what an operator would act on — the tenant, the collection,
//! and the reason — mirroring the rolling-window accounting `lag_warner` uses
//! for the other repeating CDC condition.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long one `(tenant, collection, reason)` stays silent after warning.
const WINDOW: Duration = Duration::from_secs(60);

/// Map size at which an idle sweep runs inline. Keeps the map bounded on a
/// long-running server whose tenants and collections churn.
const IDLE_GC_TRIGGER_SIZE: usize = 1024;

/// Multiple of [`WINDOW`] after which an entry is reclaimable.
const IDLE_GC_WINDOWS: u32 = 10;

/// Why an event was withheld from a subscriber.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum WithheldReason {
    /// The subscription captured no roles, so no policy keyed on one can be
    /// evaluated for it.
    UnprovableEntitlement,
    /// A rule covers the collection but the payload is not a stored row map.
    UnreadablePayload,
}

/// When each `(tenant, collection, reason)` last warned.
type Windows = HashMap<(u64, String, WithheldReason), Instant>;

fn windows() -> &'static Mutex<Windows> {
    static WINDOWS: OnceLock<Mutex<Windows>> = OnceLock::new();
    WINDOWS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Whether this condition may warn now, marking the window as spent if so.
pub(super) fn should_warn(tenant_id: u64, collection: &str, reason: WithheldReason) -> bool {
    let mut windows = windows().lock().unwrap_or_else(|p| p.into_inner());
    let now = Instant::now();

    if windows.len() >= IDLE_GC_TRIGGER_SIZE {
        let idle_after = WINDOW * IDLE_GC_WINDOWS;
        windows.retain(|_, last| now.duration_since(*last) < idle_after);
    }

    let key = (tenant_id, collection.to_string(), reason);
    match windows.get(&key) {
        Some(last) if now.duration_since(*last) < WINDOW => false,
        _ => {
            windows.insert(key, now);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A distinct tenant per test keeps the process-wide window independent of
    /// what the other tests recorded.
    #[test]
    fn first_warn_passes_and_the_repeat_is_silenced() {
        assert!(should_warn(
            9_001,
            "users",
            WithheldReason::UnprovableEntitlement
        ));
        assert!(!should_warn(
            9_001,
            "users",
            WithheldReason::UnprovableEntitlement
        ));
    }

    /// The window is per collection and per reason: neither an event on another
    /// collection nor a different failure may be swallowed by the first.
    #[test]
    fn collections_and_reasons_hold_independent_windows() {
        assert!(should_warn(
            9_002,
            "users",
            WithheldReason::UnprovableEntitlement
        ));
        assert!(should_warn(
            9_002,
            "orders",
            WithheldReason::UnprovableEntitlement
        ));
        assert!(should_warn(
            9_002,
            "users",
            WithheldReason::UnreadablePayload
        ));
    }

    /// An expired window warns again, so a condition that persists for hours
    /// keeps reporting itself instead of falling silent after one line.
    #[test]
    fn an_expired_window_warns_again() {
        assert!(should_warn(
            9_003,
            "users",
            WithheldReason::UnprovableEntitlement
        ));
        {
            let mut windows = windows().lock().unwrap_or_else(|p| p.into_inner());
            let key = (
                9_003,
                "users".to_string(),
                WithheldReason::UnprovableEntitlement,
            );
            let expired = Instant::now() - WINDOW - Duration::from_secs(1);
            windows.insert(key, expired);
        }
        assert!(should_warn(
            9_003,
            "users",
            WithheldReason::UnprovableEntitlement
        ));
    }
}
