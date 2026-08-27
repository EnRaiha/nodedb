// SPDX-License-Identifier: BUSL-1.1

//! `mirror_lag_transitions`: verifies the lag status machine transitions.
//!
//! - `Following` (lag < 5 s) → `Degraded { lag_ms }` when lag > 5 s
//! - `Degraded` → `Disconnected` when no AppendEntries for 30 s
//! - `Disconnected` → `Following` once entries resume and lag < 5 s
//! - `Disconnected` → `Bootstrapping` when LSN gap requires fresh snapshot

use nodedb::control::mirror::{
    LAG_DEGRADED_MS, LAG_DISCONNECTED_MS, LagTransition, compute_lag_transition,
};
use nodedb_types::MirrorStatus;

use super::helpers::now_ms;

#[test]
fn mirror_lag_transitions_following_to_degraded() {
    let now = now_ms();
    let lag = LAG_DEGRADED_MS + 500;
    let t = compute_lag_transition(&MirrorStatus::Following, lag, now, false);
    assert_eq!(
        t,
        LagTransition::BecomeDegraded { lag_ms: lag },
        "Following with lag > {LAG_DEGRADED_MS} ms must transition to Degraded"
    );
}

#[test]
fn mirror_lag_transitions_following_unchanged_below_threshold() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Following, 100, now, false);
    assert_eq!(
        t,
        LagTransition::Unchanged,
        "Following with lag < {LAG_DEGRADED_MS} ms must be Unchanged"
    );
}

#[test]
fn mirror_lag_transitions_degraded_to_disconnected_after_30s() {
    // last_entry_received > 30 s ago
    let old_received = now_ms().saturating_sub(LAG_DISCONNECTED_MS + 2_000);
    let lag = LAG_DEGRADED_MS + 1;
    let t = compute_lag_transition(
        &MirrorStatus::Degraded { lag_ms: lag },
        lag,
        old_received,
        false,
    );
    assert_eq!(
        t,
        LagTransition::BecomeDisconnected,
        "Degraded with no entry for 30+ s must become Disconnected"
    );
}

#[test]
fn mirror_lag_transitions_degraded_recovers_when_lag_drops() {
    let now = now_ms();
    // lag has dropped below threshold
    let t = compute_lag_transition(
        &MirrorStatus::Degraded { lag_ms: 8_000 },
        50, // well below threshold
        now,
        false,
    );
    assert_eq!(
        t,
        LagTransition::BecomeFollowing,
        "Degraded with lag below threshold must recover to Following"
    );
}

#[test]
fn mirror_lag_transitions_disconnected_to_following_when_lag_ok() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Disconnected, 100, now, false);
    assert_eq!(
        t,
        LagTransition::BecomeFollowing,
        "Disconnected with low lag and no snapshot needed must return to Following"
    );
}

#[test]
fn mirror_lag_transitions_disconnected_to_bootstrapping_on_lsn_gap() {
    let now = now_ms();
    let t = compute_lag_transition(&MirrorStatus::Disconnected, 100, now, true);
    assert_eq!(
        t,
        LagTransition::BecomeBootstrapping {
            bytes_done: 0,
            bytes_total: 0
        },
        "Disconnected with LSN gap must restart bootstrap"
    );
}

#[test]
fn mirror_lag_transitions_exact_threshold_boundary() {
    let now = now_ms();
    // Exactly at the threshold — must NOT degrade.
    let t = compute_lag_transition(&MirrorStatus::Following, LAG_DEGRADED_MS, now, false);
    assert_eq!(
        t,
        LagTransition::Unchanged,
        "Lag exactly at {LAG_DEGRADED_MS} ms must not trigger Degraded (strict >)"
    );

    // One millisecond over — must degrade.
    let lag_over = LAG_DEGRADED_MS + 1;
    let t2 = compute_lag_transition(&MirrorStatus::Following, lag_over, now, false);
    assert_eq!(
        t2,
        LagTransition::BecomeDegraded { lag_ms: lag_over },
        "Lag at {lag_over} ms must trigger Degraded"
    );
}
