// SPDX-License-Identifier: BUSL-1.1

//! Integration test: StartupSequencer phase ordering.
//!
//! Verifies that:
//! - Phases advance only when all gates for that phase have fired.
//! - Registering gates out of order is accepted; the phase each gate belongs to
//!   is determined by the `StartupPhase` passed to `register_gate`.
//! - Firing a later-phase gate before an earlier-phase gate does not advance
//!   past the earlier phase until all earlier gates also fire.
//! - `GatewayEnable` is only reached after all prior phases complete.

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::startup::{StartupGate, StartupPhase, StartupSequencer};

/// Assert that the gate reaches at least `expected`, timing out after 500 ms.
///
/// The current phase may have advanced beyond `expected` by the time we
/// observe it, so we only assert `current_phase() >= expected`.
async fn assert_phase_reaches(gate: &Arc<StartupGate>, expected: StartupPhase) {
    tokio::time::timeout(Duration::from_millis(500), gate.await_phase(expected))
        .await
        .expect("timed out waiting for phase")
        .expect("sequencer failed while waiting for phase");
    assert!(
        gate.current_phase() >= expected,
        "expected phase >= {expected:?}, got {:?}",
        gate.current_phase()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phases_advance_in_order_when_gates_fire() {
    let (seq, gate) = StartupSequencer::new();

    // Register one gate per phase (skipping Boot which is the initial phase).
    let wal_gate = seq.register_gate(StartupPhase::WalRecovery, "wal");
    let catalog_gate = seq.register_gate(StartupPhase::ClusterCatalogOpen, "catalog");
    let raft_gate = seq.register_gate(StartupPhase::RaftMetadataReplay, "raft");
    let schema_gate = seq.register_gate(StartupPhase::SchemaCacheWarmup, "schema");
    let sanity_gate = seq.register_gate(StartupPhase::CatalogSanityCheck, "sanity");
    let data_gate = seq.register_gate(StartupPhase::DataGroupsReplay, "data");
    let transport_gate = seq.register_gate(StartupPhase::TransportBind, "transport");
    let peers_gate = seq.register_gate(StartupPhase::WarmPeers, "peers");
    let health_gate = seq.register_gate(StartupPhase::HealthLoopStart, "health");
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway");

    // Initial phase is Boot.
    assert_eq!(gate.current_phase(), StartupPhase::Boot);

    // Fire gates in strict phase order.
    wal_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::WalRecovery).await;

    catalog_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::ClusterCatalogOpen).await;

    raft_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::RaftMetadataReplay).await;

    schema_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::SchemaCacheWarmup).await;

    sanity_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::CatalogSanityCheck).await;

    data_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::DataGroupsReplay).await;

    transport_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::TransportBind).await;

    peers_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::WarmPeers).await;

    health_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::HealthLoopStart).await;

    gw_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::GatewayEnable).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_phase_gate_fires_first_does_not_advance_past_earlier_phase() {
    let (seq, gate) = StartupSequencer::new();

    let wal_gate = seq.register_gate(StartupPhase::WalRecovery, "wal");
    let gw_gate = seq.register_gate(StartupPhase::GatewayEnable, "gateway");

    // Fire GatewayEnable first — phase must not advance past Boot until WalRecovery fires.
    gw_gate.fire();

    // Wait a bit and confirm we're still at Boot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        gate.current_phase(),
        StartupPhase::Boot,
        "phase advanced past Boot even though WalRecovery gate has not fired"
    );

    // Now fire WalRecovery — phase should advance all the way to GatewayEnable
    // since the GatewayEnable gate already fired.
    wal_gate.fire();
    assert_phase_reaches(&gate, StartupPhase::GatewayEnable).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_gates_for_same_phase_all_must_fire() {
    let (seq, gate) = StartupSequencer::new();

    // Register two gates for the same phase.
    let wal_gate_a = seq.register_gate(StartupPhase::WalRecovery, "wal-primary");
    let wal_gate_b = seq.register_gate(StartupPhase::WalRecovery, "wal-secondary");

    // Fire only the first — phase must not advance yet.
    wal_gate_a.fire();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        gate.current_phase(),
        StartupPhase::Boot,
        "phase advanced after only one of two WalRecovery gates fired"
    );

    // Fire the second — now the phase should advance.
    wal_gate_b.fire();
    assert_phase_reaches(&gate, StartupPhase::WalRecovery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_fire_is_idempotent() {
    let (seq, gate) = StartupSequencer::new();

    let wal_gate = seq.register_gate(StartupPhase::WalRecovery, "wal");

    // Firing the same gate multiple times must not cause errors or double-advance.
    wal_gate.fire();
    wal_gate.fire();
    wal_gate.fire();

    // Firing three times must succeed and advance the phase at least to WalRecovery.
    // With no later gates registered, the sequencer may advance all the way to
    // GatewayEnable — that is expected and correct.
    assert_phase_reaches(&gate, StartupPhase::WalRecovery).await;
}
