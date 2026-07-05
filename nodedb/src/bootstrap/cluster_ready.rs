// SPDX-License-Identifier: BUSL-1.1

//! Cluster readiness gate: raft election wait, catalog sanity check, peer warm-up.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::bootstrap::schema_rehydrate::rehydrate_schema_registry;
use crate::control::startup::ReadyGate;
use crate::control::state::SharedState;

/// All readiness gates passed to [`await_cluster_ready`].
pub struct ClusterReadyGates {
    pub raft_gate: ReadyGate,
    pub schema_gate: ReadyGate,
    pub sanity_gate: ReadyGate,
    pub data_groups_gate: ReadyGate,
    pub transport_gate: ReadyGate,
    pub warm_peers_gate: ReadyGate,
    pub health_loop_gate: ReadyGate,
    pub gateway_enable_gate: ReadyGate,
}

/// Wait for the metadata raft group to be ready, run catalog sanity checks,
/// warm the QUIC peer cache, and fire the remaining startup gates.
///
/// In single-node mode `raft_ready_rx` is `None` and the raft-ready wait is
/// skipped. Gate fires are always performed regardless of cluster mode.
pub async fn await_cluster_ready(
    shared: &Arc<SharedState>,
    raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>>,
    gates: ClusterReadyGates,
) -> anyhow::Result<()> {
    let ClusterReadyGates {
        raft_gate,
        schema_gate,
        sanity_gate,
        data_groups_gate,
        transport_gate,
        warm_peers_gate,
        health_loop_gate,
        gateway_enable_gate,
    } = gates;
    // Boot-time readiness gate: in cluster mode, wait until the
    // metadata raft group has applied its first entry on this node
    // before opening any client-facing listener. This eliminates the
    // restart-window race where the first DDL would observe
    // `metadata propose: not leader` because election had not yet
    // completed.
    if let Some(mut ready_rx) = raft_ready_rx {
        const RAFT_READY_TIMEOUT: Duration = Duration::from_secs(30);
        match tokio::time::timeout(RAFT_READY_TIMEOUT, ready_rx.wait_for(|v| *v)).await {
            Ok(Ok(_)) => {
                info!("metadata raft group ready — opening client listeners");
            }
            Ok(Err(_)) => {
                raft_gate.fail("raft readiness watch dropped before signalling ready");
                return Err(anyhow::anyhow!(
                    "raft readiness watch dropped before signalling ready"
                ));
            }
            Err(_) => {
                raft_gate.fail(format!(
                    "raft readiness timeout after {RAFT_READY_TIMEOUT:?}"
                ));
                return Err(anyhow::anyhow!(
                    "raft readiness timeout after {RAFT_READY_TIMEOUT:?} — \
                     metadata group failed to apply first entry"
                ));
            }
        }
    }
    // Metadata raft group has applied its first entry (or we're
    // in single-node mode with no raft).
    raft_gate.fire();

    // Authoritatively rehydrate the Data Plane per-core schema registry
    // from the durable catalog, in both single-node and cluster mode.
    // This is NOT a raft-replay side effect: it enumerates every active
    // stored collection and re-registers it directly, awaited and
    // fail-closed, so no client listener can open against a collection
    // whose schema (including strict-mode `StrictSchema`) hasn't been
    // re-registered to every Data Plane core after a restart.
    if let Err(e) = rehydrate_schema_registry(shared).await {
        schema_gate.fail(format!("schema registry rehydration failed: {e}"));
        return Err(anyhow::anyhow!("schema registry rehydration failed: {e}"));
    }
    schema_gate.fire();

    // Catalog sanity check: applied-index gate, redb
    // cross-table integrity, and in-memory registry ⇔ redb
    // verification. Any unrepairable divergence or any redb
    // integrity violation aborts startup.
    let verify_report = crate::control::cluster::verify_and_repair(shared).await?;
    if verify_report.is_acceptable() {
        info!(report = %verify_report, "catalog sanity check passed");
    } else {
        sanity_gate.fail(format!("catalog sanity check failed: {verify_report}"));
        return Err(anyhow::anyhow!(
            "catalog sanity check failed: {verify_report}"
        ));
    }
    sanity_gate.fire();
    data_groups_gate.fire();
    transport_gate.fire();

    // Warm the QUIC peer cache so the first replicated request
    // after boot doesn't pay a cold dial.
    if let (Some(transport), Some(topology)) = (
        shared.cluster_transport.as_ref(),
        shared.cluster_topology.as_ref(),
    ) {
        // Clone the topology snapshot so the read guard is dropped
        // before awaiting — clippy::await_holding_lock.
        let topo_snapshot = {
            let guard = topology.read().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        };
        let warm_report = crate::control::cluster::warm_known_peers(
            transport,
            &topo_snapshot,
            shared.node_id,
            Duration::from_secs(2),
        )
        .await;
        if warm_report.attempted > 0 {
            info!(report = %warm_report, "peer cache warm-up complete");
            if !warm_report.is_complete() {
                for (id, err) in &warm_report.failed {
                    tracing::warn!(node_id = id, error = %err, "peer warm failed");
                }
            }
        }
    }
    warm_peers_gate.fire();
    health_loop_gate.fire();
    gateway_enable_gate.fire();

    Ok(())
}
