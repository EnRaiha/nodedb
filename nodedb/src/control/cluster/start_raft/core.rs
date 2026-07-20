// SPDX-License-Identifier: BUSL-1.1

//! `start_raft` orchestration: bootstrap the sequencer Raft group and
//! phase-1 dependencies ([`super::group_setup`]), build the cross-plane
//! hooks ([`super::hooks`]), construct the `RaftLoop` and start cluster
//! subsystems ([`super::loop_build`]), wire the sync/async Raft proposer and
//! spawn the apply loop ([`super::proposer_wiring`]), and finally publish
//! observability handles and spawn the tick loop / sequencer service / RPC
//! server / health monitor ([`super::observability`]).

use std::sync::Arc;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

use super::group_setup::build_group_setup;
use super::hooks::build_hooks;
use super::loop_build::build_raft_loop;
use super::observability::{ObservabilityInputs, finish_observability};
use super::proposer_wiring::wire_proposers;

/// Start the Raft event loop and RPC server.
///
/// Must be called after `SharedState` is constructed (needs the WAL and
/// dispatcher for the `SpscCommitApplier`). Moves the `MultiRaft` out of
/// `handle.multi_raft` into the `RaftLoop`; must be called **exactly
/// once** per handle.
pub fn start_raft(
    handle: &ClusterHandle,
    shared: Arc<SharedState>,
    data_dir: &std::path::Path,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<tokio::sync::watch::Receiver<bool>> {
    let (multi_raft, setup) = build_group_setup(handle, &shared, data_dir, transport_tuning)?;
    let hooks = build_hooks(handle, &shared, data_dir)?;
    let loop_build = build_raft_loop(handle, &shared, data_dir, multi_raft, setup, hooks)?;

    wire_proposers(
        &shared,
        &loop_build.raft_loop,
        loop_build.tracker,
        loop_build.apply_rx,
        loop_build.calvin_read_result_senders,
        loop_build.sequencer_state_machine,
    );

    let ready_rx = finish_observability(
        handle,
        &shared,
        transport_tuning,
        loop_build.raft_loop,
        ObservabilityInputs {
            sequencer_inbox: loop_build.sequencer_inbox,
            reservation_inbox: loop_build.reservation_inbox,
            sequencer_metrics: loop_build.sequencer_metrics,
            calvin_completion_registry: loop_build.calvin_completion_registry,
            ollp_orchestrator: loop_build.ollp_orchestrator,
            sequencer_service: loop_build.sequencer_service,
        },
    );

    Ok(ready_rx)
}
