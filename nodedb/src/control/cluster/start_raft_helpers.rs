// SPDX-License-Identifier: BUSL-1.1

use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_cluster::calvin::SequencerStateMachine;
use nodedb_cluster::distributed_array::{ArrayLocalExecutor, handle_array_shard_rpc};
use nodedb_cluster::vshard_handler::{DispatchTarget, dispatch_by_type};
use nodedb_cluster::wire::VShardEnvelope;

use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::read_last_applied_epoch;
use crate::control::cluster::calvin::{ReadResultEvent, Scheduler, SchedulerConfig};
use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

/// Build the `VShardEnvelopeHandler` closure used by `RaftLoop`.
///
/// The closure receives raw envelope bytes from the QUIC transport layer,
/// dispatches based on `msg_type`, and returns a serialized response.
pub(super) fn build_vshard_handler(
    array_executor: Arc<dyn ArrayLocalExecutor>,
) -> nodedb_cluster::VShardEnvelopeHandler {
    Arc::new(move |bytes: Vec<u8>| {
        let executor = array_executor.clone();
        let fut: Pin<
            Box<dyn std::future::Future<Output = nodedb_cluster::error::Result<Vec<u8>>> + Send>,
        > = Box::pin(async move {
            let envelope = VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: "vshard_handler: failed to deserialize VShardEnvelope".into(),
                }
            })?;

            let target = dispatch_by_type(&envelope);
            match target {
                DispatchTarget::ArrayShard => {
                    let opcode = envelope.msg_type as u32;
                    let resp_payload = handle_array_shard_rpc(
                        opcode,
                        envelope.vshard_id,
                        &envelope.payload,
                        &executor,
                    )
                    .await?;

                    // Response opcode = request opcode + 1 for all array shard RPCs.
                    // Resolve the msg_type variant via a minimal scratch envelope parse
                    // (avoids any unsafe transmute — the `from_bytes` mapping in wire.rs
                    // is the canonical source of truth for the opcode→variant table).
                    let resp_opcode = opcode + 1;
                    let resp_msg_type = resolve_vshard_msg_type(resp_opcode)?;
                    let resp_envelope = VShardEnvelope::new(
                        resp_msg_type,
                        envelope.target_node,
                        envelope.source_node,
                        envelope.vshard_id,
                        resp_payload,
                    );
                    Ok(resp_envelope.to_bytes())
                }

                other => Err(nodedb_cluster::error::ClusterError::Transport {
                    detail: format!(
                        "vshard_handler: no handler registered for dispatch target {other:?}"
                    ),
                }),
            }
        });
        fut
    })
}

/// Type alias for the shared per-vShard read-result sender registry.
type ReadResultSenders =
    Arc<Mutex<std::collections::BTreeMap<u32, tokio::sync::mpsc::Sender<ReadResultEvent>>>>;

/// The vShards this node currently hosts: the union of `vshards_for_group` over
/// every Raft group whose member set includes this node, read from the live
/// routing table.
fn hosted_vshards(routing: &RwLock<nodedb_cluster::RoutingTable>, node_id: u64) -> Vec<u32> {
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut vshards = Vec::new();
    for (group_id, info) in routing.group_members() {
        if info.members.contains(&node_id) {
            vshards.extend(routing.vshards_for_group(*group_id));
        }
    }
    vshards.sort_unstable();
    vshards.dedup();
    vshards
}

/// Idempotently ensure a Calvin `Scheduler` is running for every vShard this
/// node currently hosts.
///
/// A vShard is considered already-served iff it has a registered read-result
/// sender (the schedulers' presence registry). Only newly-hosted vShards get a
/// fresh scheduler — this pass never double-spawns. Returns the number of NEW
/// schedulers started.
///
/// This is `add-only`: it never tears down a scheduler for a vShard that has
/// left this node. vShard removal happens via migration / decommission, which
/// own their own teardown path; wiring scheduler removal into that lifecycle is
/// tracked as a separate follow-up.
#[allow(clippy::too_many_arguments)]
fn reconcile_vshard_schedulers(
    node_id: u64,
    routing: &Arc<RwLock<nodedb_cluster::RoutingTable>>,
    shared: &Arc<SharedState>,
    raft_loop_handle: &Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    sequencer_state_machine: &Arc<Mutex<SequencerStateMachine>>,
    calvin_read_result_senders: &ReadResultSenders,
    scheduler_config: &SchedulerConfig,
) -> crate::Result<usize> {
    let mut spawned = 0usize;
    for vshard_id in hosted_vshards(routing, node_id) {
        // Already-served vShards keep their running scheduler untouched.
        if calvin_read_result_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&vshard_id)
        {
            continue;
        }

        let last_applied_epoch = read_last_applied_epoch(&shared.wal, vshard_id)?;
        let (sequenced_tx, sequenced_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);
        sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_vshard_sender(vshard_id, sequenced_tx);

        let (read_result_tx, read_result_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);
        calvin_read_result_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, read_result_tx);

        let scheduler = Scheduler::new(
            vshard_id,
            sequenced_rx,
            Arc::clone(shared),
            raft_loop_handle.clone(),
            last_applied_epoch,
            last_applied_epoch,
            scheduler_config.clone(),
            SchedulerMetrics::new(),
            read_result_rx,
        );
        let shutdown = shared.shutdown.subscribe();
        tokio::spawn(async move {
            scheduler.run(shutdown).await;
        });
        spawned += 1;
    }
    Ok(spawned)
}

/// Spawn Calvin `Scheduler` tasks for this node's vShards and keep the set in
/// sync with cluster membership.
///
/// Scheduler ownership is derived from the routing table's group membership, but
/// a JOINING node's membership is established AFTER `start_raft` runs (it
/// propagates via conf-change once the node is admitted to each data group). A
/// one-shot snapshot at startup therefore misses every vShard on a freshly
/// joined node, leaving cross-shard Calvin transactions whose participants live
/// there permanently un-dispatched (no completion ack → submit times out).
///
/// To make scheduler registration correct regardless of when membership lands —
/// and resilient to later ownership changes — this runs an initial reconcile
/// (covers the bootstrap node, which already sees its membership) and then
/// spawns a background task that re-reconciles on a short interval until
/// shutdown. Reconcile is idempotent and add-only.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_vshard_schedulers(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    raft_loop_handle: Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    sequencer_state_machine: &Arc<Mutex<SequencerStateMachine>>,
    calvin_read_result_senders: &ReadResultSenders,
    scheduler_config: &SchedulerConfig,
) -> crate::Result<()> {
    let node_id = handle.node_id;
    let routing = Arc::clone(&handle.routing);

    // Initial reconcile: schedulers for vShards this node already knows it hosts.
    reconcile_vshard_schedulers(
        node_id,
        &routing,
        shared,
        &raft_loop_handle,
        sequencer_state_machine,
        calvin_read_result_senders,
        scheduler_config,
    )?;

    // Background reconcile: pick up vShards whose membership lands after startup
    // (joiner admission) or shifts later (rebalancing). The routing table has no
    // change-notification, so a short fixed-interval reconcile is the simplest
    // self-healing mechanism; each pass is a cheap routing read + map probe and
    // a no-op once the set has converged.
    let shared_task = Arc::clone(shared);
    let sm_task = Arc::clone(sequencer_state_machine);
    let rr_task = Arc::clone(calvin_read_result_senders);
    let cfg_task = scheduler_config.clone();
    let mut shutdown = shared.shutdown.subscribe();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait_cancelled() => break,
                _ = tick.tick() => {
                    if let Err(e) = reconcile_vshard_schedulers(
                        node_id,
                        &routing,
                        &shared_task,
                        &raft_loop_handle,
                        &sm_task,
                        &rr_task,
                        &cfg_task,
                    ) {
                        tracing::warn!(node_id, error = %e, "calvin scheduler reconcile pass failed");
                    }
                }
            }
        }
    });

    Ok(())
}

/// Resolve a raw opcode `u32` to a `VShardMessageType` variant.
///
/// Uses `VShardEnvelope::from_bytes` as the canonical opcode→variant mapping
/// so this helper stays in sync with the wire format without duplicating the
/// match table.
pub(super) fn resolve_vshard_msg_type(
    opcode: u32,
) -> nodedb_cluster::error::Result<nodedb_cluster::wire::VShardMessageType> {
    let mut scratch = [0u8; 26];
    scratch[0..2].copy_from_slice(&1u16.to_le_bytes()); // version
    scratch[2..4].copy_from_slice(&(opcode as u16).to_le_bytes()); // msg_type

    VShardEnvelope::from_bytes(&scratch)
        .map(|e| e.msg_type)
        .ok_or_else(|| nodedb_cluster::error::ClusterError::Codec {
            detail: format!("resolve_vshard_msg_type: unknown opcode {opcode}"),
        })
}
