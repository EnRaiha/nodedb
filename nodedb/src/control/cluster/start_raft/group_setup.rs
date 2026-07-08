// SPDX-License-Identifier: BUSL-1.1

//! Phase 1 of `start_raft`: bootstrap the sequencer Raft group, build the
//! propose tracker + distributed applier + Calvin state, the metadata
//! applier, the plan/array executors and vshard handler, and load the
//! snapshot-transfer / replication-factor config that must be read before
//! `pending_subsystems` is consumed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, Sender};

use nodedb_cluster::calvin::{CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerStateMachine};

use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::metadata_applier::MetadataCommitApplier;
use crate::control::cluster::spsc_applier::SpscCommitApplier;
use crate::control::cluster::start_raft_helpers::build_vshard_handler;
use crate::control::distributed_applier::{ApplyBatch, ProposeTracker, create_distributed_applier};
use crate::control::state::SharedState;

/// Everything phase 1 produces that later phases (loop construction,
/// proposer wiring, observability) need.
pub(super) struct GroupSetup {
    pub(super) tracker: Arc<ProposeTracker>,
    pub(super) data_applier: SpscCommitApplier,
    pub(super) apply_rx: mpsc::Receiver<ApplyBatch>,
    pub(super) calvin_completion_registry: Arc<CalvinCompletionRegistry>,
    pub(super) sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
    pub(super) calvin_read_result_senders: Arc<Mutex<BTreeMap<u32, Sender<ReadResultEvent>>>>,
    pub(super) metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier>,
    pub(super) plan_executor: Arc<crate::control::LocalPlanExecutor>,
    pub(super) vshard_handler: nodedb_cluster::VShardEnvelopeHandler,
    pub(super) tick_interval: Duration,
    pub(super) snapshot_chunk_bytes: u64,
    pub(super) orphan_partial_max_age_secs: u64,
    pub(super) replication_factor: u32,
}

/// Move the `MultiRaft` out of `handle.multi_raft`, add the sequencer Raft
/// group if this is a bootstrap/restart node, and build every phase-1
/// dependency. Returns the extracted `MultiRaft` (which the loop-build phase
/// moves into `RaftLoop::new`) alongside the rest of the setup.
pub(super) fn build_group_setup(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    transport_tuning: &nodedb_types::config::tuning::ClusterTransportTuning,
) -> crate::Result<(nodedb_cluster::multi_raft::MultiRaft, GroupSetup)> {
    // Move the MultiRaft constructed by `start_cluster` into this
    // function. Rebuilding it here from the routing table would lose
    // learner membership for joining nodes and would double-open
    // per-group redb log files.
    let mut multi_raft = handle
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: cluster multi_raft already consumed".into(),
        })?;

    // Bootstrap/restart nodes create the sequencer group here. A fresh joiner
    // already reconstructed it as a learner from JoinResponse; replacing that
    // group with a topology-derived voter set would fork the Raft membership.
    if !multi_raft.contains_group(SEQUENCER_GROUP_ID) {
        let sequencer_peers: Vec<u64> = {
            let topo = handle.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.all_nodes()
                .filter(|node| node.node_id != handle.node_id && node.state.receives_log())
                .map(|node| node.node_id)
                .collect()
        };
        multi_raft
            .add_group(SEQUENCER_GROUP_ID, sequencer_peers)
            .map_err(|e| crate::Error::Config {
                detail: format!("sequencer raft group add: {e}"),
            })?;
    }

    // Build the propose tracker and distributed applier.
    //
    // The tracker is wired with the per-group apply watermark
    // registry so every `tracker.complete(group_id, idx, _)` call
    // also bumps the watcher — coupling the "data applied on this
    // node" signal to the single source of truth that proposers
    // and cross-node visibility waits both consume.
    let tracker =
        Arc::new(ProposeTracker::new().with_group_watchers(handle.group_watchers.clone()));
    let (dist_applier, apply_rx) = create_distributed_applier(tracker.clone());
    let dist_applier = Arc::new(dist_applier);
    let calvin_completion_registry = CalvinCompletionRegistry::new();
    let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        std::collections::HashMap::new(),
        Arc::clone(&calvin_completion_registry),
    )));
    let calvin_read_result_senders =
        Arc::new(Mutex::new(BTreeMap::<u32, Sender<ReadResultEvent>>::new()));

    // Install the propose tracker so CP dispatch paths can await commit.
    if shared.propose_tracker.set(tracker.clone()).is_err() {
        tracing::warn!("propose_tracker already set — start_raft appears to have run twice");
    }

    let data_applier = SpscCommitApplier::new(
        shared.clone(),
        dist_applier,
        Arc::clone(&sequencer_state_machine),
    );

    // Production metadata applier: writes to the shared cache,
    // writes back to the `SystemCatalog` redb so every non-cache
    // reader observes the change, bumps the applied-index watcher,
    // broadcasts `CatalogChangeEvent`, and spawns Data Plane
    // `Register` dispatches on committed `CollectionDdl::Create`.
    let metadata_applier_concrete = Arc::new(MetadataCommitApplier::new(
        handle.metadata_cache.clone(),
        shared.catalog_change_tx.clone(),
        shared.credentials.clone(),
    ));
    // Install the Weak<SharedState> before the raft loop starts
    // ticking so no commit can reach the applier without it.
    metadata_applier_concrete.install_shared(Arc::downgrade(shared));
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
        metadata_applier_concrete.clone();

    // LocalPlanExecutor is the C-β physical-plan execution path (C-δ.6: sole execution path).
    let plan_executor = Arc::new(crate::control::LocalPlanExecutor::new(shared.clone()));

    // Build the real ArrayLocalExecutor that bridges incoming array shard RPCs
    // into the local Data Plane via the SPSC bridge, then the vshard handler
    // that wraps it.
    let array_executor: Arc<dyn nodedb_cluster::distributed_array::ArrayLocalExecutor> =
        Arc::new(DataPlaneArrayExecutor::new(shared.clone()));
    let vshard_handler = build_vshard_handler(array_executor);

    let tick_interval = Duration::from_millis(transport_tuning.raft_tick_interval_ms);

    // Read snapshot-transfer config from the pending subsystem config before
    // the raft_loop is constructed (pending is consumed after the loop).
    let (snapshot_chunk_bytes, orphan_partial_max_age_secs) = {
        let guard = handle
            .pending_subsystems
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let cfg = guard.as_ref().ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: pending_subsystems already consumed".into(),
        })?;
        (
            cfg.config.install_snapshot_chunk_bytes,
            cfg.config.orphan_partial_max_age_secs,
        )
    };

    // Load the replication factor from persisted cluster settings. This
    // function is always called after bootstrap has written those settings —
    // `None` here indicates the node was never bootstrapped, which is an
    // invariant violation (not a recoverable condition).
    let replication_factor = match handle.catalog.load_cluster_settings().map_err(|e| {
        crate::Error::Config {
            detail: format!("start_raft: failed to load cluster settings: {e}"),
        }
    })? {
        Some(s) => s.replication_factor,
        None => {
            // Settings not yet persisted on this path — fall back to the
            // in-memory config RF (the same value bootstrap would persist).
            // Error only if neither source is available.
            handle
                .pending_subsystems
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .map(|p| p.config.replication_factor as u32)
                .ok_or_else(|| crate::Error::Config {
                    detail: "start_raft: no replication factor available (catalog and config both absent)".to_string(),
                })?
        }
    };

    let setup = GroupSetup {
        tracker,
        data_applier,
        apply_rx,
        calvin_completion_registry,
        sequencer_state_machine,
        calvin_read_result_senders,
        metadata_applier,
        plan_executor,
        vshard_handler,
        tick_interval,
        snapshot_chunk_bytes,
        orphan_partial_max_age_secs,
        replication_factor,
    };

    Ok((multi_raft, setup))
}
