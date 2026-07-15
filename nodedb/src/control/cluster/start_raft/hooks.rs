// SPDX-License-Identifier: BUSL-1.1

//! Phase 2 of `start_raft`: build the snapshot quarantine hook, the
//! per-group snapshot builder/applier (including follower boot-restore of
//! any persisted `.snap` files), and the cross-node shuffle / surrogate /
//! Calvin routing hooks that bridge `RaftLoop` callbacks to `SharedState`.

use std::sync::Arc;

use tracing::info;

use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

/// Everything phase 2 produces: the hook trait objects `RaftLoop::new`'s
/// builder methods install.
pub(super) struct Hooks {
    pub(super) quarantine_hook:
        Arc<crate::control::cluster::snapshot_hook::RaftSnapshotQuarantineHook>,
    pub(super) snapshot_builder: Arc<dyn nodedb_cluster::SnapshotBuilder>,
    pub(super) snapshot_applier: Arc<dyn nodedb_cluster::SnapshotApplier>,
    pub(super) shuffle_receiver: Arc<dyn nodedb_cluster::ShuffleReceiver>,
    pub(super) shuffle_producer: Arc<dyn nodedb_cluster::ShuffleProducer>,
    pub(super) shuffle_consumer: Arc<dyn nodedb_cluster::ShuffleConsumer>,
    pub(super) shuffle_aggregator: Arc<dyn nodedb_cluster::ShuffleAggregator>,
    pub(super) assign_remote_surrogate: Arc<dyn nodedb_cluster::AssignRemoteSurrogate>,
    pub(super) calvin_submit: Arc<dyn nodedb_cluster::CalvinSubmit>,
    pub(super) calvin_submit_inbox: Arc<dyn nodedb_cluster::CalvinSubmitInbox>,
    pub(super) reserve_read: Arc<dyn nodedb_cluster::ReserveRead>,
    pub(super) release_reservation: Arc<dyn nodedb_cluster::ReleaseReservation>,
}

/// Build every cross-plane hook `RaftLoop` needs, including the follower
/// boot-restore of persisted snapshots (which must run before
/// `run_apply_loop` is spawned, since the leader's log-compaction discards
/// the pre-snapshot log prefix the apply loop would otherwise need to
/// replay). `start_raft` itself is sync, so the async restore call is driven
/// via `block_in_place` + `block_on`, matching the surrounding style used
/// for other cluster subsystems rather than introducing a new runtime entry.
pub(super) fn build_hooks(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
) -> crate::Result<Hooks> {
    let quarantine_hook = Arc::new(
        crate::control::cluster::snapshot_hook::RaftSnapshotQuarantineHook {
            registry: Arc::clone(&shared.quarantine_registry),
        },
    );

    // Per-group snapshot builder for the SEND path: on the leader, build the
    // real serialized engine state for a lagging follower's group vshards
    // (replacing the prior empty stub bytes).
    let snapshot_builder: Arc<dyn nodedb_cluster::SnapshotBuilder> = Arc::new(
        crate::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder::new(shared.clone()),
    );

    // Per-group snapshot applier for the RECEIVE path: on the follower, apply a
    // received per-group snapshot to the local Data-Plane state machine (via the
    // existing restore handler with replace_mode = true) before Raft advances.
    let snapshot_applier_concrete =
        crate::control::cluster::snapshot_applier::DataPlaneSnapshotApplier::new(shared.clone());

    // Follower boot-restore: re-install any persisted `.snap` snapshots from a
    // prior run BEFORE the apply loop is spawned. The leader's log-compaction
    // discards the pre-snapshot prefix, so the post-snapshot log tail the apply
    // loop will replay can NOT reconstruct that prefix — the persisted snapshot
    // is the only source for it. Must precede `run_apply_loop` for that reason.
    // Match the surrounding block_in_place style used for other cluster
    // subsystems rather than introducing a new runtime entry.
    let restored = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(
            crate::control::cluster::boot_restore::restore_persisted_snapshots(
                data_dir,
                &snapshot_applier_concrete,
            ),
        )
    })?;
    if restored > 0 {
        info!(
            node_id = handle.node_id,
            restored, "follower boot-restore re-installed persisted snapshots"
        );
    }

    let snapshot_applier: Arc<dyn nodedb_cluster::SnapshotApplier> =
        Arc::new(snapshot_applier_concrete);

    // Cross-node streaming-shuffle receiver (E1): bridge the cluster
    // `ShufflePush` read-loop to the in-process registry on `SharedState`.
    let shuffle_receiver: Arc<dyn nodedb_cluster::ShuffleReceiver> = Arc::new(
        crate::control::server::shuffle::RegistryShuffleReceiver::new(Arc::clone(
            &shared.shuffle_registry,
        )),
    );

    // Cross-node shuffle PRODUCER (E4a): runs a local scan through the streaming
    // executor + fan-out sink when a `ShuffleProduce` trigger arrives.
    let shuffle_producer: Arc<dyn nodedb_cluster::ShuffleProducer> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleProducer::new(shared.clone()));

    // Cross-node shuffle CONSUMER (E4b): runs the node-local grace join over the
    // part's staged sides when a `ShuffleConsume` trigger arrives.
    let shuffle_consumer: Arc<dyn nodedb_cluster::ShuffleConsumer> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleConsumer::new(shared.clone()));

    // Cross-node distributed GROUP BY shuffle CONSUMER (E5b): SINGLE-SIDED
    // aggregate sibling of the consumer — merges + finalizes the part's single
    // staged producer side when a `ShuffleAggregateConsume` trigger arrives.
    let shuffle_aggregator: Arc<dyn nodedb_cluster::ShuffleAggregator> =
        Arc::new(crate::control::server::shuffle::RegistryShuffleAggregator::new(shared.clone()));

    // Routed-surrogate-exchange (F1b): when this node is the home vShard's leader
    // for a `(collection, pk)` endpoint key, assign-or-return the authoritative
    // surrogate via a LOCAL `SurrogateAssigner::assign`.
    let assign_remote_surrogate: Arc<dyn nodedb_cluster::AssignRemoteSurrogate> = Arc::new(
        crate::control::server::surrogate_exchange::RegistryAssignRemoteSurrogate::new(
            shared.clone(),
        ),
    );

    // Routed Calvin-submit (Cv1): when this node is the sequencer-group leader,
    // submit a forwarded `TxClass` to the local Calvin sequencer inbox and await
    // its completion. Lets a cross-shard write submitted on a NON-leader
    // coordinator route here and actually commit.
    let calvin_submit: Arc<dyn nodedb_cluster::CalvinSubmit> =
        Arc::new(crate::control::server::calvin_submit::RegistryCalvinSubmit::new(shared.clone()));

    // Routed Calvin-INBOX submit (Cv1): the OLLP dependent sibling of the
    // submit-and-await hook above. When this node is the sequencer-group leader,
    // submit a forwarded dependent `TxClass` to the local Calvin sequencer inbox
    // and return its ASSIGNMENT immediately (without awaiting completion) so a
    // non-leader OLLP coordinator can drive the dependent transaction itself.
    let calvin_submit_inbox: Arc<dyn nodedb_cluster::CalvinSubmitInbox> = Arc::new(
        crate::control::server::calvin_submit::RegistryCalvinSubmitInbox::new(shared.clone()),
    );

    // Routed reserve-read (reservation admission): when this node is the
    // sequencer-group leader, submit a forwarded reserve-read to the local
    // reservation inbox and reply with the assigned owner.
    let reserve_read: Arc<dyn nodedb_cluster::ReserveRead> =
        Arc::new(crate::control::server::reservation::RegistryReserveRead::new(shared.clone()));

    // Routed release-reservation: when this node is the sequencer-group
    // leader, submit a forwarded release to the local reservation inbox.
    let release_reservation: Arc<dyn nodedb_cluster::ReleaseReservation> = Arc::new(
        crate::control::server::reservation::RegistryReleaseReservation::new(shared.clone()),
    );

    Ok(Hooks {
        quarantine_hook,
        snapshot_builder,
        snapshot_applier,
        shuffle_receiver,
        shuffle_producer,
        shuffle_consumer,
        shuffle_aggregator,
        assign_remote_surrogate,
        calvin_submit,
        calvin_submit_inbox,
        reserve_read,
        release_reservation,
    })
}
