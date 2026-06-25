// SPDX-License-Identifier: BUSL-1.1

//! Start the Raft event loop, RPC server, and both appliers.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::info;

use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerConfig, SequencerService,
    SequencerStateMachine, new_inbox,
};
use nodedb_cluster::distributed_array::ArrayLocalExecutor;
use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::control::cluster::array_executor::DataPlaneArrayExecutor;
use crate::control::cluster::calvin::executor::ollp::OllpConfig;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::cluster::calvin::{ReadResultEvent, SchedulerConfig};
use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::metadata_applier::MetadataCommitApplier;
use crate::control::cluster::snapshot_hook::RaftSnapshotQuarantineHook;
use crate::control::cluster::spsc_applier::SpscCommitApplier;
use crate::control::cluster::start_raft_helpers::{build_vshard_handler, spawn_vshard_schedulers};
use crate::control::distributed_applier::{
    ProposeTracker, create_distributed_applier, run_apply_loop,
};
use crate::control::state::SharedState;

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
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<tokio::sync::watch::Receiver<bool>> {
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
    let calvin_read_result_senders = Arc::new(Mutex::new(std::collections::BTreeMap::<
        u32,
        tokio::sync::mpsc::Sender<ReadResultEvent>,
    >::new()));

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
    metadata_applier_concrete.install_shared(Arc::downgrade(&shared));
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
        metadata_applier_concrete.clone();

    // LocalPlanExecutor is the C-β physical-plan execution path (C-δ.6: sole execution path).
    let plan_executor = Arc::new(crate::control::LocalPlanExecutor::new(shared.clone()));

    // Build the real ArrayLocalExecutor that bridges incoming array shard RPCs
    // into the local Data Plane via the SPSC bridge.
    let array_executor: Arc<dyn ArrayLocalExecutor> =
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

    let quarantine_hook = Arc::new(RaftSnapshotQuarantineHook {
        registry: Arc::clone(&shared.quarantine_registry),
    });

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

    let raft_loop = Arc::new(
        nodedb_cluster::RaftLoop::new(
            multi_raft,
            handle.transport.clone(),
            handle.topology.clone(),
            data_applier,
        )
        .with_plan_executor(plan_executor)
        .with_metadata_applier(metadata_applier)
        .with_vshard_handler(vshard_handler)
        .with_tick_interval(tick_interval)
        .with_group_watchers(handle.group_watchers.clone())
        .with_snapshot_quarantine_hook(quarantine_hook)
        .with_snapshot_builder(snapshot_builder)
        .with_snapshot_applier(snapshot_applier)
        .with_shuffle_receiver(shuffle_receiver)
        .with_shuffle_producer(shuffle_producer)
        .with_shuffle_consumer(shuffle_consumer)
        .with_shuffle_aggregator(shuffle_aggregator)
        .with_assign_remote_surrogate(assign_remote_surrogate)
        .with_calvin_submit(calvin_submit)
        .with_calvin_submit_inbox(calvin_submit_inbox)
        .with_data_dir(data_dir.to_path_buf())
        .with_snapshot_chunk_bytes(snapshot_chunk_bytes)
        .with_orphan_partial_max_age_secs(orphan_partial_max_age_secs),
    );

    // Spawn cluster subsystems now that the loop owns `MultiRaft`.
    // They share the same `Arc<Mutex<MultiRaft>>` the loop holds, so
    // shutdown is symmetric (subsystems are torn down before the
    // loop's strong ref drops). See `nodedb_cluster::start_cluster`
    // doc for the two-phase startup rationale.
    let pending = handle
        .pending_subsystems
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: pending_subsystems already consumed".into(),
        })?;
    let raft_loop_handle = raft_loop.multi_raft_handle();

    let sequencer_config = SequencerConfig::default();
    let (sequencer_inbox, sequencer_inbox_rx) = new_inbox(10_000, &sequencer_config);
    let ollp_orchestrator = Arc::new(OllpOrchestrator::new(OllpConfig::default()));
    let mut sequencer_service = SequencerService::new(
        sequencer_config,
        handle.node_id,
        raft_loop_handle.clone(),
        sequencer_inbox_rx,
        sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next_epoch(),
        Arc::clone(&calvin_completion_registry),
    );
    let sequencer_metrics = Arc::clone(&sequencer_service.metrics);

    let scheduler_config = SchedulerConfig::default();
    spawn_vshard_schedulers(
        handle,
        &shared,
        raft_loop_handle.clone(),
        &sequencer_state_machine,
        &calvin_read_result_senders,
        &scheduler_config,
    )?;

    let running = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(nodedb_cluster::start_cluster_subsystems(
            &pending.config,
            Arc::clone(&handle.topology),
            Arc::clone(&handle.routing),
            Arc::clone(&handle.transport),
            raft_loop_handle,
        ))
    })
    .map_err(|e| crate::Error::Config {
        detail: format!("cluster subsystem start: {e}"),
    })?;
    *handle
        .running_cluster
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(running);

    // Wire the Raft proposer into SharedState so CP dispatch paths
    // (pgwire, HTTP, array inbound) can route writes through Raft.
    let raft_loop_for_propose = raft_loop.clone();
    let proposer: Arc<crate::control::wal_replication::RaftProposer> =
        Arc::new(move |vshard_id, data| {
            raft_loop_for_propose
                .propose(vshard_id, data)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft propose: {e}"),
                })
        });
    if shared.raft_proposer.set(proposer).is_err() {
        tracing::warn!("raft_proposer already set — start_raft appears to have run twice");
    }

    // Wire the Raft log-compaction trigger. `run_apply_loop` invokes this
    // after a committed entry has been durably applied to the Data Plane,
    // so compaction is gated on the data-plane applied watermark — never
    // raft's commit index. A no-op for groups whose
    // `log_compaction_threshold` is `None`.
    let raft_loop_for_compact = raft_loop.clone();
    let compactor: Arc<crate::control::wal_replication::RaftCompactor> =
        Arc::new(move |group_id, applied_index| {
            raft_loop_for_compact
                .maybe_compact_group(group_id, applied_index)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("raft log compaction: {e}"),
                })
        });
    if shared.raft_compactor.set(compactor).is_err() {
        tracing::warn!("raft_compactor already set — start_raft appears to have run twice");
    }

    // Install the async proposer with transparent leader forwarding.
    //
    // Proposes via the data group leader (forwarding to a remote leader if
    // needed), then registers a ProposeTracker waiter and awaits apply.
    //
    // The ProposeTracker is race-safe: if `run_apply_loop` calls complete()
    // before register() is called (possible on fast clusters where the entry
    // commits and applies on this node before the proposer returns), the
    // result is stored and register() picks it up immediately with no timeout.
    let raft_loop_async = raft_loop.clone();
    let tracker_for_proposer = tracker.clone();
    let deadline_secs = shared.tuning.network.default_deadline_secs;
    let async_proposer: Arc<crate::control::wal_replication::AsyncRaftProposer> =
        Arc::new(move |vshard_id, idempotency_key, data| {
            let rl = raft_loop_async.clone();
            let tk = tracker_for_proposer.clone();
            Box::pin(async move {
                let (group_id, log_index) = rl
                    .propose_via_data_leader(vshard_id, data)
                    .await
                    .map_err(|e| crate::Error::Internal {
                        detail: format!("raft propose (async): {e}"),
                    })?;

                // Register the waiter with the proposer's idempotency
                // key. The apply path compares against the committed
                // entry's key so a leader-change overwrite at the same
                // (group_id, log_index) — by either an empty no-op or a
                // different proposer's real entry — surfaces as
                // `RetryableLeaderChange` instead of leaking a
                // not-our-payload back to the caller.
                let rx = tk.register(group_id, log_index, idempotency_key);
                tokio::time::timeout(std::time::Duration::from_secs(deadline_secs), rx)
                    .await
                    .map_err(|_| crate::Error::Dispatch {
                        detail: format!(
                            "raft commit timeout for group {group_id} index {log_index}"
                        ),
                    })?
                    .map_err(|_| crate::Error::Dispatch {
                        detail: "propose waiter channel closed".into(),
                    })?
                    // Preserve `RetryableLeaderChange` so the gateway
                    // retry loop can re-propose against the new leader
                    // — wrapping it in `Dispatch` would hide the
                    // retryable signal and surface as silent INSERT
                    // success. Other errors stay wrapped for
                    // diagnostics.
                    .map_err(|e| match e {
                        crate::Error::RetryableLeaderChange { .. } => e,
                        other => crate::Error::Dispatch {
                            detail: format!("apply error: {other}"),
                        },
                    })
            })
        });
    if shared.async_raft_proposer.set(async_proposer).is_err() {
        tracing::warn!("async_raft_proposer already set — start_raft appears to have run twice");
    }

    // Spawn the background apply loop. It reads from the mpsc channel
    // pushed by `DistributedApplier::apply_committed`, dispatches to the
    // Data Plane, and notifies propose waiters.
    let apply_state = shared.clone();
    let apply_tracker = tracker.clone();
    let apply_calvin_read_result_senders = Arc::clone(&calvin_read_result_senders);
    let sr_apply = shutdown_rx.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = run_apply_loop(
                apply_rx,
                apply_state,
                apply_tracker,
                apply_calvin_read_result_senders,
            ) => {}
            _ = async {
                let mut rx = sr_apply;
                let _ = rx.changed().await;
            } => {}
        }
    });

    let _ = shared.sequencer_inbox.set(sequencer_inbox);
    let _ = shared.sequencer_metrics.set(sequencer_metrics);
    let _ = shared
        .calvin_completion_registry
        .set(calvin_completion_registry);
    let _ = shared.ollp_orchestrator.set(ollp_orchestrator);

    // Publish the cluster observability handle to SharedState before
    // any listener starts serving.
    let observer = Arc::new(nodedb_cluster::ClusterObserver::new(
        handle.node_id,
        handle.lifecycle.clone(),
        handle.topology.clone(),
        handle.routing.clone(),
        raft_loop.clone() as Arc<dyn nodedb_cluster::GroupStatusProvider + Send + Sync>,
    ));
    if shared.cluster_observer.set(observer).is_err() {
        tracing::warn!("cluster_observer already set — start_raft appears to have run twice");
    }

    // Publish a live Raft leader-status snapshot fn so routing (gateway +
    // graph scatter) resolves group leadership from CURRENT Raft state
    // rather than the (lagging) routing-table hint. Wraps the raft loop's
    // `group_statuses()` snapshot.
    let raft_loop_for_status = raft_loop.clone();
    if shared
        .raft_status_fn
        .set(Arc::new(move || raft_loop_for_status.group_statuses()))
        .is_err()
    {
        tracing::warn!("raft_status_fn already set — start_raft appears to have run twice");
    }

    // Publish the raft loop handle into SharedState so the metadata
    // proposer can reach it. The handle is type-erased behind a
    // trait object to keep the SharedState field concrete.
    let proposer_handle: Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle> =
        Arc::new(crate::control::metadata_proposer::RaftLoopProposerHandle::new(raft_loop.clone()));
    if shared.metadata_raft.set(proposer_handle).is_err() {
        tracing::warn!("metadata_raft already set — start_raft appears to have run twice");
    }

    // Allow the surrogate assigner's flush path to propose
    // `SurrogateAlloc` entries to the Raft group so followers advance
    // their in-memory HWM on every checkpoint.
    shared
        .surrogate_assigner
        .install_shared(Arc::downgrade(&shared));
    // Routing can lag or be self-only during cluster bring-up, but
    // topology already tells us whether this process can collide with
    // peer allocators. Latch HiLo mode before the eager refiller starts.
    let cluster_member_count = handle
        .topology
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .all_nodes()
        .filter(|node| node.state.receives_log())
        .count();
    if cluster_member_count > 1 {
        shared.surrogate_assigner.enable_reservation_mode();
    }

    // Spawn the per-node surrogate reservation refiller. It owns ALL batch
    // reservation so the latency-critical `assign` insert path never blocks
    // on the metadata-Raft round-trip in steady state: it eagerly reserves
    // the first batch on its first iteration (before inserts arrive) and
    // tops the batch up whenever the hot path nudges it below the
    // low-watermark. The loop self-gates via `should_use_reservation`, so it
    // is a cheap park on single-node / single-member deployments. Same
    // lifetime/shutdown pattern as the sequencer ticker below.
    let refiller = shared.surrogate_assigner.clone();
    let refiller_shared = Arc::downgrade(&shared);
    let sr_refill = shutdown_rx.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = refiller.run_refill_loop(refiller_shared) => {}
            _ = async {
                let mut rx = sr_refill;
                let _ = rx.changed().await;
            } => {}
        }
        info!("surrogate refill loop stopped");
    });

    // Subscribe to the boot-time readiness watch BEFORE spawning the
    // tick loop so we cannot miss the first transition. The receiver
    // is returned to `main.rs`, which awaits it before binding any
    // client-facing listener.
    let ready_rx = raft_loop.subscribe_ready();

    // Register the raft-tick loop's standardized metrics so the
    // `/metrics` route can expose them alongside every other driver.
    shared
        .loop_metrics_registry
        .register(raft_loop.loop_metrics());

    // Start the Raft tick loop.
    let rl_run = raft_loop.clone();
    let sr_raft = shutdown_rx.clone();
    tokio::spawn(async move {
        rl_run.run(sr_raft).await;
        info!("raft loop stopped");
    });

    let sr_sequencer = shutdown_rx.clone();
    tokio::spawn(async move {
        sequencer_service.run(sr_sequencer).await;
        info!("sequencer service stopped");
    });

    // Start the RPC server (accepts inbound QUIC connections).
    let transport_serve = handle.transport.clone();
    let rl_handler = raft_loop.clone();
    let sr_serve = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = transport_serve.serve(rl_handler, sr_serve).await {
            tracing::error!(error = %e, "raft RPC server failed");
        }
    });

    // Wire version of every node is now carried on the live
    // `NodeInfo` in `cluster_topology`. Log the derived view for observability.
    {
        let view = shared.cluster_version_view();
        let compat = crate::control::rolling_upgrade::should_compat_mode(&view);
        info!(
            node_id = handle.node_id,
            nodes = view.node_count,
            min_version = view.min_version,
            max_version = view.max_version,
            mixed = view.is_mixed_version(),
            compat_mode = compat,
            "cluster version view derived from topology"
        );
    }

    // Start the health monitor (periodic pings, failure detection,
    // topology re-broadcast).
    let health_config = nodedb_cluster::HealthConfig {
        ping_interval: Duration::from_secs(transport_tuning.health_ping_interval_secs),
        failure_threshold: transport_tuning.health_failure_threshold,
    };
    let health_monitor = Arc::new(nodedb_cluster::HealthMonitor::new(
        handle.node_id,
        handle.transport.clone(),
        handle.topology.clone(),
        handle.catalog.clone(),
        health_config,
    ));
    shared
        .loop_metrics_registry
        .register(health_monitor.loop_metrics());
    if shared.health_monitor.set(health_monitor.clone()).is_err() {
        tracing::warn!("health_monitor already set — start_raft appears to have run twice");
    }
    let sr_health = shutdown_rx;
    tokio::spawn(async move {
        health_monitor.run(sr_health).await;
    });

    info!(node_id = handle.node_id, "raft loop and RPC server started");

    Ok(ready_rx)
}
