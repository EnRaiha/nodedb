// SPDX-License-Identifier: BUSL-1.1

//! Cluster Raft start, descriptor-lease renewal loop, response poller,
//! and the full Event Plane background-loop spawn.

use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::control::cluster::ClusterHandle;
use nodedb::control::state::SharedState;

/// Everything the caller needs to hold alive (or read later) from this
/// phase, bundled so the call site doesn't juggle three separate `let`s
/// with different provenance.
pub(crate) struct BackgroundLoops {
    /// Flips to `true` after the metadata raft group applies its first
    /// entry on this node. `None` on single-node deployments. Awaited
    /// just before binding client-facing listeners.
    pub(crate) raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>>,
    /// Held only so the join handle isn't dropped before shutdown; the
    /// loop itself subscribes to `shutdown_rx` and exits on signal.
    pub(crate) _lease_renewal: Option<tokio::task::JoinHandle<()>>,
    /// Owns the Event Plane shutdown supervisor. It is registered with the
    /// canonical shutdown bus before signal handling is armed, and owns all
    /// consumer join handles until they drain or the configured deadline
    /// requires an abort.
    pub(crate) _event_plane_shutdown: tokio::task::JoinHandle<()>,
}

/// Inputs this phase needs, bundled to keep the call site to one struct
/// literal instead of nine positional arguments.
///
/// `cluster_handle` is borrowed, not owned — `main()` still needs it
/// afterward for `spawn_protocol_listeners`.
pub(crate) struct BackgroundLoopsInputs<'a> {
    pub(crate) cluster_handle: Option<&'a ClusterHandle>,
    pub(crate) wal: Arc<nodedb::wal::WalManager>,
    pub(crate) event_consumers: Vec<nodedb::event::bus::EventConsumerRx>,
    pub(crate) watermark_store: Arc<nodedb::event::watermark::WatermarkStore>,
    pub(crate) trigger_dlq: Arc<std::sync::Mutex<nodedb::event::trigger::TriggerDlq>>,
    pub(crate) num_cores: usize,
}

/// Start cluster Raft (if configured), spawn the descriptor lease
/// renewal loop, start the response poller, and spawn every Event
/// Plane background loop. Pure relocation of what used to be inline in
/// `main()` between shutdown-bus wiring and connection-semaphore setup.
pub(crate) fn spawn(
    shared: &Arc<SharedState>,
    config: &ServerConfig,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    inputs: BackgroundLoopsInputs<'_>,
) -> anyhow::Result<BackgroundLoops> {
    let BackgroundLoopsInputs {
        cluster_handle,
        wal,
        event_consumers,
        watermark_store,
        trigger_dlq,
        num_cores,
    } = inputs;

    // Start cluster Raft loop if in cluster mode. The returned
    // receiver flips to `true` after the metadata raft group has
    // applied its first entry on this node — see
    // `nodedb-cluster::RaftLoop::subscribe_ready`. We hold on to it
    // and await it just before binding client-facing listeners so
    // the first DDL after process start cannot race against an
    // uninitialized metadata group.
    let raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>> =
        if let Some(handle) = cluster_handle {
            Some(nodedb::control::cluster::start_raft(
                handle,
                Arc::clone(shared),
                &config.server.data_dir,
                &config.tuning.cluster_transport,
            )?)
        } else {
            None
        };

    // Spawn the descriptor lease renewal loop. Returns None on
    // single-node clusters (no metadata raft handle wired) — the
    // returned JoinHandle is dropped on the floor because the loop
    // subscribes to `shutdown_rx` and exits cleanly on Ctrl+C.
    let _lease_renewal = nodedb::control::lease::LeaseRenewalLoop::spawn(
        Arc::clone(shared),
        &config.tuning.cluster_transport,
        shutdown_rx.clone(),
    )
    .map(|(join, metrics)| {
        shared.loop_metrics_registry.register(metrics);
        join
    });

    // Start response poller (routes Data Plane responses to waiting sessions).
    bootstrap::background_loops::spawn_response_poller(shared);

    // Spawn all persistent background loops and subsystems, then transfer
    // Event Plane ownership to its shutdown supervisor. The supervisor's
    // critical guard is registered now, before signal handling can initiate
    // the canonical shutdown bus.
    let event_plane = bootstrap::background_loops::spawn_background_loops(
        shared,
        shutdown_bus.clone(),
        bootstrap::background_loops::EventPlaneComponents {
            wal: Arc::clone(&wal),
            event_consumers,
            watermark_store,
            trigger_dlq,
        },
        config,
        num_cores,
        shutdown_rx.clone(),
    );
    let _event_plane_shutdown =
        event_plane.spawn_shutdown_supervisor(shutdown_bus, config.tuning.shutdown.deadline());

    Ok(BackgroundLoops {
        raft_ready_rx,
        _lease_renewal,
        _event_plane_shutdown,
    })
}
