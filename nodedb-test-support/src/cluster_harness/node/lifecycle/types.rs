// SPDX-License-Identifier: BUSL-1.1

//! [`TestClusterNode`] struct definition.

use std::net::SocketAddr;
use std::sync::Arc;

use nodedb::control::state::SharedState;
use nodedb::event::EventPlane;

use super::client_slot::ClusterTestClient;

/// The sole trust identity the cluster harness bootstraps on every node
/// (see `spawn_full::spawn_with_full_config_at`'s
/// `credentials.bootstrap_trust_superuser(HARNESS_SUPERUSER)`). Both the
/// pre-wired `client` field (pgwire, `user=nodedb`) and
/// [`super::super::native_client`]'s helper authenticate as this identity —
/// single source of truth so the two paths cannot drift apart.
pub const HARNESS_SUPERUSER: &str = "nodedb";

/// Ownership of this node's on-disk data directory.
///
/// `Owned` is the historical behaviour: the node minted its own `tempdir()`
/// and deletes it on drop. `Borrowed` backs
/// [`super::spawn_variants`]'s `..._on_path` entry points, where the CALLER
/// supplies the path (typically its own `tempfile::TempDir` kept alive across
/// a WAL-only restart) — dropping this node must NOT delete it.
pub(in crate::cluster_harness::node) enum DataDir {
    Owned(tempfile::TempDir),
    Borrowed,
}

/// Running cluster node.
pub struct TestClusterNode {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub pg_addr: SocketAddr,
    /// Native (MessagePack) protocol listener port. Bound on an ephemeral port
    /// so `TestClusterNode::native_client` / `native_client_with` (which build
    /// a `NativeClient` authenticated as the harness superuser) work in tests.
    pub native_port: u16,
    pub client: ClusterTestClient,
    pub shared: Arc<SharedState>,
    pub(in crate::cluster_harness::node) _data_dir: DataDir,
    // The handles below are wrapped in `Option` so that
    // `graceful_shutdown_wal_only(self)` can `.take()` and `.await` them
    // without moving a field out of a type that has a `Drop` impl (E0509).
    // `Drop` checks each one and is a no-op when already taken.
    pub(in crate::cluster_harness::node) _conn_handle: Option<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) pg_shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    pub(in crate::cluster_harness::node) poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(in crate::cluster_harness::node) cluster_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(in crate::cluster_harness::node) core_stop_txs: Vec<std::sync::mpsc::Sender<()>>,
    pub(in crate::cluster_harness::node) _pg_handle: Option<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) _native_handle: Option<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) _poller_handle: Option<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) _core_handles: Vec<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) _event_plane: Option<EventPlane>,
    /// `LeaseRenewalLoop::spawn`'s `JoinHandle` — previously bound to a local
    /// (`_lease_renewal`) and dropped/detached when `spawn_with_full_config_at`
    /// returned. Retained here so `graceful_shutdown_wal_only` can abort+await
    /// it, releasing its `Arc<SharedState>` clone before returning. `None` on
    /// single-node clusters that never wire `metadata_raft`
    /// (`LeaseRenewalLoop::spawn` returns `None` in that case).
    pub(in crate::cluster_harness::node) _lease_renewal_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cluster subsystem tasks (SWIM, reachability, decommission,
    /// rebalancer) started by `start_raft` and stashed on the
    /// `ClusterHandle` it was given. Taken out of the handle right after
    /// `start_raft` returns (the handle itself is not otherwise retained)
    /// so `graceful_shutdown_wal_only` can `.take()` and `shutdown_all`
    /// it — releasing its clone of the shared `MultiRaft` (and
    /// transitively `Arc<SharedState>`) before returning. `Option` so it
    /// can be `.take()`n without violating the `Drop` impl, mirroring
    /// `_lease_renewal_handle`.
    pub(in crate::cluster_harness::node) _running_cluster: Option<nodedb_cluster::RunningCluster>,
}
