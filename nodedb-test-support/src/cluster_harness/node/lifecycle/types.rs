// SPDX-License-Identifier: BUSL-1.1

//! [`TestClusterNode`] struct definition.

use std::net::SocketAddr;
use std::sync::Arc;

use nodedb::control::state::SharedState;
use nodedb::event::EventPlane;

/// Running cluster node.
pub struct TestClusterNode {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub pg_addr: SocketAddr,
    /// Native (MessagePack) protocol listener port. Bound on an ephemeral port
    /// so `NativeClient::connect("127.0.0.1:<native_port>")` works in tests.
    pub native_port: u16,
    pub client: tokio_postgres::Client,
    pub shared: Arc<SharedState>,
    pub(in crate::cluster_harness::node) _data_dir: tempfile::TempDir,
    pub(in crate::cluster_harness::node) _conn_handle: tokio::task::JoinHandle<()>,
    pub(in crate::cluster_harness::node) pg_shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    pub(in crate::cluster_harness::node) poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(in crate::cluster_harness::node) cluster_shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(in crate::cluster_harness::node) core_stop_txs: Vec<std::sync::mpsc::Sender<()>>,
    pub(in crate::cluster_harness::node) _pg_handle: tokio::task::JoinHandle<()>,
    pub(in crate::cluster_harness::node) _native_handle: tokio::task::JoinHandle<()>,
    pub(in crate::cluster_harness::node) _poller_handle: tokio::task::JoinHandle<()>,
    pub(in crate::cluster_harness::node) _core_handles: Vec<tokio::task::JoinHandle<()>>,
    pub(in crate::cluster_harness::node) _event_plane: EventPlane,
}
