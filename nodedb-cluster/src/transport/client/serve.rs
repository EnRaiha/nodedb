// SPDX-License-Identifier: BUSL-1.1

//! Inbound RPC accept loop.

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::error::Result;
use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::rpc_handler::RaftRpcHandler;
use crate::transport::server;

use super::transport::NexarTransport;

impl NexarTransport {
    /// Run the inbound RPC accept loop using the same identity store installed
    /// into the TLS verifiers at transport construction.
    pub async fn serve<H: RaftRpcHandler>(
        &self,
        handler: Arc<H>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        self.serve_with_identity(handler, Arc::clone(&self.identity_store), shutdown)
            .await
    }

    /// Run the inbound RPC accept loop with a caller-supplied identity store.
    ///
    /// For each incoming connection, spawns a task that accepts bidi streams
    /// and dispatches RPCs to the handler. The `shutdown` watch receiver is
    /// **cloned into every spawned child task** (per-connection and
    /// per-stream) so that a single `shutdown.send(true)` cancels every
    /// in-flight RPC at its next `.await` point and drops the handler Arc
    /// clones each task captured. Without this propagation, a shutdown of the
    /// top-level serve loop would leave grandchild tasks blocked forever on
    /// `quinn::Connection::accept_bi` / `quinn::RecvStream::read_exact`,
    /// pinning the handler Arc (and any redb file handles it holds) for the
    /// lifetime of the runtime.
    pub async fn serve_with_identity<H: RaftRpcHandler, S: PeerIdentityStore + ?Sized>(
        &self,
        handler: Arc<H>,
        identity_store: Arc<S>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        info!(node_id = self.node_id, addr = %self.local_addr(), "raft RPC server started");

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok(conn) => {
                            let peer = conn.remote_address();
                            debug!(%peer, "accepted raft connection");
                            let h = handler.clone();
                            let conn_shutdown = shutdown.clone();
                            let conn_auth = self.auth().clone();
                            let conn_id_store = identity_store.clone();
                            tokio::spawn(async move {
                                if let Err(e) =
                                    server::handle_connection(conn, h, conn_auth, conn_id_store, conn_shutdown)
                                        .await
                                {
                                    debug!(%peer, error = %e, "raft connection ended");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "raft accept failed");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(node_id = self.node_id, "raft RPC server shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
