// SPDX-License-Identifier: BUSL-1.1

//! Bind the MAC-authenticated sender of an inbound stream to its mTLS leaf
//! identity.

use tracing::{debug, warn};

use crate::error::{ClusterError, Result};
use crate::rpc_codec::RaftRpc;
use crate::transport::auth_context::AuthContext;
use crate::transport::identity_admission::enrollment_matches;
use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::peer_identity_verifier::{
    IDENTITY_MISMATCH_QUIC_ERROR, VerifyOutcome, verify_peer_identity,
};

/// Close `conn` for a peer identity mismatch and return the matching error.
pub(super) fn reject_peer_identity(conn: &quinn::Connection, node_id: u64) -> Result<()> {
    warn!(node_id, "peer identity mismatch — closing connection");
    conn.close(IDENTITY_MISMATCH_QUIC_ERROR, b"peer identity mismatch");
    Err(ClusterError::Transport {
        detail: format!("peer identity mismatch for node {node_id}"),
    })
}

/// Check that `from_node_id` matches the leaf certificate `peer_cert_der`.
///
/// Self-addressed frames and stores that do not enforce peer identity pass.
/// An unknown identity may submit only a `JoinRequest` whose node id and
/// advertised pins match that leaf. Every other RPC fails closed until the
/// join is visible in topology. A mismatch closes `conn`.
pub(super) fn verify_stream_identity<S: PeerIdentityStore + ?Sized>(
    conn: &quinn::Connection,
    identity_store: &S,
    peer_cert_der: Option<&[u8]>,
    auth: &AuthContext,
    from_node_id: u64,
    request: &RaftRpc,
) -> Result<()> {
    if from_node_id == auth.local_node_id || !identity_store.enforces_peer_identity() {
        return Ok(());
    }
    let cert_der = peer_cert_der.ok_or_else(|| ClusterError::Transport {
        detail: "authenticated cluster peer did not present a leaf certificate".into(),
    })?;
    match identity_store.get_node_info(from_node_id) {
        Some(ref info) => match verify_peer_identity(info, cert_der) {
            VerifyOutcome::Accepted { method } => {
                debug!(node_id = from_node_id, ?method, "peer identity verified");
                Ok(())
            }
            VerifyOutcome::Rejected => reject_peer_identity(conn, from_node_id),
        },
        None if enrollment_matches(request, from_node_id, cert_der, identity_store) => {
            debug!(
                node_id = from_node_id,
                "accepted identity-bound cluster join enrollment"
            );
            Ok(())
        }
        None => reject_peer_identity(conn, from_node_id),
    }
}
