// SPDX-License-Identifier: BUSL-1.1

//! Advisory acknowledgement and catch-up handlers for array sync.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::{ArrayAckMsg, ArrayCatchupRequestMsg, ArrayRejectMsg};
use tracing::warn;

use super::catchup::OriginCatchupServer;
use super::inbound::{InboundOutcome, OriginArrayInbound};

impl OriginArrayInbound {
    /// Record a peer ack for GC frontier tracking.
    ///
    /// Forwards the ack into the `ArrayAckRegistry` on `SharedState` so the
    /// GC task can compute the min-ack frontier for each array.
    pub fn handle_ack(&self, msg: &ArrayAckMsg) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let ack_hlc = Hlc::from_bytes(&msg.ack_hlc_bytes);
        let replica_id = nodedb_array::sync::replica_id::ReplicaId::new(msg.replica_id);
        self.shared
            .array_ack_registry
            .record(&msg.array, replica_id, ack_hlc);
        tracing::debug!(
            array = %msg.array,
            replica_id = msg.replica_id,
            ack_hlc = ?ack_hlc,
            "array_inbound: peer ack recorded"
        );
        Ok(InboundOutcome::AckRecorded)
    }

    /// Handle a catch-up request from a Lite peer.
    ///
    /// Delegates to [`OriginCatchupServer`] which validates the array, selects
    /// the op-stream or snapshot delivery path, and enqueues outbound frames.
    pub fn handle_catchup_request(
        &self,
        msg: &ArrayCatchupRequestMsg,
        session_id: &str,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let server = OriginCatchupServer::new(
            Arc::clone(&self.shared.array_sync_op_log),
            Arc::clone(&self.schemas),
            Arc::clone(&self.shared.array_snapshot_store),
            Arc::clone(&self.shared.array_delivery),
            Arc::clone(&self.shared.array_subscriber_cursors),
            Arc::clone(&self.shared.array_ack_registry),
        );

        if let Err(error) = server.serve(msg, session_id) {
            warn!(
                session = %session_id,
                array = %msg.array,
                error = %error,
                "array_inbound: catchup server error"
            );
        }

        Ok(InboundOutcome::CatchupRequested)
    }
}
