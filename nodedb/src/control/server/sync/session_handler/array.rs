// SPDX-License-Identifier: BUSL-1.1

//! Array-sync frame handling for a Lite WebSocket session.
//!
//! Builds the per-session inbound array engine and routes inbound array
//! frames (deltas, snapshots, schema, acks, catchup) to it. The inbound
//! engine applies them and may return a reject frame to send back to the
//! client.

use std::sync::Arc;

use tracing::warn;

use nodedb_types::sync::wire::array::{
    ArrayAckMsg, ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg,
    ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};

use super::super::wire::{SyncFrame, SyncMessageType};
use crate::control::state::SharedState;

/// Build the per-session inbound array engine bound to `tenant_id`, or `None`
/// when `SharedState` is absent (the no-op listener path used in tests).
///
/// `tenant_id` MUST be the session's handshake-authenticated tenant: the
/// inbound engine stamps it onto every replicated array write (Raft-log
/// routing) and the fan-out uses it to match subscriber shapes. The caller
/// therefore builds this lazily, only after authentication — building it under
/// a placeholder tenant would misroute every inbound array delta.
pub(super) fn build_array_inbound(
    shared: &Option<Arc<SharedState>>,
    tenant_id: crate::types::TenantId,
) -> Option<Arc<crate::control::array_sync::OriginArrayInbound>> {
    shared.as_ref().map(|s| {
        let engine = Arc::new(crate::control::array_sync::OriginApplyEngine::new(
            Arc::clone(&s.array_sync_schemas),
            Arc::clone(&s.array_sync_op_log),
        ));
        let fanout = Arc::new(crate::control::array_sync::ArrayFanout::new(
            Arc::clone(&s.shape_registry),
            Arc::clone(&s.array_delivery),
            Arc::clone(&s.array_subscriber_cursors),
            Arc::clone(&s.array_snapshot_hlcs),
            Arc::clone(&s.array_merger_registry),
            0,
            tenant_id.as_u64(),
        ));
        let inbound = crate::control::array_sync::OriginArrayInbound::new(
            engine,
            Arc::clone(&s.array_sync_schemas),
            Arc::clone(s),
            tenant_id,
        )
        .with_observer(fanout);
        Arc::new(inbound)
    })
}

/// True for the array message types this session routes to the inbound engine.
pub(super) fn is_array_frame(msg_type: SyncMessageType) -> bool {
    matches!(
        msg_type,
        SyncMessageType::ArrayDelta
            | SyncMessageType::ArrayDeltaBatch
            | SyncMessageType::ArraySnapshot
            | SyncMessageType::ArraySnapshotChunk
            | SyncMessageType::ArraySchema
            | SyncMessageType::ArrayAck
            | SyncMessageType::ArrayReject
            | SyncMessageType::ArrayCatchupRequest
    )
}

/// Route one inbound array frame to the inbound engine, returning a reject
/// frame to send back when the engine rejects the operation.
pub(super) async fn dispatch_array_frame(
    frame: &SyncFrame,
    inbound: &crate::control::array_sync::OriginArrayInbound,
    session_id: &str,
) -> Option<SyncFrame> {
    match frame.msg_type {
        SyncMessageType::ArrayDelta => {
            if let Some(msg) = frame.decode_body::<ArrayDeltaMsg>() {
                match inbound.handle_delta(&msg).await {
                    Ok(_) => None,
                    Err(Some(r)) => SyncFrame::try_encode(SyncMessageType::ArrayReject, &r),
                    Err(None) => None,
                }
            } else {
                None
            }
        }
        SyncMessageType::ArrayDeltaBatch => {
            if let Some(msg) = frame.decode_body::<ArrayDeltaBatchMsg>() {
                let outcomes = inbound.handle_delta_batch(&msg).await;
                outcomes.into_iter().find_map(|r| match r {
                    Err(Some(reject)) => {
                        SyncFrame::try_encode(SyncMessageType::ArrayReject, &reject)
                    }
                    _ => None,
                })
            } else {
                None
            }
        }
        SyncMessageType::ArraySnapshot => {
            if let Some(msg) = frame.decode_body::<ArraySnapshotMsg>() {
                match inbound.handle_snapshot_header(&msg) {
                    Ok(_) => None,
                    Err(Some(r)) => SyncFrame::try_encode(SyncMessageType::ArrayReject, &r),
                    Err(None) => None,
                }
            } else {
                None
            }
        }
        SyncMessageType::ArraySnapshotChunk => {
            if let Some(msg) = frame.decode_body::<ArraySnapshotChunkMsg>() {
                match inbound.handle_snapshot_chunk(&msg).await {
                    Ok(_) => None,
                    Err(Some(r)) => SyncFrame::try_encode(SyncMessageType::ArrayReject, &r),
                    Err(None) => None,
                }
            } else {
                None
            }
        }
        SyncMessageType::ArraySchema => {
            if let Some(msg) = frame.decode_body::<ArraySchemaSyncMsg>() {
                match inbound.handle_schema(&msg).await {
                    Ok(_) => None,
                    Err(Some(r)) => SyncFrame::try_encode(SyncMessageType::ArrayReject, &r),
                    Err(None) => None,
                }
            } else {
                None
            }
        }
        SyncMessageType::ArrayAck => {
            if let Some(msg) = frame.decode_body::<ArrayAckMsg>() {
                let _ = inbound.handle_ack(&msg);
            }
            None
        }
        SyncMessageType::ArrayCatchupRequest => {
            if let Some(msg) = frame.decode_body::<ArrayCatchupRequestMsg>() {
                let _ = inbound.handle_catchup_request(&msg, session_id);
            }
            None
        }
        SyncMessageType::ArrayReject => {
            if let Some(msg) = frame.decode_body::<ArrayRejectMsg>() {
                warn!(
                    session = %session_id,
                    array = %msg.array,
                    reason = ?msg.reason,
                    "sync: received ArrayReject (outbound-only); ignoring"
                );
            }
            None
        }
        _ => None,
    }
}
