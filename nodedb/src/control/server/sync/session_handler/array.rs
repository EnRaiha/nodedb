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
    ArrayRejectReason, ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
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
    identity: crate::control::security::identity::AuthenticatedIdentity,
) -> Option<Arc<crate::control::array_sync::OriginArrayInbound>> {
    let tenant_id = identity.tenant_id;
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
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
            crate::control::server::sync::shape::ShapeScope {
                tenant_id: tenant_id.as_u64(),
                database_id,
            },
        ));
        let inbound = crate::control::array_sync::OriginArrayInbound::new(
            engine,
            Arc::clone(&s.array_sync_schemas),
            Arc::clone(s),
            identity,
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

/// Return a bounded rejection when an array frame lacks the current
/// authenticated handshake binding.
///
/// The caller invokes this before constructing or reusing an inbound engine,
/// so an unauthenticated frame cannot affect catalog, data, or fan-out state.
pub(super) fn unauthenticated_array_reject(
    frame: &SyncFrame,
    authenticated: bool,
    identity_present: bool,
) -> Option<SyncFrame> {
    if !is_array_frame(frame.msg_type) || (authenticated && identity_present) {
        return None;
    }

    let reject = ArrayRejectMsg {
        // Do not reflect untrusted frame contents into the response: this
        // keeps the rejection bounded even for a deliberately oversized body.
        array: String::new(),
        op_hlc_bytes: [0; 18],
        reason: ArrayRejectReason::EngineRejected,
        detail: "unauthenticated array frame".into(),
    };
    SyncFrame::try_encode(SyncMessageType::ArrayReject, &reject)
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
    use crate::wal::WalManager;

    /// Regression guard for the tenant-isolation bug: `build_array_inbound`
    /// must bind the inbound array engine to the tenant it is GIVEN, never a
    /// hardcoded placeholder. The engine's tenant flows into every replicated
    /// array write (`ReplicatedEntry`) and into
    /// `ArrayId::in_database(tenant, database, array)`,
    /// so a reverted-to-0 tenant here silently routes one tenant's array
    /// writes under another. Building a real `SharedState` (no cluster) is
    /// enough — the tenant threading is entirely local to this function.
    #[tokio::test]
    async fn build_array_inbound_binds_given_tenant_not_placeholder() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).unwrap();

        // A deliberately non-zero, non-default tenant so a placeholder-0
        // regression is unmistakable.
        let tenant = crate::types::TenantId::new(7);
        let identity = AuthenticatedIdentity::new_regular(
            7,
            "array-writer",
            tenant,
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        let inbound = build_array_inbound(&Some(shared), identity.clone())
            .expect("SharedState present => engine built");
        assert_eq!(
            inbound.tenant_id(),
            tenant,
            "inbound array engine must bind the session tenant, not a placeholder"
        );
        assert_eq!(
            inbound.database_id(),
            crate::types::DatabaseId::DEFAULT,
            "an identity without an explicit database remains compatible with default"
        );

        // No SharedState (the no-op listener path) => no engine.
        assert!(build_array_inbound(&None, identity).is_none());

        drop(dir);
    }

    #[tokio::test]
    async fn build_array_inbound_binds_non_default_session_database() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        let database_id = crate::types::DatabaseId::new(4_096);
        let identity = AuthenticatedIdentity::new_regular(
            8,
            "non-default-array-writer",
            crate::types::TenantId::new(8),
            AuthMethod::ApiKey,
            Vec::new(),
            Some(database_id),
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![database_id]),
        );

        let inbound = build_array_inbound(&Some(shared), identity)
            .expect("SharedState present => engine built");
        assert_eq!(inbound.database_id(), database_id);
    }

    #[test]
    fn unauthenticated_array_schema_and_delta_are_rejected_before_inbound_dispatch() {
        let delta = SyncFrame::try_encode(
            SyncMessageType::ArrayDelta,
            &ArrayDeltaMsg {
                array: "never-dispatched".into(),
                op_payload: vec![1, 2, 3],
                producer_id: 1,
                epoch: 1,
                seq: 1,
            },
        )
        .expect("encode delta");
        let schema = SyncFrame::try_encode(
            SyncMessageType::ArraySchema,
            &ArraySchemaSyncMsg {
                array: "never-dispatched".into(),
                replica_id: 1,
                schema_hlc_bytes: [1; 18],
                snapshot_payload: vec![1],
            },
        )
        .expect("encode schema");

        for frame in [&delta, &schema] {
            for (authenticated, identity_present) in [(false, false), (true, false)] {
                let reject = unauthenticated_array_reject(frame, authenticated, identity_present)
                    .expect("unauthenticated array frame must be rejected before inbound dispatch");
                assert_eq!(reject.msg_type, SyncMessageType::ArrayReject);
                let body: ArrayRejectMsg = reject.decode_body().expect("decode rejection");
                assert_eq!(body.reason, ArrayRejectReason::EngineRejected);
                assert_eq!(body.detail, "unauthenticated array frame");
                assert!(body.array.is_empty(), "rejection must remain bounded");
            }
            assert!(
                unauthenticated_array_reject(frame, true, true).is_none(),
                "a current authenticated identity must reach normal array dispatch"
            );
        }
    }
}
