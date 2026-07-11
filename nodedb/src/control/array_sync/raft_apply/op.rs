// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed `ArrayOp` entry on the local node.

use std::sync::Arc;

use tracing::warn;

use super::common::{
    AppliedPosition, await_data_plane, build_array_request, ensure_array_open, vshard_for_array_op,
};
use crate::control::array_sync::OriginApplyEngine;
use crate::control::distributed_applier::ProposeTracker;
use crate::control::state::SharedState;

/// Apply a committed `ArrayOp` entry on the local node.
///
/// Decodes the op, dispatches it to the Data Plane via SPSC, and records it
/// in the op-log so future `already_seen` checks return `true`. This is the
/// authoritative idempotency gate — it runs on every replica after Raft commit.
///
/// Returns `true` when the op was durably applied (or was already applied via
/// the idempotency gate), `false` on any decode/dispatch/apply failure. The
/// caller uses this to gate Raft log compaction on a safe applied watermark.
pub(crate) async fn apply_array_op(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    array: &str,
    op_bytes: &[u8],
    provenance_bytes: Option<&[u8]>,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;
    use crate::types::TenantId;
    use nodedb_array::sync::op_codec;

    // Decode the replicated provenance so the epoch fence runs on this replica
    // exactly as it did on the node that first received the op. Absent
    // provenance (`None`) is normal for non-sync array ops. Provenance bytes
    // that are present but fail to decode signal version skew or corrupt
    // replicated state: the epoch fence cannot run, but the engine's HLC
    // `already_seen` dedup is still authoritative for idempotency, so we apply
    // without a fence rather than poison the entry — and surface it loudly.
    let provenance: Option<nodedb_types::sync::wire::SyncProvenance> = match provenance_bytes {
        None => None,
        Some(b) => match zerompk::from_msgpack::<nodedb_types::sync::wire::SyncProvenance>(b) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(
                    group_id, index = log_index, array = %array, error = %e,
                    "apply_array_op: provenance decode failed; applying without epoch fence (version skew or corruption)"
                );
                None
            }
        },
    };

    let op = match op_codec::decode_op(op_bytes) {
        Ok(op) => op,
        Err(e) => {
            warn!(
                group_id, index = log_index, array = %array, error = %e,
                "apply_array_op: decode failed"
            );
            tracker.complete(
                group_id,
                log_index,
                applied_key,
                Err(crate::Error::Internal {
                    detail: format!("array op decode: {e}"),
                }),
            );
            return false;
        }
    };

    // Authoritative idempotency check: if already applied, skip Data Plane
    // dispatch and return success so the proposer waiter is unblocked.
    let engine = OriginApplyEngine::new(
        Arc::clone(&state.array_sync_schemas),
        Arc::clone(&state.array_sync_op_log),
    );
    if engine.already_seen(&op.header.array, op.header.hlc) {
        tracker.complete(group_id, log_index, applied_key, Ok(vec![]));
        return true;
    }

    // Compute vshard for dispatch.
    let vshard = vshard_for_array_op(state, &op);

    // Build Data Plane plan.
    use nodedb_array::sync::op::ArrayOpKind;
    use nodedb_physical::physical_plan::ArrayOp as DataArrayOp;

    let tenant_id = TenantId::new(0); // array ops are tenant-0 at the sync layer
    let array_id = nodedb_array::types::ArrayId::new(tenant_id, &op.header.array);

    // Ensure the Data Plane has opened this array before we try to Put/Delete.
    // The Data Plane `ArrayEngine` requires an explicit `OpenArray` dispatch
    // before any write; the catalog entry carries all required schema info.
    if let Err(e) = ensure_array_open(state, &array_id, vshard, tenant_id).await {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_op: ensure_array_open failed"
        );
        tracker.complete(group_id, log_index, applied_key, Err(e));
        return false;
    }

    let data_op = match op.kind {
        ArrayOpKind::Put => {
            let cells = vec![crate::engine::array::wal::ArrayPutCell {
                coord: op.coord.clone(),
                attrs: op.attrs.clone().unwrap_or_default(),
                surrogate: nodedb_types::Surrogate::ZERO,
                system_from_ms: op.header.system_from_ms,
                valid_from_ms: op.header.valid_from_ms,
                valid_until_ms: op.header.valid_until_ms,
            }];
            let cells_msgpack = match zerompk::to_msgpack_vec(&cells) {
                Ok(b) => b,
                Err(e) => {
                    warn!(group_id, index = log_index, error = %e, "apply_array_op: cells encode failed");
                    tracker.complete(
                        group_id,
                        log_index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: format!("cells encode: {e}"),
                        }),
                    );
                    return false;
                }
            };
            DataArrayOp::Put {
                array_id,
                cells_msgpack,
                wal_lsn: 0,
                provenance: provenance.clone(),
            }
        }
        ArrayOpKind::Delete | ArrayOpKind::Erase => {
            let coords = vec![op.coord.clone()];
            let coords_msgpack = match zerompk::to_msgpack_vec(&coords) {
                Ok(b) => b,
                Err(e) => {
                    warn!(group_id, index = log_index, error = %e, "apply_array_op: coords encode failed");
                    tracker.complete(
                        group_id,
                        log_index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: format!("coords encode: {e}"),
                        }),
                    );
                    return false;
                }
            };
            DataArrayOp::Delete {
                array_id,
                coords_msgpack,
                wal_lsn: 0,
                provenance,
            }
        }
    };

    let plan = crate::bridge::envelope::PhysicalPlan::Array(data_op);
    let request = build_array_request(state, tenant_id, vshard, plan);
    let request_id = request.request_id;
    let mut rx = state.tracker.register(request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request),
        Err(poisoned) => poisoned.into_inner().dispatch(request),
    };

    if let Err(e) = dispatch_result {
        warn!(group_id, index = log_index, error = %e, "apply_array_op: dispatch failed");
        tracker.complete(
            group_id,
            log_index,
            applied_key,
            Err(crate::Error::Internal {
                detail: format!("dispatch: {e}"),
            }),
        );
        return false;
    }

    let result = await_data_plane(async move { rx.recv().await.ok_or(()) }, "array op").await;
    match result {
        Ok(payload) => {
            // Record applied — authoritative idempotency entry.
            if let Err(e) = engine.record_applied(&op) {
                tracing::error!(
                    group_id, index = log_index, array = %op.header.array,
                    error = %e,
                    "apply_array_op: op applied but op-log append failed"
                );
            }
            tracker.complete(group_id, log_index, applied_key, Ok(payload));
            true
        }
        Err(e) => {
            tracker.complete(group_id, log_index, applied_key, Err(e));
            false
        }
    }
}
