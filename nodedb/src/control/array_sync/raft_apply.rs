// SPDX-License-Identifier: BUSL-1.1

//! Array CRDT apply helpers invoked by the distributed Raft apply loop.
//!
//! These run on the Control Plane after Raft commit. They decode the replicated
//! entry, dispatch the resulting Data Plane plan via SPSC, and update the
//! authoritative op-log / schema registry. See [`crate::control::distributed_applier`]
//! for the loop that calls these.

use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::bridge::envelope::{Priority, Request, Response, Status};

/// Identifies a committed Raft entry within the apply loop.
///
/// Groups the three fields that always travel together: the Raft group, the
/// log index within that group, and the idempotency key extracted from the
/// `ReplicatedEntry` header. All three are forwarded together to
/// `ProposeTracker::complete` after each apply.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AppliedPosition {
    pub group_id: u64,
    pub log_index: u64,
    pub applied_key: u64,
}
use crate::control::array_sync::OriginApplyEngine;
use crate::control::distributed_applier::{ProposeResult, ProposeTracker};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TraceId};

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
    use crate::types::{TenantId, VShardId};
    use nodedb_array::sync::op_codec;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_cluster::array_routing::{array_vshard_for_name, vshard_for_array_coord};

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
    let tile_extents = state.array_sync_schemas.tile_extents(&op.header.array);
    let vshard = if let Some(extents) = tile_extents {
        let coord_u64: Vec<u64> = op
            .coord
            .iter()
            .map(|c| match c {
                CoordValue::Int64(v) | CoordValue::TimestampMs(v) => *v as u64,
                CoordValue::Float64(v) => v.to_bits(),
                CoordValue::String(_) => 0,
            })
            .collect();
        VShardId::new(vshard_for_array_coord(
            &op.header.array,
            &coord_u64,
            &extents,
        ))
    } else {
        VShardId::new(array_vshard_for_name(&op.header.array))
    };

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

/// Ensure the Data Plane has the array open before dispatching Put/Delete.
///
/// Looks up the catalog entry for `array_id.name`, then dispatches `OpenArray`
/// to the Data Plane. This is idempotent on the Data Plane side: if the array
/// is already open with the same schema hash, the handler returns `Ok`.
///
/// Returns an error if the catalog entry is missing (the array was never
/// registered on this node) or if the `OpenArray` dispatch fails.
async fn ensure_array_open(
    state: &Arc<SharedState>,
    array_id: &nodedb_array::types::ArrayId,
    vshard: crate::types::VShardId,
    tenant_id: crate::types::TenantId,
) -> crate::Result<()> {
    let (schema_msgpack, schema_hash, prefix_bits) = {
        let cat = state
            .array_catalog
            .read()
            .unwrap_or_else(|p| p.into_inner());
        match cat.lookup_by_name(&array_id.name) {
            Some(entry) => (
                entry.schema_msgpack.clone(),
                entry.schema_hash,
                entry.prefix_bits,
            ),
            None => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "ensure_array_open: array '{}' not in catalog — register it before applying ops",
                        array_id.name
                    ),
                });
            }
        }
    };

    let open_plan = crate::bridge::envelope::PhysicalPlan::Array(
        nodedb_physical::physical_plan::ArrayOp::OpenArray {
            array_id: array_id.clone(),
            schema_msgpack,
            schema_hash,
            prefix_bits,
        },
    );
    let open_request = build_array_request(state, tenant_id, vshard, open_plan);
    let open_request_id = open_request.request_id;
    let mut open_rx = state.tracker.register(open_request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(open_request),
        Err(poisoned) => poisoned.into_inner().dispatch(open_request),
    };

    if let Err(e) = dispatch_result {
        return Err(crate::Error::Internal {
            detail: format!("ensure_array_open: dispatch failed: {e}"),
        });
    }

    await_data_plane(async move { open_rx.recv().await.ok_or(()) }, "OpenArray")
        .await
        .map(|_| ())
}

/// Payload extracted from a `ReplicatedWrite::ArraySchema` entry.
pub(crate) struct ArraySchemaPayload<'a> {
    pub array: &'a str,
    pub snapshot_payload: &'a [u8],
    pub schema_hlc_bytes: [u8; 18],
}

/// Apply a committed `ArraySchema` entry on the local node.
///
/// 1. Imports the Loro snapshot into the local `OriginSchemaRegistry`.
/// 2. Decodes the `ArraySchema` and registers an `ArrayCatalogEntry` so the
///    Data Plane can open the array when a subsequent `ArrayOp` arrives.
///    This is the canonical DDL propagation path for followers: the Raft
///    `ArraySchema` entry is the single source of truth — no out-of-band
///    catalog registration is needed.
///
/// Returns `true` when the schema snapshot was durably imported, `false` when
/// the import failed. The caller uses this to gate Raft log compaction.
pub(crate) fn apply_array_schema(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    payload: ArraySchemaPayload<'_>,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;
    use nodedb_array::sync::hlc::Hlc;

    let ArraySchemaPayload {
        array,
        snapshot_payload,
        schema_hlc_bytes,
    } = payload;
    let remote_hlc = Hlc::from_bytes(&schema_hlc_bytes);

    // Use the replicated import path so every replica converges to the same
    // schema_hlc (the one committed in the Raft log entry) rather than each
    // bumping independently via their local HLC generator.
    if let Err(e) =
        state
            .array_sync_schemas
            .import_snapshot_replicated(array, snapshot_payload, remote_hlc)
    {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: import_snapshot_replicated failed"
        );
        tracker.complete(
            group_id,
            log_index,
            applied_key,
            Err(crate::Error::Internal {
                detail: format!("schema import: {e}"),
            }),
        );
        return false;
    }

    // Decode the ArraySchema from the just-imported Loro document and register
    // it in the array catalog so the Data Plane can open the array on this
    // node. Shared with the single-node direct-import path in `inbound.rs`
    // via `catalog_register::register_array_catalog_entry` so both codepaths
    // converge on the same catalog-visibility guarantee.
    //
    // Warn-and-continue: the schema snapshot import above already committed
    // durably and this apply loop has no fail-back path (the caller only
    // gates Raft log compaction on our `bool`, not correctness of catalog
    // state). A missing entry here is caught by the next `ensure_array_open`
    // lookup failure or by drift detection, not by re-running Raft apply.
    if let Err(e) = super::catalog_register::register_array_catalog_entry(state, array) {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: register_array_catalog_entry failed (non-fatal)"
        );
    }

    tracker.complete(group_id, log_index, applied_key, Ok(vec![]));
    true
}

/// Build a `Request` for an array apply/open with default deadline / priority.
///
/// Centralises the six boilerplate fields that are identical for every
/// Control-Plane → Data-Plane dispatch originating from the array apply path.
fn build_array_request(
    state: &Arc<SharedState>,
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Request {
    Request {
        request_id: state.next_request_id(),
        tenant_id,
        database_id: DatabaseId::DEFAULT,
        vshard_id,
        plan,
        deadline: std::time::Instant::now() + Duration::from_secs(30),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::CrdtSync,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    }
}

/// Await a Data Plane response, mapping timeout / channel-closed / error-status
/// into `crate::Error::Internal` with a contextual `op_label`.
async fn await_data_plane(
    rx: impl std::future::Future<Output = Result<Response, ()>>,
    op_label: &str,
) -> ProposeResult {
    match tokio::time::timeout(Duration::from_secs(30), rx).await {
        Ok(Ok(resp)) if resp.status == Status::Ok => Ok(resp.payload.to_vec()),
        Ok(Ok(resp)) => {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{op_label} error: {c:?}"))
                .unwrap_or_else(|| format!("{op_label} returned error status"));
            Err(crate::Error::Internal { detail })
        }
        Ok(Err(_)) => Err(crate::Error::Internal {
            detail: format!("{op_label}: response channel closed"),
        }),
        Err(_) => Err(crate::Error::Internal {
            detail: format!("{op_label}: deadline exceeded"),
        }),
    }
}
