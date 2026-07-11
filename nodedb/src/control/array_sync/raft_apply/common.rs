// SPDX-License-Identifier: BUSL-1.1

//! Shared scaffolding for the array Raft apply paths.
//!
//! Holds the pieces reused across every per-concern apply module (op, schema,
//! and the upcoming cell path): the committed-entry position identifier, the
//! Data-Plane `Request` builder, the response-await helper, the array-open
//! bootstrap, and the vShard derivation from an array op's coordinate (its
//! Hilbert-prefix tile placement).

use std::sync::Arc;
use std::time::Duration;

use crate::bridge::envelope::{Priority, Request, Response, Status};
use crate::control::distributed_applier::ProposeResult;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

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

/// Derive the dispatch vShard for an array op from its coordinate.
///
/// When the array has known tile extents the coordinate is mapped to its tile
/// via the Hilbert-prefix routing (`vshard_for_array_coord`); otherwise the
/// array name alone selects the vShard. Shared by every array-op apply path so
/// each concern (op / cell) routes identically.
pub(super) fn vshard_for_array_op(
    state: &Arc<SharedState>,
    op: &nodedb_array::sync::op::ArrayOp,
) -> VShardId {
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_cluster::array_routing::{array_vshard_for_name, vshard_for_array_coord};

    let tile_extents = state.array_sync_schemas.tile_extents(&op.header.array);
    if let Some(extents) = tile_extents {
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
pub(super) async fn ensure_array_open(
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

/// Build a `Request` for an array apply/open with default deadline / priority.
///
/// Centralises the six boilerplate fields that are identical for every
/// Control-Plane → Data-Plane dispatch originating from the array apply path.
pub(super) fn build_array_request(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    vshard_id: VShardId,
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
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    }
}

/// Await a Data Plane response, mapping timeout / channel-closed / error-status
/// into `crate::Error::Internal` with a contextual `op_label`.
pub(super) async fn await_data_plane(
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
