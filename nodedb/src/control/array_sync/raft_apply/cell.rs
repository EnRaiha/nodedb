// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed Raft-native array cell write (`ArrayCellPut` /
//! `ArrayCellDelete`) on the local node.
//!
//! This is the apply half of the cluster SQL DML array path: the owner
//! proposed `ReplicatedWrite::ArrayCellPut` / `ArrayCellDelete` to the shard's
//! data Raft group; every replica (the proposer included) lands here after
//! commit. Unlike the plain committed-write path, an array Put/Delete requires
//! the Data Plane to have the array OPEN first, so this reuses the array-open
//! bootstrap in [`super::common`] before dispatching.
//!
//! Distinct from [`super::op::apply_array_op`], which applies a single Lite-sync
//! CRDT op through the array-sync op-log / HLC dedup. Here idempotency is the
//! Raft log's exactly-once ordering plus the array engine's coord-keyed
//! overwrite semantics (re-applying a Put re-writes the identical cell; a
//! Delete of an absent coord is a no-op), so no op-log entry is recorded.

use std::sync::Arc;

use tracing::warn;

use super::common::{AppliedPosition, await_data_plane, build_array_request, ensure_array_open};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::distributed_applier::ProposeTracker;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

/// Apply a decoded array cell write plan (`PhysicalPlan::Array(Put | Delete)`)
/// on the local node.
///
/// `vshard` is the owning shard's vShard, carried verbatim in the committed
/// `ReplicatedEntry` header (set by the proposer from the array's Hilbert-tile
/// placement) — the same group every replica of this shard applies from.
///
/// Returns `true` when the write durably applied, `false` on any
/// open/dispatch/apply failure. The caller gates Raft log compaction on this.
pub(crate) async fn apply_array_cell_write(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    tenant_id: TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;

    // The caller (the distributed apply loop) only routes decoded
    // `ArrayOp::Put` / `Delete` plans here, so this match is exhaustive in
    // practice; the guard arm turns a dispatch bug into a loud error rather
    // than a silent mis-apply.
    let array_id = match &plan {
        PhysicalPlan::Array(ArrayOp::Put { array_id, .. })
        | PhysicalPlan::Array(ArrayOp::Delete { array_id, .. }) => array_id.clone(),
        other => {
            let e = crate::Error::Internal {
                detail: format!(
                    "apply_array_cell_write called with a non-array-cell plan: {other:?}"
                ),
            };
            tracker.complete(group_id, log_index, applied_key, Err(e));
            return false;
        }
    };

    // A follower must have the array open on the Data Plane before a Put/Delete
    // can land. Idempotent on the Data Plane side (re-open with the same schema
    // hash returns Ok).
    if let Err(e) = ensure_array_open(state, &array_id, vshard, tenant_id).await {
        warn!(
            group_id, index = log_index, array = %array_id.name, error = %e,
            "apply_array_cell_write: ensure_array_open failed"
        );
        tracker.complete(group_id, log_index, applied_key, Err(e));
        return false;
    }

    let request = build_array_request(state, tenant_id, vshard, plan);
    let request_id = request.request_id;
    let mut rx = state.tracker.register(request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request),
        Err(poisoned) => poisoned.into_inner().dispatch(request),
    };

    if let Err(e) = dispatch_result {
        warn!(group_id, index = log_index, error = %e, "apply_array_cell_write: dispatch failed");
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

    match await_data_plane(async move { rx.recv().await.ok_or(()) }, "array cell write").await {
        Ok(payload) => {
            tracker.complete(group_id, log_index, applied_key, Ok(payload));
            true
        }
        Err(e) => {
            tracker.complete(group_id, log_index, applied_key, Err(e));
            false
        }
    }
}
