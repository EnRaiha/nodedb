// SPDX-License-Identifier: BUSL-1.1

//! Authorize, dispatch, and merge one clone CoW read: the augmented set
//! (target task + every source-side task the chain walk produced) is
//! authorized together — the source tasks are derived here and were never
//! part of the caller's own pre-authorized task list — dispatched to the
//! Data Plane, then merged into one `Response` with tombstoned source rows
//! filtered out.

use crate::bridge::envelope::Response;
use crate::control::security::audit::AuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::server::dispatch_utils::{dispatch_to_data_plane, reject_data_plane_error};
use crate::control::server::response_shape::kv::apply_kv_wrap;
use crate::control::server::response_shape::types::{PlanKind, describe_plan};
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::merge::{
    filter_kv_tombstoned_rows, merge_msgpack_arrays, unwrap_single_row, wrap_single_map_as_array,
};
use crate::control::clone::resolver::filter_tombstoned_rows;

/// Inputs for [`dispatch_augmented`] — one struct because the augmented
/// read needs everything the authorizer and the dispatcher each need, which
/// exceeds a readable positional argument count.
pub(super) struct DispatchAugmentedParams<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    pub emitter: &'a dyn AuditEmitter,
    pub target_task: PhysicalTask,
    pub source_tasks: Vec<PhysicalTask>,
    pub target_collection_key: &'a str,
}

/// Authorize, dispatch, and merge `target_task` with every task in
/// `source_tasks`, filtering rows tombstoned or superseded-by-copy-up in
/// `target_collection_key`.
pub(super) async fn dispatch_augmented(
    params: DispatchAugmentedParams<'_>,
) -> crate::Result<Response> {
    let DispatchAugmentedParams {
        state,
        identity,
        permissions,
        roles,
        emitter,
        target_task,
        source_tasks,
        target_collection_key,
    } = params;
    // The caller's own shaping keys on this same classifier and expects a
    // bare row for `SingleDocument` — the merge below works in arrays.
    let target_plan_kind = describe_plan(&target_task.plan);

    let mut augmented: Vec<PhysicalTask> = Vec::with_capacity(1 + source_tasks.len());
    augmented.push(target_task.clone());
    augmented.extend(source_tasks.iter().cloned());
    let _authorized_augmented =
        authorize_task_set(identity, &augmented, permissions, roles, emitter)
            .map_err(crate::Error::from)?;

    let trace_id = TraceId::ZERO;

    let target_resp = dispatch_one(state, target_task, trace_id).await?;
    reject_data_plane_error(&target_resp)?;

    // Suppressed source surrogates: tombstones plus copy-ups (a copy-up
    // leaves the superseded source row in place, so merging it back would
    // double it). Each catalog handle is a temporary, dropped before the
    // next `.await`, so nothing non-`Send` is held across a suspend point.
    let mut tombstoned = state
        .credentials
        .catalog()
        .list_clone_tombstones(target_collection_key)?;
    tombstoned.extend(
        state
            .credentials
            .catalog()
            .list_clone_copyups(target_collection_key)?,
    );
    let kv_tombstoned = state
        .credentials
        .catalog()
        .list_kv_clone_tombstones(target_collection_key)?;

    let mut merged_payload = wrap_single_map_as_array(target_resp.payload.as_ref().to_vec());

    for source_task in source_tasks {
        if point_read_surrogate(&source_task.plan).is_some_and(|s| tombstoned.contains(&s)) {
            continue;
        }
        let source_resp = dispatch_one(state, source_task.clone(), trace_id).await?;
        reject_data_plane_error(&source_resp)?;

        // KvOp::Get: inject the primary key field for projection/column checks.
        let normalized_payload = apply_kv_wrap(&source_task.plan, source_resp.payload.as_ref());
        // KvOp::Get responses arrive as a single map; normalize to a
        // 1-element array so tombstone filters and merge work uniformly.
        let normalized_payload = wrap_single_map_as_array(normalized_payload);

        // Post-normalization the input is always a valid array, so `None`
        // signals upstream corruption — log and pass through unchanged.
        let source_payload = match filter_tombstoned_rows(&normalized_payload, &tombstoned) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    payload_len = normalized_payload.len(),
                    "clone read: filter_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                );
                normalized_payload
            }
        };
        let source_payload = if kv_tombstoned.is_empty() {
            source_payload
        } else {
            match filter_kv_tombstoned_rows(&source_payload, &kv_tombstoned) {
                Some(p) => p,
                None => {
                    tracing::warn!(
                        payload_len = source_payload.len(),
                        "clone read: filter_kv_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                    );
                    source_payload
                }
            }
        };

        merged_payload = merge_msgpack_arrays(&merged_payload, &source_payload)?;
    }

    // A point read merges to at most one row; the target's row (pushed
    // first) wins if somehow more than one survived.
    if matches!(target_plan_kind, PlanKind::SingleDocument) {
        merged_payload = unwrap_single_row(merged_payload);
    }

    Ok(Response {
        payload: merged_payload.into(),
        ..target_resp
    })
}

/// Build a synthetic empty `Response` for a query whose time predates the
/// clone's creation.
pub(super) fn empty_response(state: &SharedState) -> Response {
    use crate::bridge::envelope::{Payload, Status};
    Response {
        request_id: state.next_request_id(),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::empty(),
        watermark_lsn: crate::types::Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

async fn dispatch_one(
    state: &SharedState,
    task: PhysicalTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    dispatch_to_data_plane(
        state,
        task.tenant_id,
        task.database_id,
        task.vshard_id,
        task.plan,
        trace_id,
    )
    .await
}

/// The source surrogate a rewritten document point-read fetches.
/// A point-get answers with the row body alone, no surrogate — deciding
/// suppression from the plan keeps it consistent with scans.
fn point_read_surrogate(plan: &PhysicalPlan) -> Option<u32> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { surrogate, .. }) => Some(surrogate.as_u32()),
        _ => None,
    }
}
