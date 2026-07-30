// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns a full [`Response`].
//!
//! Used by the columnar, timeseries, FTS, spatial, and vector sync handlers,
//! which need the raw `Response` to extract the payload themselves.

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::control::wal_replication::to_replicated_entry;
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::propose::propose_sync_write;

pub async fn dispatch_authorized_sync_response(
    state: &SharedState,
    authorized: AuthorizedTask,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    dispatch_sync_response_inner(
        state,
        task.tenant_id,
        task.database_id,
        task.vshard_id,
        task.plan,
        trace_id,
        event_source,
    )
    .await
}

/// Trusted-internal sync-shaped dispatch used by DDL index maintenance.
pub(crate) async fn dispatch_trusted_internal_sync_response(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    dispatch_sync_response_inner(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
    )
    .await
}

/// Cluster path: proposes through Raft, then wraps the apply payload in a
/// `Status::Ok` `Response`. The gate verdict is carried in the payload (as a
/// zerompk-encoded `SyncAckResult`); `Status::Ok` is always correct here
/// because a non-`Ok` status signals a protocol error, not an idempotency
/// gate rejection.
///
/// Single-node path: falls through to
/// [`crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source`].
async fn dispatch_sync_response_inner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    reject_unadmitted_crdt_apply(&plan)?;
    if let Some(proposer) = state.async_raft_proposer()
        && let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &plan)
    {
        let payload = propose_sync_write(state, entry, proposer).await?;
        let request_id = state.next_request_id();
        return Ok(Response {
            request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::new(0),
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        });
    }

    crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
    )
    .await
}

/// Sync-path convenience over authorized sync dispatch: dispatches `plan`
/// tagged [`EventSource::CrdtSync`] (so AFTER triggers are not re-fired on
/// synced data) with a zero trace id, and returns just the apply-payload
/// bytes — which carry the zerompk-encoded `SyncAckResult` the per-engine
/// handlers decode for the gate verdict.
///
/// Every `SharedState*Dispatcher` funnels through here so the dispatch policy
/// (event source, trace id, payload extraction) lives in exactly one place.
pub async fn dispatch_sync_payload(
    state: &SharedState,
    authorized: AuthorizedTask,
) -> crate::Result<Vec<u8>> {
    let response =
        dispatch_authorized_sync_response(state, authorized, TraceId::ZERO, EventSource::CrdtSync)
            .await?;
    Ok(response.payload.to_vec())
}

/// Build the loud error every `NoOp*Dispatcher` returns when a sync op reaches
/// a path that lacks `SharedState`.
///
/// Such a path would ACK the Lite client while silently dropping the write, so
/// the dispatcher fails loudly instead of no-op'ing. `op` names the operation
/// for the diagnostic, e.g. `"vector insert"` or `"timeseries push"`.
pub fn noop_dispatch_error(op: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "{op} routed through path lacking SharedState; \
             check listener wiring — {op} was ACKed but NOT applied"
        ),
    }
}
