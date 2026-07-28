// SPDX-License-Identifier: BUSL-1.1

//! Async Data-Plane dispatch for system-initiated and authorized work.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::system_task::SystemTask;

/// Send system-initiated work to the Data Plane and await its payload.
///
/// This is async — it yields the Tokio thread while waiting, so the response
/// poller can deliver the result without deadlocking.
pub(crate) async fn dispatch_system(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let tenant_id = task.tenant_id;
    let resp =
        dispatch_system_response_with_source(state, task, timeout, crate::event::EventSource::User)
            .await?;

    if resp.status != Status::Ok {
        // DDL/DSL callers receive the flattened message form. Callers that need
        // to classify the Data-Plane rejection by type use
        // `dispatch_system_response_with_source` and inspect `resp.error_code`.
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(crate::Error::Internal { detail });
    }

    // Advance the tenant's observed write-HLC high-water. Used by RESTORE to
    // reject stale envelopes. Tracking on every dispatch (not just known-write
    // ops) is intentional: advance is monotonic, and capturing the backup
    // envelope's watermark AFTER its own fan-out ensures envelope.wm >=
    // tenant_wm on a fresh backup (so a same-cluster roundtrip passes the
    // staleness gate). Reached only after the `resp.status != Ok` early-return
    // above, so this point is the "success" branch per the
    // advance_tenant_write_hlc contract.
    state.advance_tenant_write_hlc(tenant_id.as_u64());

    Ok(resp.payload.to_vec())
}

/// Send system-initiated work and await the full [`Response`], preserving the
/// typed [`crate::bridge::envelope::ErrorCode`] on a non-`Ok` status instead of
/// flattening it to a string.
///
/// Infrastructure failures (dispatch, timeout, channel close) still surface as
/// typed `Error` variants. Callers that must classify a Data-Plane rejection by
/// type (e.g. the CRDT sync delta path) use this and inspect `resp.error_code`;
/// [`dispatch_system`] wraps this and flattens the code to a message
/// for DDL/DSL callers. This function does **not** advance the tenant write-HLC
/// — the caller does that on its own success path.
pub(crate) async fn dispatch_system_response_with_source(
    state: &SharedState,
    task: SystemTask<'_>,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let vshard_id = VShardId::from_collection_in_database(task.database_id, task.collection);
    tracing::trace!(
        reason = task.reason.label(),
        collection = task.collection,
        "system-initiated data plane dispatch"
    );
    dispatch_plan(
        state,
        task.tenant_id,
        task.database_id,
        vshard_id,
        task.plan,
        timeout,
        event_source,
    )
    .await
}

/// Send already-authorized work to the Data Plane and await its payload.
///
/// The capability is consumed here: the plan that reaches storage is the plan
/// authorization approved, so a caller cannot authorize one shape and dispatch
/// another. Client-reachable paths use this rather than the system door.
pub(crate) async fn dispatch_authorized(
    state: &SharedState,
    authorized: AuthorizedTask,
    collection: &str,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let task = authorized.into_physical_task();
    let vshard_id = VShardId::from_collection_in_database(task.database_id, collection);
    let tenant_id = task.tenant_id;
    let resp = dispatch_plan(
        state,
        tenant_id,
        task.database_id,
        vshard_id,
        task.plan,
        timeout,
        crate::event::EventSource::User,
    )
    .await?;

    if resp.status != Status::Ok {
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(crate::Error::Internal { detail });
    }

    Ok(resp.payload.to_vec())
}

/// Shared transport: build the request envelope, dispatch, await the response.
async fn dispatch_plan(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let request_id = state.next_request_id();

    let request = Request {
        request_id,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        deadline: Instant::now() + timeout,
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };

    let mut rx = state.tracker.register(request_id);

    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request).map_err(|e| crate::Error::Internal {
            detail: e.to_string(),
        })?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| crate::Error::Internal {
                detail: e.to_string(),
            })?,
    };

    // Await with timeout — yields the thread so the response poller can run.
    tokio::time::timeout(timeout, async { rx.recv().await.ok_or(()) })
        .await
        .map_err(|_| crate::Error::Internal {
            detail: format!("dispatch timeout after {}ms", timeout.as_millis()),
        })?
        .map_err(|_| crate::Error::Internal {
            detail: "response channel closed".into(),
        })
}
