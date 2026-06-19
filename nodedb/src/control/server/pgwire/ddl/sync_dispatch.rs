// SPDX-License-Identifier: BUSL-1.1

//! Shared async dispatch helper for DDL and DSL handlers.
//!
//! Sends a [`PhysicalPlan`] to the Data Plane and awaits the response.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

/// Send `plan` to the Data Plane and await the response.
///
/// This is async — it yields the Tokio thread while waiting, so the
/// response poller can deliver the result without deadlocking.
pub async fn dispatch_async(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    dispatch_async_with_source(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        timeout,
        crate::event::EventSource::User,
    )
    .await
}

/// Send `plan` to the Data Plane with an explicit event source.
///
/// CRDT sync paths pass `EventSource::CrdtSync` so that the Data Plane
/// emits WriteEvents with the correct source tag — preventing the Event Plane
/// from firing triggers on replicated deltas.
pub async fn dispatch_async_with_source(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Vec<u8>> {
    let resp = dispatch_async_response_with_source(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        timeout,
        event_source,
    )
    .await?;

    if resp.status != Status::Ok {
        // DDL/DSL callers receive the flattened message form. Callers that need
        // to classify the Data-Plane rejection by type use
        // `dispatch_async_response_with_source` and inspect `resp.error_code`.
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(crate::Error::Internal { detail });
    }

    // Advance the tenant's observed write-HLC high-water. Used by
    // RESTORE to reject stale envelopes. Tracking on every dispatch
    // (not just known-write ops) is intentional: advance is
    // monotonic, and capturing the backup envelope's watermark AFTER
    // its own fan-out ensures envelope.wm ≥ tenant_wm on a fresh
    // backup (so a same-cluster roundtrip passes the staleness gate).
    // Reached only after the `resp.status != Ok` early-return above, so
    // this point is the "success" branch per the advance_tenant_write_hlc
    // contract.
    state.advance_tenant_write_hlc(tenant_id.as_u64());

    Ok(resp.payload.to_vec())
}

/// Send `plan` to the Data Plane and await the full [`Response`], preserving the
/// typed [`crate::bridge::envelope::ErrorCode`] on a non-`Ok` status instead of
/// flattening it to a string.
///
/// Infrastructure failures (dispatch, timeout, channel close) still surface as
/// typed `Error` variants. Callers that must classify a Data-Plane rejection by
/// type (e.g. the CRDT sync delta path) use this and inspect `resp.error_code`;
/// [`dispatch_async_with_source`] wraps this and flattens the code to a message
/// for DDL/DSL callers. This function does **not** advance the tenant write-HLC
/// — the caller does that on its own success path.
pub(crate) async fn dispatch_async_response_with_source(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
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
