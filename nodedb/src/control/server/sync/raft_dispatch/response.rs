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

/// Parameters for [`dispatch_sync_response_inner`], bundled to keep the
/// argument list under clippy's `too_many_arguments` threshold.
struct SyncResponseDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
    /// The LSN of the redo record the caller already appended for this
    /// write, or `None` when the caller minted none. See
    /// [`dispatch_authorized_sync_response`] for the durability contract.
    wal_lsn: Option<Lsn>,
}

/// `wal_lsn` is the LSN of the redo record the caller already appended for this
/// write, or `None` when the caller minted none. It is threaded into the write
/// funnel so the durable-at-ack barrier fsyncs that record before this call
/// returns — the sync handlers ack their peer off this return value, and a peer
/// that reads "applied" retires the batch and never re-sends it.
pub async fn dispatch_authorized_sync_response(
    state: &SharedState,
    authorized: AuthorizedTask,
    trace_id: TraceId,
    event_source: EventSource,
    wal_lsn: Option<Lsn>,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    dispatch_sync_response_inner(
        state,
        SyncResponseDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id,
            event_source,
            wal_lsn,
        },
    )
    .await
}

/// Trusted-internal sync-shaped dispatch used by DDL index maintenance.
///
/// These callers mint no redo of their own — the DDL that drives them is
/// already durable — so they supply no LSN.
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
        SyncResponseDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            wal_lsn: None,
        },
    )
    .await
}

/// Cluster path: proposes through Raft, then wraps the apply payload in a
/// `Status::Ok` `Response`. The gate verdict is carried in the payload (as a
/// zerompk-encoded `SyncAckResult`); `Status::Ok` is always correct here
/// because a non-`Ok` status signals a protocol error, not an idempotency
/// gate rejection.
///
/// Single-node path: falls through to the Control-Plane write funnel carrying
/// `wal_lsn`, so the record the caller appended is fsync-durable before the
/// response — and therefore the peer's ack — is produced.
async fn dispatch_sync_response_inner(
    state: &SharedState,
    params: SyncResponseDispatch,
) -> crate::Result<Response> {
    let SyncResponseDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        wal_lsn,
    } = params;
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

    crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
        state,
        crate::control::server::dispatch_utils::WriteDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id: None,
            // The caller appended this write's redo before dispatching, so the
            // funnel must not append a second one — it stamps this LSN onto the
            // request and waits on it at the durable-at-ack barrier.
            wal_lsn,
            // No engine on this path resolves a wall-clock instant at append
            // time; only a TTL-bearing KV write does, and KV has no sync handler.
            resolved_now_ms: None,
        },
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
///
/// `wal_lsn` is the LSN of the redo the dispatcher appended for this write. It
/// is not optional in spirit: these engines rebuild their state only by WAL
/// replay, so acking the peer without waiting on that record loses an
/// acknowledged write on `kill -9`.
pub async fn dispatch_sync_payload(
    state: &SharedState,
    authorized: AuthorizedTask,
    wal_lsn: Option<Lsn>,
) -> crate::Result<Vec<u8>> {
    let response = dispatch_authorized_sync_response(
        state,
        authorized,
        TraceId::ZERO,
        EventSource::CrdtSync,
        wal_lsn,
    )
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::durability_test_support::{
        append_buffered_record, authorized_write, fixture, respond_once,
    };
    use super::dispatch_sync_payload;

    /// The defect this guards: the vector / FTS / spatial / columnar sync
    /// handlers append their redo, dispatch, and ack the peer off this return
    /// value. Those engines rebuild only from WAL replay, so returning while the
    /// record is still buffered acks a write a `kill -9` erases — and the peer,
    /// having read "applied", retires the batch and never re-sends it.
    #[tokio::test]
    async fn a_supplied_lsn_is_fsync_durable_before_the_payload_returns() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "the append must only buffer, or this test proves nothing"
        );
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_sync_payload(&state, authorized, Some(lsn))
            .await
            .expect("sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() >= lsn.as_u64(),
            "the supplied redo must be fsync-durable before the peer is acked"
        );
    }

    /// The counterpart: a caller that appended nothing supplies no LSN and the
    /// funnel has nothing to wait on. This is what makes the assertion above a
    /// statement about the threaded LSN rather than about dispatch in general.
    #[tokio::test]
    async fn no_supplied_lsn_leaves_an_unrelated_buffered_record_alone() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_sync_payload(&state, authorized, None)
            .await
            .expect("sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "nothing appended by this dispatch means nothing to fsync"
        );
    }
}
