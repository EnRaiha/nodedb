// SPDX-License-Identifier: BUSL-1.1

//! The dispatch core: resolves Exchange data-movement nodes, then hands the
//! plan to the shared Control-Plane write funnel (`submit_write`), which owns
//! write admission, the WAL append, the enqueue, and the response collect.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::submit_write::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use super::types::{AutocommitWrite, DataPlaneDispatch, WriteDispatch};

/// Dispatch a physical plan to the Data Plane and await the response.
///
/// Creates a request envelope, registers with the tracker for correlation,
/// dispatches via the SPSC bridge, and awaits the response with a timeout.
pub async fn dispatch_to_data_plane(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Response> {
    dispatch_to_data_plane_with_source(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        crate::event::EventSource::User,
    )
    .await
}

/// Dispatch a physical plan to the Data Plane with an explicit event source.
///
/// Trigger-generated writes pass `EventSource::Trigger` so the Data Plane
/// emits WriteEvents with the correct source tag (preventing cascade
/// re-triggering in the Event Plane).
pub async fn dispatch_to_data_plane_with_source(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id: None,
            // The caller (trigger / sync / internal funnel) owns durability on its
            // own path; the funnel does not append here.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
            },
        },
    )
    .await
}

/// Dispatch a write to the Data Plane carrying the WAL LSN allocated for it.
///
/// Used by autocommit write endpoints that call `wal_append_if_write` and then
/// dispatch: the returned LSN is stamped onto the `Request` so the Data Plane
/// records the committed per-key / per-collection write version. The write's
/// identity and LSN travel in a [`WriteDispatch`] to keep the argument list
/// short; `wal_lsn` is `None` when the write was WAL-bypassed (e.g.
/// `timeseries` `wal=false`). `resolved_now_ms` carries the wall-clock instant
/// the Control Plane resolved for a TTL-bearing KV write's `expire_at_ms` — see
/// [`WriteDispatch::resolved_now_ms`].
pub(crate) async fn dispatch_write_to_data_plane(
    shared: &SharedState,
    write: WriteDispatch,
) -> crate::Result<Response> {
    let WriteDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        wal_lsn,
        resolved_now_ms,
    } = write;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // Caller pre-appended and supplied `wal_lsn` (e.g. the procedural
            // batch-flush path whose dispatched plan is a `TransactionBatch`
            // whose per-task records were appended upstream): the funnel must not
            // append again.
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms,
            },
        },
    )
    .await
}

/// Dispatch an autocommit write whose WAL append the funnel performs *under the
/// write-admission guard*, immediately before the enqueue.
///
/// This is the entry point for single-node local writes that own their own
/// autocommit durability (the native SQL / direct-op boot path, HTTP query,
/// RESP KV write, protocol-neutral INSERT/UPSERT). The WAL LSN must be minted
/// after admission and just before the dispatcher enqueue so that WAL-LSN order
/// equals Data-Plane apply order per key; performing the append inside the
/// funnel (rather than at the caller, before admission) is what closes that
/// ordering gap. `wal_lsn` / `resolved_now_ms` are therefore *not* caller
/// inputs — the funnel resolves them.
pub(crate) async fn dispatch_autocommit_write(
    shared: &SharedState,
    write: AutocommitWrite,
) -> crate::Result<Response> {
    let AutocommitWrite {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
    } = write;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // The funnel appends the WAL record under the admission guard just
            // before enqueue and stamps the minted LSN onto the `Request`.
            durability: WalDurability::AppendHere { now_override: None },
        },
    )
    .await
}

/// Dispatch a physical plan to the Data Plane carrying an explicit transaction
/// id so the Data Plane can resolve this transaction's staging overlay
/// (read-your-own-writes) and route `StageWrite`. Used by the native endpoint,
/// whose in-transaction tasks flow through this shared path.
pub async fn dispatch_to_data_plane_with_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
            // Staged in-transaction writes are not yet durably committed; the
            // committed write version is recorded at COMMIT via the batch funnel,
            // so durability is not the funnel's to append here.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
            },
        },
    )
    .await
}

async fn dispatch_to_data_plane_inner(
    shared: &SharedState,
    params: DataPlaneDispatch,
) -> crate::Result<Response> {
    let DataPlaneDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        durability,
    } = params;
    // Resolve any Exchange data-movement nodes before dispatch: a root-level
    // Gather fans the child to all cores and returns the merged response here;
    // a Broadcast join child is gathered and embedded so the plan reaching a
    // core is self-contained. Safe no-op for the many non-Exchange callers
    // (writes, metrics, triggers). Catalog materialization is identity-scoped
    // and already done upstream on the pgwire/native paths.
    // Internal funnel (COPY, cursors, materialized-view refresh, constraint
    // subqueries): not session-transaction-scoped, so `None`.
    let plan = match crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
        None,
    )
    .await?
    {
        crate::control::server::exchange::Resolved::Gathered(
            resp,
            _shard_watermarks,
            _shuffle_reads,
        ) => {
            return Ok(resp);
        }
        crate::control::server::exchange::Resolved::Plan(p) => p,
        // Internal funnel callers want a fully-collected Response, not a lazy
        // stream: materialize the stream into one merged-array Response,
        // preserving the prior gather-then-return behaviour on this path.
        crate::control::server::exchange::Resolved::Stream(s) => {
            return crate::control::server::exchange::gather::stream_to_response(s).await;
        }
    };

    submit_write(
        shared,
        SubmitWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // Internal / autocommit funnel: no session user to attribute.
            user_id: None,
            durability,
            ordering: WriteOrdering::Gate,
            // The autocommit / internal funnel is the path that feeds `/cdc`
            // and WS-RPC subscribers; every other caller of `submit_write` is
            // `Unowned`.
            change_feed: ChangeFeedOwner::Funnel,
        },
    )
    .await
    .map(|outcome| outcome.response)
}
