// SPDX-License-Identifier: BUSL-1.1

//! The dispatch core: sends a physical plan to the Data Plane over the SPSC
//! bridge and awaits its response, funneling every write through the
//! write-admission gate and (for autocommit writes) the WAL append.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;

use super::change_events::{extract_write_metadata, publish_change_event};
use super::collect::{DispatchCollectError, collect_bounded_response};
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
            wal_lsn: None,
            resolved_now_ms: None,
            // The caller (trigger / sync / internal funnel) owns durability on its
            // own path; the core does not append here.
            append_wal: false,
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
            wal_lsn,
            resolved_now_ms,
            // Caller pre-appended and supplied `wal_lsn` (e.g. the procedural
            // batch-flush path whose dispatched plan is a `TransactionBatch`
            // whose per-task records were appended upstream): the core must not
            // append again.
            append_wal: false,
        },
    )
    .await
}

/// Dispatch an autocommit write whose WAL append the core performs *under the
/// write-admission guard*, immediately before the enqueue.
///
/// This is the funnel for single-node local writes that own their own
/// autocommit durability (the native SQL / direct-op boot path, HTTP query,
/// RESP KV write, protocol-neutral INSERT/UPSERT). The WAL LSN must be minted
/// after admission and just before the dispatcher enqueue so that WAL-LSN order
/// equals Data-Plane apply order per key; performing the append inside the core
/// (rather than at the caller, before admission) is what closes that ordering
/// gap. `wal_lsn` / `resolved_now_ms` are therefore *not* caller inputs — the
/// core resolves them.
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
            wal_lsn: None,
            resolved_now_ms: None,
            // The core appends the WAL record under the admission guard just
            // before enqueue and stamps the minted LSN onto the `Request`.
            append_wal: true,
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
            // committed write version is recorded at COMMIT via the batch funnel.
            wal_lsn: None,
            resolved_now_ms: None,
            // Durability for a staged write is deferred to COMMIT — the core
            // must not append here.
            append_wal: false,
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
        wal_lsn,
        resolved_now_ms,
        append_wal,
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
        crate::control::server::exchange::Resolved::Gathered(resp, _shard_watermarks) => {
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

    // Extract write metadata before the plan is moved into the request.
    let is_columnar_collection = matches!(
        &plan,
        PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. })
    );
    let change_meta = extract_write_metadata(&plan, tenant_id);

    // Write-admission gate: every write-class plan on this near-universal
    // autocommit / internal funnel passes here. An uncontended point write takes
    // the fast path holding its per-vShard deterministic locks; a contended or
    // bulk write is submitted through the deterministic scheduler and its applied
    // response is surfaced here; reads / control ops are `Exempt`.
    //
    // Ordering (fast path): the guard is acquired FIRST, then — for an autocommit
    // write that owns its durability (`append_wal`) — the WAL append happens HERE,
    // under the guard, minting the LSN just before the enqueue below. This makes
    // WAL-LSN order equal to dispatcher-enqueue order per key; the strict-FIFO
    // per-database WFQ then makes apply order follow enqueue order, so restart
    // replay (in LSN order) cannot diverge from live state. The guard is released
    // immediately after the enqueue (not across the response await).
    use crate::control::server::shared::write_admission::{
        WriteAdmission, WriteTarget, admit, bare_ok_response, route_write_to_calvin,
    };
    let (admission, admission_guard) = match admit(
        shared,
        &WriteTarget {
            tenant_id,
            database_id,
            vshard_id,
            plan: &plan,
        },
    ) {
        WriteAdmission::ExemptRead => (
            crate::bridge::envelope::Admission::Exempt(crate::bridge::envelope::ExemptReason::Read),
            None,
        ),
        WriteAdmission::FastPath { guard } => (crate::bridge::envelope::Admission::Admitted, guard),
        WriteAdmission::RouteToCalvin => {
            // The deterministic scheduler applies the write (emitting its own
            // WriteEvents) and returns the applied response; a plain write with
            // no RETURNING rows yields `None`, synthesized into a bare `Ok`. Calvin
            // owns durability on this route (the sequenced TxClass plus its own
            // `CalvinApplied` WAL record), so no local append happens here.
            let routed =
                route_write_to_calvin(shared, tenant_id, database_id, vshard_id, plan).await?;
            return Ok(routed.unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0))));
        }
    };

    // Fast-path autocommit durability: append the write to the WAL now, while the
    // admission guard is held, so the LSN is minted in the same order the request
    // is about to be enqueued. `wal_append_if_write` is a no-op (returns `None`)
    // for the exempt-read case, but gating on `append_wal` keeps caller-supplied
    // LSNs (procedural batch flush, staged-txn, trigger/sync paths) untouched.
    let (wal_lsn, resolved_now_ms) = if append_wal {
        let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
            &shared.wal,
            tenant_id,
            vshard_id,
            database_id,
            &plan,
        )?;
        (outcome.lsn, outcome.resolved_now_ms)
    } else {
        (wal_lsn, resolved_now_ms)
    };

    // Per-vShard QPS + latency timer. `dispatch_started` marks the
    // wall-clock moment the request enters the Control Plane dispatch
    // site; observation happens on every exit path (success, budget
    // over-run, timeout) so the histogram captures the true end-to-end
    // shape of the work routed to this vshard.
    let dispatch_started = Instant::now();
    let vshard_u32 = vshard_id.as_u32();

    let request_id = shared.next_request_id();
    let request = Request {
        request_id,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        deadline: Instant::now() + Duration::from_secs(shared.tuning.network.default_deadline_secs),
        priority: Priority::Normal,
        trace_id,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn,
        resolved_now_ms,
        admission,
    };

    let mut rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request)?,
        Err(poisoned) => poisoned.into_inner().dispatch(request)?,
    };

    // Release the write-admission guard immediately after the enqueue, before the
    // Data-Plane round-trip. The per-database WFQ is strict FIFO, so once LSN
    // order equals enqueue order the apply order follows from the queue alone;
    // holding the guard across the response await would only serialize same-key
    // throughput needlessly. (`None` when no lock manager was registered.)
    drop(admission_guard);

    // Collect response(s). For non-streaming queries, exactly one arrives.
    // For streaming queries, multiple partial chunks arrive before the final.
    // The mpsc channel is bounded (see `RequestTracker::register`); here we
    // additionally cap the *total* accumulated payload so a runaway scan
    // can't pin Control-Plane RAM — any query whose combined result
    // exceeds `tuning.network.max_query_result_bytes` is cancelled with
    // a typed `ExecutionLimitExceeded` error.
    let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
    let observe = |shared: &SharedState| {
        let latency_us = dispatch_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        shared.per_vshard_metrics.observe(vshard_u32, latency_us);
    };
    let response = tokio::time::timeout(
        Duration::from_secs(shared.tuning.network.default_deadline_secs),
        collect_bounded_response(&mut rx, max_result_bytes),
    )
    .await
    .map_err(|_| {
        observe(shared);
        crate::Error::DeadlineExceeded { request_id }
    })?;

    let response = match response {
        Ok(r) => r,
        Err(DispatchCollectError::OverBudget { bytes }) => {
            shared.tracker.cancel(&request_id);
            observe(shared);
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "query result exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            });
        }
        Err(DispatchCollectError::ChannelClosed) => {
            observe(shared);
            return Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            });
        }
    };

    // Publish change events for successful writes.
    if response.status == crate::bridge::envelope::Status::Ok
        && let Some(meta) = change_meta
    {
        publish_change_event(
            shared,
            tenant_id,
            database_id,
            is_columnar_collection,
            meta,
            &response,
        );
    }

    // Advance the tenant's observed write-HLC high-water on any
    // successful dispatch. Used by RESTORE staleness gate. Advance
    // on every success (not just writes) is intentionally
    // conservative — envelope.watermark is captured AFTER fan-out so
    // it always dominates the tenant_wm of a fresh backup.
    if response.status == crate::bridge::envelope::Status::Ok {
        shared.advance_tenant_write_hlc(tenant_id.as_u64());
    }

    observe(shared);
    Ok(response)
}
