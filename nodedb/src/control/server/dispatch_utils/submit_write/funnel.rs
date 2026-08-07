// SPDX-License-Identifier: BUSL-1.1

//! THE single Control-Plane write funnel.
//!
//! Every Control-Plane path that puts a write on the SPSC bridge routes through
//! [`submit_write`]: the autocommit / internal funnel (`dispatch.rs`), the
//! pgwire local-dispatch path (`pgwire::handler::submit`), and the Raft
//! apply loop (`distributed_applier::apply_loop`). The funnel owns write
//! admission, the WAL redo append, the enqueue, the bounded response collect,
//! the post-apply redo, the durable-at-ack barrier, and — for the caller that
//! owns it (see [`ChangeFeedOwner`]) — the CDC publish, in that order, which is
//! the correctness contract.
//!
//! A path that reimplements these steps drifts silently: it is not a compile
//! error to omit the redo append or the change-event publish, and the omission
//! only surfaces as lost data after a crash, or as a change stream that never
//! fires. Add the step here, once, and every caller gets it.
//!
//! It also owns the mirror of the redo append: when it appended the record
//! itself and the Data Plane then REFUSED the write, it cancels that record
//! before returning the error. See [`super::super::write_abort`] for which verdicts
//! qualify, the residual crash window it does not close, and the latency it
//! costs a rejection.

use std::time::{Duration, Instant};

use crate::bridge::envelope::{Priority, Request, Status};
use crate::control::server::wal_dispatch::{self, WalAppendRequest};
use crate::control::state::SharedState;
use crate::types::ReadConsistency;

use super::super::change_events::{extract_write_change_set, publish_change_set};
use super::super::collect::{DispatchCollectError, collect_bounded_response};
use super::ambiguous_ddl::preserve_ambiguous_array_ddl;
use super::params::{ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering};

/// Admit, make durable, enqueue, collect, and publish one write.
///
/// See [`SubmitOutcome`] for what comes back.
pub(crate) async fn submit_write(
    shared: &SharedState,
    params: SubmitWrite,
) -> crate::Result<SubmitOutcome> {
    let SubmitWrite {
        tenant_id,
        database_id,
        vshard_id,
        mut plan,
        trace_id,
        event_source,
        txn_id,
        user_id,
        durability,
        ordering,
        change_feed,
    } = params;

    // Change metadata is derived from the plan HERE, before it is moved into
    // the request — the publish itself happens after apply, once the response
    // (which carries the event's LSN) exists, by which point the plan is gone.
    // Extraction is a pure match that clones out collection / document
    // identity, so a caller whose change feed is `Unowned` skips it rather than
    // allocating tuples nothing will read.
    let change_set = match change_feed {
        ChangeFeedOwner::Funnel => Some(extract_write_change_set(&plan, tenant_id)),
        ChangeFeedOwner::Unowned => None,
    };

    // Post-apply redo classification, computed before `plan` is moved (the
    // RouteToCalvin admit arm moves it). For a write whose autocommit WAL path
    // mints no redo of its own but whose effect must survive a WAL-only restart
    // (a document PointUpdate on a collection carrying a secondary vector
    // index), the durable redo is minted AFTER apply from the surrogate +
    // post-image the Data Plane returns in `Response::write_set`.
    // `Some(collection)` for such a write, else `None`.
    let post_apply = wal_dispatch::plan_post_apply_redo(&plan);
    let appends_here = matches!(&durability, WalDurability::AppendHere { .. });

    // Durable-at-ack obligation, also computed before `plan` moves. `Some` only
    // for a write whose redo record THIS funnel is required to mint; a caller
    // that appended upstream (or declared durability owned elsewhere) is not
    // held to it, because the LSN it does or does not supply is its own
    // contract. See `durability_barrier` for why this is narrower than
    // "write-class plan with no LSN".
    let funnel_redo_engine = if appends_here {
        super::super::durability_barrier::funnel_minted_redo_engine(&plan)
    } else {
        None
    };

    // Write-admission gate: every write-class plan whose ordering is not already
    // final passes here. An uncontended point write takes the fast path holding
    // its per-vShard deterministic locks; a contended or bulk write is submitted
    // through the deterministic scheduler and its applied response is surfaced
    // here; reads / control ops are `Exempt`.
    //
    // Ordering (fast path): the guard is acquired FIRST, then — for a write that
    // owns its durability (`AppendHere`) — the WAL append happens below, under
    // the guard, minting the LSN just before the enqueue. The guard is released
    // immediately after the enqueue (not across the response await).
    use crate::control::server::shared::write_admission::{
        WriteAdmission, WriteTarget, admit, bare_ok_response, route_write_to_calvin,
    };
    let (admission, admission_guard, order_guard) = match ordering {
        WriteOrdering::AlreadyOrdered => (
            crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
            None,
            None,
        ),
        WriteOrdering::Gate => match admit(
            shared,
            &WriteTarget {
                tenant_id,
                database_id,
                vshard_id,
                plan: &plan,
            },
        ) {
            WriteAdmission::ExemptRead => (
                crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::Read,
                ),
                None,
                None,
            ),
            WriteAdmission::FastPath { guard } => {
                (crate::bridge::envelope::Admission::Admitted, guard, None)
            }
            WriteAdmission::FastPathBlocking { key, keyed_lock } => {
                // Single-node serialization point: acquire the per-key FIFO
                // order-lock FIRST, before the WAL append and enqueue below.
                // `tokio::sync::Mutex` is fair, so concurrent same-key writers
                // are admitted in arrival order — the WAL append + enqueue then
                // happen in that order, giving WAL-LSN order == enqueue order ==
                // apply order per key. Distinct keys use distinct per-key mutexes
                // and never contend.
                let order_guard = keyed_lock.lock_owned(key).await;
                (
                    crate::bridge::envelope::Admission::Admitted,
                    None,
                    Some(order_guard),
                )
            }
            WriteAdmission::RouteToCalvin => {
                // The deterministic scheduler applies the write (emitting its own
                // WriteEvents) and returns the applied response; a plain write with
                // no RETURNING rows yields `None`, synthesized into a bare `Ok`.
                // Calvin owns durability on this route (the sequenced TxClass plus
                // its own `CalvinApplied` WAL record), so no local append happens.
                let routed =
                    route_write_to_calvin(shared, tenant_id, database_id, vshard_id, plan).await?;
                return Ok(SubmitOutcome {
                    response: routed
                        .unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0))),
                    wal_lsn: None,
                });
            }
        },
    };

    // Array DDL conversion is intentionally read-only. Once this task has
    // passed authorization and admission, install its durable catalog state
    // immediately before the Data-Plane dispatch. The mirror is changed only
    // after the redb transaction commits.
    let ddl_transition = crate::control::array_catalog::ddl::apply_authorized_ddl(
        shared,
        tenant_id,
        database_id,
        &plan,
    )?;
    macro_rules! rollback_ddl {
        ($result:expr) => {
            match $result {
                Ok(value) => value,
                Err(error) => {
                    let _ = ddl_transition.rollback(shared);
                    return Err(error.into());
                }
            }
        };
    }

    // Durability, under the guard, immediately before the enqueue: the LSN is
    // minted in the same order the request is about to be enqueued.
    let (wal_lsn, resolved_now_ms) = match durability {
        WalDurability::AppendHere { now_override } => {
            let outcome = rollback_ddl!(wal_dispatch::wal_append(WalAppendRequest {
                wal: &shared.wal,
                tenant_id,
                vshard_id,
                database_id,
                plan: &plan,
                credentials: None,
                now_override,
            }));
            (outcome.lsn, outcome.resolved_now_ms)
        }
        WalDurability::CallerSupplied {
            wal_lsn,
            resolved_now_ms,
        } => (wal_lsn, resolved_now_ms),
    };

    // Write the resolved LSN back into the plan itself. The envelope's
    // `wal_lsn` is where most engines read their committed version from, but the
    // array engine stamps its tile versions from the LSN carried in the plan
    // while replay stamps them from the record header — so the plan the Data
    // Plane is about to execute must name the record that reproduces it. This is
    // the only place that knows both, and it knows them for every caller: no
    // upstream path may allocate an LSN of its own and hope it matches.
    if let Some(lsn) = wal_lsn {
        wal_dispatch::stamp_minted_lsn(&mut plan, lsn);
    }

    // Per-vShard QPS + latency timer. `dispatch_started` marks the wall-clock
    // moment the request enters the Control Plane dispatch site; observation
    // happens on every exit path (success, budget over-run, timeout) so the
    // histogram captures the true end-to-end shape of the work routed to this
    // vshard.
    let dispatch_started = Instant::now();
    let vshard_u32 = vshard_id.as_u32();
    let observe = |shared: &SharedState| {
        let latency_us = dispatch_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        shared.per_vshard_metrics.observe(vshard_u32, latency_us);
    };

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
        user_id,
        statement_digest: None,
        txn_id,
        wal_lsn,
        resolved_now_ms,
        admission,
    };

    let mut rx = shared.tracker.register(request_id);

    match shared.dispatcher.lock() {
        Ok(mut d) => rollback_ddl!(d.dispatch(request)),
        Err(poisoned) => rollback_ddl!(poisoned.into_inner().dispatch(request)),
    };

    // Release the write-admission guards immediately after the enqueue, before
    // the Data-Plane round-trip. The per-database WFQ is strict FIFO, so once LSN
    // order equals enqueue order the apply order follows from the queue alone;
    // holding the guards across the response await would only serialize same-key
    // throughput needlessly.
    //
    // EXCEPTION — a post-apply-redo write (`post_apply.is_some()`) mints its
    // durable redo AFTER apply, from the write-set on the response; the guards
    // MUST stay held across the response collect + that append so two concurrent
    // same-surrogate writes cannot reorder their redo appends. Both guard types
    // are `Send`, so holding them across the `.await` is sound. Moved into an
    // `Option` so the release is a single, unconditional `drop` below regardless
    // of which path took it. (`None` guard slots when no lock manager was
    // registered / for the exempt-read / Calvin / already-ordered cases.)
    let deferred_guards = if post_apply.is_some() {
        Some((admission_guard, order_guard))
    } else {
        drop(admission_guard);
        drop(order_guard);
        None
    };

    // Collect response(s). For non-streaming queries, exactly one arrives.
    // For streaming queries, multiple partial chunks arrive before the final.
    // The mpsc channel is bounded (see `RequestTracker::register`); here we
    // additionally cap the *total* accumulated payload so a runaway scan
    // can't pin Control-Plane RAM — any query whose combined result exceeds
    // `tuning.network.max_query_result_bytes` is cancelled with a typed
    // `ExecutionLimitExceeded` error.
    let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
    let response = match tokio::time::timeout(
        Duration::from_secs(shared.tuning.network.default_deadline_secs),
        collect_bounded_response(&mut rx, max_result_bytes),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => {
            observe(shared);
            // Dispatch completed, but the Data Plane may have applied CREATE
            // or ALTER before this deadline. Never roll that catalog state
            // back on an ambiguous post-enqueue outcome.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            return Err(crate::Error::DeadlineExceeded { request_id });
        }
    };

    let response = match response {
        Ok(r) => r,
        Err(DispatchCollectError::OverBudget { bytes }) => {
            shared.tracker.cancel(&request_id);
            observe(shared);
            // A partial response proves dispatch began but not whether an
            // Array DDL completed; preserve CREATE/ALTER and fail-stop.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "query result exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            });
        }
        Err(DispatchCollectError::ChannelClosed) => {
            observe(shared);
            // The producer can close after applying but before sending its
            // response. CREATE/ALTER must remain catalog-finalized here.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            return Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            });
        }
    };

    if response.status != Status::Ok {
        let _ = ddl_transition.rollback(shared);
        super::super::write_abort::abort_refused_write(
            shared,
            super::super::write_abort::AbortTarget {
                tenant_id,
                database_id,
                vshard_id,
                wal_lsn,
                appends_here,
            },
            &response,
        )
        .await?;
    }

    // Mint the post-apply redo record while the guards are still held, then
    // release them. A PointUpdate whose collection carries a secondary vector
    // index returns its surrogate + post-image in `write_set`; without this
    // durable `Put` a WAL-only restart rebuilds the HNSW from the pre-update body
    // and resurrects the old embedding.
    let post_apply_lsn = if let Some(collection) = &post_apply
        && appends_here
        && response.status == Status::Ok
    {
        rollback_ddl!(wal_dispatch::append_write_set_redo(
            &shared.wal,
            tenant_id,
            vshard_id,
            database_id,
            collection,
            &response.write_set,
        ))
    } else {
        None
    };
    drop(deferred_guards);

    // Durable-at-ack barrier: an acknowledged write must be WAL-fsync-durable
    // before this response (the client ack) returns. `WalManager::append_*` only
    // buffers the record and mints its `Lsn`; without this barrier a `kill -9`
    // loses the buffered bytes, which is invisible for engines whose rows are
    // committed durably by redb but silently destroys every engine whose only
    // durability path is WAL replay: the KV hash tables, the HNSW graphs, the
    // columnar / timeseries memtables, the graph node labels, the CRDT states,
    // and the FTS index. `wal_lsn` is the forward write's LSN — minted above
    // under the admission guard for a write that owns its durability, or supplied
    // by a caller that appended upstream (procedural batch flush,
    // interactive-COMMIT transaction redo). `post_apply_lsn` covers the
    // post-apply redo appended just above. Both records are already buffered in
    // the shared WAL; one group-commit fsync coalesces concurrent writers (see
    // `WalManager::wait_durable`), and it runs here — after the admission guards
    // are released — so it never serializes same-key throughput. Reads / control
    // ops / trigger / staged-write dispatch carry no LSN and skip the barrier;
    // `durability_barrier` decides which of those skips are legitimate and makes
    // the rest loud instead of letting them ack a write nothing can recover.
    if response.status == Status::Ok {
        let durable_target = match (wal_lsn, post_apply_lsn) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        match durable_target {
            Some(lsn) => rollback_ddl!(shared.wal.wait_durable(lsn).await),
            // Nothing to fsync. Legitimate for most plans, but if this funnel
            // was the one required to mint the record, the ack below promises
            // durability the engine cannot deliver — silent until a `kill -9`
            // proves it, hence the check.
            None => super::super::durability_barrier::assert_durable_before_ack(funnel_redo_engine),
        }
    }

    if response.status == Status::Ok {
        ddl_transition.finalize(shared)?;
    }

    // Publish change events for successful writes whose change feed this funnel
    // owns. `None` is a caller whose change feed is `Unowned` — see
    // [`ChangeFeedOwner`] for why the node that applies those writes is not the
    // node that publishes them.
    if response.status == Status::Ok {
        if let Some(change_set) = change_set {
            publish_change_set(shared, tenant_id, database_id, change_set, &response);
        }

        // Advance the tenant's observed write-HLC high-water on any successful
        // dispatch. Used by the RESTORE staleness gate. Advancing on every
        // success (not just writes) is intentionally conservative —
        // envelope.watermark is captured AFTER fan-out so it always dominates
        // the tenant_wm of a fresh backup.
        shared.advance_tenant_write_hlc(tenant_id.as_u64());
    }

    observe(shared);
    Ok(SubmitOutcome { response, wal_lsn })
}
