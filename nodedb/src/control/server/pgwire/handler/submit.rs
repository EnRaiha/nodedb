// SPDX-License-Identifier: BUSL-1.1

//! `SubmitArgs` + `submit_to_data_plane`: the shared local-dispatch core used by
//! both `dispatch_local` and `dispatch_task_no_wal` (see `dispatch.rs`).

use std::sync::Arc;
use std::time::Instant;

use crate::bridge::envelope::{Priority, Request, Response};
use crate::types::{DatabaseId, Lsn, ReadConsistency, TraceId};

use super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::submit_to_data_plane`]: the request identity,
/// the plan, and the optional transaction id + committed write LSN.
pub(super) struct SubmitArgs {
    pub(super) tenant_id: crate::types::TenantId,
    pub(super) vshard_id: crate::types::VShardId,
    pub(super) database_id: DatabaseId,
    pub(super) plan: crate::bridge::envelope::PhysicalPlan,
    pub(super) user_id: Option<Arc<str>>,
    pub(super) txn_id: Option<crate::types::TxnId>,
    pub(super) wal_lsn: Option<Lsn>,
    /// Wall-clock instant the Control Plane resolved for a TTL-bearing KV
    /// write, stamped onto the `Request` alongside `wal_lsn` — see
    /// `dispatch_utils::WriteDispatch::resolved_now_ms`.
    pub(super) resolved_now_ms: Option<u64>,
    /// When `true`, `submit_to_data_plane` performs the WAL append itself —
    /// under the write-admission guard, immediately before the enqueue — and
    /// mints `wal_lsn` / `resolved_now_ms` (the caller passes `None` for both).
    /// This is the autocommit-local write path: minting the LSN after admission
    /// and just before the dispatcher enqueue keeps WAL-LSN order equal to
    /// Data-Plane apply order per key. When `false`, the caller has already
    /// recorded durability elsewhere (e.g. COMMIT's single `Transaction`
    /// record) and supplies its own `wal_lsn`; the core does not append.
    pub(super) append_wal: bool,
}

impl NodeDbPgHandler {
    /// Build a `Request`, register with the tracker, dispatch to the Data Plane,
    /// and await the response. Shared by `dispatch_local` and `dispatch_task_no_wal`.
    pub(super) async fn submit_to_data_plane(&self, args: SubmitArgs) -> crate::Result<Response> {
        let SubmitArgs {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            user_id,
            txn_id,
            wal_lsn,
            resolved_now_ms,
            append_wal,
        } = args;
        // Post-apply redo classification, computed before `plan` can be moved
        // (the RouteToCalvin admit arm moves it). For a write whose autocommit
        // WAL path mints no redo of its own but whose effect must survive a
        // WAL-only restart (a document PointUpdate on a collection carrying a
        // secondary vector index), the durable redo is minted AFTER apply from
        // the surrogate + post-image the Data Plane returns in
        // `Response::write_set`. `Some(collection)` for such a write, else `None`.
        let post_apply = crate::control::server::wal_dispatch::plan_post_apply_redo(&plan);
        // Write-admission gate. This path builds its own `Request` and enqueues
        // directly (it does not flow through the autocommit funnel). The guard is
        // acquired FIRST; for an autocommit write that owns its durability
        // (`append_wal`) the WAL append then happens below, under the guard, just
        // before the enqueue — so the minted LSN order equals dispatcher-enqueue
        // order per key and the strict-FIFO per-database WFQ makes apply order
        // follow enqueue order. An uncontended point write takes the fast path
        // holding its per-vShard deterministic locks; a contended or bulk write is
        // submitted through the deterministic scheduler and its applied response is
        // surfaced; reads / control ops are `Exempt`.
        use crate::control::server::shared::write_admission::{
            WriteAdmission, WriteTarget, admit, bare_ok_response, route_write_to_calvin,
        };
        let (admission, admission_guard, order_guard) = match admit(
            &self.state,
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
                // order-lock FIRST, before the WAL append + enqueue below. Fair
                // acquisition == arrival order, so WAL-LSN order == enqueue order
                // == apply order per key; distinct keys never contend.
                let order_guard = keyed_lock.lock_owned(key).await;
                (
                    crate::bridge::envelope::Admission::Admitted,
                    None,
                    Some(order_guard),
                )
            }
            WriteAdmission::RouteToCalvin => {
                // The deterministic scheduler applies the write (emitting its own
                // WriteEvents) and writes its own `CalvinApplied` WAL record, so
                // Calvin owns durability on this route — no local append here.
                let routed =
                    route_write_to_calvin(&self.state, tenant_id, database_id, vshard_id, plan)
                        .await?;
                return Ok(
                    routed.unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0)))
                );
            }
        };

        // Fast-path autocommit durability: append to the WAL now, while the
        // admission guard is held, so the LSN is minted in the same order the
        // request is about to be enqueued. `wal_append_if_write` is a no-op
        // (returns `None`) for reads; gating on `append_wal` leaves caller-supplied
        // LSNs (the COMMIT batch-flush path) untouched.
        let (wal_lsn, resolved_now_ms) = if append_wal {
            let outcome = self.wal_append_if_write(tenant_id, vshard_id, database_id, &plan)?;
            (outcome.lsn, outcome.resolved_now_ms)
        } else {
            (wal_lsn, resolved_now_ms)
        };

        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan,
            deadline: Instant::now()
                + std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id,
            statement_digest: None,
            txn_id,
            wal_lsn,
            resolved_now_ms,
            admission,
        };

        let mut rx = self.state.tracker.register(request_id);

        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        // Release the write-admission guards immediately after the enqueue, before
        // the Data-Plane round-trip. The per-database WFQ is strict FIFO, so once
        // LSN order equals enqueue order the apply order follows from the queue
        // alone; holding the guards across the response await would only serialize
        // same-key throughput needlessly.
        //
        // EXCEPTION — a post-apply-redo write (`post_apply.is_some()`) mints its
        // durable redo AFTER apply, from the write-set on the response; the guards
        // MUST stay held across the response collect + that append so two
        // concurrent same-surrogate writes cannot reorder their redo appends. Both
        // guard types are `Send`, so holding them across the `.await` is sound.
        // Moved into an `Option` so the release is a single, unconditional `drop`
        // below regardless of which path took it.
        let deferred_guards = if post_apply.is_some() {
            Some((admission_guard, order_guard))
        } else {
            drop(admission_guard);
            drop(order_guard);
            None
        };

        // A scan result wider than `stream_chunk_size` is emitted as several
        // `Partial` frames followed by a terminal frame. Drain and concatenate
        // every frame (bounded by `max_query_result_bytes`) rather than taking
        // only the first chunk — consuming one frame would silently truncate the
        // result to `stream_chunk_size` rows and orphan the request's tracker
        // entry. Mirrors `dispatch_to_data_plane_with_source`.
        use crate::control::server::dispatch_utils::{
            DispatchCollectError, collect_bounded_response,
        };
        let max_result_bytes = self.state.tuning.network.max_query_result_bytes as usize;
        match tokio::time::timeout(
            std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            collect_bounded_response(&mut rx, max_result_bytes),
        )
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        {
            Ok(resp) => {
                // Mint the post-apply redo record while the guards are still
                // held, then release them. A PointUpdate whose collection carries
                // a secondary vector index returns its surrogate + post-image in
                // `write_set`; without this durable `Put` a WAL-only restart
                // rebuilds the HNSW from the pre-update body and resurrects the
                // old embedding.
                let post_apply_lsn = if let Some(collection) = &post_apply
                    && append_wal
                    && resp.status == crate::bridge::envelope::Status::Ok
                {
                    crate::control::server::wal_dispatch::append_write_set_redo(
                        &self.state.wal,
                        tenant_id,
                        vshard_id,
                        database_id,
                        collection,
                        &resp.write_set,
                    )?
                } else {
                    None
                };
                drop(deferred_guards);

                // Durable-at-ack barrier: an acknowledged write must be
                // WAL-fsync-durable before this response (the client ack)
                // returns. `WalManager::append_*` only buffers the record and
                // mints its `Lsn`; without this barrier a `kill -9` loses the
                // buffered bytes, which is invisible for engines whose rows are
                // committed durably by redb but silently destroys the
                // columnar/timeseries memtable engines, whose only durability
                // path is WAL replay. `wal_lsn` is the forward write's LSN
                // (minted above for an autocommit write, or supplied by the
                // COMMIT batch-flush caller); `post_apply_lsn` covers the
                // post-apply redo appended just above. One group-commit fsync
                // coalesces concurrent writers (see `WalManager::wait_durable`),
                // and it runs after the admission guards are released so it never
                // serializes same-key throughput. Reads / control ops carry no
                // LSN and skip the barrier. Mirrors the same barrier in
                // `dispatch_utils::dispatch::dispatch_to_data_plane_inner`.
                if resp.status == crate::bridge::envelope::Status::Ok {
                    let durable_target = match (wal_lsn, post_apply_lsn) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        (a, b) => a.or(b),
                    };
                    if let Some(lsn) = durable_target {
                        self.state.wal.wait_durable(lsn).await?;
                    }
                }
                Ok(resp)
            }
            Err(DispatchCollectError::OverBudget { bytes }) => {
                self.state.tracker.cancel(&request_id);
                Err(crate::Error::ExecutionLimitExceeded {
                    detail: format!(
                        "query result exceeded max_query_result_bytes \
                         ({bytes} > {max_result_bytes} bytes)"
                    ),
                })
            }
            Err(DispatchCollectError::ChannelClosed) => Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            }),
        }
    }
}
