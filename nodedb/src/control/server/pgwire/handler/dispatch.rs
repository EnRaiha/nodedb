// SPDX-License-Identifier: BUSL-1.1

//! Core dispatch mechanics: single-task dispatch, Raft replication, and local Data Plane submission.

use std::sync::Arc;
use std::time::Instant;

use crate::bridge::envelope::{Priority, Request, Response};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::{Resolved, resolve_and_materialize};
use crate::types::{DatabaseId, Lsn, ReadConsistency, TraceId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::submit_to_data_plane`]: the request identity,
/// the plan, and the optional transaction id + committed write LSN.
struct SubmitArgs {
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    database_id: DatabaseId,
    plan: crate::bridge::envelope::PhysicalPlan,
    user_id: Option<Arc<str>>,
    txn_id: Option<crate::types::TxnId>,
    wal_lsn: Option<Lsn>,
    /// Wall-clock instant the Control Plane resolved for a TTL-bearing KV
    /// write, stamped onto the `Request` alongside `wal_lsn` — see
    /// `dispatch_utils::WriteDispatch::resolved_now_ms`.
    resolved_now_ms: Option<u64>,
}

impl NodeDbPgHandler {
    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, write operations are proposed to Raft first and only
    /// executed on the Data Plane after quorum commit. Reads bypass Raft.
    ///
    /// `user_id` is forwarded to the `Request` for DML audit attribution.
    /// Pass `None` for system-generated tasks (triggers, maintenance, etc.).
    ///
    /// `identity` is forwarded to the Exchange resolver for per-request catalog
    /// materialization (identity-scoped catalog rows). Pass `None` for internal
    /// sub-tasks where Exchange has already been resolved by an outer call.
    pub(super) async fn dispatch_task(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
    ) -> crate::Result<Response> {
        let mut shard_watermarks = Vec::new();
        self.dispatch_task_hlc(task, user_id, identity, &mut shard_watermarks)
            .await
    }

    /// Dispatch a single physical task and return both the response and the
    /// per-shard watermark LSNs a single-node fan gather observed (empty for a
    /// non-gathered read). Used by the transactional read-recording seam so a
    /// multi-core fan read records one read-set entry per participating shard.
    pub(super) async fn dispatch_task_with_watermarks(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
    ) -> crate::Result<(Response, Vec<(VShardId, Lsn)>)> {
        let mut shard_watermarks = Vec::new();
        let resp = self
            .dispatch_task_hlc(task, user_id, identity, &mut shard_watermarks)
            .await?;
        Ok((resp, shard_watermarks))
    }

    async fn dispatch_task_hlc(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
    ) -> crate::Result<Response> {
        let tenant_id = task.tenant_id;
        let result = self
            .dispatch_task_inner(task, user_id, identity, shard_watermarks)
            .await;
        // Advance per-tenant observed write-HLC high-water on any
        // successful dispatch (local, raft-replicated, or broadcast).
        // Used by RESTORE's staleness gate. Backup captures envelope
        // watermark AFTER its own fan-out, so envelope.wm dominates
        // tenant_wm on a fresh backup.
        if let Ok(ref resp) = result
            && resp.status == crate::bridge::envelope::Status::Ok
        {
            self.state.advance_tenant_write_hlc(tenant_id.as_u64());
        }
        result
    }

    async fn dispatch_task_inner(
        &self,
        mut task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
    ) -> crate::Result<Response> {
        // Reject user writes against a source database that is currently
        // frozen by a clone materializer sweep.  Reads and DDL pass through
        // unchanged.  The materializer uses `dispatch_local` (a free function
        // in `clone_materializer/dispatch.rs`) and is never routed through
        // this method, so there is no risk of blocking the materializer itself.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }

        // Mirror enforcement:
        // - Writes are rejected on non-promoted mirrors (MIRROR_READ_ONLY).
        // - Reads are gated by the session's ReadConsistency level:
        //     Strong        → STALE_READ_NOT_LEADER (mirrors are never the source leader)
        //     BoundedStaleness(d) → serve locally if lag ≤ d, else STALE_READ_NOT_LEADER
        //     Eventual      → serve locally unconditionally
        // The catalog lookup is skipped for the default database (id=0) to keep the
        // hot path allocation-free in the single-database case.
        let catalog = self.state.credentials.catalog();
        if task.database_id.as_u64() != 0
            && let Ok(Some(descriptor)) = catalog.get_database(task.database_id)
            && let Some(origin) = descriptor.mirror_origin.as_ref()
            && !matches!(origin.status, nodedb_types::MirrorStatus::Promoted)
        {
            if matches!(perm, Permission::Write | Permission::Admin) {
                return Err(crate::Error::MirrorReadOnly {
                    database: descriptor.name.clone(),
                });
            }

            use crate::control::server::pgwire::ddl::database::{
                MirrorReadOutcome, check_mirror_read_consistency,
            };
            // Consistency defaults to Strong: mirrors are not the source leader,
            // so reads are rejected unless the session has explicitly opted into
            // BoundedStaleness or Eventual.
            let outcome = check_mirror_read_consistency(
                catalog,
                task.database_id,
                origin,
                ReadConsistency::Strong,
            );
            if let MirrorReadOutcome::Reject { message, .. } = outcome {
                return Err(crate::Error::StaleReadNotLeader {
                    database: descriptor.name.clone(),
                    source_cluster: origin.source_cluster.clone(),
                    detail: message,
                });
            }
        }

        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
            )
        ) {
            return crate::control::server::broadcast::broadcast_count_to_all_cores(
                &self.state,
                task.tenant_id,
                task.database_id,
                task.plan,
                TraceId::ZERO,
                "inserted",
            )
            .await;
        }

        // `DROP ARRAY` must reach every Data-Plane core so each can release
        // its per-core store and remove the on-disk segment dir; otherwise
        // a follow-up `CREATE ARRAY` of the same name carries stale state.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Array(
                nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
            )
        ) {
            return crate::control::server::broadcast::broadcast_count_to_all_cores(
                &self.state,
                task.tenant_id,
                task.database_id,
                task.plan,
                TraceId::ZERO,
                "dropped",
            )
            .await;
        }

        // Exchange resolution: materialize catalog providers and resolve any
        // Exchange nodes (Gather/Broadcast) in the plan.  When identity is
        // available (user-facing SQL paths), per-request catalog materialization
        // runs first; on internal sub-task paths (identity = None) the plan has
        // no Exchange nodes left to resolve.
        if let Some(ident) = identity {
            match resolve_and_materialize(
                &self.state,
                ident,
                task.database_id,
                task.tenant_id,
                task.plan,
                TraceId::ZERO,
                task.txn_id,
            )
            .await?
            {
                Resolved::Gathered(resp, wms) => {
                    *shard_watermarks = wms;
                    return Ok(resp);
                }
                Resolved::Plan(resolved_plan) => {
                    task.plan = resolved_plan;
                }
                // Real pgwire streaming is handled up-front in
                // `dispatch_task_loop` (execute.rs), before `dispatch_task` is
                // ever called: it builds a lazy `QueryResponse` directly from
                // `gather_all_cores_stream`. A Stream reaching THIS materialize
                // funnel (e.g. internal pgwire sub-task paths that go through
                // `dispatch_task` rather than the loop) is collected into a
                // Response — a safe, behaviour-preserving default.
                Resolved::Stream(s) => {
                    return crate::control::server::exchange::gather::stream_to_response(s).await;
                }
            }
        }

        if let Some(async_proposer) = self.state.async_raft_proposer.get()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                &task.plan,
            )
        {
            return self.dispatch_replicated_write(entry, async_proposer).await;
        }

        self.dispatch_local(task, user_id).await
    }

    /// Dispatch a write through Raft: propose → register waiter → await apply.
    ///
    /// The `AsyncRaftProposer` handles propose + waiter registration in one
    /// step. The `ProposeTracker` is race-safe: if the entry commits and
    /// applies on this node before `register()` is called, the result is
    /// stored and `register()` picks it up immediately.
    async fn dispatch_replicated_write(
        &self,
        entry: crate::control::wal_replication::ReplicatedEntry,
        proposer: &Arc<crate::control::wal_replication::AsyncRaftProposer>,
    ) -> crate::Result<Response> {
        let request_id = self.next_request_id();

        // Propose through Raft with transparent leader-change retry. Shared with
        // the durable RESTORE re-issue path so both replicate identically.
        let payload =
            crate::control::wal_replication::propose_replicated_entry(&self.state, proposer, entry)
                .await?;

        Ok(Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::new(0),
            error_code: None,
            read_set_valid: None,
        })
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// For write operations, the WAL is appended **before** dispatching to the
    /// Data Plane. This ensures durability: if the process crashes after WAL
    /// append but before Data Plane execution, the write is replayed on recovery.
    /// Reads bypass the WAL entirely.
    async fn dispatch_local(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
    ) -> crate::Result<Response> {
        let outcome =
            self.wal_append_if_write(task.tenant_id, task.vshard_id, task.database_id, &task.plan)?;
        let txn_id = task.txn_id;
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id,
            txn_id,
            wal_lsn: outcome.lsn,
            resolved_now_ms: outcome.resolved_now_ms,
        })
        .await
    }

    /// Dispatch a task to the Data Plane WITHOUT individual WAL append.
    ///
    /// Used by COMMIT to dispatch buffered transaction tasks after the
    /// entire transaction has been written as a single `RecordType::Transaction`
    /// WAL record. Skipping per-task WAL avoids double-writing.
    pub(super) async fn dispatch_task_no_wal(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> crate::Result<Response> {
        // Same materialize-freeze gate as `dispatch_task_inner`. Without this,
        // a transaction that began before the freeze could COMMIT writes
        // *during* the freeze window — the materializer would already be
        // mid-scan, so those committed rows would either leak into target
        // (if scan hadn't reached them) or stay only in source (if past).
        // Both outcomes break the as-of contract; reject with
        // `SourceFrozen` so the client retries the COMMIT after the freeze
        // releases. Pre-freeze transactions remain consistent because their
        // staged tasks are buffered in the session, not yet visible to source.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }
        let txn_id = task.txn_id;
        // The transaction's writes were durably recorded under a single
        // `RecordType::Transaction` WAL record at COMMIT; per-task WAL append is
        // skipped here (would double-write). `wal_lsn` is that record's LSN,
        // stamped so the Data Plane records the batch's write versions.
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id,
            txn_id,
            wal_lsn,
            // The batch WAL record above does not carry a per-task resolved TTL
            // instant (see `flush_transaction_buffer`'s equivalent limitation);
            // a TTL-bearing KV write inside a multi-task COMMIT batch falls back
            // to `epoch_system_ms` / the wall clock at apply time.
            resolved_now_ms: None,
        })
        .await
    }

    /// Build a `Request`, register with the tracker, dispatch to the Data Plane,
    /// and await the response. Shared by `dispatch_local` and `dispatch_task_no_wal`.
    async fn submit_to_data_plane(&self, args: SubmitArgs) -> crate::Result<Response> {
        let SubmitArgs {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            user_id,
            txn_id,
            wal_lsn,
            resolved_now_ms,
        } = args;
        // Write-admission gate. This path builds its own `Request` and enqueues
        // directly (it does not flow through the autocommit funnel). An
        // uncontended point write takes the fast path holding its per-vShard
        // deterministic locks (guard held across enqueue + response); a contended
        // or bulk write is submitted through the deterministic scheduler and its
        // applied response is surfaced; reads / control ops are `Exempt`.
        use crate::control::server::shared::write_admission::{
            WriteAdmission, WriteTarget, admit, bare_ok_response, route_write_to_calvin,
        };
        let (admission, _admission_guard) = match admit(
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
            ),
            WriteAdmission::FastPath { guard } => {
                (crate::bridge::envelope::Admission::Admitted, guard)
            }
            WriteAdmission::RouteToCalvin => {
                let routed =
                    route_write_to_calvin(&self.state, tenant_id, database_id, vshard_id, plan)
                        .await?;
                return Ok(
                    routed.unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0)))
                );
            }
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
            Ok(resp) => Ok(resp),
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

#[cfg(test)]
mod tests {
    #[test]
    fn dispatch_task_compile_check() {
        // Confirm the dispatch module compiles without the old two-phase join
        // and broadcast_scan helpers.
        let _: () = ();
    }
}
