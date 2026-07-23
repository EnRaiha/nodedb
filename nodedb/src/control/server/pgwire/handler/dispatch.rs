// SPDX-License-Identifier: BUSL-1.1

//! Core dispatch mechanics: single-task dispatch, Raft replication, and local Data Plane submission.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils::{WalDurability, publish_origin_change_events};
use crate::control::server::exchange::resolve::{
    DistributedReadCapture, Resolved, resolve_and_materialize,
};
use crate::types::{Lsn, ReadConsistency, TraceId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::core::NodeDbPgHandler;
use super::submit::SubmitArgs;

/// Inputs for [`NodeDbPgHandler::dispatch_replicated_write`]: the entry to
/// propose, the proposer, and the identity + plan its origin CDC publish needs.
struct ReplicatedWrite<'a> {
    entry: crate::control::wal_replication::ReplicatedEntry,
    proposer: &'a Arc<crate::control::wal_replication::AsyncRaftProposer>,
    tenant_id: crate::types::TenantId,
    database_id: crate::types::DatabaseId,
    /// The plan `entry` encodes. The entry does not hand the plan back, and the
    /// change events must be derived from it after the entry is proposed, so
    /// the borrow is carried through rather than re-decoded.
    plan: &'a crate::bridge::envelope::PhysicalPlan,
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
        let mut distributed_reads = Vec::new();
        self.dispatch_task_hlc(
            task,
            user_id,
            identity,
            &mut shard_watermarks,
            &mut distributed_reads,
        )
        .await
    }

    /// Dispatch a single physical task and return the response, the per-shard
    /// watermark LSNs a single-node fan gather observed (empty for a
    /// non-gathered read), and the per-side read captures a distributed shuffle
    /// JOIN produced (empty otherwise). Used by the transactional
    /// read-recording seam so a multi-core fan read records one read-set entry
    /// per participating shard, and a shuffle join records one per join side.
    pub(super) async fn dispatch_task_with_watermarks(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
    ) -> crate::Result<(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>)> {
        let mut shard_watermarks = Vec::new();
        let mut distributed_reads = Vec::new();
        let resp = self
            .dispatch_task_hlc(
                task,
                user_id,
                identity,
                &mut shard_watermarks,
                &mut distributed_reads,
            )
            .await?;
        Ok((resp, shard_watermarks, distributed_reads))
    }

    async fn dispatch_task_hlc(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: Option<&AuthenticatedIdentity>,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        let tenant_id = task.tenant_id;
        let result = self
            .dispatch_task_inner(task, user_id, identity, shard_watermarks, distributed_reads)
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
        distributed_reads: &mut Vec<DistributedReadCapture>,
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
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(std::time::Duration::ZERO)
                .as_millis() as u64;
            let outcome = check_mirror_read_consistency(
                catalog,
                task.database_id,
                origin,
                ReadConsistency::Strong,
                now_ms,
            );
            if let MirrorReadOutcome::Reject { message, .. } = outcome {
                return Err(crate::Error::StaleReadNotLeader {
                    database: descriptor.name.clone(),
                    source_cluster: origin.source_cluster.clone(),
                    detail: message,
                });
            }
        }

        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            },
        ) = &task.plan
        {
            return crate::control::insert_select::run_insert_select(
                &self.state,
                task.tenant_id,
                task.database_id,
                target_collection,
                source_collection,
                source_filters,
                *source_limit,
            )
            .await;
        }

        // Autocommit `MERGE` is orchestrated on the Control Plane
        // (`control::merge_orchestrator`): the source is scanned, each
        // NOT-MATCHED insert row is assigned its OWN fresh, registered
        // surrogate, and all arms apply atomically. In-transaction MERGE is
        // buffered for COMMIT replay (`dispatch_task_no_wal`) and never reaches
        // this method, so this intercept fires only for autocommit.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::Merge {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
                returning: _,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: _,
            },
        ) = &task.plan
        {
            return crate::control::merge_orchestrator::run_merge(
                &self.state,
                crate::control::merge_orchestrator::MergeArgs {
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    clauses,
                },
            )
            .await;
        }

        // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control
        // Plane (`control::update_from_join_orchestrator`): the source is scanned
        // on its OWN core and the raw rows are shipped into the plan so the
        // target-core handler joins against them instead of a local read (the
        // source's vShard can live on a different core). In-transaction
        // `UPDATE ... FROM` is buffered for COMMIT replay and never reaches this
        // method, so this intercept fires only for autocommit.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filters,
                returning,
                resolve_only: false,
                source_rows: None,
            },
        ) = &task.plan
        {
            return crate::control::update_from_join_orchestrator::run_update_from_join(
                &self.state,
                crate::control::update_from_join_orchestrator::UpdateFromJoinArgs {
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    updates,
                    target_filters,
                    returning: returning.as_ref(),
                },
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
                Resolved::Gathered(resp, wms, caps) => {
                    *shard_watermarks = wms;
                    *distributed_reads = caps;
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

        if let Some(async_proposer) = self.state.async_raft_proposer()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                &task.plan,
            )
        {
            return self
                .dispatch_replicated_write(ReplicatedWrite {
                    entry,
                    proposer: async_proposer,
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    // The CDC publish inside needs the plan, which the entry
                    // encoded but does not hand back; `task` still owns it here.
                    plan: &task.plan,
                })
                .await;
        }

        self.dispatch_local(task, user_id).await
    }

    /// Dispatch a write through Raft: propose → register waiter → await apply.
    ///
    /// The `AsyncRaftProposer` handles propose + waiter registration in one
    /// step. The `ProposeTracker` is race-safe: if the entry commits and
    /// applies on this node before `register()` is called, the result is
    /// stored and `register()` picks it up immediately.
    ///
    /// This is also the origin CDC publish site for a replicated write. It runs
    /// on exactly one node — the one the client wrote to — and returns only
    /// after the entry is committed and applied, which is precisely the
    /// "acknowledged, committed, applied" point a change event names. The
    /// replicas' apply loops deliberately publish nothing (see
    /// `ChangeFeedOwner::Unowned`).
    async fn dispatch_replicated_write(
        &self,
        args: ReplicatedWrite<'_>,
    ) -> crate::Result<Response> {
        let ReplicatedWrite {
            entry,
            proposer,
            tenant_id,
            database_id,
            plan,
        } = args;
        let request_id = self.next_request_id();

        // Propose through Raft with transparent leader-change retry. Shared with
        // the durable RESTORE re-issue path so both replicate identically.
        // `write_version` is the written collection's post-write
        // `coll_write_lsn` as the applying replica recorded it — a WAL LSN, the
        // single domain the read validator compares in. Surfacing it on the
        // response lets the session record its own committed writes so a later
        // transaction's read-set can be floored at them (read-your-writes floor
        // for cross-shard OCC), instead of losing the version to `ZERO`.
        let (payload, write_version) =
            crate::control::wal_replication::propose_replicated_entry(&self.state, proposer, entry)
                .await?;

        let response = Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            // Replicated apply returns the authoritative participant WAL LSN.
            // CDC ordering must use it rather than a synthetic zero watermark.
            watermark_lsn: write_version,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: write_version,
            write_set: Vec::new(),
        };

        // The propose returned, so the entry is committed and this node has
        // applied it. Publish once, here, from the plan the entry encodes.
        publish_origin_change_events(&self.state, tenant_id, database_id, plan, &response);

        Ok(response)
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// For write operations the WAL append is performed inside the shared write
    /// funnel (`WalDurability::AppendHere`), under the write-admission guard and
    /// immediately before the enqueue, so the minted LSN order equals the
    /// Data-Plane apply order per key. Reads bypass the WAL entirely (the append
    /// helper is a no-op for non-write plans).
    async fn dispatch_local(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
    ) -> crate::Result<Response> {
        let txn_id = task.txn_id;
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id,
            txn_id,
            // The funnel mints the LSN under the admission guard just before
            // enqueue.
            durability: WalDurability::AppendHere { now_override: None },
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
            // Durability was recorded at COMMIT under a single `Transaction`
            // record whose LSN is `wal_lsn`, so the funnel must not append again.
            // That batch record carries no per-task resolved TTL instant (see
            // `flush_transaction_buffer`'s equivalent limitation), so a
            // TTL-bearing KV write inside a multi-task COMMIT batch falls back to
            // `epoch_system_ms` / the wall clock at apply time.
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms: None,
            },
        })
        .await
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
