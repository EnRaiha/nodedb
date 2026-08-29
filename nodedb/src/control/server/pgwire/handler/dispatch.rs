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
use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::core::NodeDbPgHandler;
use super::submit::SubmitArgs;

/// Inputs for [`NodeDbPgHandler::dispatch_replicated_write`]: the entry to
/// propose, the proposer, and the identity + plan its origin CDC publish needs.
struct ReplicatedWrite<'a> {
    entry: crate::control::wal_replication::ReplicatedEntry,
    proposer: &'a Arc<crate::control::wal_replication::AsyncRaftProposer>,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
}

impl NodeDbPgHandler {
    fn authorize_for_dispatch(
        &self,
        identity: &AuthenticatedIdentity,
        task: &PhysicalTask,
    ) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.state.audit));
        crate::control::server::shared::authorization::authorize_task_set(
            identity,
            std::slice::from_ref(task),
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(crate::Error::from)?
        .into_tasks()
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Internal {
            detail: "pgwire authorization returned no capability".into(),
        })
    }

    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, writes propose to Raft first and execute only after
    /// quorum commit; reads bypass Raft. `identity` must be passed for every
    /// externally derived task.
    pub(super) async fn dispatch_authorized_task(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
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

    /// Dispatch a task and return the response, per-shard watermark LSNs a fan
    /// gather observed, and per-side read captures a shuffle JOIN produced.
    /// Used by the transactional read-recording seam.
    pub(super) async fn dispatch_authorized_task_with_watermarks(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
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
        identity: &AuthenticatedIdentity,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        let tenant_id = task.tenant_id;
        let result = self
            .dispatch_task_inner(task, user_id, identity, shard_watermarks, distributed_reads)
            .await;
        // Advances per-tenant write-HLC on any successful dispatch; used by RESTORE's
        // staleness gate. Backup captures its watermark after fan-out, so it dominates.
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
        identity: &AuthenticatedIdentity,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        // Reject user writes against a database frozen by a clone materializer sweep.
        // Reads/DDL pass through.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }

        // Mirror enforcement: writes reject on non-promoted mirrors; reads gate by
        // ReadConsistency. Catalog lookup skipped for db id=0 to stay allocation-free.
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
            // Defaults to Strong: mirrors aren't the source leader, so reads reject
            // unless the session opted into BoundedStaleness or Eventual.
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

        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::insert_select::run_authorized_insert_select(
                &self.state,
                authorized,
            )
            .await;
        }

        // Autocommit `MERGE` orchestrates on the Control Plane (`control::merge_orchestrator`).
        // In-transaction MERGE buffers for COMMIT replay and never reaches this method.
        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    resolved_inserts: None,
                    ..
                }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::merge_orchestrator::run_authorized_merge(
                &self.state,
                authorized,
            )
            .await;
        }

        // Scans the source on its own core and ships raw rows into the plan (source's
        // vShard can differ). In-transaction buffers for COMMIT replay instead.
        if matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    source_rows: None,
                    ..
                }
            )
        ) {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                &self.state,
                authorized,
            )
            .await;
        }

        // Can't replicate bare over Raft — a follower has no writing identity to decide
        // `$auth.*` against. `write_resolve` resolves it while the identity is live.
        if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan)
            && self.state.async_raft_proposer().is_some()
        {
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            return crate::control::write_resolve::run_authorized_write_resolve(
                &self.state,
                authorized,
                resolver,
            )
            .await;
        }

        // `DROP ARRAY` reaches every core so each releases its store and segment dir —
        // otherwise a follow-up `CREATE ARRAY` carries stale state.
        if matches!(
            task.plan,
            crate::bridge::envelope::PhysicalPlan::Array(
                nodedb_physical::physical_plan::ArrayOp::DropArray { .. }
            )
        ) {
            // Broadcast bypasses the write funnel, so a denied DROP must not
            // delete catalog rows or surrogate bindings.
            let authorized = self.authorize_for_dispatch(identity, &task)?;
            let task = authorized.into_physical_task();
            return crate::control::array_catalog::ddl::run_authorized_drop(
                &self.state,
                task.tenant_id,
                task.database_id,
                task.plan,
                TraceId::ZERO,
            )
            .await;
        }

        // Clone-read must run first: resolving derived Exchange plans below
        // dispatches straight to the Data Plane, bypassing the clone check.
        if let Some(resp) = self
            .maybe_intercept_clone_read_early(&task, identity, perm)
            .await?
        {
            return Ok(resp);
        }

        // Resolve derived Exchange plans before authorizing the dispatched task.
        match resolve_and_materialize(
            &self.state,
            identity,
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
                let resolved_plan = *resolved_plan;
                task.plan = resolved_plan;
            }
            Resolved::Stream(stream) => {
                return crate::control::server::exchange::gather::stream_to_response(stream).await;
            }
        }

        reject_unadmitted_crdt_apply(&task.plan)?;
        let checked = self
            .intercept_and_authorize_for_dispatch(identity, task)
            .await?;
        let checked = match checked {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(resp) => {
                return Ok(resp);
            }
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(t) => t,
        };
        if let Some(async_proposer) = self.state.async_raft_proposer()
            && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                checked.tenant_id(),
                checked.database_id(),
                checked.vshard_id(),
                &crate::control::wal_replication::ReplicableWrite::decide_for_replication(
                    checked.plan(),
                )?,
            )?
        {
            return self
                .dispatch_replicated_write(ReplicatedWrite {
                    entry,
                    proposer: async_proposer,
                    authorized: checked.into_authorized(),
                })
                .await;
        }
        self.dispatch_local(checked, user_id).await
    }

    /// Dispatch a write through Raft: propose → register waiter → await apply.
    /// `ProposeTracker` is race-safe against an entry applying before register.
    ///
    /// Also the origin CDC publish site; replicas publish nothing (`ChangeFeedOwner::Unowned`).
    async fn dispatch_replicated_write(
        &self,
        args: ReplicatedWrite<'_>,
    ) -> crate::Result<Response> {
        let ReplicatedWrite {
            entry,
            proposer,
            authorized,
        } = args;
        let task = authorized.into_physical_task();
        let tenant_id = task.tenant_id;
        let database_id = task.database_id;
        let plan = task.plan;
        let request_id = self.next_request_id();

        // `write_version` is the post-write `coll_write_lsn`, surfaced so the session
        // can floor a later read-set at it (read-your-writes for cross-shard OCC).
        let (payload, write_version) =
            crate::control::wal_replication::propose_replicated_entry(&self.state, proposer, entry)
                .await?;

        let response = Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            // Authoritative participant WAL LSN — CDC ordering must use it, not zero.
            watermark_lsn: write_version,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: write_version,
            write_set: Vec::new(),
        };

        // Propose returned: entry is committed and applied. Publish once, from this plan.
        publish_origin_change_events(&self.state, tenant_id, database_id, &plan, &response);

        Ok(response)
    }

    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// WAL append happens inside the write funnel, under the admission guard just
    /// before enqueue, so LSN order equals apply order. Reads bypass the WAL entirely.
    async fn dispatch_local(
        &self,
        checked: crate::control::server::shared::clone_write::CloneCheckedTask,
        user_id: Option<Arc<str>>,
    ) -> crate::Result<Response> {
        self.submit_authorized_to_data_plane(
            checked,
            user_id,
            WalDurability::AppendHere { now_override: None },
        )
        .await
    }

    /// Dispatch a task to the Data Plane WITHOUT individual WAL append.
    ///
    /// Used by COMMIT after the transaction is written as one `RecordType::Transaction`
    /// record — per-task WAL would double-write.
    pub(super) async fn dispatch_task_no_wal(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> crate::Result<Response> {
        // Without this, a transaction begun before the freeze could COMMIT mid-scan and
        // break the as-of contract.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }
        reject_unadmitted_crdt_apply(&task.plan)?;
        let txn_id = task.txn_id;
        // Writes were durably recorded under one `RecordType::Transaction` record at
        // COMMIT; per-task WAL append is skipped. `wal_lsn` stamps that record's LSN.
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id,
            txn_id,
            // No per-task TTL instant (see `flush_transaction_buffer`), so a TTL-bearing
            // KV write falls back to `epoch_system_ms` at apply time.
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms: None,
            },
        })
        .await
    }
}

fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> crate::Result<()> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
                | CrdtOp::ApplyAuthenticated { .. }
                | CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(crate::Error::CrdtApplyRequiresAdmission);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    #[test]
    fn generic_pgwire_dispatch_rejects_unadmitted_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }

    #[test]
    fn dispatch_task_compile_check() {
        // Confirms the dispatch module compiles.
        let _: () = ();
    }
}
