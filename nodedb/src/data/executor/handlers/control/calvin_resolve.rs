// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::CalvinResolve` handler: resolve a staged Calvin transaction's
//! write plans into ONE replayable [`RedoRecord`][crate::wal::RedoRecord],
//! WITHOUT mutating base.
//!
//! Mirrors `MetaOp::ResolveTxn`'s `CoreLoop::execute_resolve_txn` exactly, but
//! sources its plans and tenant scope from Calvin's own staging state instead
//! of a session transaction's: the plans buffered in `commit_pending` under
//! `(epoch, position, vshard)` (by [`CoreLoop::execute_calvin_execute_static`])
//! and the per-core `txn_overlays` / `graph_txn_overlays` entries staged under
//! the corresponding synthetic `TxnId` (by
//! [`CoreLoop::stage_calvin_overlay`][super::calvin_overlay_stage]). Reusing
//! `execute_resolve_txn` directly means the redo serialization logic itself is
//! never duplicated.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::calvin_txn_id::calvin_synthetic_txn_id;

impl CoreLoop {
    /// Resolve the Calvin transaction staged under `(epoch, position)` on
    /// this vshard into a [`RedoRecord`][crate::wal::RedoRecord] and return
    /// its encoded bytes, without touching any base engine.
    ///
    /// Errors (rather than silently dropping data or producing an empty
    /// record) when:
    /// - no `commit_pending` entry exists for `(epoch, position, vshard)` —
    ///   the transaction was never staged (or was already flushed/dropped),
    ///   and there is nothing to resolve;
    /// - the staged plan set contains a `DocumentOp::BulkUpdate` /
    ///   `BulkDelete`. `stage_calvin_overlay` deliberately does not stage
    ///   these predicate writes into the overlay yet (see its module docs),
    ///   so resolving them here would silently produce a redo record missing
    ///   those rows. Aborting loudly is strictly better than a non-durable
    ///   commit.
    pub(in crate::data::executor) fn execute_calvin_resolve(
        &self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(e) => return self.response_error(task, e),
        };

        let Some(pending) = self.commit_pending.get(&(epoch, position, vshard_id)) else {
            return self.response_error(
                task,
                crate::Error::Internal {
                    detail: format!(
                        "calvin resolve: no staged commit for epoch={epoch} \
                         position={position} vshard={vshard_id} (must be staged via \
                         CalvinExecuteStatic before CalvinResolve)"
                    ),
                },
            );
        };

        if let Some(plan) = pending.plans.iter().find(|plan| {
            matches!(
                plan,
                PhysicalPlan::Document(
                    DocumentOp::BulkUpdate { .. } | DocumentOp::BulkDelete { .. }
                )
            )
        }) {
            return self.response_error(
                task,
                crate::Error::Internal {
                    detail: format!(
                        "calvin resolve: DocumentOp::BulkUpdate/BulkDelete not yet supported \
                         for multi-shard redo durability (predicate writes need \
                         determinism-preserving overlay staging); aborting rather than \
                         committing non-durably: {plan:?}"
                    ),
                },
            );
        }

        let tid = pending.tenant_id.as_u64();
        let plans = &pending.plans;
        self.execute_resolve_txn(task, tid, synthetic_txn_id, plans)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_types::Surrogate;
    use nodedb_types::Value;

    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
    use crate::types::{DatabaseId, RequestId, TenantId, TraceId, VShardId};
    use crate::wal::RedoRecord;

    /// A minimal `ExecutionTask` homing to vShard 0, tenant 1, database
    /// DEFAULT, matching what the Calvin scheduler dispatches with (see
    /// `dispatch.rs`'s `DatabaseId::DEFAULT` for `CalvinExecuteStatic` /
    /// `CalvinFlush` / `CalvinDrop`; `CalvinResolve` must match).
    fn make_task() -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "x".into(),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    fn doc_value(field: &str, val: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(field.to_string(), Value::String(val.into()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn point_insert_plan(collection: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            value: doc_value("a", "1"),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
        })
    }

    fn bulk_update_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: collection.to_string(),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        })
    }

    fn stage(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        plans: &[PhysicalPlan],
    ) {
        let ctx = CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(task, ctx, &TenantId::new(1), plans, &[]);
        assert_eq!(resp.status, Status::Ok, "staging must succeed: {resp:?}");
    }

    #[test]
    fn calvin_resolve_returns_redo_for_staged_point_writes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        stage(
            &mut core,
            &task,
            1,
            0,
            &[point_insert_plan("orders", "o1", 7)],
        );

        let resp = core.execute_calvin_resolve(&task, 1, 0);
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");

        let record = RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record");
        assert!(
            record.calvin_stamp.is_none(),
            "calvin_stamp is filled in by a later unit, not resolve itself"
        );
        assert_eq!(record.ops.len(), 1, "one staged document put");

        let (collection, doc_id, value, prov, surrogate): (
            String,
            String,
            Vec<u8>,
            Option<nodedb_types::sync::wire::SyncProvenance>,
            u32,
        ) = zerompk::from_msgpack(&record.ops[0].payload).expect("decode document put sub-record");
        assert_eq!(collection, "orders");
        assert_eq!(doc_id, "o1");
        assert_eq!(
            value,
            crate::data::executor::doc_format::canonicalize_document_for_storage(&doc_value(
                "a", "1"
            ))
        );
        assert!(prov.is_none());
        assert_eq!(surrogate, 7);
    }

    #[test]
    fn calvin_resolve_rejects_bulk_update() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        stage(&mut core, &task, 2, 0, &[bulk_update_plan("orders")]);

        let resp = core.execute_calvin_resolve(&task, 2, 0);
        assert_eq!(
            resp.status,
            Status::Error,
            "the completeness guard must reject an unstaged BulkUpdate rather \
             than silently produce an empty redo record"
        );
    }

    #[test]
    fn calvin_resolve_missing_pending_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        let resp = core.execute_calvin_resolve(&task, 99, 0);
        assert_eq!(
            resp.status,
            Status::Error,
            "resolving an (epoch, position) that was never staged must error"
        );
    }
}
