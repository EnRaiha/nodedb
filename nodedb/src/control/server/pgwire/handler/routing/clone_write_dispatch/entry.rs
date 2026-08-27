// SPDX-License-Identifier: BUSL-1.1

//! Single hooked-in clone CoW write-interception entry point. Routes by plan
//! shape to the Document or KV copy-up/tombstone protocol.

use pgwire::error::PgWireResult;

use nodedb_types::TenantId;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::core::NodeDbPgHandler;

/// Outcome of write-path clone interception.
pub(in crate::control::server::pgwire::handler::routing) enum CloneWriteOutcome {
    /// No interception needed — caller must dispatch normally.
    Passthrough,
    /// The write was fully handled by the clone path. Caller uses this response.
    Handled(Response),
}

impl NodeDbPgHandler {
    /// Intercept a single write task for a cloned collection.
    ///
    /// Call before every write dispatch — an insert of a key the source already
    /// holds needs the source row suppressed too.
    pub(in crate::control::server::pgwire::handler::routing) async fn maybe_intercept_clone_write(
        &self,
        task: &mut PhysicalTask,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        // Classify first: the shape check borrows the plan, and the document
        // arm needs it mutably (a copy-up retargets the plan's surrogate).
        enum Shape {
            Document,
            KvMutate,
            KvInsert,
            None,
        }
        let shape = match &task.plan {
            PhysicalPlan::Document(
                DocumentOp::PointUpdate { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointPut { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::BatchInsert { .. },
            ) => Shape::Document,
            PhysicalPlan::Kv(KvOp::FieldSet { .. } | KvOp::Delete { .. }) => Shape::KvMutate,
            PhysicalPlan::Kv(
                KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::BatchPut { .. },
            ) => Shape::KvInsert,
            _ => Shape::None,
        };
        match shape {
            Shape::Document => {
                self.intercept_doc_clone_write(task, identity, tenant_id)
                    .await
            }
            Shape::KvMutate => {
                self.intercept_kv_clone_write(task, identity, tenant_id)
                    .await
            }
            Shape::KvInsert => self.intercept_kv_clone_insert(task, tenant_id).await,
            Shape::None => Ok(CloneWriteOutcome::Passthrough),
        }
    }
}
