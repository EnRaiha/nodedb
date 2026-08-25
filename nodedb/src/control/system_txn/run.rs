// SPDX-License-Identifier: BUSL-1.1

//! Running a planned set of tasks as one system transaction.

use std::sync::Arc;

use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, StagingGateError, commit, lifecycle, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TraceId;
use nodedb_physical::physical_plan::meta::MetaOp;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;

use super::data_plane::SystemTxnDataPlane;
use super::scope::SystemTxnScope;

/// Why a system transaction did not commit.
#[derive(Debug, thiserror::Error)]
pub enum SystemTxnError {
    /// The transaction block could not be opened.
    #[error("system transaction could not begin: {source}")]
    Begin {
        #[source]
        source: crate::Error,
    },

    /// A statement failed before COMMIT. Nothing durable was written: every
    /// task is buffered or staged until COMMIT, and the block is rolled back.
    #[error("system transaction statement {index} of {total} failed: {source}")]
    Statement {
        index: usize,
        total: usize,
        #[source]
        source: crate::Error,
    },

    /// COMMIT itself aborted. The transaction applied nothing.
    #[error("system transaction aborted at commit: {detail}")]
    Commit { detail: String },

    /// A DDL task cannot be staged through a system transaction. DDL proposes
    /// through the metadata path with its own epoch fencing; staging it would
    /// bypass the all-or-nothing guarantee. Fail fast so a trigger body never
    /// half-applies DDL.
    #[error("DDL is not allowed inside a system transaction: {detail}")]
    Ddl { detail: String },
}

/// True when `task` is DDL that must never stage through a system
/// transaction. Today that is collection conversion (`ConvertCollection`),
/// the one DDL-shaped op that reaches the Data Plane; other DDL proposes
/// through the metadata path and never becomes a `PhysicalTask`.
pub(crate) fn is_system_ddl(task: &PhysicalTask) -> bool {
    matches!(task.plan, PhysicalPlan::Meta(MetaOp::ConvertCollection { .. }))
}

/// Push ONE planned task into an active scope, staged exactly like
/// `run_tasks_atomically` stages a pre-planned batch. Procedural trigger
/// bodies plan statement-by-statement (Option B′), so each statement's tasks
/// are pushed as they are planned instead of all upfront.
///
/// `lease_scope` is retained by the scope until COMMIT (Gap 1: a per-statement
/// `Arc<QueryLeaseScope>` must not drop before the version fence runs).
///
/// Gap 3: DDL is rejected before staging — it would propose through the
/// metadata path and bypass the all-or-nothing guarantee.
pub async fn push_task_into_scope(
    scope: &SystemTxnScope,
    state: &SharedState,
    task: PhysicalTask,
    lease_scope: Arc<QueryLeaseScope>,
    event_source: EventSource,
) -> Result<(), SystemTxnError> {
    if is_system_ddl(&task) {
        return Err(SystemTxnError::Ddl {
            detail: "collection conversion (DDL) inside a system transaction".into(),
        });
    }

    let buffered_before = scope.sessions().buffered_task_count(scope.session_id());
    let routed = route_in_tx_write(
        state,
        scope.sessions(),
        scope.session_id(),
        task,
        |staged| dispatch_staged(state, staged, event_source),
    )
    .await;
    if let Err(error) = routed {
        return Err(SystemTxnError::Statement {
            index: 0,
            total: 1,
            source: staging_error(error),
        });
    }

    // Retain the plan's leases on whatever this task buffered, so the COMMIT
    // fence has versions to compare. Gap 1: also keep the Arc alive in the
    // scope — a per-statement Arc must not drop before the fence runs.
    if scope.sessions().buffered_task_count(scope.session_id()) > buffered_before {
        if !scope.sessions().attach_tx_lease_scope_since(
            scope.session_id(),
            buffered_before,
            Arc::clone(&lease_scope),
        ) {
            return Err(SystemTxnError::Statement {
                index: 0,
                total: 1,
                source: crate::Error::Internal {
                    detail: "retaining descriptor leases for a system transaction failed".into(),
                },
            });
        }
        scope.retain_lease_scope(lease_scope);
    }
    Ok(())
}

/// Run every task as one transaction: all of them apply, or none do.
///
/// This is what makes a deferred action safe to retry. Dispatching the tasks
/// one at a time leaves a failure part-applied, and re-running a part-applied
/// action repeats whatever already landed; a transaction has no such state.
///
/// `lease_scope` is the plan's descriptor lease scope. It is retained on the
/// buffered tasks so COMMIT re-checks the versions the plan was built against
/// before it writes anything.
pub async fn run_tasks_atomically(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
    lease_scope: Arc<QueryLeaseScope>,
    event_source: EventSource,
) -> Result<(), SystemTxnError> {
    let scope = SystemTxnScope::begin(state).map_err(|source| SystemTxnError::Begin { source })?;
    let total = tasks.len();
    let dp = SystemTxnDataPlane {
        state,
        event_source,
    };

    for (index, task) in tasks.into_iter().enumerate() {
        if let Err(error) =
            push_task_into_scope(&scope, state, task, Arc::clone(&lease_scope), event_source).await
        {
            // A statement or DDL failure may have staged earlier tasks:
            // roll back the whole block before reporting (execute_block's
            // caller keeps the block alive after an error, so this must be
            // explicit here, not deferred to Drop).
            if matches!(
                error,
                SystemTxnError::Statement { .. } | SystemTxnError::Ddl { .. }
            ) {
                lifecycle::run_rollback(scope.sessions(), scope.session_id(), identity, state, &dp)
                    .await;
            }
            return Err(match error {
                SystemTxnError::Statement { source, .. } => SystemTxnError::Statement {
                    index,
                    total,
                    source,
                },
                other => other,
            });
        }
    }

    match commit::run_commit(scope.sessions(), scope.session_id(), identity, state, &dp).await {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::Aborted { reason } => Err(SystemTxnError::Commit {
            detail: describe(&reason),
        }),
    }
}

/// COMMIT a scope that procedural (statement-serial) execution filled via
/// [`push_task_into_scope`]. All buffered tasks apply, or none do.
pub async fn commit_scope(
    scope: &SystemTxnScope,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    event_source: EventSource,
) -> Result<(), SystemTxnError> {
    let dp = SystemTxnDataPlane {
        state,
        event_source,
    };
    match commit::run_commit(scope.sessions(), scope.session_id(), identity, state, &dp).await {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::Aborted { reason } => Err(SystemTxnError::Commit {
            detail: describe(&reason),
        }),
    }
}

/// ROLLBACK a scope after a statement (or body) failed. Best-effort: lease
/// GC backstops anything the rollback could not release.
pub async fn rollback_scope(
    scope: &SystemTxnScope,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    event_source: EventSource,
) {
    let dp = SystemTxnDataPlane {
        state,
        event_source,
    };
    lifecycle::run_rollback(scope.sessions(), scope.session_id(), identity, state, &dp).await;
}

/// Apply one stageable write to the transaction's overlay.
async fn dispatch_staged(
    state: &SharedState,
    task: PhysicalTask,
    event_source: EventSource,
) -> crate::Result<crate::bridge::envelope::Response> {
    dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
        state,
        dispatch_utils::WriteDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id: TraceId::ZERO,
            event_source,
            txn_id: task.txn_id,
            wal_lsn: None,
            resolved_now_ms: None,
        },
    )
    .await
}

/// Flatten a staging-gate refusal into the crate error type.
fn staging_error(error: StagingGateError) -> crate::Error {
    match error {
        StagingGateError::Dispatch(e) => e,
        StagingGateError::Rejected { code } => match code {
            Some(code) => crate::Error::DataPlane(code),
            None => crate::Error::Internal {
                detail: "a staged write was rejected without an error code".into(),
            },
        },
    }
}

/// Render a commit abort for the caller's log and retry record.
fn describe(reason: &AbortReason) -> String {
    match reason {
        AbortReason::Serialization => "serialization failure against a concurrent write".to_owned(),
        AbortReason::NoTransaction => "the transaction block was already gone".to_owned(),
        AbortReason::BatchRejected { code } => match code {
            Some(code) => format!("the data plane rejected the batch: {code:?}"),
            None => "the data plane rejected the batch".to_owned(),
        },
        AbortReason::CalvinCancelled => "the cross-shard coordinator cancelled".to_owned(),
        AbortReason::CalvinTimeout => "the cross-shard coordinator timed out".to_owned(),
        AbortReason::SchemaChanged { detail } => detail.clone(),
        AbortReason::Dispatch(e) => e.to_string(),
        AbortReason::DdlPropose(e) => e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::meta::MetaOp;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_task::PhysicalTask;

    fn task_with(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: crate::types::TenantId::new(1),
            vshard_id: nodedb_types::id::VShardId::new(0),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn is_system_ddl_detects_collection_conversion() {
        // The one DDL-shaped op that reaches the Data Plane: must be rejected.
        let ddl = task_with(PhysicalPlan::Meta(MetaOp::ConvertCollection {
            collection: "orders".to_string(),
            target_type: "document".to_string(),
            schema_json: "{}".to_string(),
        }));
        assert!(is_system_ddl(&ddl));

        // A non-DDL meta op must pass.
        let housekeeping = task_with(PhysicalPlan::Meta(MetaOp::ListContinuousAggregates));
        assert!(!is_system_ddl(&housekeeping));
    }
}
