// SPDX-License-Identifier: BUSL-1.1

//! Running one scheduled job's body as one transaction.

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::{AtomicBody, StatementExecutor};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::types::ScheduleDef;

/// Execute a single scheduled job under a wall-clock `ExecutionBudget`.
///
/// `job_timeout_secs` bounds total statement-executor time for the job,
/// so a runaway body cannot hold `Arc<SharedState>` past the shutdown
/// deadline. Returns the duration in milliseconds on success.
pub(super) async fn execute_job(
    state: &SharedState,
    sched: &ScheduleDef,
    job_timeout_secs: u64,
) -> crate::Result<u64> {
    use crate::control::planner::procedural::executor::fuel::ExecutionBudget;

    let start = std::time::Instant::now();
    let identity = scheduler_identity(TenantId::new(sched.tenant_id), &sched.owner);

    // Pre-execution guard: reject unbounded `SELECT *` bodies before we
    // dispatch a single task, so the runtime byte ceiling isn't the only
    // thing standing between a careless schedule and hours of wasted
    // scanning.
    if let Err(msg) = super::body_guard::validate_scheduled_body(&sched.body_sql) {
        return Err(crate::Error::BadRequest {
            detail: format!("schedule '{}': {msg}", sched.name),
        });
    }

    let block = crate::control::planner::procedural::parse_block(&sched.body_sql).map_err(|e| {
        crate::Error::BadRequest {
            detail: format!("schedule '{}' body parse error: {e}", sched.name),
        }
    })?;

    // The body is one transaction, so a failed attempt applies nothing and
    // the retry below cannot repeat a part that already landed.
    let executor = StatementExecutor::with_source_in_database(
        state,
        identity.clone(),
        TenantId::new(sched.tenant_id),
        crate::types::DatabaseId::new(sched.database_id),
        0,
        crate::event::EventSource::User,
    )
    .with_atomic_body(AtomicBody::scheduled_job(&sched.name));
    let bindings = RowBindings::empty();
    // One budget for the whole job — retries consume the same wall-clock
    // and fuel pool as the first attempt so a runaway job can't double
    // its timeout by failing once.
    let mut budget = ExecutionBudget::new(100_000, job_timeout_secs);

    // A job runs with no cross-shard origin, so it holds no cross-node
    // write, and its `PUBLISH TO` messages commit in its redo record.
    match executor
        .execute_block_with_budget(&block, &bindings, &mut budget)
        .await
    {
        Ok(()) => {}
        Err(first_err) => {
            tracing::warn!(
                schedule = %sched.name,
                error = %first_err,
                "scheduled job failed, retrying once (possible vShard migration)"
            );
            let retry_executor = StatementExecutor::with_source_in_database(
                state,
                identity,
                TenantId::new(sched.tenant_id),
                crate::types::DatabaseId::new(sched.database_id),
                0,
                crate::event::EventSource::User,
            )
            .with_atomic_body(AtomicBody::scheduled_job(&sched.name));
            retry_executor
                .execute_block_with_budget(&block, &bindings, &mut budget)
                .await?;
        }
    }

    Ok(start.elapsed().as_millis() as u64)
}

/// Build the owner's identity for scheduled job execution (SECURITY DEFINER).
///
/// SECURITY DEFINER semantics: scheduled jobs always run as superuser
/// under the creator's username. The username drives audit attribution;
/// privilege is fixed at superuser regardless of the creator's current
/// role membership.
fn scheduler_identity(tenant_id: TenantId, owner: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        owner,
        tenant_id,
        vec![Role::Superuser],
        true,
        None,
        crate::control::security::identity::DatabaseSet::All,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_identity_is_superuser() {
        let id = scheduler_identity(TenantId::new(1), "admin");
        assert!(id.is_superuser);
        assert_eq!(id.username, "admin");
    }
}
