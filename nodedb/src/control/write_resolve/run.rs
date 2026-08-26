// SPDX-License-Identifier: BUSL-1.1

//! The one resolve -> apply -> propose -> retry-on-drift loop.

use nodedb_types::TenantId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;

use super::propose::{ProposeOutcome, propose_resolved};
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// Attempts a governed predicate write makes before a resolution that keeps
/// drifting under concurrent writes is reported rather than retried forever.
/// Mirrors `update_from_join_orchestrator`'s bound: the retry exists to absorb
/// concurrent drift between resolve and apply, not to mask a resolution that
/// can never converge.
pub const MAX_WRITE_RESOLVE_RETRIES: u32 = 8;

/// Consume an authorized, governed, replicated predicate write at the
/// orchestration boundary.
///
/// `resolver` is the one the caller already built via
/// [`super::resolver_for_plan`] from this same task's plan before
/// authorizing it — never rebuilt here, so the predicate/filters/updates
/// clones it carries happen exactly once per write.
pub async fn run_authorized_write_resolve(
    state: &SharedState,
    authorized: AuthorizedTask,
    resolver: Box<dyn EngineWriteResolver>,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let ctx = WriteResolveContext {
        tenant_id: task.tenant_id,
        database_id: task.database_id,
    };
    run_write_resolve(state, ctx, &*resolver)
        .await
        .map_err(|e| surface_policy_refusal(e, ctx.tenant_id))
}

/// Drive `resolver` through resolve -> apply -> propose, re-resolving on
/// concurrent drift up to [`MAX_WRITE_RESOLVE_RETRIES`].
pub async fn run_write_resolve(
    state: &SharedState,
    ctx: WriteResolveContext,
    resolver: &dyn EngineWriteResolver,
) -> crate::Result<Response> {
    let mut attempt: u32 = 0;
    loop {
        let resolve_op = resolver.build_resolve_op();
        let resolved = resolver.resolve(state, ctx, resolve_op).await?;
        let resolved_plan = resolver.apply(resolved)?;

        match propose_resolved(state, ctx, resolver.collection(), resolved_plan).await? {
            ProposeOutcome::Applied(response) => return Ok(response),
            ProposeOutcome::RetryRequired => {
                attempt += 1;
                if attempt > MAX_WRITE_RESOLVE_RETRIES {
                    return Err(crate::Error::OllpExhausted {
                        retries: MAX_WRITE_RESOLVE_RETRIES.min(u8::MAX as u32) as u8,
                    });
                }
            }
        }
    }
}

/// Turn the Data Plane's policy refusal into the Control Plane's own
/// authorization error.
///
/// The resolve step runs the write gate inside the Data Plane, so a refused
/// row comes back wrapped as `Error::DataPlane`. That wrapper's `Display` is
/// the debug form of the code, which would put an internal enum on the wire.
/// A refusal reaches the client through the same error a directly dispatched
/// statement uses, so the SQLSTATE and the message stay the same either way.
fn surface_policy_refusal(error: crate::Error, tenant_id: TenantId) -> crate::Error {
    match error {
        crate::Error::DataPlane(ErrorCode::RejectedAuthz { resource }) => {
            crate::Error::RejectedAuthz {
                tenant_id,
                resource,
            }
        }
        other => other,
    }
}
