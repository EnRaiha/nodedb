// SPDX-License-Identifier: BUSL-1.1

//! Run a plan on the node that owns its collection's vShard.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::ErrorCode;
use crate::control::gateway::core::QueryContext;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::PhysicalPlan;

/// Run `plan` on the node that owns its vShard and return its payload.
///
/// The plan routes through the gateway like any statement: a read runs
/// where the owning group serves it, confirmed linearizable, and a write
/// goes through the owner's replicated write path.
///
/// A `NotFound` verdict returns an empty payload. A plan that answers from
/// several shards is refused: every caller reads one owner. `caller` names
/// the caller in that refusal.
pub(crate) async fn dispatch_to_owner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
    caller: &str,
) -> crate::Result<Vec<u8>> {
    let gateway = state.installed_gateway()?;
    let ctx = QueryContext {
        tenant_id,
        trace_id: TraceId::generate(),
        database_id,
        txn_id: None,
        linearizable: true,
    };
    let mut payloads = match gateway.execute_internal(&ctx, plan).await {
        Ok(payloads) => payloads,
        Err(crate::Error::DataPlane(ErrorCode::NotFound)) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    if payloads.len() > 1 {
        return Err(crate::Error::Storage {
            engine: caller.to_string(),
            detail: format!(
                "plan on '{collection_qualified}' answered from {} shards; \
                 the caller reads one owner",
                payloads.len()
            ),
        });
    }
    Ok(payloads.pop().unwrap_or_default())
}
