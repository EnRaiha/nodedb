// SPDX-License-Identifier: BUSL-1.1

//! AFTER STATEMENT trigger firing logic.
//!
//! AFTER STATEMENT triggers fire once per DML statement, not per row.
//! They receive TG_OP and TG_TABLE_NAME but no NEW/OLD row references
//! (since there is no single row context).
//!
//! Supports all three execution modes:
//! - SYNC: fires in the Control Plane after all rows are dispatched
//! - ASYNC: fires in the Event Plane after statement commits
//! - DEFERRED: fires at COMMIT time, batched

use super::TriggerScope;
use super::fire_common::{FireTriggersParams, check_cascade_depth, fire_triggers};
use super::registry::DmlEvent;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::security::catalog::trigger_types::{
    TriggerExecutionMode, TriggerGranularity, TriggerTiming,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Fire AFTER STATEMENT triggers for the given operation.
///
/// Called once after all rows of a DML statement have been dispatched.
/// `mode_filter` controls which execution mode triggers are fired
/// (same semantics as the ROW-level fire functions).
///
/// Statement-level triggers receive:
/// - `TG_OP`: the operation name ("INSERT", "UPDATE", "DELETE")
/// - `TG_TABLE_NAME`: the collection name
/// - `TG_WHEN`: "AFTER"
/// - NO `NEW` or `OLD` row references
pub async fn fire_after_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    scope: TriggerScope,
    collection: &str,
    event: DmlEvent,
    cascade_depth: u32,
    mode_filter: Option<TriggerExecutionMode>,
) -> crate::Result<()> {
    let triggers = state.trigger_registry.get_matching(
        scope.database_id,
        scope.tenant_id.as_u64(),
        collection,
        event,
    );

    let statement_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Statement)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if statement_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    // Statement-level bindings: TG_OP + TG_TABLE_NAME, no NEW/OLD.
    let bindings = RowBindings::statement(collection, event.as_str());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id: scope.tenant_id,
        collection,
        triggers: &statement_triggers,
        bindings: &bindings,
        cascade_depth,
        // STATEMENT triggers are outside the Event-Plane async ROW-trigger
        // cross-shard sender path (see tracked follow-up).
        cross_shard_origin: None,
    })
    .await
}
