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
use super::fire_common::{
    FireErrorPolicy, FireReport, FireTriggersParams, check_cascade_depth, fire_triggers,
};
use super::registry::DmlEvent;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::system_txn::SystemTxnScope;
use std::sync::Arc;
use crate::control::security::catalog::trigger_types::{
    TriggerExecutionMode, TriggerGranularity, TriggerTiming,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Parameters for [`fire_after_statement`].
pub struct FireAfterStatementParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database and tenant scope for trigger lookup and execution.
    pub scope: TriggerScope,
    /// Target collection name.
    pub collection: &'a str,
    /// The DML event the statement performed.
    pub event: DmlEvent,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// System transaction scope (Event-Plane fire path); `None` otherwise.
    pub system_scope: Option<Arc<SystemTxnScope>>,
    /// Restricts firing to the one named trigger; `None` fires every match.
    ///
    /// A retry sets this so it re-runs only the trigger that failed.
    pub only_trigger: Option<&'a str>,
}

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
pub async fn fire_after_statement(params: FireAfterStatementParams<'_>) -> FireReport {
    let FireAfterStatementParams {
        state,
        identity,
        scope,
        collection,
        event,
        cascade_depth,
        mode_filter,
        on_error,
        system_scope,
        only_trigger,
    } = params;
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
        .filter(|t| only_trigger.is_none_or(|name| t.name == name))
        .collect();

    if statement_triggers.is_empty() {
        return FireReport::default();
    }

    if let Err(error) = check_cascade_depth(cascade_depth, collection) {
        return FireReport::from_precondition(error);
    }

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
        on_error,
        system_scope,
    })
    .await
}
