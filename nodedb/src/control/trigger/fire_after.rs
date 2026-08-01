// SPDX-License-Identifier: BUSL-1.1

//! AFTER trigger firing logic.
//!
//! Called after DML operations to fire matching AFTER ROW triggers.
//! Matches triggers by (collection, event), evaluates WHEN clauses,
//! and invokes the statement executor for each matching trigger body.
//!
//! Supports three execution modes via `mode_filter`:
//! - `Some(Sync)`: fire in the Control Plane write path (same transaction)
//! - `Some(Async)`: fire from Event Plane (eventually consistent)
//! - `Some(Deferred)`: fire at COMMIT time (same transaction, batched)
//! - `None`: fire all AFTER triggers regardless of mode (legacy behavior)

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::{TriggerExecutionMode, TriggerTiming};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use std::collections::HashMap;

use super::fire_common::{FireTriggersParams, check_cascade_depth, fire_triggers};
use super::registry::DmlEvent;

/// Parameters for [`fire_after_insert`].
pub struct FireAfterInsertParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Inserted row fields (bound as `NEW.*`).
    pub new_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
}

/// Fire AFTER ROW triggers for an INSERT operation.
///
/// Called after a successful INSERT dispatch. `new_fields` contains the
/// inserted row's field values. The trigger body's DML is dispatched through
/// the normal plan+SPSC path, executing in the same logical transaction context.
///
/// `mode_filter` selects which execution mode to fire:
/// - `Some(Sync)`: only fire SYNC triggers (called from write path)
/// - `Some(Async)`: only fire ASYNC triggers (called from Event Plane)
/// - `None`: fire all AFTER triggers regardless of mode (legacy behavior)
pub async fn fire_after_insert(params: FireAfterInsertParams<'_>) -> crate::Result<()> {
    let FireAfterInsertParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        new_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Insert,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_insert(collection, new_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
    })
    .await
}

/// Parameters for [`fire_after_update`].
pub struct FireAfterUpdateParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Row fields before the update (bound as `OLD.*`).
    pub old_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Row fields after the update (bound as `NEW.*`).
    pub new_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
}

/// Fire AFTER ROW triggers for an UPDATE operation.
///
/// `old_fields` is the row before the update, `new_fields` is after.
/// Both are available as OLD.field and NEW.field in the trigger body.
pub async fn fire_after_update(params: FireAfterUpdateParams<'_>) -> crate::Result<()> {
    let FireAfterUpdateParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        old_fields,
        new_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Update,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_update(collection, old_fields.clone(), new_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
    })
    .await
}

/// Parameters for [`fire_after_delete`].
pub struct FireAfterDeleteParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// Deleted row fields (bound as `OLD.*`).
    pub old_fields: &'a HashMap<String, nodedb_types::Value>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Cross-shard origin context (Event-Plane fire path only).
    pub cross_shard_origin: Option<CrossShardOrigin>,
}

/// Fire AFTER ROW triggers for a DELETE operation.
///
/// `old_fields` is the deleted row. Available as OLD.field in the trigger body.
pub async fn fire_after_delete(params: FireAfterDeleteParams<'_>) -> crate::Result<()> {
    let FireAfterDeleteParams {
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        old_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
    } = params;

    let triggers = state.trigger_registry.get_matching(
        database_id,
        tenant_id.as_u64(),
        collection,
        DmlEvent::Delete,
    );

    let after_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_triggers.is_empty() {
        return Ok(());
    }

    check_cascade_depth(cascade_depth, collection)?;

    let bindings = RowBindings::after_delete(collection, old_fields.clone());

    fire_triggers(FireTriggersParams {
        state,
        identity,
        tenant_id,
        collection,
        triggers: &after_triggers,
        bindings: &bindings,
        cascade_depth,
        cross_shard_origin,
    })
    .await
}

/// Execute raw SQL in a trigger-like context (no row bindings).
///
/// Used by the cross-shard receiver to execute trigger-originated DML
/// on the target node. The SQL is parsed and executed through the normal
/// Control Plane → Data Plane path with cascade depth tracking.
pub async fn fire_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    sql: &str,
    cascade_depth: u32,
) -> crate::Result<()> {
    use crate::control::planner::procedural::executor::bindings::RowBindings;
    use crate::control::planner::procedural::executor::core::{
        MAX_CASCADE_DEPTH, StatementExecutor,
    };

    if cascade_depth >= MAX_CASCADE_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!("cross-shard cascade depth exceeded ({MAX_CASCADE_DEPTH})"),
        });
    }

    let block = crate::control::planner::procedural::parse_block(sql).map_err(|e| {
        crate::Error::BadRequest {
            detail: format!("cross-shard SQL parse error: {e}"),
        }
    })?;

    let executor = StatementExecutor::with_source_in_database(
        state,
        identity.clone(),
        tenant_id,
        database_id,
        cascade_depth,
        crate::event::EventSource::Trigger,
    );
    let bindings = RowBindings::empty();

    executor
        .execute_block(&block, &bindings)
        .await
        .map_err(|e| crate::Error::BadRequest {
            detail: format!("cross-shard SQL execution failed: {e}"),
        })
}
