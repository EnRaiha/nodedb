// SPDX-License-Identifier: BUSL-1.1

//! Single-event trigger dispatch: one `WriteEvent` → matching AFTER triggers.
//!
//! For each incoming `WriteEvent` with a triggerable source, this path:
//! 1. Deserializes `new_value` / `old_value` from MessagePack to
//!    `HashMap<String, nodedb_types::Value>`
//! 2. Fires every matching AFTER trigger through `control::trigger::fire`
//! 3. Queues one retry record per trigger that failed
//!
//! Every matching trigger fires even when one of them fails. These triggers
//! share no transaction, so a failure carries no reason to cancel the rest,
//! and a retry re-runs only the trigger named on its record — a sibling
//! skipped here would never run at all.

use std::sync::Arc;

use tracing::{trace, warn};

use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::control::trigger::TriggerScope;
use crate::control::trigger::fire;
use crate::control::trigger::fire_common::{FireErrorPolicy, FireReport};
use crate::control::trigger::fire_statement::{FireAfterStatementParams, fire_after_statement};
use crate::control::system_txn::{SystemTxnScope, commit_scope, rollback_scope};
use crate::control::trigger::row_identity::inject_row_identity;
use crate::event::action::ActionRetryQueue;
use crate::event::types::{EventSource, WriteEvent, WriteOp, deserialize_event_payload};
use crate::types::TenantId;

use super::enqueue::{ActionSource, record_row_failures, record_statement_failures};
use super::identity::trigger_identity;

/// Dispatch a `WriteEvent` to matching AFTER triggers.
///
/// Skips events not from `EventSource::User` / `Deferred` (cascade
/// prevention). Trigger failures never propagate to the caller — they are
/// queued for retry and, once out of attempts, routed to the DLQ.
pub async fn dispatch_triggers(
    event: &WriteEvent,
    state: &Arc<SharedState>,
    queue: &mut ActionRetryQueue,
) {
    let mode_filter = match event.source {
        EventSource::User => Some(TriggerExecutionMode::Async),
        EventSource::Deferred => Some(TriggerExecutionMode::Deferred),
        _ => {
            trace!(
                source = %event.source,
                collection = %event.collection,
                "skipping trigger dispatch for non-triggerable event source"
            );
            return;
        }
    };

    let new_fields = row_fields(event.new_value.as_deref(), event.row_id.as_str());
    let old_fields = row_fields(event.old_value.as_deref(), event.row_id.as_str());

    let identity = trigger_identity(event.tenant_id);
    let op_str = event.op.to_string();

    // System-transaction scope for the Event-Plane ASYNC fire path: every
    // body fired by this event stages into ONE transaction, so a mid-body
    // failure rolls back the whole event (all-or-nothing + retry-safe).
    // Sync and DEFERRED fires must NOT be wrapped — they already run inside
    // the client's transaction.
    let system_scope = if event.source == EventSource::User {
        match SystemTxnScope::begin(state) {
            Ok(scope) => Some(Arc::new(scope)),
            Err(e) => {
                warn!(
                    error = %e,
                    "trigger dispatch: system txn begin failed; firing without atomicity"
                );
                None
            }
        }
    } else {
        None
    };
    let source = ActionSource {
        database_id: event.database_id,
        tenant_id: event.tenant_id.as_u64(),
        collection: &event.collection,
        row_id: event.row_id.as_str(),
        operation: &op_str,
        source_lsn: event.lsn.as_u64(),
        source_sequence: event.sequence,
        source_vshard: event.vshard_id.as_u32(),
        cascade_depth: 0,
    };

    // Bulk events are only created during WAL replay and always carry
    // `new_value: None` / `old_value: None` — they are aggregate metadata (a
    // count of affected rows), not per-row payloads. The Data Plane ring
    // buffer emits an individual Insert/Delete event per row, so ROW triggers
    // fire on those. A bulk event still represents one complete statement, so
    // STATEMENT triggers do fire on it.
    let is_bulk = matches!(
        event.op,
        WriteOp::BulkInsert { .. } | WriteOp::BulkDelete { .. }
    );

    let mut row_failed = false;
    if !is_bulk {
        let report = fire_for_operation(FireForOperationParams {
            operation: &op_str,
            state,
            identity: &identity,
            database_id: event.database_id,
            tenant_id: event.tenant_id,
            collection: &event.collection,
            new_fields: new_fields.as_ref(),
            old_fields: old_fields.as_ref(),
            cascade_depth: 0,
            mode_filter,
            cross_shard_origin: Some(CrossShardOrigin {
                source_lsn: event.lsn.as_u64(),
                source_sequence: event.sequence,
                source_vshard: event.vshard_id.as_u32(),
                source_collection: event.collection.to_string(),
            }),
            on_error: FireErrorPolicy::Continue,
            only_trigger: None,
            system_scope: system_scope.clone(),
        })
        .await;
        row_failed = report.has_failure();
        record_row_failures(
            &source,
            report,
            new_fields.as_ref(),
            old_fields.as_ref(),
            queue,
        );
    }

    // STATEMENT triggers are a separate action from the ROW triggers of the
    // same write, so they fire whether or not a ROW trigger failed.
    let Some(dml_event) = dml_event_of(&event.op) else {
        return;
    };
    let report = fire_after_statement(FireAfterStatementParams {
        state,
        identity: &identity,
        scope: TriggerScope {
            database_id: event.database_id,
            tenant_id: event.tenant_id,
        },
        collection: &event.collection,
        event: dml_event,
        cascade_depth: 0,
        mode_filter,
        on_error: FireErrorPolicy::Continue,
        only_trigger: None,
        system_scope: system_scope.clone(),
    })
    .await;
    let stmt_failed = report.has_failure();
    record_statement_failures(&source, report, queue);

    if let Some(ref scope) = system_scope {
        if row_failed || stmt_failed {
            rollback_scope(scope, &identity, state, event.source).await;
            trace!(
                collection = %event.collection,
                "trigger body failed; fired-event system transaction rolled back"
            );
        } else if let Err(e) = commit_scope(scope, &identity, state, event.source).await {
            warn!(error = %e, "trigger fired-event system transaction commit failed");
        }
    }
}

/// Decode one side of an event payload into trigger row bindings.
fn row_fields(
    payload: Option<&[u8]>,
    row_id: &str,
) -> Option<std::collections::HashMap<String, nodedb_types::Value>> {
    let map = deserialize_event_payload(payload?)?;
    let mut fields: std::collections::HashMap<String, nodedb_types::Value> = map
        .into_iter()
        .map(|(k, v)| (k, nodedb_types::Value::from(v)))
        .collect();
    inject_row_identity(&mut fields, row_id);
    Some(fields)
}

/// The statement-level DML event a write op represents, if any.
fn dml_event_of(op: &WriteOp) -> Option<crate::control::trigger::DmlEvent> {
    match op {
        WriteOp::Insert | WriteOp::BulkInsert { .. } => {
            Some(crate::control::trigger::DmlEvent::Insert)
        }
        WriteOp::Update => Some(crate::control::trigger::DmlEvent::Update),
        WriteOp::Delete | WriteOp::BulkDelete { .. } => {
            Some(crate::control::trigger::DmlEvent::Delete)
        }
        _ => None,
    }
}

/// Parameters for [`fire_for_operation`].
pub(super) struct FireForOperationParams<'a> {
    /// DML operation string (`"INSERT"` / `"UPDATE"` / `"DELETE"`).
    pub operation: &'a str,
    /// Shared server state (trigger registry, block cache).
    pub state: &'a Arc<SharedState>,
    /// Effective identity used to fire the trigger.
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: crate::types::DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// Target collection name.
    pub collection: &'a str,
    /// NEW row fields, when the operation carries a NEW row.
    pub new_fields: Option<&'a std::collections::HashMap<String, nodedb_types::Value>>,
    /// OLD row fields, when the operation carries an OLD row.
    pub old_fields: Option<&'a std::collections::HashMap<String, nodedb_types::Value>>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// Restricts firing to a single execution mode; `None` fires all modes.
    pub mode_filter: Option<TriggerExecutionMode>,
    /// Source-write context, so a trigger body writing to a remote-homed
    /// collection is dispatched to the owning node instead of the local core.
    pub cross_shard_origin: Option<CrossShardOrigin>,
    /// System transaction scope (Event-Plane fire path); `None` otherwise.
    pub system_scope: Option<Arc<SystemTxnScope>>,
    /// What a failing trigger does to the triggers queued behind it.
    pub on_error: FireErrorPolicy,
    /// Restricts firing to the one named trigger; `None` fires every match.
    pub only_trigger: Option<&'a str>,
}

/// Shared trigger fire logic: routes to the correct `fire_after_*` function.
///
/// Used by both initial dispatch (from a `WriteEvent`) and retry (from a
/// queued action).
pub(super) async fn fire_for_operation(params: FireForOperationParams<'_>) -> FireReport {
    let FireForOperationParams {
        operation,
        state,
        identity,
        database_id,
        tenant_id,
        collection,
        new_fields,
        old_fields,
        cascade_depth,
        mode_filter,
        cross_shard_origin,
        system_scope,
        on_error,
        only_trigger,
    } = params;

    match operation {
        "INSERT" => match new_fields {
            Some(new) => {
                fire::fire_after_insert(fire::FireAfterInsertParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    new_fields: new,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
        system_scope,
                    on_error,
                    only_trigger,
                })
                .await
            }
            None => FireReport::default(),
        },
        "UPDATE" => match (old_fields, new_fields) {
            (Some(old), Some(new)) => {
                fire::fire_after_update(fire::FireAfterUpdateParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    old_fields: old,
                    new_fields: new,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
        system_scope,
                    on_error,
                    only_trigger,
                })
                .await
            }
            _ => FireReport::default(),
        },
        "DELETE" => match old_fields {
            Some(old) => {
                fire::fire_after_delete(fire::FireAfterDeleteParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection,
                    old_fields: old,
                    cascade_depth,
                    mode_filter,
                    cross_shard_origin,
        system_scope,
                    on_error,
                    only_trigger,
                })
                .await
            }
            None => FireReport::default(),
        },
        _ => FireReport::default(),
    }
}

#[cfg(test)]
mod tests {
    use crate::event::types::deserialize_event_payload;

    #[test]
    fn deserialize_json_payload() {
        let json = serde_json::json!({"id": 1, "name": "test"});
        let bytes = serde_json::to_vec(&json).unwrap();
        let map = deserialize_event_payload(&bytes).unwrap();
        assert_eq!(map.get("id").unwrap(), &serde_json::json!(1));
        assert_eq!(map.get("name").unwrap(), &serde_json::json!("test"));
    }

    #[test]
    fn deserialize_msgpack_payload() {
        let json = serde_json::json!({"status": "active", "count": 42});
        let bytes = nodedb_types::json_to_msgpack(&json).unwrap();
        let map = deserialize_event_payload(&bytes).unwrap();
        assert_eq!(map.get("status").unwrap(), &serde_json::json!("active"));
    }

    #[test]
    fn deserialize_non_object_returns_none() {
        let bytes = serde_json::to_vec(&serde_json::json!([1, 2, 3])).unwrap();
        assert!(deserialize_event_payload(&bytes).is_none());
    }
}
