// SPDX-License-Identifier: BUSL-1.1

//! Single-event trigger dispatch: one `WriteEvent` → matching AFTER triggers.
//!
//! For each incoming `WriteEvent` with a triggerable source, this path:
//! 1. Deserializes `new_value` / `old_value` from MessagePack to
//!    `HashMap<String, nodedb_types::Value>`
//! 2. Calls the `fire_after_insert/update/delete` entry point in
//!    `control::trigger::fire`
//! 3. On failure, enqueues into the retry queue (exponential backoff)
//! 4. After max retries, the consumer routes the entry into the trigger DLQ

use std::sync::Arc;

use tracing::{debug, trace, warn};

use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::control::trigger::TriggerScope;
use crate::control::trigger::fire;
use crate::control::trigger::row_identity::inject_row_identity;
use crate::event::types::{EventSource, WriteEvent, WriteOp, deserialize_event_payload};
use crate::types::TenantId;

use super::super::retry::{RetryEntry, TriggerRetryQueue};
use super::identity::trigger_identity;

/// Dispatch a `WriteEvent` to matching AFTER triggers.
///
/// Skips events not from `EventSource::User` / `Deferred` (cascade prevention).
/// Returns `Ok(())` even if trigger execution fails — failures are
/// handled via retry queue + DLQ, not propagated to the caller.
pub async fn dispatch_triggers(
    event: &WriteEvent,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
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

    // Deserialize at the Event Plane boundary — the only remaining
    // serde_json::Map → HashMap conversion point.
    let new_fields: Option<std::collections::HashMap<String, nodedb_types::Value>> = event
        .new_value
        .as_ref()
        .and_then(|v| deserialize_event_payload(v))
        .map(|map| {
            let mut fields: std::collections::HashMap<String, nodedb_types::Value> = map
                .into_iter()
                .map(|(k, v)| (k, nodedb_types::Value::from(v)))
                .collect();
            inject_row_identity(&mut fields, event.row_id.as_str());
            fields
        });
    let old_fields: Option<std::collections::HashMap<String, nodedb_types::Value>> = event
        .old_value
        .as_ref()
        .and_then(|v| deserialize_event_payload(v))
        .map(|map| {
            let mut fields: std::collections::HashMap<String, nodedb_types::Value> = map
                .into_iter()
                .map(|(k, v)| (k, nodedb_types::Value::from(v)))
                .collect();
            inject_row_identity(&mut fields, event.row_id.as_str());
            fields
        });

    let identity = trigger_identity(event.tenant_id);

    let op_str = event.op.to_string();
    let result = match event.op {
        WriteOp::BulkInsert { .. } | WriteOp::BulkDelete { .. } => {
            // Bulk events are only created during WAL replay (wal_replay.rs)
            // and always carry new_value: None / old_value: None — they are
            // aggregate metadata (count of affected rows), not per-row payloads.
            //
            // The Data Plane ring buffer path emits individual Insert/Delete
            // events for each row in a batch, so triggers fire on those
            // individual events. Bulk events are safe to skip for ROW triggers.
            //
            // STATEMENT-level triggers fire on bulk events since they
            // represent a complete DML statement.
            let dml_event = match event.op {
                WriteOp::BulkInsert { .. } => crate::control::trigger::DmlEvent::Insert,
                _ => crate::control::trigger::DmlEvent::Delete,
            };
            crate::control::trigger::fire_statement::fire_after_statement(
                state,
                &identity,
                TriggerScope {
                    database_id: event.database_id,
                    tenant_id: event.tenant_id,
                },
                &event.collection,
                dml_event,
                0,
                mode_filter,
            )
            .await
        }
        _ => {
            let row_result = fire_for_operation(FireForOperationParams {
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
            })
            .await;

            // Also fire STATEMENT-level triggers for individual point operations
            // (a point INSERT/UPDATE/DELETE is also a complete statement).
            if row_result.is_ok() {
                let dml_event = match event.op {
                    WriteOp::Insert => crate::control::trigger::DmlEvent::Insert,
                    WriteOp::Update => crate::control::trigger::DmlEvent::Update,
                    WriteOp::Delete => crate::control::trigger::DmlEvent::Delete,
                    _ => return, // Heartbeat, etc.
                };
                if let Err(e) = crate::control::trigger::fire_statement::fire_after_statement(
                    state,
                    &identity,
                    TriggerScope {
                        database_id: event.database_id,
                        tenant_id: event.tenant_id,
                    },
                    &event.collection,
                    dml_event,
                    0,
                    mode_filter,
                )
                .await
                {
                    warn!(
                        collection = %event.collection,
                        op = %event.op,
                        error = %e,
                        "AFTER STATEMENT trigger failed, enqueuing for retry"
                    );
                    retry_queue.enqueue(RetryEntry {
                        database_id: event.database_id,
                        tenant_id: event.tenant_id.as_u64(),
                        collection: event.collection.to_string(),
                        row_id: String::new(),
                        operation: op_str.clone(),
                        trigger_name: String::new(),
                        new_fields: None,
                        old_fields: None,
                        attempts: 0,
                        last_error: e.to_string(),
                        next_retry_at: std::time::Instant::now(),
                        source_lsn: event.lsn.as_u64(),
                        source_sequence: event.sequence,
                        source_vshard: event.vshard_id.as_u32(),
                        cascade_depth: 0,
                    });
                }
            }

            row_result
        }
    };

    if let Err(e) = result {
        warn!(
            collection = %event.collection,
            op = %event.op,
            error = %e,
            "trigger execution failed, enqueuing for retry"
        );
        retry_queue.enqueue(RetryEntry {
            database_id: event.database_id,
            tenant_id: event.tenant_id.as_u64(),
            collection: event.collection.to_string(),
            row_id: event.row_id.as_str().to_string(),
            operation: event.op.to_string(),
            trigger_name: String::new(),
            new_fields,
            old_fields,
            attempts: 0,
            last_error: e.to_string(),
            next_retry_at: std::time::Instant::now(),
            source_lsn: event.lsn.as_u64(),
            source_sequence: event.sequence,
            source_vshard: event.vshard_id.as_u32(),
            cascade_depth: 0,
        });
    }
}

/// Retry a single entry. On failure, re-enqueues into the retry queue.
///
/// Called from the consumer loop where the DLQ mutex is NOT held,
/// avoiding holding a `MutexGuard` across await points.
pub async fn retry_single(
    entry: &RetryEntry,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
) {
    let identity = trigger_identity(TenantId::new(entry.tenant_id));
    let result = retry_fire(entry, state, &identity).await;

    if let Err(e) = result {
        debug!(
            trigger = %entry.trigger_name,
            attempt = entry.attempts,
            error = %e,
            "trigger retry failed, re-enqueuing"
        );
        let mut re = entry.clone();
        re.last_error = e.to_string();
        retry_queue.enqueue(re);
    }
}

async fn retry_fire(
    entry: &RetryEntry,
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
) -> crate::Result<()> {
    fire_for_operation(FireForOperationParams {
        operation: entry.operation.as_str(),
        state,
        identity,
        database_id: entry.database_id,
        tenant_id: TenantId::new(entry.tenant_id),
        collection: &entry.collection,
        new_fields: entry.new_fields.as_ref(),
        old_fields: entry.old_fields.as_ref(),
        cascade_depth: entry.cascade_depth,
        mode_filter: Some(TriggerExecutionMode::Async), // Retries are always ASYNC
        cross_shard_origin: Some(CrossShardOrigin {
            source_lsn: entry.source_lsn,
            source_sequence: entry.source_sequence,
            source_vshard: entry.source_vshard,
            source_collection: entry.collection.clone(),
        }),
    })
    .await
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
}

/// Shared trigger fire logic: routes to the correct `fire_after_*` function.
///
/// Used by both initial dispatch (from `WriteEvent`) and retry (from
/// `RetryEntry`). `mode_filter` controls which execution mode triggers fire.
pub(super) async fn fire_for_operation(params: FireForOperationParams<'_>) -> crate::Result<()> {
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
    } = params;

    match operation {
        "INSERT" => {
            if let Some(new) = new_fields {
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
                })
                .await
            } else {
                Ok(())
            }
        }
        "UPDATE" => {
            if let (Some(old), Some(new)) = (old_fields, new_fields) {
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
                })
                .await
            } else {
                Ok(())
            }
        }
        "DELETE" => {
            if let Some(old) = old_fields {
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
                })
                .await
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
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
