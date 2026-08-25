// SPDX-License-Identifier: BUSL-1.1

//! Re-running one queued action.
//!
//! A retry runs exactly the action its record names. Re-delivering the source
//! write instead would re-fire every trigger that write matched, including the
//! ones that already succeeded, so each of their side effects would happen
//! twice per retry round.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::state::SharedState;
use crate::control::trigger::TriggerScope;
use crate::control::trigger::fire_common::FireErrorPolicy;
use crate::control::trigger::fire_statement::{FireAfterStatementParams, fire_after_statement};
use crate::event::action::{ActionId, ActionPayload, ActionRetryQueue, FailedAction};
use crate::types::TenantId;

use super::identity::trigger_identity;
use super::single::{FireForOperationParams, fire_for_operation};

/// Re-run one queued action. Re-queues it on failure, and clears it from the
/// durable set once it completes.
pub async fn retry_action(
    action: &FailedAction,
    state: &Arc<SharedState>,
    queue: &mut ActionRetryQueue,
) {
    match run(action, state).await {
        Ok(()) => queue.complete(&action.key),
        Err(failure) if failure.retryable => {
            debug!(
                owner = %action.owner(),
                attempt = action.attempts,
                error = %failure.error,
                "deferred action retry failed, re-queued"
            );
            let mut next = action.clone();
            next.last_error = failure.error.to_string();
            queue.enqueue(next);
        }
        Err(failure) => {
            // Re-running would repeat what already applied. The action stops
            // here rather than spending its remaining attempts duplicating
            // its own side effects.
            warn!(
                owner = %action.owner(),
                attempt = action.attempts,
                error = %failure.error,
                "deferred action retry failed and cannot be re-run"
            );
            queue.complete(&action.key);
        }
    }
}

/// A retry failure and whether running the action again is safe.
struct ActionFailure {
    error: crate::Error,
    retryable: bool,
}

impl ActionFailure {
    /// A failure that can be tried again: the action applied nothing.
    fn retryable(error: crate::Error) -> Self {
        Self {
            error,
            retryable: true,
        }
    }
}

/// Execute the action its record names, collapsing to a single result: a
/// retry targets one action, so there is only ever one outcome to report.
async fn run(action: &FailedAction, state: &Arc<SharedState>) -> Result<(), ActionFailure> {
    let tenant_id = TenantId::new(action.context.tenant_id);
    let identity = trigger_identity(tenant_id);

    match (&action.key.action, &action.payload) {
        (
            ActionId::TriggerRow { trigger_name },
            ActionPayload::TriggerRow {
                operation,
                new_fields,
                old_fields,
            },
        ) => {
            fire_for_operation(FireForOperationParams {
                operation,
                state,
                identity: &identity,
                database_id: action.context.database_id,
                tenant_id,
                collection: &action.context.collection,
                new_fields: new_fields.as_ref(),
                old_fields: old_fields.as_ref(),
                cascade_depth: action.context.cascade_depth,
                // A retry is always ASYNC: the in-transaction modes fire on
                // the write path and have no queue behind them.
                mode_filter: Some(TriggerExecutionMode::Async),
                cross_shard_origin: Some(CrossShardOrigin {
                    source_lsn: action.key.source_lsn,
                    source_sequence: action.key.source_sequence,
                    source_vshard: action.key.source_vshard,
                    source_collection: action.context.collection.clone(),
                }),
                on_error: FireErrorPolicy::Abort,
                system_scope: None,
                only_trigger: Some(trigger_name),
            })
            .await
            .into_result()
            .map_err(ActionFailure::retryable)
        }
        (
            ActionId::TriggerStatement { trigger_name },
            ActionPayload::TriggerStatement { operation },
        ) => {
            let Some(dml_event) = dml_event_of(operation) else {
                return Ok(());
            };
            fire_after_statement(FireAfterStatementParams {
                state,
                identity: &identity,
                scope: TriggerScope {
                    database_id: action.context.database_id,
                    tenant_id,
                },
                collection: &action.context.collection,
                event: dml_event,
                cascade_depth: action.context.cascade_depth,
                mode_filter: Some(TriggerExecutionMode::Async),
                on_error: FireErrorPolicy::Abort,
                system_scope: None,
                only_trigger: Some(trigger_name),
            })
            .await
            .into_result()
            .map_err(ActionFailure::retryable)
        }
        (ActionId::EventAction { event_name, .. }, ActionPayload::EventAction { sql }) => {
            crate::control::event_trigger::run_event_action_sql(
                Arc::clone(state),
                action.context.database_id,
                tenant_id,
                sql,
                event_name,
            )
            .await
            .map_err(|error| ActionFailure {
                retryable: error.is_retryable(),
                error: crate::Error::from(error),
            })
        }
        (id, payload) => Err(ActionFailure {
            error: crate::Error::Internal {
                detail: format!(
                    "queued action {id:?} does not match its payload {payload:?}; \
                     the record was written inconsistently"
                ),
            },
            retryable: false,
        }),
    }
}

/// Parse the stored operation name back into a DML event.
fn dml_event_of(operation: &str) -> Option<crate::control::trigger::DmlEvent> {
    match operation {
        "INSERT" => Some(crate::control::trigger::DmlEvent::Insert),
        "UPDATE" => Some(crate::control::trigger::DmlEvent::Update),
        "DELETE" => Some(crate::control::trigger::DmlEvent::Delete),
        _ => None,
    }
}
