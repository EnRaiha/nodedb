// SPDX-License-Identifier: BUSL-1.1

//! Turning a fire pass's outcomes into retry records.
//!
//! One failed trigger becomes one [`FailedAction`]. A pass that was refused
//! outright — currently only a cascade-depth stop — becomes none: re-running
//! it would hit the same depth, so it is reported and dropped rather than
//! retried until it exhausts its attempts.

use std::collections::HashMap;

use tracing::warn;

use crate::control::trigger::fire_common::FireReport;
use crate::event::action::ActionRetryQueue;
use crate::event::action::{ActionContext, ActionId, ActionKey, ActionPayload, FailedAction};
use crate::types::DatabaseId;

/// The write that caused a set of actions.
pub(super) struct ActionSource<'a> {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub row_id: &'a str,
    pub operation: &'a str,
    pub source_lsn: u64,
    pub source_sequence: u64,
    pub source_vshard: u32,
    pub cascade_depth: u32,
}

impl ActionSource<'_> {
    fn context(&self) -> ActionContext {
        ActionContext {
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            collection: self.collection.to_owned(),
            row_id: self.row_id.to_owned(),
            cascade_depth: self.cascade_depth,
        }
    }

    fn key(&self, action: ActionId) -> ActionKey {
        ActionKey {
            source_lsn: self.source_lsn,
            source_sequence: self.source_sequence,
            source_vshard: self.source_vshard,
            action,
        }
    }
}

/// Queue one record per failed ROW trigger.
pub(super) fn record_row_failures(
    source: &ActionSource<'_>,
    report: FireReport,
    new_fields: Option<&HashMap<String, nodedb_types::Value>>,
    old_fields: Option<&HashMap<String, nodedb_types::Value>>,
    queue: &mut ActionRetryQueue,
) {
    if report_refused(source, &report, "row") {
        return;
    }
    for outcome in report.into_failures() {
        let Some(error) = outcome.error else {
            continue;
        };
        warn!(
            trigger = %outcome.trigger_name,
            collection = %source.collection,
            operation = %source.operation,
            error = %error,
            "trigger failed, queued for retry"
        );
        queue.enqueue(FailedAction {
            key: source.key(ActionId::TriggerRow {
                trigger_name: outcome.trigger_name,
            }),
            payload: ActionPayload::TriggerRow {
                operation: source.operation.to_owned(),
                new_fields: new_fields.cloned(),
                old_fields: old_fields.cloned(),
            },
            context: source.context(),
            attempts: 0,
            last_error: error.to_string(),
        });
    }
}

/// Queue one record per failed STATEMENT trigger.
pub(super) fn record_statement_failures(
    source: &ActionSource<'_>,
    report: FireReport,
    queue: &mut ActionRetryQueue,
) {
    if report_refused(source, &report, "statement") {
        return;
    }
    for outcome in report.into_failures() {
        let Some(error) = outcome.error else {
            continue;
        };
        warn!(
            trigger = %outcome.trigger_name,
            collection = %source.collection,
            operation = %source.operation,
            error = %error,
            "statement trigger failed, queued for retry"
        );
        queue.enqueue(FailedAction {
            key: source.key(ActionId::TriggerStatement {
                trigger_name: outcome.trigger_name,
            }),
            payload: ActionPayload::TriggerStatement {
                operation: source.operation.to_owned(),
            },
            context: source.context(),
            attempts: 0,
            last_error: error.to_string(),
        });
    }
}

/// Report a whole-pass refusal. Returns whether the pass was refused, in
/// which case it has no per-trigger outcomes to queue.
fn report_refused(source: &ActionSource<'_>, report: &FireReport, scope: &str) -> bool {
    match report.refusal() {
        Some(error) => {
            warn!(
                collection = %source.collection,
                operation = %source.operation,
                cascade_depth = source.cascade_depth,
                scope,
                error = %error,
                "trigger pass refused before firing; not retryable"
            );
            true
        }
        None => false,
    }
}
