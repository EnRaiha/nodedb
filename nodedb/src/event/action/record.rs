// SPDX-License-Identifier: BUSL-1.1

//! The unit of deferred-action failure: one action that did not complete.
//!
//! A deferred action is one trigger body or one DEFINE EVENT THEN clause,
//! run by the Event Plane after the write that caused it already committed.
//! Retrying the *cause* — re-delivering the source write — re-runs every
//! action that write matched, including the ones that already succeeded. The
//! retryable unit is therefore the action, and [`FailedAction`] carries
//! exactly enough to re-run that one action and nothing else.
//!
//! Payloads name what to run, never how to run it: a trigger by name, an
//! event action by its SQL text. Both are re-resolved and re-planned at retry
//! time, so an action queued before a DDL runs against the catalog as it
//! stands when the retry fires rather than against a frozen plan.

use std::collections::HashMap;

use nodedb_types::{DatabaseId, Value};

/// Which action failed, stable across retries of the same source write.
///
/// Two enqueues carrying equal keys describe the same failed action, so a
/// queue can deduplicate on this rather than accumulating one entry per
/// delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ActionKey {
    /// LSN of the write that caused the action.
    pub source_lsn: u64,
    /// Sequence number of the causing write within its vShard.
    pub source_sequence: u64,
    /// vShard that owns the source collection. Carried so a retried
    /// cross-shard origination keeps its deduplication identity.
    pub source_vshard: u32,
    /// The action itself, within that source write.
    pub action: ActionId,
}

/// One action of one source write.
#[derive(Debug, Clone, PartialEq, Eq, Hash, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum ActionId {
    /// A named ROW trigger.
    TriggerRow { trigger_name: String },
    /// A named STATEMENT trigger.
    TriggerStatement { trigger_name: String },
    /// One THEN clause of a named DEFINE EVENT, by position in that event.
    EventAction { event_name: String, index: usize },
}

impl ActionId {
    /// The trigger or event this action belongs to, for logs and DLQ rows.
    pub fn owner(&self) -> &str {
        match self {
            Self::TriggerRow { trigger_name } | Self::TriggerStatement { trigger_name } => {
                trigger_name
            }
            Self::EventAction { event_name, .. } => event_name,
        }
    }
}

/// What a retry must re-run.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum ActionPayload {
    /// Re-fire one ROW trigger against the row that caused it. The body is
    /// read from the registry at retry time, not stored here, so a trigger
    /// altered since the failure retries as its current definition.
    TriggerRow {
        operation: String,
        new_fields: Option<HashMap<String, Value>>,
        old_fields: Option<HashMap<String, Value>>,
    },
    /// Re-fire one STATEMENT trigger. Statement triggers bind no row.
    TriggerStatement { operation: String },
    /// Re-run one rendered THEN action. The SQL is stored already rendered —
    /// its template variables were substituted from an event that is gone by
    /// retry time — and is re-planned against the current catalog.
    EventAction { sql: String },
}

/// Scope and provenance shared by every payload kind.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ActionContext {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    /// Collection whose write caused the action.
    pub collection: String,
    /// Row that caused the action; empty for statement-scoped actions.
    pub row_id: String,
    /// Cascade depth of the causing write.
    ///
    /// Carried on the record rather than reset to zero at retry: a durable
    /// queue re-runs actions that themselves write, and a cycle that resets
    /// its depth every hop never reaches the cascade limit.
    pub cascade_depth: u32,
}

/// One action that failed, with everything needed to run it again.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct FailedAction {
    pub key: ActionKey,
    pub payload: ActionPayload,
    pub context: ActionContext,
    /// Attempts already made. Zero on first enqueue.
    pub attempts: u32,
    /// Error text from the most recent attempt.
    pub last_error: String,
}

impl FailedAction {
    /// The trigger or event this action belongs to.
    pub fn owner(&self) -> &str {
        self.key.action.owner()
    }
}
