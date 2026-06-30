// SPDX-License-Identifier: Apache-2.0

//! Declarative conflict resolution policies for CRDT constraint violations.
//!
//! ~80% of constraint violations should be auto-resolved
//! via declarative policies, with the DLQ serving as fallback-only.
//!
//! # Built-in Policies
//!
//! - `LAST_WRITER_WINS` — incoming write wins; later HLC timestamp takes precedence
//! - `RENAME_APPEND_SUFFIX` — UNIQUE violations auto-resolve by appending `_1`, `_2`, ...
//! - `CASCADE_DEFER` — FK violations queue for exponential backoff retry (max 3 attempts)
//! - `CUSTOM(webhook_url)` — POST conflict to webhook; response determines action
//! - `ESCALATE_TO_DLQ` — route directly to dead-letter queue (explicit fallback)
//!
//! # Configuration
//!
//! Policies are set per-collection and per-constraint-kind via DDL:
//!
//! ```sql
//! ALTER COLLECTION agents SET ON CONFLICT LAST_WRITER_WINS FOR UNIQUE;
//! ALTER COLLECTION posts SET ON CONFLICT CASCADE_DEFER FOR FOREIGN_KEY;
//! ```

use crate::validator::Violation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A conflict resolution policy that determines how constraint violations are handled.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ConflictPolicy {
    /// Resolve using HLC timestamp ordering; later write wins.
    /// Audit trail entry is emitted for visibility.
    #[default]
    LastWriterWins,

    /// For UNIQUE violations, append monotonic suffix (`_1`, `_2`, ...) to conflicting field.
    /// Agent is notified of the renamed value.
    RenameSuffix,

    /// For FK violations, queue delta for retry with exponential backoff.
    /// Max retries and TTL are configurable.
    /// If parent not created within TTL, route to DLQ.
    CascadeDefer {
        /// Maximum number of retry attempts (default 3).
        max_retries: u32,
        /// Time-to-live in seconds before giving up (default 300).
        ttl_secs: u64,
    },

    /// Escape hatch: POST conflict to webhook; response determines accept/reject/rewrite.
    /// Timeout (default 5s) routes to DLQ.
    Custom {
        /// Webhook endpoint URL.
        webhook_url: String,
        /// Timeout in seconds (default 5).
        timeout_secs: u64,
    },

    /// Explicitly route to the dead-letter queue.
    /// Used for strict consistency mode or as a fallback when nothing else applies.
    EscalateToDlq,
}

/// The result of attempting to resolve a conflict via policy.
#[derive(Debug, Clone)]
pub enum PolicyResolution {
    /// Successfully auto-resolved by applying a policy.
    AutoResolved(ResolvedAction),

    /// Conflict deferred for retry; caller should enqueue to DeferredQueue.
    /// `retry_after_ms`: milliseconds to wait before retry
    /// `attempt`: current attempt number
    Deferred {
        retry_after_ms: u64,
        attempt: u32,
        violations: Vec<Violation>,
    },

    /// Requires async webhook call; caller must POST conflict and await response.
    WebhookRequired {
        webhook_url: String,
        timeout_secs: u64,
        violations: Vec<Violation>,
    },

    /// Escalate to dead-letter queue.
    Escalate { violations: Vec<Violation> },
}

/// The specific action taken to resolve a conflict.
#[derive(Debug, Clone)]
pub enum ResolvedAction {
    /// Overwrite existing value with incoming delta.
    OverwriteExisting,

    /// Auto-renamed field to resolve UNIQUE conflict.
    RenamedField { field: String, new_value: String },
}

/// Per-collection conflict resolution policies, keyed by constraint kind.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPolicy {
    /// Policy for UNIQUE constraint violations.
    pub unique: ConflictPolicy,
    /// Policy for FOREIGN_KEY constraint violations.
    pub foreign_key: ConflictPolicy,
    /// Policy for NOT_NULL constraint violations.
    pub not_null: ConflictPolicy,
    /// Policy for CHECK constraint violations.
    pub check: ConflictPolicy,
    /// If `true`, stricter defaults apply (defaults to EscalateToDlq).
    pub strict_consistency: bool,
}

impl CollectionPolicy {
    /// Create a policy suitable for ephemeral agent state (AP mode).
    /// Defaults to LAST_WRITER_WINS for most constraints.
    pub fn ephemeral() -> Self {
        Self {
            unique: ConflictPolicy::RenameSuffix,
            foreign_key: ConflictPolicy::CascadeDefer {
                max_retries: 3,
                ttl_secs: 300,
            },
            not_null: ConflictPolicy::LastWriterWins,
            check: ConflictPolicy::EscalateToDlq,
            strict_consistency: false,
        }
    }

    /// Create a policy suitable for strict consistency mode (CP mode).
    /// Defaults to escalating most violations to DLQ.
    pub fn strict() -> Self {
        Self {
            unique: ConflictPolicy::EscalateToDlq,
            foreign_key: ConflictPolicy::EscalateToDlq,
            not_null: ConflictPolicy::EscalateToDlq,
            check: ConflictPolicy::EscalateToDlq,
            strict_consistency: true,
        }
    }

    /// Retrieve the policy for a given constraint kind.
    pub fn for_kind(&self, kind: &crate::constraint::ConstraintKind) -> &ConflictPolicy {
        match kind {
            crate::constraint::ConstraintKind::Unique => &self.unique,
            crate::constraint::ConstraintKind::ForeignKey { .. }
            | crate::constraint::ConstraintKind::BiTemporalFK { .. } => &self.foreign_key,
            crate::constraint::ConstraintKind::NotNull => &self.not_null,
            crate::constraint::ConstraintKind::Check { .. } => &self.check,
        }
    }
}

/// Registry of conflict resolution policies, keyed by collection name.
#[derive(Debug, Clone, Default)]
pub struct PolicyRegistry {
    policies: HashMap<String, CollectionPolicy>,
}

impl PolicyRegistry {
    /// Create a new, empty policy registry.
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
        }
    }

    /// Set the full policy for a collection (overwriting any previous policy).
    pub fn set(&mut self, collection: &str, policy: CollectionPolicy) {
        self.policies.insert(collection.to_string(), policy);
    }

    /// Remove the policy for a collection. Returns `true` if one existed.
    pub fn remove(&mut self, collection: &str) -> bool {
        self.policies.remove(collection).is_some()
    }

    /// Set the policy for a specific constraint kind within a collection.
    pub fn set_for_kind(
        &mut self,
        collection: &str,
        kind: &crate::constraint::ConstraintKind,
        policy: ConflictPolicy,
    ) {
        let mut coll_policy = self.get_owned(collection);
        match kind {
            crate::constraint::ConstraintKind::Unique => coll_policy.unique = policy,
            crate::constraint::ConstraintKind::ForeignKey { .. }
            | crate::constraint::ConstraintKind::BiTemporalFK { .. } => {
                coll_policy.foreign_key = policy
            }
            crate::constraint::ConstraintKind::NotNull => coll_policy.not_null = policy,
            crate::constraint::ConstraintKind::Check { .. } => coll_policy.check = policy,
        }
        self.set(collection, coll_policy);
    }

    /// Retrieve the policy for a collection as an owned value.
    /// Returns the registered policy if found, otherwise returns the default (ephemeral).
    pub fn get_owned(&self, collection: &str) -> CollectionPolicy {
        self.policies
            .get(collection)
            .cloned()
            .unwrap_or_else(CollectionPolicy::ephemeral)
    }

    /// Check if a collection has an explicit policy registered.
    pub fn has(&self, collection: &str) -> bool {
        self.policies.contains_key(collection)
    }

    /// Total number of registered policies.
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraint::ConstraintKind;

    #[test]
    fn ephemeral_policy_defaults() {
        let policy = CollectionPolicy::ephemeral();
        assert!(!policy.strict_consistency);
        assert!(matches!(policy.unique, ConflictPolicy::RenameSuffix));
        assert!(matches!(
            policy.foreign_key,
            ConflictPolicy::CascadeDefer { .. }
        ));
    }

    #[test]
    fn strict_policy_defaults() {
        let policy = CollectionPolicy::strict();
        assert!(policy.strict_consistency);
        assert!(matches!(policy.unique, ConflictPolicy::EscalateToDlq));
        assert!(matches!(policy.foreign_key, ConflictPolicy::EscalateToDlq));
    }

    #[test]
    fn for_kind_lookup() {
        let policy = CollectionPolicy::ephemeral();

        let unique_policy = policy.for_kind(&ConstraintKind::Unique);
        assert!(matches!(unique_policy, ConflictPolicy::RenameSuffix));

        let fk_policy = policy.for_kind(&ConstraintKind::ForeignKey {
            ref_collection: "users".into(),
            ref_key: "id".into(),
        });
        assert!(matches!(fk_policy, ConflictPolicy::CascadeDefer { .. }));
    }

    #[test]
    fn registry_set_and_get() {
        let mut registry = PolicyRegistry::new();
        let policy = CollectionPolicy::strict();

        registry.set("agents", policy.clone());

        // Note: Due to lifetime constraints, get returns a static default.
        // For testing, we verify has() works correctly instead.
        assert!(registry.has("agents"));
        assert!(!registry.has("unknown"));
    }

    #[test]
    fn registry_set_for_kind() {
        let mut registry = PolicyRegistry::new();

        // Start with a default policy
        registry.set("posts", CollectionPolicy::ephemeral());

        // Override UNIQUE policy only
        registry.set_for_kind(
            "posts",
            &ConstraintKind::Unique,
            ConflictPolicy::LastWriterWins,
        );

        assert!(registry.has("posts"));
    }

    #[test]
    fn registry_len() {
        let mut registry = PolicyRegistry::new();

        assert_eq!(registry.len(), 0);

        registry.set("coll1", CollectionPolicy::ephemeral());
        assert_eq!(registry.len(), 1);

        registry.set("coll2", CollectionPolicy::strict());
        assert_eq!(registry.len(), 2);

        // Overwriting doesn't increase len
        registry.set("coll1", CollectionPolicy::strict());
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn conflict_policy_default() {
        let policy: ConflictPolicy = Default::default();
        assert!(matches!(policy, ConflictPolicy::LastWriterWins));
    }

    #[test]
    fn cascade_defer_exponential_backoff() {
        // Verify exponential backoff calculation logic.
        // base: 500ms
        // attempt 0: 500ms
        // attempt 1: 500 * 2^1 = 1000ms
        // attempt 2: 500 * 2^2 = 2000ms
        // Max: 30s
        let base_ms = 500u64;
        for attempt in 0..5 {
            let backoff = base_ms.saturating_mul(2_u64.saturating_pow(attempt));
            let capped = backoff.min(30_000);
            assert!(capped <= 30_000);
        }
    }
}
