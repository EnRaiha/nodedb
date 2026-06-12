// SPDX-License-Identifier: Apache-2.0

//! Policy resolution dispatch for validation violations.

use crate::CrdtAuthContext;
use crate::constraint::{Constraint, ConstraintKind};
use crate::error::Result;
use crate::policy::{ConflictPolicy, PolicyResolution, ResolvedAction};
use crate::state::CrdtState;

use super::core::Validator;
use super::types::{ProposedChange, ValidationOutcome};

impl Validator {
    /// Validate with declarative policy resolution.
    ///
    /// This is the new core validation method. It attempts to resolve violations
    /// via policy before falling back to the DLQ.
    ///
    /// # Arguments
    ///
    /// * `state` — current CRDT state
    /// * `peer_id` — source peer ID
    /// * `change` — proposed change
    /// * `delta_bytes` — raw delta bytes
    /// * `hlc_timestamp` — Hybrid Logical Clock timestamp of the incoming write
    ///
    /// Returns:
    /// - `Ok(PolicyResolution::AutoResolved(_))` if the policy auto-fixed the violation
    /// - `Ok(PolicyResolution::Deferred { .. })` if deferred for retry (entry already enqueued)
    /// - `Ok(PolicyResolution::WebhookRequired { .. })` if webhook call needed (caller's responsibility)
    /// - `Ok(PolicyResolution::Escalate)` if escalating to DLQ (entry already enqueued)
    /// - `Err(_)` if an internal error occurred
    pub fn validate_with_policy(
        &mut self,
        state: &CrdtState,
        peer_id: u64,
        auth: CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
        hlc_timestamp: u64,
    ) -> Result<PolicyResolution> {
        match self.validate(state, change) {
            ValidationOutcome::Accepted => {
                // No violation; return synthetic "auto-resolved" to maintain API consistency
                Ok(PolicyResolution::AutoResolved(
                    ResolvedAction::OverwriteExisting,
                ))
            }
            ValidationOutcome::Rejected(violations) => {
                // Exactly one violation per constraint (current design)
                let v = &violations[0];
                let constraint = self
                    .constraints
                    .all()
                    .iter()
                    .find(|c| c.name == v.constraint_name)
                    .cloned()
                    .unwrap_or_else(|| Constraint {
                        name: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        field: String::new(),
                        kind: ConstraintKind::NotNull,
                    });

                let policy = self.policies.get_owned(&change.collection);
                let policy_for_kind = policy.for_kind(&constraint.kind);

                // Attempt policy resolution
                match policy_for_kind {
                    ConflictPolicy::LastWriterWins => {
                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            timestamp = hlc_timestamp,
                            reason = %v.reason,
                            "resolved via LAST_WRITER_WINS"
                        );
                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::OverwriteExisting,
                        ))
                    }

                    ConflictPolicy::RenameSuffix => {
                        let counter_key = (change.collection.clone(), constraint.field.clone());
                        let suffix = self.suffix_counter.entry(counter_key).or_insert(0);
                        *suffix += 1;
                        let new_value = format!(
                            "{}_{}",
                            change
                                .fields
                                .iter()
                                .find(|(f, _)| f == &constraint.field)
                                .map(|(_, v)| format!("{:?}", v))
                                .unwrap_or_else(|| "unknown".to_string()),
                            suffix
                        );

                        tracing::info!(
                            constraint = %v.constraint_name,
                            field = %constraint.field,
                            new_value = %new_value,
                            "resolved via RENAME_APPEND_SUFFIX"
                        );

                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::RenamedField {
                                field: constraint.field.clone(),
                                new_value,
                            },
                        ))
                    }

                    ConflictPolicy::CascadeDefer {
                        max_retries,
                        ttl_secs,
                    } => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let base_ms = 500u64;
                        let first_retry_after_ms = base_ms;

                        let id = self.deferred.enqueue(
                            peer_id,
                            auth.user_id,
                            auth.tenant_id,
                            delta_bytes,
                            change.collection.clone(),
                            constraint.name.clone(),
                            0,
                            *max_retries,
                            now_ms,
                            first_retry_after_ms,
                            *ttl_secs,
                        );

                        tracing::info!(
                            constraint = %v.constraint_name,
                            deferred_id = id,
                            reason = %v.reason,
                            "resolved via CASCADE_DEFER (queued for retry)"
                        );

                        Ok(PolicyResolution::Deferred {
                            retry_after_ms: first_retry_after_ms,
                            attempt: 0,
                        })
                    }

                    ConflictPolicy::Custom {
                        webhook_url,
                        timeout_secs,
                    } => {
                        tracing::info!(
                            constraint = %v.constraint_name,
                            webhook_url = %webhook_url,
                            "escalated to webhook"
                        );

                        Ok(PolicyResolution::WebhookRequired {
                            webhook_url: webhook_url.clone(),
                            timeout_secs: *timeout_secs,
                        })
                    }

                    ConflictPolicy::EscalateToDlq => {
                        self.dlq.enqueue(
                            peer_id,
                            auth.user_id,
                            auth.tenant_id,
                            delta_bytes,
                            &constraint,
                            v.reason.clone(),
                            v.hint.clone(),
                        )?;

                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            "escalated to DLQ"
                        );

                        Ok(PolicyResolution::Escalate)
                    }
                }
            }
        }
    }
}
