// SPDX-License-Identifier: Apache-2.0

//! Basic validate() + validate_or_reject() entry points.

use crate::CrdtAuthContext;
use crate::error::{CrdtError, Result};
use crate::policy::PolicyResolution;
use crate::row_lookup::RowLookup;

use super::core::Validator;
use super::types::{ProposedChange, ValidationOutcome, Violation};

/// Convert the first violation into a [`CrdtError::ConstraintViolation`], appending
/// `suffix` (e.g. `"deferred for retry"`) to the reason when provided.
///
/// Returns `Ok(())` when `violations` is empty — this branch is unreachable in
/// practice (only reachable from `PolicyResolution` variants that are only ever
/// constructed from a non-empty violation list) but is kept as a safe fallback.
fn first_violation_err(
    violations: Vec<Violation>,
    collection: &str,
    suffix: Option<&str>,
) -> Result<()> {
    match violations.into_iter().next() {
        Some(v) => Err(CrdtError::ConstraintViolation {
            constraint: v.constraint_name,
            collection: collection.to_string(),
            detail: match suffix {
                Some(s) => format!("{} ({})", v.reason, s),
                None => v.reason,
            },
        }),
        None => Ok(()),
    }
}

impl Validator {
    /// Validate a proposed change against all applicable constraints.
    ///
    /// Returns `Accepted` if all constraints pass, or `Rejected` with
    /// detailed violation information.
    pub fn validate(&self, state: &impl RowLookup, change: &ProposedChange) -> ValidationOutcome {
        let constraints = self.constraints.for_collection(&change.collection);
        let mut violations = Vec::new();

        for constraint in constraints {
            if let Some(violation) = self.check_constraint(state, change, constraint) {
                violations.push(violation);
            }
        }

        if violations.is_empty() {
            ValidationOutcome::Accepted
        } else {
            ValidationOutcome::Rejected(violations)
        }
    }

    /// Validate and apply declarative policy resolution.
    ///
    /// ## Replay protection
    ///
    /// When `auth.delta_signature` is non-zero, the following steps execute
    /// in this order to prevent replay attacks at minimum cost:
    ///
    /// 1. **Cheap seq_no check** — `seq_no > last_seen[(user_id, device_id)]`.
    ///    Fails fast before any HMAC computation.
    /// 2. **HMAC verification** — constant-time comparison prevents timing attacks.
    /// 3. **Atomic seq update** — `last_seen` advances only on success.
    ///
    /// For accepted changes, returns Ok(()).
    /// For violations, applies policy and:
    /// - If AutoResolved: returns Ok(())
    /// - If Deferred/Webhook/Escalate: returns appropriate error
    pub fn validate_or_reject(
        &mut self,
        state: &impl RowLookup,
        peer_id: u64,
        auth: CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> Result<()> {
        // Check auth expiry: agents that accumulated deltas offline must
        // re-authenticate before syncing.
        if auth.auth_expires_at > 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms > auth.auth_expires_at {
                return Err(CrdtError::AuthExpired {
                    user_id: auth.user_id,
                    expired_at: auth.auth_expires_at,
                });
            }
        }

        // Replay protection + signature verification (signed path only).
        //
        // The unsigned path (all-zeros signature) bypasses replay protection.
        // Old clients that send device_id=0 / seq_no=0 with a non-zero
        // signature will be rejected by the seq_no check (0 is never > 0).
        if auth.delta_signature != [0u8; 32]
            && let Some(ref verifier) = self.delta_verifier
        {
            // Step 1: cheap seq_no check before any HMAC computation.
            verifier
                .registry()
                .check_seq(auth.user_id, auth.device_id, auth.seq_no)?;

            // Step 2: constant-time HMAC verification.
            verifier.verify(
                auth.user_id,
                auth.device_id,
                auth.seq_no,
                &delta_bytes,
                &auth.delta_signature,
            )?;

            // Step 3: advance last_seen atomically on success.
            verifier
                .registry()
                .commit_seq(auth.user_id, auth.device_id, auth.seq_no)?;
        }

        let hlc_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        match self.validate_with_policy(state, peer_id, auth, change, delta_bytes, hlc_timestamp)? {
            PolicyResolution::AutoResolved(_) => Ok(()),
            PolicyResolution::Deferred { violations, .. } => {
                // Violation was deferred for retry; return error to signal this.
                // The deferred entry was already enqueued by validate_with_policy.
                first_violation_err(violations, &change.collection, Some("deferred for retry"))
            }
            PolicyResolution::WebhookRequired { violations, .. } => {
                // Webhook decision required; return error.
                first_violation_err(violations, &change.collection, Some("webhook required"))
            }
            PolicyResolution::Escalate { violations } => {
                // Already enqueued to DLQ by validate_with_policy.
                first_violation_err(violations, &change.collection, None)
            }
        }
    }
}
