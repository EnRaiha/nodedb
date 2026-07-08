// SPDX-License-Identifier: BUSL-1.1

//! Apply-and-validate a peer delta on the sync path.
//!
//! Unlike the bare [`TenantCrdtEngine::apply_committed_delta`] import, this
//! path re-reads the rows the delta *actually* wrote and validates each against
//! the constraints installed for its collection. A violation is routed to the
//! dead-letter queue and surfaced as a deterministic [`ViolationType`] the
//! caller can carry back to the client — while the import itself is always
//! kept (the sync high-water-mark must advance regardless, so a rejected or
//! malformed delta never wedges the stream).

use nodedb_crdt::validator::{ValidationOutcome, Violation};
use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;

use super::core::TenantCrdtEngine;

/// Outcome of applying and validating one peer delta.
#[derive(Debug)]
pub enum ValidatedApplyOutcome {
    /// The delta imported and every row it wrote satisfied its constraints.
    Clean,
    /// The delta imported but a row violated a constraint. The violation has
    /// been enqueued to the DLQ; the translated type is carried for the caller.
    Rejected(ViolationType),
    /// The delta bytes could not be imported (corrupt / undecodable). Treated
    /// as an idempotent no-op so the stream is not wedged.
    Malformed,
}

impl TenantCrdtEngine {
    /// Import a peer delta, then validate the rows it wrote against installed
    /// constraints.
    ///
    /// The import is always retained: a violating row is routed to the DLQ and
    /// its violation returned as [`ValidatedApplyOutcome::Rejected`], and a
    /// corrupt blob returns [`ValidatedApplyOutcome::Malformed`] rather than
    /// propagating an error. Neither wedges the sync stream — the caller still
    /// advances the high-water-mark.
    ///
    /// `surrogate` / `document_id` bind the sender's claimed target row so its
    /// UNIQUE / FK probes reference the correct cross-engine identity; other
    /// rows the delta happened to touch are validated with `Surrogate::ZERO`.
    pub fn apply_committed_delta_validated(
        &mut self,
        collection: &str,
        delta: &[u8],
        surrogate: Surrogate,
        document_id: &str,
        peer_id: u64,
    ) -> ValidatedApplyOutcome {
        // Import inside a scoped borrow of the per-collection state, diffing the
        // frontier to learn exactly which rows the delta wrote. The `&mut`
        // borrow is dropped before validation / DLQ enqueue below take other
        // fields of `self`.
        let write_set = {
            let state = match self.state_mut(collection) {
                Ok(s) => s,
                // Engine construction failure is not a delta-content problem;
                // treat as a no-op so the stream still advances.
                Err(_) => return ValidatedApplyOutcome::Malformed,
            };
            let before = state.frontier();
            if state.import(delta).is_err() {
                return ValidatedApplyOutcome::Malformed;
            }
            match state.write_set_since(&before) {
                Ok(ws) => ws,
                Err(_) => return ValidatedApplyOutcome::Malformed,
            }
        };

        for (coll, row) in write_set {
            let sg = if row == document_id {
                surrogate
            } else {
                Surrogate::ZERO
            };
            let ValidationOutcome::Rejected(violations) =
                self.validate_committed_row(&coll, &row, sg)
            else {
                continue;
            };
            let Some(violation) = violations.into_iter().next() else {
                continue;
            };
            return ValidatedApplyOutcome::Rejected(
                self.dlq_and_translate(&coll, delta, peer_id, violation),
            );
        }

        ValidatedApplyOutcome::Clean
    }

    /// Enqueue a rejected delta to the DLQ and translate the internal violation
    /// into the deterministic wire [`ViolationType`].
    ///
    /// The DLQ entry carries the INTERNAL compensation hint verbatim; the wire
    /// hint the client eventually sees is derived from the returned
    /// `ViolationType`, never from the DLQ. The DLQ id / timestamp are
    /// node-local and non-deterministic and are deliberately not returned.
    fn dlq_and_translate(
        &mut self,
        collection: &str,
        delta: &[u8],
        peer_id: u64,
        violation: Violation,
    ) -> ViolationType {
        // No authenticated user identity is threaded to the apply path in this
        // layer, so the DLQ records `0` (unauthenticated/legacy). A real
        // user_id would come from the sync session's auth context once that is
        // carried alongside `SyncProvenance` into the delta apply.
        let user_id = 0u64;
        let tenant_id = self.tenant_id().as_u64();

        // Look up the violated constraint by name so the DLQ entry records the
        // real collection/field. If it cannot be found, fall back to a
        // best-effort ManualIntervention entry rather than panicking.
        let constraint = self
            .constraints_for_collection(collection)
            .into_iter()
            .find(|c| c.name == violation.constraint_name);

        let reason = violation.reason.clone();
        match constraint {
            Some(constraint) => {
                if let Err(e) =
                    self.validator
                        .dlq_mut()
                        .enqueue(nodedb_crdt::EnqueueDeadLetterArgs {
                            peer_id,
                            user_id,
                            tenant_id,
                            delta: delta.to_vec(),
                            constraint: &constraint,
                            reason,
                            hint: violation.hint.clone(),
                        })
                {
                    tracing::warn!(
                        tenant = tenant_id,
                        collection,
                        error = %e,
                        "crdt: failed to enqueue rejected delta to DLQ"
                    );
                }
            }
            None => {
                let fallback = nodedb_crdt::Constraint {
                    name: violation.constraint_name.clone(),
                    collection: collection.to_string(),
                    field: String::new(),
                    kind: nodedb_crdt::ConstraintKind::Check {
                        expr: String::new(),
                        description: "unresolved constraint".to_string(),
                    },
                };
                let hint = nodedb_crdt::CompensationHint::ManualIntervention {
                    reason: reason.clone(),
                };
                if let Err(e) =
                    self.validator
                        .dlq_mut()
                        .enqueue(nodedb_crdt::EnqueueDeadLetterArgs {
                            peer_id,
                            user_id,
                            tenant_id,
                            delta: delta.to_vec(),
                            constraint: &fallback,
                            reason,
                            hint,
                        })
                {
                    tracing::warn!(
                        tenant = tenant_id,
                        collection,
                        error = %e,
                        "crdt: failed to enqueue rejected delta to DLQ (unresolved constraint)"
                    );
                }
            }
        }

        violation_to_type(&violation)
    }
}

/// Translate an internal [`Violation`] into the deterministic wire
/// [`ViolationType`] by matching the internal compensation hint.
///
/// `ViolationType` is `#[non_exhaustive]`; any hint we do not model maps to the
/// generic `ConstraintViolation` carrying the human-readable reason.
fn violation_to_type(violation: &Violation) -> ViolationType {
    use nodedb_crdt::CompensationHint;
    match &violation.hint {
        CompensationHint::RetryWithDifferentValue {
            field,
            conflicting_value,
            ..
        } => ViolationType::UniqueViolation {
            field: field.clone(),
            value: conflicting_value.clone(),
        },
        CompensationHint::CreateReferencedRow { ref_key, .. } => ViolationType::ForeignKeyMissing {
            referenced_id: ref_key.clone(),
        },
        CompensationHint::ProvideRequiredField { field } => ViolationType::SchemaViolation {
            field: field.clone(),
            reason: "required field missing".into(),
        },
        CompensationHint::DeleteThenRetry { .. } | CompensationHint::ManualIntervention { .. } => {
            ViolationType::ConstraintViolation {
                detail: violation.reason.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::CompensationHint;
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::policy::CollectionPolicy;
    use nodedb_crdt::state::CrdtState;
    use nodedb_crdt::validator::Violation;

    use super::*;
    use crate::types::TenantId;

    fn unique_engine() -> TenantCrdtEngine {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, cs).unwrap();
        // Strict policy so a UNIQUE clash escalates to a rejection rather than
        // auto-resolving.
        engine.set_collection_policy_typed("users", CollectionPolicy::strict());
        engine
    }

    /// Build a delta that writes a single row with the given fields.
    fn row_delta(peer: u64, row_id: &str, email: &str, name: &str) -> Vec<u8> {
        let state = CrdtState::new(peer).unwrap();
        state
            .upsert(
                "users",
                row_id,
                &[
                    ("email", LoroValue::String(email.into())),
                    ("name", LoroValue::String(name.into())),
                ],
            )
            .unwrap();
        state.export_snapshot().unwrap()
    }

    #[test]
    fn valid_delta_is_clean() {
        let mut engine = unique_engine();
        let delta = row_delta(2, "a", "x@y.com", "A");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta,
            nodedb_types::Surrogate::ZERO,
            "a",
            2,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Clean));
        assert!(engine.row_exists("users", "a"));
        assert_eq!(engine.dlq_len(), 0);
    }

    #[test]
    fn unique_dup_is_rejected_and_dlqd() {
        let mut engine = unique_engine();
        // Seed row A.
        let delta_a = row_delta(2, "a", "x@y.com", "A");
        let clean = engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            nodedb_types::Surrogate::ZERO,
            "a",
            2,
        );
        assert!(matches!(clean, ValidatedApplyOutcome::Clean));

        // Row B reuses A's email — UNIQUE violation.
        let delta_b = row_delta(3, "b", "x@y.com", "B");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            nodedb_types::Surrogate::ZERO,
            "b",
            3,
        );
        match outcome {
            ValidatedApplyOutcome::Rejected(ViolationType::UniqueViolation { field, value }) => {
                assert_eq!(field, "email");
                assert_eq!(value, "x@y.com");
            }
            other => panic!("expected UniqueViolation, got {other:?}"),
        }
        assert_eq!(engine.dlq_len(), 1);
    }

    #[test]
    fn corrupt_delta_is_malformed() {
        let mut engine = unique_engine();
        let outcome = engine.apply_committed_delta_validated(
            "users",
            b"not a valid loro snapshot",
            nodedb_types::Surrogate::ZERO,
            "z",
            9,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Malformed));
        assert_eq!(engine.dlq_len(), 0);
    }

    fn violation_with(hint: CompensationHint) -> Violation {
        Violation {
            constraint_name: "c".into(),
            reason: "boom".into(),
            hint,
        }
    }

    #[test]
    fn translator_maps_each_hint() {
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::RetryWithDifferentValue {
                field: "email".into(),
                conflicting_value: "x".into(),
                suggestion: "x2".into(),
            })),
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::CreateReferencedRow {
                ref_collection: "orgs".into(),
                ref_key: "org-7".into(),
                missing_value: "org-7".into(),
            })),
            ViolationType::ForeignKeyMissing {
                referenced_id: "org-7".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ProvideRequiredField {
                field: "name".into(),
            })),
            ViolationType::SchemaViolation {
                field: "name".into(),
                reason: "required field missing".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ManualIntervention {
                reason: "nope".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::DeleteThenRetry {
                collection: "users".into(),
                conflicting_key: "a".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
    }

    /// Contract guard: the peer-delta apply module stays deterministic.
    ///
    /// The Raft-committed apply path (`apply_committed_delta_validated` →
    /// pure `Validator::validate`) must never reach for the local write
    /// path's signed/seq-gated check (the `validate` + `or_reject` helper on
    /// `core.rs`), which is nondeterministic per replica (SystemTime + HMAC
    /// signature + seq monotonicity). Pinning this to the source stops a
    /// future edit from silently diverging replicas at identical log indices.
    #[test]
    fn apply_module_stays_deterministic() {
        const SRC: &str = include_str!("apply_validated.rs");
        // Concatenated so this test's own source carries no contiguous token
        // that would self-match the guard.
        let forbidden = concat!("validate", "_or_", "reject");
        assert!(
            !SRC.contains(forbidden),
            "apply_validated.rs must not reference the local write path's \
             signed/seq-gated check — the Raft-applied peer-delta path must \
             stay deterministic (pure Validator::validate only)"
        );
    }
}
