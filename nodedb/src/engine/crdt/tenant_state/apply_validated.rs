// SPDX-License-Identifier: BUSL-1.1

//! Apply-and-validate a peer delta on the sync path.
//!
//! Unlike the bare [`TenantCrdtEngine::apply_committed_delta`] import, this
//! path applies into a detached candidate, re-reads the rows the delta
//! *actually* wrote, and validates each against installed constraints. Only a
//! clean candidate replaces authoritative state. A violation is routed to the
//! dead-letter queue and surfaced as a deterministic [`ViolationType`].

use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ValidationOutcome, Violation};
use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;

use super::core::TenantCrdtEngine;

/// Server-derived signing context for an externally synchronized delta.
pub struct DeltaSigningAdmission {
    pub auth: nodedb_crdt::CrdtAuthContext,
    pub required: bool,
    /// The Control Plane verified this signature against the authenticated
    /// session's catalog-backed key before constructing the authenticated
    /// physical-plan variant. WAL/Raft replay preserves that admission result.
    pub preverified: bool,
}

/// Outcome of applying and validating one peer delta.
#[derive(Debug)]
pub enum ValidatedApplyOutcome {
    /// The delta imported and every row it wrote satisfied its constraints.
    ///
    /// `write_set` lists the `(collection, row_id)` pairs the delta actually
    /// wrote. The caller inspects it to enforce the one-document-per-delta
    /// sync contract: cross-engine identity binds exactly one Control-Plane
    /// surrogate per delta, so the Data Plane can only materialize the single
    /// frame-declared row. A delta that wrote other or additional rows (a
    /// client that coalesced N upserts into one delta) must be rejected
    /// loudly — materializing just one row would silently drop the rest.
    Clean { write_set: Vec<(String, String)> },
    /// The candidate violated a constraint and was discarded. The violation
    /// has been enqueued to the DLQ and translated for the caller.
    Rejected(ViolationType),
    /// The delta bytes could not be imported (corrupt / undecodable). Treated
    /// as an idempotent no-op so the stream is not wedged.
    Malformed,
}

impl TenantCrdtEngine {
    /// Import a peer delta, then validate the rows it wrote against installed
    /// constraints.
    ///
    /// Import and validation occur on a detached candidate. A violating row is
    /// routed to the DLQ without mutating authoritative state, and a corrupt
    /// blob returns [`ValidatedApplyOutcome::Malformed`].
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
        self.apply_committed_delta_authenticated(
            collection,
            delta,
            surrogate,
            document_id,
            peer_id,
            DeltaSigningAdmission {
                auth: nodedb_crdt::CrdtAuthContext::default(),
                required: false,
                preverified: false,
            },
        )
    }

    /// Apply a sync delta after enforcing the catalog-owned signing policy.
    pub fn apply_committed_delta_authenticated(
        &mut self,
        collection: &str,
        delta: &[u8],
        surrogate: Surrogate,
        document_id: &str,
        peer_id: u64,
        admission: DeltaSigningAdmission,
    ) -> ValidatedApplyOutcome {
        if admission.required && admission.auth.delta_signature == [0; 32] {
            return ValidatedApplyOutcome::Malformed;
        }
        if !admission.preverified
            && self
                .validator
                .verify_delta_auth(collection, &admission.auth, delta)
                .is_err()
        {
            return ValidatedApplyOutcome::Malformed;
        }
        let candidate = match CrdtState::new(self.peer_id) {
            Ok(state) => state,
            Err(_) => return ValidatedApplyOutcome::Malformed,
        };
        if let Some(current) = self.collections.get(collection) {
            let snapshot = match current.export_snapshot() {
                Ok(snapshot) => snapshot,
                Err(_) => return ValidatedApplyOutcome::Malformed,
            };
            if candidate.import(&snapshot).is_err() {
                return ValidatedApplyOutcome::Malformed;
            }
        }
        let before = candidate.frontier();
        if candidate.import(delta).is_err() {
            return ValidatedApplyOutcome::Malformed;
        }
        let write_set = match candidate.write_set_since(&before) {
            Ok(write_set) => write_set,
            Err(_) => return ValidatedApplyOutcome::Malformed,
        };
        if write_set.iter().any(|(written, row)| {
            written != collection || (!document_id.is_empty() && row != document_id)
        }) {
            return ValidatedApplyOutcome::Malformed;
        }

        // Install the candidate only while validation reads it. Keep the exact
        // previous state available for a no-fail rollback on rejection.
        let previous = self.collections.insert(collection.to_owned(), candidate);
        for (coll, row) in &write_set {
            let sg = if row.as_str() == document_id {
                surrogate
            } else {
                Surrogate::ZERO
            };
            let ValidationOutcome::Rejected(violations) =
                self.validate_committed_row(coll, row, sg)
            else {
                continue;
            };
            let Some(violation) = violations.into_iter().next() else {
                continue;
            };
            let violation = self.dlq_and_translate(coll, delta, peer_id, violation);
            match previous {
                Some(previous) => {
                    self.collections.insert(collection.to_owned(), previous);
                }
                None => {
                    self.collections.remove(collection);
                }
            }
            return ValidatedApplyOutcome::Rejected(violation);
        }

        ValidatedApplyOutcome::Clean { write_set }
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
        assert!(matches!(outcome, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "a"));
        assert_eq!(engine.dlq_len(), 0);
    }

    /// A multi-row frame is rejected before its detached candidate can replace
    /// authoritative state.
    #[test]
    fn multi_doc_delta_does_not_mutate_authoritative_state() {
        let mut engine = unique_engine();
        // One Loro delta that writes two distinct rows.
        let state = CrdtState::new(7).unwrap();
        state
            .upsert(
                "users",
                "a",
                &[("email", LoroValue::String("a@y.com".into()))],
            )
            .unwrap();
        state
            .upsert(
                "users",
                "b",
                &[("email", LoroValue::String("b@y.com".into()))],
            )
            .unwrap();
        let delta = state.export_snapshot().unwrap();

        // Frame claims only row "a".
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta,
            nodedb_types::Surrogate::ZERO,
            "a",
            7,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Malformed));
        assert!(!engine.row_exists("users", "a"));
        assert!(!engine.row_exists("users", "b"));
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
        assert!(matches!(clean, ValidatedApplyOutcome::Clean { .. }));

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
        assert!(
            engine.read_row("users", "b").is_none(),
            "constraint-rejected delta must not mutate authoritative state"
        );
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
