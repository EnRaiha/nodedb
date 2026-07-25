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
    ///
    /// `write_set` lists the `(collection, row_id)` pairs the delta actually
    /// wrote. The caller inspects it to enforce the one-document-per-delta
    /// sync contract: cross-engine identity binds exactly one Control-Plane
    /// surrogate per delta, so the Data Plane can only materialize the single
    /// frame-declared row. A delta that wrote other or additional rows (a
    /// client that coalesced N upserts into one delta) must be rejected
    /// loudly — materializing just one row would silently drop the rest.
    Clean { write_set: Vec<(String, String)> },
    /// The delta imported but a row violated a constraint. The violation has
    /// been enqueued to the DLQ; the translated type is carried for the caller.
    Rejected(ViolationType),
    /// The delta bytes could not be imported (corrupt / undecodable). Treated
    /// as an idempotent no-op so the stream is not wedged.
    Malformed,
    /// The delta's causal predecessors are absent from this collection's
    /// document, so Loro buffered its operations as pending and the applied
    /// state did not move.
    ///
    /// Distinct from [`Self::Malformed`]: malformed bytes can never apply and
    /// are safely skipped, whereas these operations are well-formed and simply
    /// arrived without their history. Treating them as a no-op would advance
    /// the high-water-mark past a write that was never applied — the silent
    /// data-loss path. The caller must refuse the delta instead.
    PendingDependencies,
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
            match state.import(delta) {
                Ok(()) => {}
                // Well-formed operations that arrived without their causal
                // history. The state did not move, so this is NOT a no-op the
                // caller may acknowledge.
                Err(nodedb_crdt::CrdtError::ImportPendingDependencies) => {
                    return ValidatedApplyOutcome::PendingDependencies;
                }
                Err(_) => return ValidatedApplyOutcome::Malformed,
            }
            match state.write_set_since(&before) {
                Ok(ws) => ws,
                Err(_) => return ValidatedApplyOutcome::Malformed,
            }
        };

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
            return ValidatedApplyOutcome::Rejected(
                self.dlq_and_translate(coll, delta, peer_id, violation),
            );
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

    /// A delta that wrote more than the one frame-declared row surfaces every
    /// written `(collection, row_id)` in the `Clean` write-set, so the sync
    /// handler can detect the one-document-per-delta contract violation
    /// instead of silently materializing a single row (regression for the
    /// batch-coalesced-delta data-loss bug: N upserts merged into one delta
    /// tagged with a synthetic frame id materialized zero or one of N rows).
    #[test]
    fn multi_doc_delta_reports_full_write_set() {
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
        let ValidatedApplyOutcome::Clean { write_set } = outcome else {
            panic!("expected Clean with a populated write-set");
        };
        // Both rows are reported, including the one the frame did NOT declare.
        assert!(write_set.iter().any(|(c, r)| c == "users" && r == "a"));
        assert!(write_set.iter().any(|(c, r)| c == "users" && r == "b"));
    }

    /// A peer whose local CRDT document spans several collections exports each
    /// delta as an incremental slice of ONE shared oplog. Origin keeps one Loro
    /// document PER COLLECTION, so a delta for collection `probe` can causally
    /// depend on operations that were routed into a different collection's
    /// document and are therefore absent here.
    ///
    /// Loro accepts such a delta and buffers its operations as causally
    /// pending: the import returns success, but the applied state never
    /// advances and the row is never written. That MUST NOT be reported as a
    /// clean apply — a clean apply means the caller may advance the
    /// high-water-mark and acknowledge the write as durable, which turns a
    /// deferred operation into permanent, silent data loss.
    #[test]
    fn causally_pending_delta_is_not_clean() {
        let mut engine = unique_engine();

        // Peer-local document spanning two collections, exporting one
        // incremental delta per write — the shape every multi-collection
        // embedded client produces.
        let peer = CrdtState::new(11).unwrap();

        let v0 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("u1@y.com".into()))],
        )
        .unwrap();
        let delta_u1 = peer.export_updates_since(&v0).unwrap();

        let v1 = peer.oplog_version_vector();
        peer.upsert("signals", "s1", &[("value", LoroValue::String("v".into()))])
            .unwrap();
        let _delta_s1 = peer.export_updates_since(&v1).unwrap();

        let v2 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "u2",
            &[("email", LoroValue::String("u2@y.com".into()))],
        )
        .unwrap();
        let delta_u2 = peer.export_updates_since(&v2).unwrap();

        // The first `users` delta has no missing predecessor and applies.
        let first = engine.apply_committed_delta_validated(
            "users",
            &delta_u1,
            nodedb_types::Surrogate::ZERO,
            "u1",
            11,
        );
        assert!(matches!(first, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "u1"));

        // The second `users` delta depends on the intervening `signals`
        // operation, which lives in a different document on this side.
        let second = engine.apply_committed_delta_validated(
            "users",
            &delta_u2,
            nodedb_types::Surrogate::ZERO,
            "u2",
            11,
        );

        // The load-bearing assertion: an apply that did not actually write the
        // row must not be indistinguishable from one that did.
        assert!(
            !matches!(second, ValidatedApplyOutcome::Clean { .. }),
            "a delta whose operations stayed causally pending must not report a \
             clean apply; got {second:?}"
        );

        // Guard against the specific silent-loss shape: reporting
        // `Clean { write_set: [] }` passes every downstream contract check
        // (an empty write-set is a legal no-op delete) while dropping the row.
        if let ValidatedApplyOutcome::Clean { write_set } = &second {
            assert!(
                !write_set.is_empty(),
                "clean apply with an empty write-set silently drops the row"
            );
        }
    }

    /// A delta must never be reported `Clean` while the row it claimed to
    /// write is absent. Paired with [`causally_pending_delta_is_not_clean`],
    /// this pins the observable consequence rather than only the outcome enum:
    /// whatever remedy the apply path chooses, "reported clean" and "row
    /// readable" must agree.
    ///
    /// This models a peer that keeps ONE document for several collections and
    /// therefore emits deltas that are not self-contained. Such a delta cannot
    /// be materialized here — its predecessors live in another document — so
    /// the correct outcome is a loud refusal, not a clean apply over a missing
    /// row. A peer that keeps one document per collection emits self-contained
    /// deltas and is covered end to end on the client side.
    #[test]
    fn causally_pending_delta_does_not_lose_its_row() {
        let mut engine = unique_engine();
        let peer = CrdtState::new(12).unwrap();

        let v0 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "a",
            &[("email", LoroValue::String("a@y.com".into()))],
        )
        .unwrap();
        let delta_a = peer.export_updates_since(&v0).unwrap();

        let v1 = peer.oplog_version_vector();
        peer.upsert("audit", "e1", &[("op", LoroValue::String("w".into()))])
            .unwrap();

        let v2 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "b",
            &[("email", LoroValue::String("b@y.com".into()))],
        )
        .unwrap();
        let delta_b = peer.export_updates_since(&v2).unwrap();
        let _ = v1;

        engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            nodedb_types::Surrogate::ZERO,
            "a",
            12,
        );
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            nodedb_types::Surrogate::ZERO,
            "b",
            12,
        );

        let reported_clean = matches!(outcome, ValidatedApplyOutcome::Clean { .. });
        assert_eq!(
            reported_clean,
            engine.row_exists("users", "b"),
            "apply reported clean={reported_clean} but row_exists={}; a clean \
             apply must leave its row readable and an unreadable row must not \
             be reported clean",
            engine.row_exists("users", "b")
        );
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
