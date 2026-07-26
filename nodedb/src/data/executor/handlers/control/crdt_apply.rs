// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta-apply handler: validate + materialize an applied Loro delta,
//! for both the non-sync (SQL / native client) and sync (peer) paths.
//!
//! Split out of `crdt.rs` to keep that file within the file-size limit. The
//! one-document-per-delta contract enforced here is the crux: cross-engine
//! identity assigns exactly one Control-Plane surrogate per delta, so the Data
//! Plane can materialize only the single frame-declared row. A delta whose
//! write-set names any other or additional row must be rejected loudly rather
//! than materialized partially — silently dropping the extra rows is the
//! data-loss bug this guard closes.

use tracing::{debug, warn};

use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::sync_gate::SyncAdmit;
use crate::engine::crdt::tenant_state::{DeltaSigningAdmission, ValidatedApplyOutcome};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_crdt_apply`].
pub(in crate::data::executor) struct CrdtApplyParams<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub delta: &'a [u8],
    pub surrogate: Surrogate,
    pub peer_id: u64,
    pub provenance: Option<&'a SyncProvenance>,
    pub constraint_version_required: u64,
    pub expected_frontier_digest: Option<[u8; 32]>,
    pub auth_user_id: u64,
    pub auth_device_id: u64,
    pub auth_seq_no: u64,
    pub delta_signature: [u8; 32],
    pub signing_required: bool,
}

impl CoreLoop {
    /// Read the current domain-bound CRDT frontier without creating an engine.
    fn current_crdt_frontier_digest(
        &self,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
        collection: &str,
    ) -> [u8; 32] {
        self.crdt_engines
            .get(&(database_id, tenant_id))
            .map(|engine| engine.frontier_digest(database_id, collection))
            .unwrap_or_else(|| {
                nodedb_crdt::state::frontier_digest::domain_frontier_digest(
                    tenant_id.as_u64(),
                    database_id.as_u64(),
                    collection,
                    None,
                )
            })
    }

    /// Enforce the one-document-per-delta contract: every row a validated delta
    /// wrote must be exactly the frame-declared `(collection, document_id)`.
    ///
    /// Cross-engine identity assigns exactly one Control-Plane surrogate per
    /// delta, so the Data Plane can materialize only that single row. A delta
    /// whose write-set names any other or additional row — a client that
    /// coalesced N document upserts into one delta, or tagged the frame with a
    /// synthetic id matching no written row — cannot be materialized without a
    /// surrogate per extra row; materializing just one would silently drop the
    /// rest. Returns a human-readable detail naming the offending rows so the
    /// caller surfaces the violation instead of losing data.
    pub(crate) fn single_document_write_set(
        collection: &str,
        document_id: &str,
        write_set: &[(String, String)],
    ) -> Result<(), String> {
        let foreign: Vec<String> = write_set
            .iter()
            .filter(|(coll, row)| coll != collection || row != document_id)
            .map(|(coll, row)| format!("{coll}/{row}"))
            .collect();
        if foreign.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "delta for {collection}/{document_id} wrote {} row(s) outside its frame \
                 target: [{}]; a delta must carry exactly one document (cross-engine \
                 identity binds one surrogate per delta)",
                foreign.len(),
                foreign.join(", ")
            ))
        }
    }

    pub(in crate::data::executor) fn execute_crdt_apply(
        &mut self,
        task: &ExecutionTask,
        params: CrdtApplyParams<'_>,
    ) -> Response {
        let CrdtApplyParams {
            collection,
            document_id,
            delta,
            surrogate,
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        } = params;
        let tenant_id = task.request.tenant_id;

        let Some(prov) = provenance else {
            // Non-sync path (SQL / native client): validate + apply, no gate.
            if let Some(expected) = expected_frontier_digest {
                let actual = self.current_crdt_frontier_digest(
                    task.request.database_id,
                    tenant_id,
                    collection,
                );
                if expected != actual {
                    return self.response_error(
                        task,
                        ErrorCode::CrdtFrontierMismatch { expected, actual },
                    );
                }
            }
            // There is no client to reject here, so the validated outcome is
            // only observed for its DLQ side effect and logged.
            // Borrow the engine in a nested block so the &mut borrow is dropped
            // before the sparse write below takes &self. On a Clean apply we
            // read the merged row back and encode it while the borrow is live,
            // carrying the materialized bytes out.
            let mut imported_authoritative = false;
            let materialized = {
                let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };
                let outcome = engine.apply_committed_delta_validated(
                    collection,
                    delta,
                    surrogate,
                    document_id,
                    peer_id,
                );
                match outcome {
                    ValidatedApplyOutcome::Clean { write_set } => {
                        imported_authoritative = true;
                        // Enforce the one-document-per-delta contract before
                        // materializing: a delta that wrote rows other than the
                        // frame target has no surrogate for those rows, so
                        // materializing only `document_id` would silently drop
                        // the rest.
                        match Self::single_document_write_set(collection, document_id, &write_set) {
                            Ok(()) => {
                                if surrogate != Surrogate::ZERO {
                                    Ok(Self::encode_crdt_row(engine, collection, document_id))
                                } else {
                                    Ok(None)
                                }
                            }
                            Err(detail) => Err(detail),
                        }
                    }
                    ValidatedApplyOutcome::Rejected(vt) => {
                        imported_authoritative = true;
                        debug!(core = self.core_id, %collection, reason = %vt, "crdt apply violated constraint (DLQ)");
                        Ok(None)
                    }
                    ValidatedApplyOutcome::Malformed => {
                        warn!(core = self.core_id, %collection, "crdt apply skipped malformed delta");
                        Ok(None)
                    }
                }
            };
            // Engine borrow dropped here. A clean or constraint-rejected Loro
            // import changed authoritative state; malformed bytes did not.
            if imported_authoritative {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
            }
            // Materialize into the sparse document store so DocumentScan /
            // ShapeSnapshot see the synced document — unless the delta violated
            // the one-document-per-delta contract, in which case reject loudly
            // rather than materialize a partial row.
            match materialized {
                Ok(Some(bytes)) => {
                    self.materialize_synced_document(
                        task,
                        tenant_id.as_u64(),
                        collection,
                        surrogate,
                        &bytes,
                    );
                    if imported_authoritative {
                        self.note_collection_write_lsn(task, collection);
                    }
                }
                Ok(None) if imported_authoritative => {
                    // Headless and constraint-rejected imports have no sparse
                    // projection, but still changed authoritative Loro state.
                    self.note_collection_write_lsn(task, collection);
                }
                Ok(None) => {}
                Err(detail) => {
                    if imported_authoritative {
                        self.note_collection_write_lsn(task, collection);
                    }
                    warn!(
                        core = self.core_id,
                        %collection,
                        %document_id,
                        detail = %detail,
                        "crdt apply rejected: multi-document delta violates one-document-per-delta contract"
                    );
                    return self.response_error(
                        task,
                        ErrorCode::RejectedConstraint {
                            constraint: "crdt_single_document_delta".to_string(),
                            detail,
                        },
                    );
                }
            }
            return self.response_ok(task);
        };

        // A frontier fence is an admission precondition, not an apply-time
        // validation. Classify first without consuming a newer producer epoch;
        // for every non-Apply result retain the established mutating admission
        // path and its epoch-floor semantics.
        let classified = self.sync_classify(prov);
        if !matches!(classified, SyncAdmit::Apply) {
            let admit = self.sync_admit(prov);
            let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            let (status, applied_seq) = match admit {
                SyncAdmit::Duplicate => (AckStatus::Duplicate, current_hwm),
                SyncAdmit::Fenced => (AckStatus::Fenced, current_hwm),
                SyncAdmit::Gap { expected } => (AckStatus::Gap { expected }, current_hwm),
                SyncAdmit::Apply => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "sync admission classification changed unexpectedly".into(),
                        },
                    );
                }
            };
            return self.sync_ack_response_ext(task, status, applied_seq, None);
        }

        // Verify a fenced write against immutable state before creating an
        // engine, observing constraints, advancing an epoch floor/HWM, or
        // touching checkpoint and sparse state. An absent engine has the
        // canonical empty-domain digest.
        if let Some(expected) = expected_frontier_digest {
            let actual =
                self.current_crdt_frontier_digest(task.request.database_id, tenant_id, collection);
            if expected != actual {
                return self
                    .response_error(task, ErrorCode::CrdtFrontierMismatch { expected, actual });
            }
        }

        // The fence passed, so now perform normal mutating admission. This is
        // intentionally after the frontier check: a stale higher-epoch frame
        // must leave the producer floor unchanged.
        let admit = self.sync_admit(prov);
        let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);

        let (status, applied_seq, reject) = match admit {
            SyncAdmit::Apply => {
                // Borrow the engine in a nested block so the &mut borrow is
                // dropped before sync_commit takes &mut self for sync_hwm.
                // The validated apply never fails: a violation is DLQ'd and a
                // corrupt blob is a no-op, so the HWM always advances and the
                // stream cannot wedge.
                //
                // Before validating, fence the delta against the constraint
                // version it was admitted against. `SetConstraints` rides the
                // same per-vshard data Raft log as this `CrdtApply`, so at
                // this log index every replica has applied the identical log
                // prefix and therefore has the identical installed
                // `constraint_versions[collection]` — the gate decision is
                // deterministic across replicas, no divergence.
                enum GateOutcome {
                    Pending { installed: u64 },
                    Applied(ValidatedApplyOutcome),
                }
                let (outcome, materialized) = {
                    let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            );
                        }
                    };
                    let installed = engine.installed_constraint_version(collection);
                    if constraint_version_required > installed {
                        (GateOutcome::Pending { installed }, None)
                    } else {
                        let applied = engine.apply_committed_delta_authenticated(
                            collection,
                            delta,
                            surrogate,
                            document_id,
                            peer_id,
                            DeltaSigningAdmission {
                                auth: nodedb_crdt::CrdtAuthContext {
                                    user_id: auth_user_id,
                                    tenant_id: tenant_id.as_u64(),
                                    auth_expires_at: 0,
                                    delta_signature,
                                    device_id: auth_device_id,
                                    seq_no: auth_seq_no,
                                },
                                required: signing_required,
                                preverified: true,
                            },
                        );
                        // On a Clean apply, read the merged row back and encode
                        // it while the engine borrow is still live so the bytes
                        // can be materialized into the sparse store below.
                        let mat = if matches!(applied, ValidatedApplyOutcome::Clean { .. })
                            && surrogate != Surrogate::ZERO
                        {
                            Self::encode_crdt_row(engine, collection, document_id)
                        } else {
                            None
                        };
                        (GateOutcome::Applied(applied), mat)
                    }
                };
                // engine borrow is dropped here; mark_dirty / sync_commit take
                // &mut self, and the sparse materialize takes &self.
                let mut imported_authoritative = false;
                let reject = match outcome {
                    GateOutcome::Pending { installed } => {
                        // Create-race: the constraints this delta was admitted
                        // against are not yet installed on THIS replica (the
                        // reconcile loop delivers SetConstraints
                        // asynchronously). Do NOT import an unvalidated delta
                        // — that is exactly the hole this fence closes.
                        // Carry a retryable reject; the client re-pushes once
                        // the install catches up. This is NOT a dead letter,
                        // so it is not DLQ'd.
                        debug!(
                            core = self.core_id,
                            %collection,
                            required = constraint_version_required,
                            installed,
                            "crdt apply fenced: constraint version pending (retryable)"
                        );
                        Some(ViolationType::ConstraintVersionPending {
                            collection: collection.to_string(),
                            required: constraint_version_required,
                            installed,
                        })
                    }
                    GateOutcome::Applied(ValidatedApplyOutcome::Clean { write_set }) => {
                        imported_authoritative = true;
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                        // Enforce the one-document-per-delta sync contract. A
                        // delta that coalesced multiple documents (or targeted
                        // a synthetic frame id that matches no written row)
                        // cannot be materialized past its single surrogate;
                        // reject it loudly so the client re-pushes one delta per
                        // document instead of silently losing rows.
                        match Self::single_document_write_set(collection, document_id, &write_set) {
                            Ok(()) => None,
                            Err(detail) => {
                                warn!(
                                    core = self.core_id,
                                    %collection,
                                    %document_id,
                                    detail = %detail,
                                    "crdt sync apply rejected: multi-document delta violates one-document-per-delta contract"
                                );
                                Some(ViolationType::ConstraintViolation { detail })
                            }
                        }
                    }
                    GateOutcome::Applied(ValidatedApplyOutcome::Rejected(vt)) => {
                        imported_authoritative = true;
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                        Some(vt)
                    }
                    GateOutcome::Applied(ValidatedApplyOutcome::Malformed) => {
                        warn!(core = self.core_id, %collection, "crdt apply skipped malformed delta");
                        None
                    }
                };
                // Materialize the merged document into the sparse store so
                // DocumentScan / ShapeSnapshot see the synced write. `materialized`
                // is Some only on a Clean apply with an assigned surrogate, and a
                // contract-violating delta (`reject` set) must not surface a
                // partial row.
                if reject.is_none()
                    && let Some(bytes) = materialized
                {
                    self.materialize_synced_document(
                        task,
                        tenant_id.as_u64(),
                        collection,
                        surrogate,
                        &bytes,
                    );
                }
                if imported_authoritative {
                    self.note_collection_write_lsn(task, collection);
                }
                // Advance the HWM unconditionally after apply — a rejected,
                // fenced, or malformed delta must not wedge the sync stream.
                self.sync_commit(prov);
                (AckStatus::Applied, prov.seq, reject)
            }
            SyncAdmit::Duplicate => (AckStatus::Duplicate, current_hwm, None),
            SyncAdmit::Fenced => (AckStatus::Fenced, current_hwm, None),
            SyncAdmit::Gap { expected } => (AckStatus::Gap { expected }, current_hwm, None),
        };

        self.sync_ack_response_ext(task, status, applied_seq, reject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(c, r)| (c.to_string(), r.to_string()))
            .collect()
    }

    #[test]
    fn single_matching_row_is_accepted() {
        assert!(CoreLoop::single_document_write_set("users", "a", &ws(&[("users", "a")])).is_ok());
    }

    #[test]
    fn empty_write_set_is_accepted() {
        // A delete / no-op delta wrote no rows — nothing to materialize, no
        // contract violation.
        assert!(CoreLoop::single_document_write_set("users", "a", &ws(&[])).is_ok());
    }

    #[test]
    fn additional_row_is_rejected() {
        // Frame targets "a" but the delta also wrote "b": the extra row has no
        // surrogate and would be silently dropped.
        let err = CoreLoop::single_document_write_set(
            "users",
            "a",
            &ws(&[("users", "a"), ("users", "b")]),
        )
        .expect_err("multi-row delta must be rejected");
        assert!(
            err.contains("users/b"),
            "detail names the offending row: {err}"
        );
    }

    #[test]
    fn synthetic_frame_id_matching_no_written_row_is_rejected() {
        // The batch-coalesced bug: frame id "5_ops" matches no real written
        // row, so every written row is "foreign" and the delta is rejected
        // instead of materializing zero rows.
        let err = CoreLoop::single_document_write_set(
            "entries",
            "5_ops",
            &ws(&[("entries", "u1"), ("entries", "u2")]),
        )
        .expect_err("synthetic frame id must be rejected");
        assert!(err.contains("entries/u1") && err.contains("entries/u2"));
    }

    #[test]
    fn foreign_collection_is_rejected() {
        let err = CoreLoop::single_document_write_set("users", "a", &ws(&[("orders", "a")]))
            .expect_err("row in a different collection must be rejected");
        assert!(err.contains("orders/a"));
    }
}
