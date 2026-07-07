// SPDX-License-Identifier: BUSL-1.1

//! CRDT operation handlers: read, versioned read, version vector, delta export,
//! restore, compact, list insert/delete/move, and apply.

use tracing::{debug, warn};

use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::sync_gate::SyncAdmit;
use crate::engine::crdt::tenant_state::ValidatedApplyOutcome;

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
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_crdt_read(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
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
        match engine.read_snapshot(collection, document_id) {
            Ok(Some(snapshot)) => self.response_with_payload(task, snapshot),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt read snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Read a CRDT document at a historical version.
    pub(in crate::data::executor) fn execute_crdt_read_at_version(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        version_vector_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.read_at_version_json(collection, document_id, version_vector_json) {
            Ok(Some(json_bytes)) => self.response_with_payload(task, json_bytes),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Get the current CRDT version vector.
    pub(in crate::data::executor) fn execute_crdt_get_version_vector(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.version_vector_json(collection) {
            Ok(json) => self.response_with_payload(task, json.into_bytes()),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Export CRDT delta from a version to current.
    pub(in crate::data::executor) fn execute_crdt_export_delta(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        from_version_json: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.export_delta(collection, from_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Restore a CRDT document to a historical version.
    pub(in crate::data::executor) fn execute_crdt_restore(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt restore");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.restore_to_version(collection, document_id, target_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Compact CRDT history at a specific version.
    pub(in crate::data::executor) fn execute_crdt_compact(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, "crdt compact at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.compact_at_version(collection, target_version_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
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
        } = params;
        let tenant_id = task.request.tenant_id;

        let Some(prov) = provenance else {
            // Non-sync path (SQL / native client): validate + apply, no gate.
            // There is no client to reject here, so the validated outcome is
            // only observed for its DLQ side effect and logged.
            // Borrow the engine in a nested block so the &mut borrow is dropped
            // before the sparse write below takes &self. On a Clean apply we
            // read the merged row back and encode it while the borrow is live,
            // carrying the materialized bytes out.
            let materialized = {
                let engine = match self.get_crdt_engine(tenant_id) {
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
                    ValidatedApplyOutcome::Clean => {
                        if surrogate != Surrogate::ZERO {
                            Self::encode_crdt_row(engine, collection, document_id)
                        } else {
                            None
                        }
                    }
                    ValidatedApplyOutcome::Rejected(vt) => {
                        debug!(core = self.core_id, %collection, reason = %vt, "crdt apply violated constraint (DLQ)");
                        None
                    }
                    ValidatedApplyOutcome::Malformed => {
                        warn!(core = self.core_id, %collection, "crdt apply skipped malformed delta");
                        None
                    }
                }
            };
            // engine borrow dropped here; materialize into the sparse document
            // store so DocumentScan / ShapeSnapshot see the synced document.
            if let Some(bytes) = materialized {
                self.materialize_synced_document(
                    task,
                    tenant_id.as_u64(),
                    collection,
                    surrogate,
                    &bytes,
                );
            }
            self.checkpoint_coordinator.mark_dirty("crdt", 1);
            return self.response_ok(task);
        };

        // Sync path: run the idempotency gate before touching the engine.
        // Call sync_admit first (exclusive &mut self borrow, no engine borrow).
        let admit = self.sync_admit(prov);

        // Snapshot the current HWM for Duplicate / Fenced / Gap responses
        // before any engine borrow.
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
                    let engine = match self.get_crdt_engine(tenant_id) {
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
                        let applied = engine.apply_committed_delta_validated(
                            collection,
                            delta,
                            surrogate,
                            document_id,
                            peer_id,
                        );
                        // On a Clean apply, read the merged row back and encode
                        // it while the engine borrow is still live so the bytes
                        // can be materialized into the sparse store below.
                        let mat = if matches!(applied, ValidatedApplyOutcome::Clean)
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
                    GateOutcome::Applied(ValidatedApplyOutcome::Clean) => {
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                        None
                    }
                    GateOutcome::Applied(ValidatedApplyOutcome::Rejected(vt)) => {
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
                // is Some only on a Clean apply with an assigned surrogate.
                if let Some(bytes) = materialized {
                    self.materialize_synced_document(
                        task,
                        tenant_id.as_u64(),
                        collection,
                        surrogate,
                        &bytes,
                    );
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

    /// Import a per-collection Loro snapshot into the tenant CRDT engine.
    ///
    /// The durable RESTORE re-issue path replicates this through Raft so every
    /// replica of the data group lands the same snapshot. `import_snapshot_bytes`
    /// is a monotonic, idempotent, commutative Loro merge, so applying the same
    /// bytes on every replica converges deterministically — there is no sync
    /// idempotency gate and no per-document surrogate to bind.
    pub(in crate::data::executor) fn execute_crdt_import_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        bytes: &[u8],
    ) -> Response {
        let tid = crate::types::TenantId::new(tenant_id);
        debug!(core = self.core_id, %tid, %collection, "crdt import snapshot");
        let engine = match self.get_crdt_engine(tid) {
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
        match engine.import_snapshot_bytes(collection, bytes) {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.response_ok(task)
            }
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt import snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Read the current HWM for a `(producer_id, stream_id)` pair without
    /// advancing it. Returns `0` when no frame from this producer has been
    /// committed on this stream yet.
    pub(in crate::data::executor) fn sync_hwm_value(
        &self,
        producer_id: u64,
        stream_id: u64,
    ) -> u64 {
        *self.sync_hwm.get(&(producer_id, stream_id)).unwrap_or(&0)
    }
}
