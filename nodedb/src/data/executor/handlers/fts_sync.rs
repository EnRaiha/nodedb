// SPDX-License-Identifier: BUSL-1.1

//! FTS sync ingest handlers: index/delete documents through the idempotency gate.
//!
//! Called by `dispatch_text` when the plan variant is
//! `TextOp::FtsIndexDoc` or `TextOp::FtsDeleteDoc` and the op carries
//! a `SyncProvenance`.
//!
//! Without provenance (local non-sync path) the handlers apply directly
//! as before — no gate overhead.

use tracing::warn;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use nodedb_types::Surrogate;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

impl CoreLoop {
    /// Index a document's text into the inverted BM25 index, optionally gating
    /// on the `SyncProvenance` for idempotent replay.
    ///
    /// Without provenance behaves identically to the pre-gate implementation.
    /// With provenance: runs the idempotency gate (`sync_admit`) before
    /// writing; on `Apply` commits the HWM after the engine write succeeds;
    /// returns a msgpack-encoded `SyncAckResult` in `Response.payload`.
    pub(in crate::data::executor) fn execute_fts_index_doc(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        text: &str,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {
                    // Fall through to engine write below.
                }
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        // ── Engine write ────────────────────────────────────────────────────
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();
        match self
            .inverted
            .index_document(database_id, tenant_id, collection, surrogate, text)
        {
            Ok(()) => {
                // Advance the collection floor for this committed FTS write.
                self.note_collection_write_lsn(task, collection);
                if let Some(prov) = provenance {
                    self.sync_commit(prov);
                    return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
                }
                self.response_ok(task)
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    surrogate = surrogate.as_u32(),
                    error = %e,
                    "FtsIndexDoc: inverted index write failed"
                );
                self.response_error(task, e)
            }
        }
    }

    /// Remove a document from the inverted BM25 index, optionally gating on
    /// `SyncProvenance` for idempotent replay.
    ///
    /// Without provenance behaves identically to the pre-gate implementation.
    pub(in crate::data::executor) fn execute_fts_delete_doc(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // ── Idempotency gate ────────────────────────────────────────────────
        if let Some(prov) = provenance {
            match self.sync_admit(prov) {
                SyncAdmit::Apply => {}
                admit @ (SyncAdmit::Duplicate | SyncAdmit::Fenced | SyncAdmit::Gap { .. }) => {
                    let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&admit),
                        applied_seq,
                    );
                }
            }
        }

        // ── Engine write ────────────────────────────────────────────────────
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();
        match self
            .inverted
            .remove_document(database_id, tenant_id, collection, surrogate)
        {
            Ok(()) => {
                // Advance the collection floor for this committed FTS delete.
                self.note_collection_write_lsn(task, collection);
                if let Some(prov) = provenance {
                    self.sync_commit(prov);
                    return self.sync_ack_response(task, AckStatus::Applied, prov.seq);
                }
                self.response_ok(task)
            }
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    surrogate = surrogate.as_u32(),
                    error = %e,
                    "FtsDeleteDoc: inverted index removal failed"
                );
                self.response_error(task, e)
            }
        }
    }
}
