// SPDX-License-Identifier: BUSL-1.1

//! Point-get overlay consultation: read-your-own-writes for in-transaction
//! point lookups.
//!
//! Non-temporal point-get reads consult the issuing transaction's staging
//! overlay before falling back to the doc cache / base storage, so a point
//! read inside `BEGIN..COMMIT` observes writes staged earlier in the same
//! transaction. Temporal (`AS OF`) reads never consult the overlay — staged
//! bodies only represent the current version, not a historical one.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::Surrogate;

impl CoreLoop {
    /// Consult the active transaction's staging overlay for a point-get.
    ///
    /// Returns `None` when there is no active transaction on this task, or
    /// the transaction has no overlay entry for this collection/surrogate —
    /// callers should fall through to the normal cache/base-storage lookup.
    ///
    /// Returns `Some(Ok(body))` when the overlay holds a staged put — the
    /// caller runs the SAME RLS filtering and strict-decode framing it would
    /// run on a base-storage hit.
    ///
    /// Returns `Some(Err(response))` when the overlay holds a tombstone —
    /// the row is staged-deleted, so the caller should return the given
    /// not-found response immediately (mirrors the base path's empty-result
    /// response for a missing row).
    pub(in crate::data::executor) fn overlay_point_lookup(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
    ) -> Option<Result<Vec<u8>, Response>> {
        let txn_id = task.request.txn_id?;
        let coll_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // A staged-only insert has no base surrogate yet, so the read plan's
        // `surrogate` is unresolved (zero) — resolve by document id first, then
        // fall back to the surrogate for rows that already exist in base.
        let overlay = self.txn_overlays.get(&txn_id)?;
        // A staged-only insert has no base surrogate yet, so the read plan's
        // `surrogate` is unresolved (zero) — resolve by document id first, then
        // fall back to the surrogate for rows that already exist in base.
        let staged = overlay
            .get_by_doc_id(&coll_key, document_id)
            .or_else(|| overlay.get(&coll_key, surrogate.0))?;
        match staged {
            Staged::Put(body) => Some(Ok(body.clone())),
            Staged::Tombstone => Some(Err(self.response_with_payload(task, Vec::new()))),
        }
    }
}
