// SPDX-License-Identifier: BUSL-1.1

//! Stage-time OLLP predicate verification for the dependent-read ACTIVE Calvin
//! path.
//!
//! The STATIC path detects conflict via the LSN-versioned read-set vote
//! (`read_set_still_current`) and runs NO OLLP check at stage time. The ACTIVE
//! (dependent-read) path carries no versioned read-set — its conflict detector
//! is the leader-only OLLP predicate re-check (`actual != predicted` →
//! `OllpRetryRequired`) that the single-shot apply used to run inside
//! `execute_transaction_batch`. Converting ACTIVE to stage → resolve → redo →
//! flush moves that apply to FLUSH, which is AFTER the redo is WAL-appended and
//! where a flush-time `OllpRetryRequired` is swallowed as a degraded shard. So
//! the check must run HERE, before any staging, so drift surfaces on the stage
//! response (where the scheduler releases locks and re-recons) and nothing is
//! staged, resolved, or WAL-appended on a mismatch.
//!
//! On a follower (`ollp_is_group_leader == false`) every prediction is accepted
//! verbatim — identical to the bulk-DML apply handlers — so all replicas stage
//! the same predicted set (Calvin determinism).
//!
//! The op set verified here mirrors the `BulkDelete` / `BulkUpdate` arms of
//! [`CoreLoop::stage_calvin_overlay`]: those are the only plans that carry an
//! OLLP prediction. A new predicate-DML variant that carries a prediction must
//! gain an arm in BOTH places (and the bulk-DML handlers).

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::scan::{
    ollp_edges_match, ollp_predicted_doc_ids, ollp_surrogates_match,
};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Leader-only OLLP verification of every predicate-DML plan in a
    /// dependent-read ACTIVE Calvin txn, BEFORE staging.
    ///
    /// Returns `Ok(true)` when every carried prediction still matches live
    /// state (or this node is a follower, or no plan carries a prediction);
    /// `Ok(false)` when any surrogate- or edge-set prediction drifted (the
    /// caller returns `OllpRetryRequired` and stages nothing); `Err` on a scan
    /// or filter-decode failure. Point ops and non-predicate plans carry no
    /// prediction and are vacuously matching.
    pub(in crate::data::executor) fn verify_calvin_active_ollp(
        &self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) -> crate::Result<bool> {
        if !self.ollp_is_group_leader {
            return Ok(true);
        }
        let database_id = task.request.database_id.as_u64();
        for plan in plans {
            // Only the document engine carries an OLLP prediction. Listed
            // exhaustively so a new `PhysicalPlan` variant forces a decision
            // here rather than falling silently into "nothing to verify".
            let document_op = match plan {
                PhysicalPlan::Document(op) => op,
                PhysicalPlan::Vector(_)
                | PhysicalPlan::Graph(_)
                | PhysicalPlan::Kv(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Columnar(_)
                | PhysicalPlan::Timeseries(_)
                | PhysicalPlan::Spatial(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Query(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::Array(_)
                | PhysicalPlan::ClusterArray(_)
                | PhysicalPlan::ClusterEvent(_) => continue,
            };
            let (collection, filter_bytes, predicted_surrogates, predicted_edges) =
                match document_op {
                    DocumentOp::BulkDelete {
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                        ..
                    }
                    | DocumentOp::BulkUpdate {
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                        ..
                    } => (
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                    ),
                    // Mirrors `stage_calvin_overlay`'s non-bulk arms: no OLLP
                    // prediction to verify. Exhaustive so a new predicate-DML
                    // variant that carries a prediction cannot be added without
                    // being named here.
                    DocumentOp::PointGet { .. }
                    | DocumentOp::PointPut { .. }
                    | DocumentOp::PointInsert { .. }
                    | DocumentOp::PointDelete { .. }
                    | DocumentOp::PointUpdate { .. }
                    | DocumentOp::Upsert { .. }
                    | DocumentOp::BatchInsert { .. }
                    | DocumentOp::Scan { .. }
                    | DocumentOp::RangeScan { .. }
                    | DocumentOp::Register { .. }
                    | DocumentOp::IndexLookup { .. }
                    | DocumentOp::IndexedFetch { .. }
                    | DocumentOp::DropIndex { .. }
                    | DocumentOp::BackfillIndex { .. }
                    | DocumentOp::Truncate { .. }
                    | DocumentOp::EstimateCount { .. }
                    | DocumentOp::InsertSelect { .. }
                    | DocumentOp::UpdateFromJoin { .. }
                    | DocumentOp::Merge { .. }
                    | DocumentOp::MaterializeScan { .. }
                    | DocumentOp::ApplyBalanceDelta { .. } => continue,
                };
            let Some(predicted) = predicted_surrogates.as_deref() else {
                // A bulk op with no prediction is the non-OLLP (static-set)
                // shape; `stage_calvin_overlay` rejects it loudly at staging.
                continue;
            };

            let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(filter_bytes).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("calvin active ollp verify: deserialize filters: {e}"),
                })?
            };
            let matching_ids =
                self.scan_matching_documents(database_id, tid, collection, &filters)?;

            if !ollp_surrogates_match(&matching_ids, predicted) {
                return Ok(false);
            }
            if let Some(predicted_edges) = predicted_edges.as_deref() {
                let apply_ids = ollp_predicted_doc_ids(predicted);
                let actual = self.ollp_actual_edges(database_id, tid, collection, &apply_ids);
                if !ollp_edges_match(actual, predicted_edges) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}
