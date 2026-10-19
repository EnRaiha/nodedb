// SPDX-License-Identifier: BUSL-1.1

//! Calvin overlay staging for predicate DML — `DocumentOp::BulkUpdate` /
//! `BulkDelete` — split out of `calvin_overlay_stage.rs` to stay within the
//! file-size limit.
//!
//! # The determinism rule
//!
//! Staging resolves EXACTLY the CP-injected `ollp_predicted_surrogates` set,
//! verbatim, via the `ollp_predicted_doc_ids` primitive. The install applies
//! the redo record `CalvinResolve` builds from this staging, so the staged
//! rows are the rows the install writes.
//!
//! # The prediction check
//!
//! Before a bulk plan stages, the leader checks its prediction against the
//! rows the predicate matches at the plan's turn: BASE with the overlay of
//! the plans staged before it. The surrogate set, the implicit edges of
//! matched edge documents, and the materialized-sum coverage must all hold.
//! A miss refuses the stage with `OllpRetryRequired`, which votes
//! `PredictionDrift`. Only the data-group leader stages, and a bulk plan
//! holds its collection's lock, so a lagging follower never judges.
//!
//! Reading each predicted surrogate's CURRENT body (for `BulkUpdate`'s
//! post-image and read-your-own-writes) is still safe to source from local
//! BASE ∪ OVERLAY: Calvin's deterministic total order guarantees every
//! replica has applied the identical prior-ops prefix before this op stages,
//! so the content at a fixed, already-agreed-upon key is identical across
//! replicas — unlike predicate *membership*, which is what the surrogate-set
//! fixing above protects against.
//!
//! `BulkUpdate`'s per-row transform reuses `CoreLoop::stage_apply_update`,
//! the decode → apply-updates → recompute-generated → re-encode pipeline
//! `execute_bulk_update` and `stage_point_update` share.
//!
//! Each stage returns the number of rows it staged, the affected count the
//! statement reports.

use nodedb_physical::physical_plan::{ResolvedSumTarget, UpdateValue};
use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::handlers::bulk_dml::scan::ollp_predicted_doc_ids;
use crate::data::executor::handlers::bulk_dml::{TxnPrediction, decode_bulk_filters};
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::handlers::transaction::stage_write::{
    StagedStatement, stored_row_identity,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// What a Calvin bulk plan carries for its prediction check.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct CalvinBulkCheck<'a> {
    /// The predicate's serialized filters.
    pub filters: &'a [u8],
    /// The rows the Control Plane predicted. `None` when the plan carries none.
    pub prediction: Option<TxnPrediction<'a>>,
    /// The materialized-sum resolution the plan carries.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// Borrowed inputs for [`CoreLoop::stage_calvin_bulk_delete`], grouped so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct CalvinBulkDeleteStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub check: CalvinBulkCheck<'a>,
    /// Compiled RLS write policy deciding each removed row's pre-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// The collection's DDL-declared primary key, when it has one. Names the
    /// column each removed row's identity is read from.
    pub declared_primary_key: Option<&'a str>,
}

/// Loudly reject a Calvin bulk predicate plan that reached overlay staging
/// without a predicted surrogate set. A Calvin-reachable bulk plan always
/// carries one (injected at Control-Plane recon before dispatch); a plan
/// shape missing it must never be staged as if it were a no-op, or the redo
/// this staging feeds would silently omit the write entirely.
fn missing_prediction_error(collection: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "calvin bulk predicate write reached overlay staging without \
             ollp_predicted_surrogates for collection '{collection}'; a Calvin bulk \
             plan must be recon-injected before dispatch"
        ),
    }
}

/// Borrowed inputs for [`CoreLoop::stage_calvin_bulk_update`], grouped so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct CalvinBulkUpdateStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub check: CalvinBulkCheck<'a>,
    /// Compiled RLS write policy gating each staged post-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// The collection's DDL-declared primary key, when it has one. A staged
    /// post-image that nulls it is refused.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Stage a Calvin `BulkDelete` into the overlay: one tombstone per
    /// predicted surrogate, resolved to its doc-id via
    /// `ollp_predicted_doc_ids` — the identical primitive the install
    /// uses to derive `apply_ids`. NOT a live predicate rescan. Returns the
    /// number of predicted rows that exist.
    pub(in crate::data::executor) fn stage_calvin_bulk_delete(
        &mut self,
        params: CalvinBulkDeleteStage<'_>,
    ) -> crate::Result<usize> {
        let CalvinBulkDeleteStage {
            task,
            tid,
            txn_id,
            collection,
            check,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(prediction) = check.prediction else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        self.check_calvin_prediction(txn_id, &coll_key, &[], &check, prediction)?;
        let predicted = prediction.surrogates;

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();
        let doc_ids = ollp_predicted_doc_ids(predicted);

        // Each row's identity is read once from its current body under
        // BASE ∪ OVERLAY, by the rule INSERT minted it with, so an earlier
        // plan of the same transaction is observed. A row with no current
        // body removes nothing; its tombstone is keyed by the decimal
        // surrogate.
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);
        let mut rows: Vec<(u32, nodedb_types::RowIdentity, Option<Vec<u8>>)> =
            Vec::with_capacity(doc_ids.len());
        for (surrogate, doc_id) in predicted_sorted.into_iter().zip(doc_ids) {
            let body = self.calvin_current_body(CalvinRowRead {
                txn_id,
                coll_key: &coll_key,
                surrogate,
                storage_key: &doc_id,
            })?;
            let identity = match &body {
                Some(body) => {
                    stored_row_identity(body, strict_schema.as_ref(), declared_primary_key, doc_id)
                }
                None => doc_id.to_identity(),
            };
            rows.push((surrogate, identity, body));
        }

        // Decide every predicted row's pre-deletion image against the write
        // policy BEFORE any tombstone is staged, so a rejected row cannot leave
        // the rows ahead of it hidden from the rest of the transaction. A row
        // with no current body removes nothing and is admitted.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for (_, identity, body) in &rows {
                if let Some(body) = body {
                    self.stage_admit_write(
                        rls_write_check,
                        body,
                        identity,
                        database_id.as_u64(),
                        tid,
                        collection,
                    )?;
                }
            }
        }

        let removed = rows.iter().filter(|(_, _, body)| body.is_some()).count();
        let overlay = self.txn_overlay_mut(txn_id);
        for (surrogate, identity, _body) in &rows {
            overlay.insert_tombstone(coll_key.clone(), *surrogate, identity);
        }
        Ok(removed)
    }

    /// Stage a Calvin `BulkUpdate` into the overlay: for each predicted
    /// surrogate, read its current BASE ∪ OVERLAY body (read-your-own-writes
    /// against earlier ops in the same Calvin transaction), apply `updates`
    /// via the exact same per-row transform `execute_bulk_update` /
    /// `stage_point_update` use (`CoreLoop::stage_apply_update`), and stage
    /// the post-image as a `Put`. Row set = predicted surrogates, matching
    /// the install set exactly (and correctly excluding rows a
    /// same-transaction `INSERT` created after the predicted set was
    /// computed at recon).
    ///
    /// A predicted surrogate that resolves to no current body (already
    /// tombstoned in this transaction, or absent from BASE) is skipped — the
    /// identical `continue`-on-miss behavior `execute_bulk_update` exhibits
    /// for its own `apply_ids` loop. Returns the number of rows staged.
    pub(in crate::data::executor) fn stage_calvin_bulk_update(
        &mut self,
        params: CalvinBulkUpdateStage<'_>,
    ) -> crate::Result<usize> {
        let CalvinBulkUpdateStage {
            task,
            tid,
            txn_id,
            collection,
            updates,
            check,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(prediction) = check.prediction else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        self.check_calvin_prediction(txn_id, &coll_key, updates, &check, prediction)?;
        let predicted = prediction.surrogates;

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);

        let mut post_images = Vec::new();
        for surrogate in predicted_sorted {
            let storage_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));

            // Current body: overlay wins over base (read-your-own-writes),
            // mirroring `stage_point_update`'s exact overlay-then-base read.
            let Some(current_bytes) = self.calvin_current_body(CalvinRowRead {
                txn_id,
                coll_key: &coll_key,
                surrogate,
                storage_key: &storage_key,
            })?
            else {
                continue;
            };

            let new_body = self.stage_apply_update(
                database_id.as_u64(),
                tid,
                collection,
                &current_bytes,
                updates,
                declared_primary_key,
            )?;
            // Decide the staged post-image against the write policy: this is
            // the row the stamped redo install writes.
            let identity = stored_row_identity(
                &new_body,
                strict_schema.as_ref(),
                declared_primary_key,
                storage_key,
            );
            self.stage_admit_write(
                rls_write_check,
                &new_body,
                &identity,
                database_id.as_u64(),
                tid,
                collection,
            )?;
            post_images.push((surrogate, identity, new_body));
        }

        // UNIQUE holds at the end of the statement: the rows are judged
        // together, so a value one of them releases is free for another.
        let judged: Vec<(u32, &[u8])> = post_images
            .iter()
            .map(|(surrogate, _, body)| (*surrogate, body.as_slice()))
            .collect();
        self.stage_stored_unique_check(
            &StagedStatement {
                database_id: database_id.as_u64(),
                tid,
                txn_id,
                coll_key: &coll_key,
            },
            &judged,
        )?;

        let updated = post_images.len();
        for (surrogate, identity, new_body) in post_images {
            self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &identity, new_body)?;
        }
        Ok(updated)
    }

    /// Refuse a Calvin bulk plan whose prediction does not hold at its turn
    /// in the transaction: BASE with the overlay of the plans staged before
    /// it. The surrogate set and the implicit edges must equal the rows'. The
    /// materialized-sum resolution must cover every join value the rows
    /// require, read after `updates` for an update. A miss refuses with
    /// `OllpRetryRequired`, which votes `PredictionDrift`.
    fn check_calvin_prediction(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        updates: &[(String, UpdateValue)],
        check: &CalvinBulkCheck<'_>,
        prediction: TxnPrediction<'_>,
    ) -> crate::Result<()> {
        let drift = || crate::Error::DataPlane(ErrorCode::OllpRetryRequired);
        let filters = decode_bulk_filters(check.filters)?;
        let rows = self.txn_matching_rows(txn_id, coll_key, &filters)?;
        if !prediction.holds(&rows) {
            return Err(drift());
        }
        let sum_check = SumTargetCheck {
            database_id: coll_key.0.as_u64(),
            tid: coll_key.1.as_u64(),
            collection: coll_key.2.as_str(),
            updates,
            resolved: check.resolved_sum_targets,
        };
        if !self.declares_materialized_sums(&sum_check) {
            return Ok(());
        }
        let images: Vec<serde_json::Value> = rows
            .iter()
            .filter_map(|(_, body)| self.decode_source_row(&sum_check, body))
            .collect();
        if self.sum_targets_diverged(&sum_check, &images) {
            return Err(drift());
        }
        Ok(())
    }
}

/// One row a Calvin bulk stage reads under BASE ∪ OVERLAY.
struct CalvinRowRead<'a> {
    txn_id: TxnId,
    coll_key: &'a (DatabaseId, TenantId, String),
    surrogate: u32,
    storage_key: &'a nodedb_types::StorageKey,
}

impl CoreLoop {
    /// The row's current stored body inside the Calvin transaction. A staged
    /// put wins over base. A staged tombstone or a staged TRUNCATE hides the
    /// row. Otherwise the body is base storage: the current version on a
    /// bitemporal collection.
    fn calvin_current_body(&self, read: CalvinRowRead<'_>) -> crate::Result<Option<Vec<u8>>> {
        let overlay = self.txn_overlays.get(&read.txn_id);
        match overlay.and_then(|o| o.get(read.coll_key, read.surrogate)) {
            Some(Staged::Put(body)) => return Ok(Some(body.clone())),
            Some(Staged::Tombstone) => return Ok(None),
            None => {}
        }
        if overlay.is_some_and(|o| !o.base_visible(read.coll_key)) {
            return Ok(None);
        }
        let (database_id, tenant, collection) = read.coll_key;
        self.current_row_body(
            database_id.as_u64(),
            tenant.as_u64(),
            collection,
            read.storage_key,
        )
    }
}
