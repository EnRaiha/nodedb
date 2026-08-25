// SPDX-License-Identifier: BUSL-1.1

//! Classify the columnar-storage-family engine ops (`ColumnarOp`,
//! `TimeseriesOp`, `TextOp`, `SpatialOp`) into an optional `ReplicatedWrite`.
//!
//! Each function is exhaustive over its op enum (not a catch-all): a new
//! variant is a compile error here, so no future write in these families is
//! silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::{columnar, entry::encode_provenance};
use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TextOp, TimeseriesOp};

/// Encode a `ColumnarOp` write variant into its `ReplicatedWrite` wire shape,
/// `Ok(None)` for scans, or an error when the op cannot be replicated safely.
///
/// `ColumnarOp::Update` / `ColumnarOp::Delete` refuse when
/// `rls_write_check.has_predicate()` — see the arm below for why: a governed
/// predicate DML must never cross the wire as a bare predicate.
pub(super) fn columnar_write(op: &ColumnarOp) -> crate::Result<Option<ReplicatedWrite>> {
    Ok(Some(match op {
        ColumnarOp::Insert {
            collection,
            payload,
            surrogates,
            schema_bytes,
            provenance,
            // wal_lsn is omitted from the wire envelope; followers allocate
            // their own LSN at apply time. intent and on_conflict_updates are
            // always Insert/empty on the sync path and are hardcoded on decode.
            ..
        } => columnar::columnar_ingest(
            collection,
            payload,
            surrogates,
            schema_bytes,
            encode_provenance(provenance),
        ),
        // The compiled RLS predicate is deliberately not replicated when NO
        // write policy restricts this collection: there is nothing for a
        // follower to decide either way, so re-scanning the predicate at each
        // replica's own committed state is exactly the deterministic-replay
        // behavior `ColumnarBulkDml` is built for.
        //
        // When a write policy DOES restrict this collection
        // (`rls_write_check.has_predicate()`), shipping the bare predicate is
        // the bug this refusal closes: a follower has no writing identity to
        // decide the predicate against, so `ColumnarBulkDml` would either
        // admit every row on every replica (silent bypass) or have the
        // leader re-decide after commit and reject what followers already
        // applied (divergence). A governed predicate DML must be resolved to
        // a concrete row set (`ColumnarOp::ResolvedUpdate` /
        // `ColumnarOp::ResolvedDelete`) before it reaches this encoder.
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check,
        } => {
            refuse_governed_predicate_dml(collection, rls_write_check)?;
            columnar::bulk_delete(collection, filters)
        }
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check,
        } => {
            refuse_governed_predicate_dml(collection, rls_write_check)?;
            columnar::bulk_update(collection, filters, updates)
        }

        // The Control Plane already resolved these rows and decided the
        // write policy against their exact images, so the wire shape carries
        // the resolved row set — never a predicate — and needs no refusal
        // check.
        ColumnarOp::ResolvedUpdate {
            collection,
            rows,
            rls_write_check: _,
        } => columnar::bulk_resolved_update(collection, rows),
        ColumnarOp::ResolvedDelete {
            collection,
            pks,
            rls_write_check: _,
        } => columnar::bulk_resolved_delete(collection, pks),

        // Not a write — reads / scans. `ResolveDml` is a read too: it mutates
        // nothing, only reports the row set a predicate DML would touch.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => return Ok(None),
    }))
}

/// Refuse a predicate `UPDATE` / `DELETE` on a collection that carries an RLS
/// write policy — see [`columnar_write`]'s `Delete` / `Update` arms for the
/// full reasoning.
fn refuse_governed_predicate_dml(
    collection: &str,
    rls_write_check: &nodedb_types::RlsWriteCheck,
) -> crate::Result<()> {
    if rls_write_check.has_predicate() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "columnar predicate UPDATE/DELETE on '{collection}' cannot be replicated as a \
                 predicate because it carries an RLS write policy: a follower has no writing \
                 identity to evaluate the predicate against. It must be resolved to a concrete \
                 row set before it is proposed."
            ),
        });
    }
    Ok(())
}

/// Encode a `TimeseriesOp` write variant into its `ReplicatedWrite` wire
/// shape, or `None` for scans.
pub(super) fn timeseries_write(op: &TimeseriesOp) -> Option<ReplicatedWrite> {
    Some(match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            surrogates,
            provenance,
            ..
        } => columnar::timeseries_ingest(
            collection,
            payload,
            format,
            surrogates,
            encode_provenance(provenance),
        ),

        // Not a write — reads / scans.
        TimeseriesOp::Scan { .. } => return None,
    })
}

/// Encode a `TextOp` write variant into its `ReplicatedWrite` wire shape, or
/// `None` for the search / DDL-config variants.
pub(super) fn text_write(op: &TextOp) -> Option<ReplicatedWrite> {
    Some(match op {
        TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance,
        } => columnar::fts_index(
            collection,
            surrogate.as_u32(),
            text,
            encode_provenance(provenance),
        ),
        TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance,
        } => columnar::fts_delete(
            collection,
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),

        // Not a write — BM25 / phrase / hybrid searches and the config-only
        // analyzer binding (single-node, non-WAL-durable).
        TextOp::Search { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::SetTextConfig { .. } => return None,
    })
}

/// Encode a `SpatialOp` write variant into its `ReplicatedWrite` wire shape,
/// or `None` for scans.
pub(super) fn spatial_write(op: &SpatialOp) -> Option<ReplicatedWrite> {
    Some(match op {
        SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        } => columnar::spatial_insert(
            collection,
            field,
            surrogate.as_u32(),
            geometry,
            encode_provenance(provenance),
        ),
        SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        } => columnar::spatial_delete(
            collection,
            field,
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),

        // Not a write — R-tree index scan.
        SpatialOp::Scan { .. } => return None,
    })
}
