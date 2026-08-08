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
/// or `None` for scans.
pub(super) fn columnar_write(op: &ColumnarOp) -> Option<ReplicatedWrite> {
    Some(match op {
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
        // The compiled RLS predicate is deliberately not replicated: it was
        // already applied by the leader that accepted this write, and it
        // resolves against the writing identity's session, which no follower
        // has. A follower re-evaluating it would deny writes the leader
        // committed.
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check: _,
        } => columnar::bulk_delete(collection, filters),
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check: _,
        } => columnar::bulk_update(collection, filters, updates),

        // Not a write — reads / scans.
        ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => return None,
    })
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
