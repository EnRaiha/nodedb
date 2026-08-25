// SPDX-License-Identifier: BUSL-1.1

//! Decode helpers for sync-engine `ReplicatedWrite` variants.
//!
//! Each function maps the destructured fields of one `ReplicatedWrite` variant
//! back to a `PhysicalPlan`, using the leader-assigned surrogates verbatim
//! rather than re-deriving identity through the local assigner. `wal_lsn` is
//! always `None` — followers allocate their own WAL LSN at apply time.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, ReturningSpec, SpatialOp, TextOp, TimeseriesOp, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

/// Decode optional sync provenance from the wire bytes.
///
/// Provenance carries the producer/epoch/seq that the Data Plane idempotency
/// gate uses to deduplicate replayed writes. A corrupt encoding must fail loud
/// (propagate) — the same contract as `geometry` decoding in
/// [`spatial_insert`] — rather than silently dropping to `None`. A silent drop
/// would blind the gate and risk double-applying the write on a follower.
pub fn decode_provenance(
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<Option<nodedb_types::sync::wire::SyncProvenance>> {
    match prov_bytes {
        Some(b) => zerompk::from_msgpack::<nodedb_types::sync::wire::SyncProvenance>(b)
            .map(Some)
            .map_err(|e| crate::Error::Internal {
                detail: format!("SyncProvenance decode failed: {e}"),
            }),
        None => Ok(None),
    }
}

/// Decode an optional msgpack-encoded RETURNING spec from the wire bytes.
///
/// Same contract as [`decode_provenance`]: a corrupt encoding fails loud
/// rather than silently dropping to `None`, which would turn a caller's
/// `RETURNING` request into a silent empty result.
pub fn decode_returning(bytes: &Option<Vec<u8>>) -> crate::Result<Option<ReturningSpec>> {
    match bytes {
        Some(b) => zerompk::from_msgpack::<ReturningSpec>(b)
            .map(Some)
            .map_err(|e| crate::Error::Internal {
                detail: format!("ReturningSpec decode failed: {e}"),
            }),
        None => Ok(None),
    }
}

/// Fields carried on `ReplicatedWrite::ColumnarIngest` needed to reconstruct
/// `ColumnarOp::Insert`. Bundled into a struct — plain positional arguments
/// here exceed clippy's arity lint.
pub struct ColumnarIngestWire<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub schema_bytes: &'a [u8],
    pub surrogates: &'a [u32],
    pub prov_bytes: &'a Option<Vec<u8>>,
    pub returning_bytes: &'a Option<Vec<u8>>,
    pub rls_filters: &'a [u8],
}

pub fn columnar_ingest(wire: ColumnarIngestWire<'_>) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(wire.prov_bytes)?;
    let returning = decode_returning(wire.returning_bytes)?;
    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: wire.collection.to_owned(),
        payload: wire.payload.to_vec(),
        format: wire.format.to_owned(),
        intent: wire.intent,
        on_conflict_updates: wire.on_conflict_updates.to_vec(),
        surrogates: wire
            .surrogates
            .iter()
            .copied()
            .map(Surrogate::new)
            .collect(),
        schema_bytes: wire.schema_bytes.to_vec(),
        provenance,
        wal_lsn: None,
        // No predicate here: this node applies an already-committed sync
        // write. The writing identity is not available on this node.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning,
        rls_filters: wire.rls_filters.to_vec(),
    }))
}

pub fn timeseries_ingest(
    collection: &str,
    payload: &[u8],
    format: &str,
    surrogates: &[u32],
    prov_bytes: &Option<Vec<u8>>,
    returning_bytes: &Option<Vec<u8>>,
    rls_filters: &[u8],
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    let returning = decode_returning(returning_bytes)?;
    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        format: format.to_owned(),
        wal_lsn: None,
        surrogates: surrogates.iter().copied().map(Surrogate::new).collect(),
        provenance,
        // No predicate here: this node applies an already-committed sync
        // write. The writing identity is not available on this node.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning,
        rls_filters: rls_filters.to_vec(),
    }))
}

pub fn fts_index(
    collection: &str,
    surrogate: u32,
    text: &str,
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Text(TextOp::FtsIndexDoc {
        collection: collection.to_owned(),
        surrogate: Surrogate::new(surrogate),
        text: text.to_owned(),
        provenance,
    }))
}

pub fn fts_delete(
    collection: &str,
    surrogate: u32,
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Text(TextOp::FtsDeleteDoc {
        collection: collection.to_owned(),
        surrogate: Surrogate::new(surrogate),
        provenance,
    }))
}

pub fn spatial_insert(
    collection: &str,
    field: &str,
    surrogate: u32,
    geometry_bytes: &[u8],
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let geometry = zerompk::from_msgpack::<nodedb_types::geometry::Geometry>(geometry_bytes)
        .map_err(|e| crate::Error::Internal {
            detail: format!("SpatialInsert geometry decode failed: {e}"),
        })?;
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Spatial(SpatialOp::Insert {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate: Surrogate::new(surrogate),
        geometry,
        provenance,
    }))
}

pub fn spatial_delete(
    collection: &str,
    field: &str,
    surrogate: u32,
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Spatial(SpatialOp::Delete {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate: Surrogate::new(surrogate),
        provenance,
    }))
}
