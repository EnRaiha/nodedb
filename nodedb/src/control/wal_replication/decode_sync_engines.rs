// SPDX-License-Identifier: BUSL-1.1

//! Decode helpers for sync-engine `ReplicatedWrite` variants.
//!
//! Each function maps the destructured fields of one `ReplicatedWrite` variant
//! back to a `PhysicalPlan`, using the leader-assigned surrogates verbatim
//! rather than re-deriving identity through the local assigner. `wal_lsn` is
//! always `None` — followers allocate their own WAL LSN at apply time.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, SpatialOp, TextOp, TimeseriesOp,
};
use nodedb_types::Surrogate;

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

pub fn columnar_ingest(
    collection: &str,
    payload: &[u8],
    schema_bytes: &[u8],
    surrogates: &[u32],
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        format: "msgpack".to_owned(),
        // The sync path always uses a plain insert with no conflict resolution.
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: surrogates.iter().copied().map(Surrogate::new).collect(),
        schema_bytes: schema_bytes.to_vec(),
        provenance,
        wal_lsn: None,
        rls_write_check: Vec::new(),
        // A replicated entry reconstructs stored rows; the response shape a
        // projection would produce belongs to the originating request only.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub fn timeseries_ingest(
    collection: &str,
    payload: &[u8],
    format: &str,
    surrogates: &[u32],
    prov_bytes: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_provenance(prov_bytes)?;
    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        format: format.to_owned(),
        wal_lsn: None,
        surrogates: surrogates.iter().copied().map(Surrogate::new).collect(),
        provenance,
        rls_write_check: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
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
