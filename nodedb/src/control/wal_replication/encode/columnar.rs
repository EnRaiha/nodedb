// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial`
//! sync-engine variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_types::Surrogate;

pub(super) fn columnar_ingest(
    collection: &str,
    payload: &[u8],
    surrogates: &[Surrogate],
    schema_bytes: &[u8],
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarIngest {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        schema_bytes: schema_bytes.to_vec(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        provenance,
    }
}

pub(super) fn timeseries_ingest(
    collection: &str,
    payload: &[u8],
    format: &str,
    surrogates: &[Surrogate],
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::TimeseriesIngest {
        collection: collection.to_owned(),
        payload: payload.to_vec(),
        format: format.to_owned(),
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        provenance,
    }
}

pub(super) fn fts_index(
    collection: &str,
    surrogate: u32,
    text: &str,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::FtsIndex {
        collection: collection.to_owned(),
        surrogate,
        text: text.to_owned(),
        provenance,
    }
}

pub(super) fn fts_delete(
    collection: &str,
    surrogate: u32,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::FtsDelete {
        collection: collection.to_owned(),
        surrogate,
        provenance,
    }
}

pub(super) fn spatial_insert(
    collection: &str,
    field: &str,
    surrogate: u32,
    geometry: &nodedb_types::geometry::Geometry,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::SpatialInsert {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate,
        // Geometry is plain serializable data — encoding is infallible (same
        // contract as `ReplicatedEntry::to_bytes`). Fail loud rather than
        // replicate empty bytes that would error on follower decode.
        geometry_bytes: zerompk::to_msgpack_vec(geometry)
            .expect("Geometry serialization is infallible"),
        provenance,
    }
}

pub(super) fn spatial_delete(
    collection: &str,
    field: &str,
    surrogate: u32,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::SpatialDelete {
        collection: collection.to_owned(),
        field: field.to_owned(),
        surrogate,
        provenance,
    }
}

/// Columnar predicate DELETE / UPDATE replicates as a `ColumnarBulkDml`
/// entry: each replica re-scans local columnar state at the committed log
/// position and applies the predicate deterministically (Raft log order ⇒
/// identical prior state ⇒ identical matching set), exactly like the
/// Document `BulkDml` sibling.
pub(super) fn bulk_delete(collection: &str, filters: &[u8]) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: false,
        updates: Vec::new(),
    }
}

pub(super) fn bulk_update(
    collection: &str,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
) -> ReplicatedWrite {
    ReplicatedWrite::ColumnarBulkDml {
        collection: collection.to_owned(),
        filters: filters.to_vec(),
        is_update: true,
        updates: updates.to_vec(),
    }
}
