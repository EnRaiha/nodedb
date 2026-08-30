// SPDX-License-Identifier: BUSL-1.1

//! Error type and small shared aliases for the array store catalog.

use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::types::coord::value::CoordValue;

use super::super::manifest::ManifestError;
use super::super::segment_handle::SegmentHandleError;

/// One materialized cell version returned by an all-versions scan:
/// `(hilbert_prefix, coord, system_from_ms, payload)`.
pub type CellVersion = (u64, Vec<CoordValue>, i64, CellPayload);

#[derive(Debug, thiserror::Error)]
pub enum ArrayStoreError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Segment(#[from] SegmentHandleError),
    #[error("array store io: {detail}")]
    Io { detail: String },
    #[error("schema_hash mismatch: store={store:x} new={new:x}")]
    SchemaHashMismatch { store: u64, new: u64 },
}
