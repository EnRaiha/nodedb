// SPDX-License-Identifier: BUSL-1.1

//! Scan-filter predicates, shared by the Control Plane and the Data Plane.
//!
//! The types come from `nodedb-query`, which Lite shares. Decoding needs the
//! Origin error type, so it lives here.

pub use nodedb_query::scan_filter::*;

/// Decode one MessagePack `Vec<ScanFilter>` set.
///
/// Empty bytes carry no predicate. `context` names the set, so a decode error
/// says which one failed.
pub fn decode_scan_filters(bytes: &[u8], context: &str) -> crate::Result<Vec<ScanFilter>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::PlanError {
        detail: format!("{context} deserialization failed: {e}"),
    })
}
