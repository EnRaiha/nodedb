// SPDX-License-Identifier: BUSL-1.1

//! Optimistic pre-execution scan for OLLP dependent-read transactions.
//!
//! Before submitting a `BulkUpdate` or `BulkDelete` via the Calvin
//! dependent-read path, the Control Plane runs this scan to collect the set of
//! document surrogates that currently match the predicate. That set is then
//! closed over inside the `tx_builder` closure passed to `dispatch_dependent_read`
//! and embedded as `ollp_predicted_surrogates` in the `BulkUpdate`/`BulkDelete`
//! plan. The active executor verifies the set at admission time and returns
//! `ErrorCode::OllpRetryRequired` on mismatch — without writing.
//!
//! # Determinism
//!
//! This function runs on the Control Plane (Tokio) and does not touch WAL
//! bytes. The returned surrogate list is sorted before returning so the
//! comparison in the executor is order-independent. No `SystemTime::now()`,
//! no unseeded RNG, no `HashMap` iteration order dependency.

use nodedb_types::TenantId;

use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// Dispatch a pre-execution scan for the given collection and serialized
/// filter bytes. Returns the sorted list of matching surrogate u32 values.
///
/// The scan is dispatched as a single-shard read to the vshard determined
/// routing used by the actual BulkUpdate/BulkDelete, so the comparison is
/// consistent.
///
/// Returns `Err` on dispatch failure (SPSC timeout, serialization error, etc.).
/// Returns `Ok(vec![])` if no documents match.
pub async fn run_preexec_scan(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: crate::types::DatabaseId,
    collection: &str,
    filter_bytes: Vec<u8>,
) -> crate::Result<Vec<u32>> {
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);

    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: collection.to_owned(),
        filters: filter_bytes,
        limit: usize::MAX,
        offset: 0,
        sort_keys: vec![],
        distinct: false,
        // Empty projection — preexec only needs the doc_id (hex surrogate),
        // which is always included as the row's `id` field regardless of
        // projection. Requesting all fields avoids a special-cased variant.
        projection: vec![],
        computed_columns: vec![],
        window_functions: vec![],
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });

    let response =
        dispatch_to_data_plane(shared, tenant_id, vshard_id, scan_plan, TraceId::ZERO).await?;

    if response.status != crate::bridge::envelope::Status::Ok {
        return Err(crate::Error::Storage {
            engine: "preexec-scan".into(),
            detail: format!("pre-execution scan failed: {:?}", response.error_code),
        });
    }

    let surrogates = decode_scan_surrogates(&response.payload);
    Ok(surrogates)
}

/// Decode the msgpack scan response payload into a sorted list of surrogate u32 values.
///
/// Each row in the response is a msgpack map with an `id` field whose value is
/// an 8-character lowercase hex string encoding the document's u32 surrogate
/// (e.g. `"0000002a"` → `42u32`). Rows whose `id` cannot be parsed are silently
/// skipped — they are legacy non-surrogate documents that predate the surrogate-
/// keyed storage format and do not participate in OLLP verification.
///
/// The output is sorted ascending so the comparison with `ollp_predicted_surrogates`
/// in the executor is a simple equality check on sorted slices.
fn decode_scan_surrogates(payload: &[u8]) -> Vec<u32> {
    if payload.is_empty() {
        return vec![];
    }

    let mut surrogates = Vec::new();

    // Transcode msgpack → JSON string and parse the id fields.
    // This avoids introducing a zerompk-level partial decode dependency
    // into the Control Plane layer: we let the existing transcoder convert
    // the payload to a JSON array, then pull the `id` fields.
    let json_str = nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned());

    if let Ok(rows) = sonic_rs::from_str::<sonic_rs::Value>(&json_str) {
        use sonic_rs::{JsonContainerTrait, JsonValueTrait};
        if rows.is_array() {
            for row in rows.as_array().into_iter().flatten() {
                if let Some(id_val) = row.get("id")
                    && let Some(id_str) = id_val.as_str()
                    && id_str.len() == 8
                    && let Ok(surrogate) = u32::from_str_radix(id_str, 16)
                {
                    surrogates.push(surrogate);
                }
            }
        }
    }

    surrogates.sort_unstable();
    surrogates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_empty_payload_returns_empty() {
        let result = decode_scan_surrogates(&[]);
        assert!(result.is_empty());
    }

    // Format-coupled coverage of decode_scan_surrogates lives in
    // `tests/executor_tests/test_ollp_verification.rs`, which exercises the
    // decoder against real scan-response payloads emitted by the Data Plane.
    // Keeping a hand-rolled msgpack mock in this unit test would duplicate
    // the wire format and break on every response_codec change.
}
