// SPDX-License-Identifier: BUSL-1.1

//! The reconnaissance scan behind the predicate-driven materialized-sum
//! resolution.
//!
//! A `BulkUpdate` / `BulkDelete` / `TRUNCATE` names its rows by PREDICATE, not
//! by body: at plan time the Control Plane holds no row to read a join key off.
//! It reads them the same way the OLLP dependent-predicate path predicts its
//! write set — one scan of the same predicate, before execution — and resolves
//! the join values that scan surfaces.
//!
//! Like the OLLP pre-execution scan, the read is routed through the gateway when
//! one is wired: a bare local dispatch on a coordinator that does not host the
//! collection's vShard returns nothing, which would silently under-resolve and
//! leave the write with no target to address.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The scan crosses the SPSC
//! bridge (or the gateway) exactly as a `SELECT` does — no storage I/O and no
//! io_uring here.

use nodedb_types::TenantId;

use crate::control::server::dispatch_utils::dispatch_to_data_plane;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// Scan `collection` for the rows `filters` matches, returning each row's full
/// decoded document.
///
/// Whole documents rather than a projection: the join column of every binding
/// the collection drives has to be readable, and so does every column an
/// expression assignment to a join column evaluates over. A projection would
/// have to enumerate all of them and would silently drop a value the assignment
/// depends on.
///
/// Empty `filters` means "no WHERE clause" — every row, which is what `TRUNCATE`
/// needs.
pub(super) async fn recon_scan_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    filters: Vec<u8>,
) -> crate::Result<Vec<serde_json::Value>> {
    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: collection.to_owned(),
        filters,
        limit: usize::MAX,
        offset: 0,
        sort_keys: vec![],
        distinct: false,
        projection: vec![],
        computed_columns: vec![],
        window_functions: vec![],
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });

    if let Some(gateway) = state.gateway.get() {
        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: TraceId::ZERO,
            database_id,
            txn_id: None,
        };
        let payloads = gateway
            .execute_internal(&gw_ctx, scan_plan)
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "materialized-sum-recon".into(),
                detail: format!("reconnaissance scan failed: {e}"),
            })?;
        let mut rows = Vec::new();
        for payload in payloads {
            rows.extend(decode_rows(&payload));
        }
        return Ok(rows);
    }

    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let response = dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard_id,
        scan_plan,
        TraceId::ZERO,
    )
    .await?;
    if response.status != crate::bridge::envelope::Status::Ok {
        return Err(crate::Error::Storage {
            engine: "materialized-sum-recon".into(),
            detail: format!("reconnaissance scan failed: {:?}", response.error_code),
        });
    }
    Ok(decode_rows(&response.payload))
}

/// Decode a document-scan payload into one document per row.
///
/// `decode_raw_scan_to_docs` is the shared reader for BOTH shapes a document
/// scan can come back in — the `{id, data}` raw-passthrough wrapper and the
/// plain per-row map — so the shape is not re-guessed here. A row body that will
/// not decode carries no readable column, so it contributes no join value; it is
/// left to the write path, which fails on the same body rather than silently
/// mis-accounting it.
fn decode_rows(payload: &[u8]) -> Vec<serde_json::Value> {
    crate::data::executor::response_codec::decode_raw_scan_to_docs(payload)
        .into_iter()
        .filter_map(|(_, body)| nodedb_types::json_from_msgpack(&body).ok())
        .collect()
}
