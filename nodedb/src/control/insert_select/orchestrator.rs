// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for `INSERT ... SELECT`.
//!
//! `INSERT ... SELECT` copies rows from a source collection into a target
//! collection. Every target row must receive its OWN globally-unique surrogate
//! (never the source row's), registered in the catalog so cross-engine search
//! (vector / FTS) can resolve a hit back to the target row's identity. Because
//! surrogate registration is Control-Plane-only (WAL-durable, under the registry
//! lock) and the Data Plane never touches storage across planes, the copy runs
//! as a DP→CP→DP round trip driven from here:
//!
//! 1. **Scan** the source collection page-by-page via `DocumentOp::MaterializeScan`
//!    (a consistent redb read snapshot per page), reusing the same cursor
//!    primitive the clone materializer uses.
//! 2. **Assign** a fresh, registered surrogate for each surviving source row,
//!    keyed on the TARGET collection's primary key exactly as a plain `INSERT`
//!    would (`assign` for a declared PK, `assign_fresh` for an auto-`_rowid`
//!    target). The source surrogate is never inherited.
//! 3. **Write** each page as ONE atomic `DocumentOp::BatchInsert` carrying the
//!    pre-assigned surrogates, so the whole page lands or none of it does.
//!
//! ## Atomicity & visibility
//!
//! Each scan page is written as a single atomic `BatchInsert` (bounded by the
//! source scan page size). A constraint violation aborts that entire page,
//! leaving the target unchanged for it. Across pages the writes are separate
//! transactions, so a multi-page copy has the same partial-visibility semantics
//! `BatchInsert` already has — a later page's rows may commit while an earlier
//! reader is in flight.
//!
//! ## Scan↔write isolation
//!
//! The source scan (phase 1) and the target write (phase 3) are distinct ops
//! separated by the surrogate-assignment round trip, so concurrent writes to the
//! SOURCE can interleave between a page's scan and its write. Each page's scan is
//! a point-in-time redb snapshot, so a copied row is internally consistent, but
//! the statement is NOT globally serializable against concurrent source mutation
//! the way the old single-core-atomic op was. This is a deliberate, documented
//! relaxation, not a silent regression.

use nodedb_types::{DatabaseId, Lsn, Surrogate, TenantId};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::maintenance::clone_materializer::{dispatch_local, scan_source_page};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::DocumentOp;

/// Drive an `INSERT ... SELECT` from `source_collection` into `target_collection`.
///
/// `target_collection` / `source_collection` are the (db-qualified) collection
/// names as they appear in the `DocumentOp::InsertSelect` plan. `source_filters`
/// is the serialized `Vec<ScanFilter>` residual `WHERE` predicate; `source_limit`
/// bounds how many source rows are copied.
///
/// Returns a `{"inserted": N}` response mirroring the shape the autocommit
/// dispatch loops shape as an `INSERT` command tag.
pub async fn run_insert_select(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    source_collection: &str,
    source_filters: &[u8],
    source_limit: usize,
) -> crate::Result<Response> {
    let spec = resolve_copy_spec(
        state,
        tenant_id,
        database_id,
        target_collection,
        source_collection,
        source_filters,
    )?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut remaining = source_limit;
    let mut total_inserted: usize = 0;
    let mut max_lsn = Lsn::ZERO;

    while remaining > 0 {
        // Phase 1: scan one source page (point-in-time snapshot).
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            database_id,
            source_collection,
            &cursor,
            None,
        )
        .await?;

        // Phase 2: normalize (strict → msgpack), filter surviving rows, and
        // assign fresh target surrogates via the shared copy pipeline.
        let rows = assign_page_rows(
            state,
            tenant_id,
            database_id,
            target_collection,
            &spec,
            entries,
            &mut remaining,
        )?;

        // Phase 3: one atomic batch write for this page.
        if !rows.is_empty() {
            let page_len = rows.len();
            let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(page_len);
            let mut surrogates: Vec<Surrogate> = Vec::with_capacity(page_len);
            for (document_id, value, surrogate) in rows {
                documents.push((document_id, value));
                surrogates.push(surrogate);
            }
            let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: target_collection.to_string(),
                documents,
                surrogates,
            });
            let resp =
                dispatch_local(state, tenant_id, database_id, target_collection, plan).await?;
            if resp.status != Status::Ok {
                // Atomic page failure (e.g. constraint violation): the page's
                // rows did not land. Surface the DP error verbatim.
                return Ok(resp);
            }
            total_inserted += decode_inserted(&resp.payload).unwrap_or(page_len);
            if resp.watermark_lsn > max_lsn {
                max_lsn = resp.watermark_lsn;
            }
        }

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }

    let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": total_inserted }))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("insert-select response: {e}"),
        })?;

    Ok(Response {
        request_id: crate::types::RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload),
        watermark_lsn: max_lsn,
        error_code: None,
        read_set_valid: None,
        write_set: Vec::new(),
    })
}

/// Read the `"inserted"` count from a `BatchInsert` response payload.
fn decode_inserted(payload: &[u8]) -> Option<usize> {
    if payload.is_empty() {
        return None;
    }
    let json: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    json.get("inserted")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
}
