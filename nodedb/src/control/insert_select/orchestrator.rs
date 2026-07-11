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

use nodedb_types::columnar::DocumentMode;
use nodedb_types::{CollectionType, DatabaseId, Lsn, Surrogate, TenantId, Value};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::bridge::scan_filter::ScanFilter;
use crate::control::maintenance::clone_materializer::{dispatch_local, scan_source_page};
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::DocumentOp;

/// How the target collection's primary key drives surrogate assignment.
enum TargetPk {
    /// Auto-generated `_rowid` (no declared PK): every copied row gets a fresh,
    /// distinct surrogate — content-addressing an absent key would collapse all
    /// rows onto one surrogate.
    AutoRowId,
    /// A declared / built-in primary-key field: the fresh surrogate is
    /// content-addressed on this field's value, matching a plain `INSERT` so a
    /// later point-get / cross-engine resolve lands on the same identity.
    Field(String),
}

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
    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(database_id, target_collection);
    let target = catalog
        .get_collection(database_id, tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target)?;

    let filters: Vec<ScanFilter> = if source_filters.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(source_filters).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("insert-select source filters: {e}"),
        })?
    };

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

        // Phase 2: filter surviving rows and assign fresh target surrogates.
        let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(entries.len());
        let mut surrogates: Vec<Surrogate> = Vec::with_capacity(entries.len());
        for (_source_doc_id, _source_surrogate, value) in entries {
            if remaining == 0 {
                break;
            }
            if !filters.is_empty() && !filters.iter().all(|f| f.matches_binary(&value)) {
                continue;
            }
            let surrogate = assign_target_surrogate(
                state,
                database_id,
                tenant_id,
                target_collection,
                &target_pk,
                &value,
            )?;
            documents.push((surrogate_to_doc_id(surrogate), value));
            surrogates.push(surrogate);
            remaining -= 1;
        }

        // Phase 3: one atomic batch write for this page.
        if !documents.is_empty() {
            let page_len = documents.len();
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
    })
}

/// Assign a fresh, registered surrogate for one copied row on the TARGET's PK.
fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    value: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection)
        }
        TargetPk::Field(field) => match extract_pk_value(value, field) {
            Some(pk) if !pk.is_empty() => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value: mint a fresh unique surrogate rather than
            // collapsing every keyless row onto one binding.
            _ => state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection),
        },
    }
}

/// Resolve how the target collection's primary key maps a copied row to a
/// surrogate, mirroring the plain-`INSERT` identity path.
fn resolve_target_pk(target: &StoredCollection) -> crate::Result<TargetPk> {
    match &target.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            match schema.columns.iter().find(|c| c.primary_key) {
                Some(col) if col.name == "_rowid" => Ok(TargetPk::AutoRowId),
                Some(col) => Ok(TargetPk::Field(col.name.clone())),
                None => Ok(TargetPk::AutoRowId),
            }
        }
        CollectionType::Document(DocumentMode::Schemaless) => Ok(TargetPk::Field(
            target
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string()),
        )),
        CollectionType::KeyValue(_) | CollectionType::Columnar(_) => Err(crate::Error::PlanError {
            detail: format!(
                "INSERT ... SELECT target '{}' must be a document collection",
                target.name
            ),
        }),
    }
}

/// Extract a stringified primary-key value from a stored MessagePack document.
fn extract_pk_value(value: &[u8], field: &str) -> Option<String> {
    let Value::Object(obj) = nodedb_types::value_from_msgpack(value).ok()? else {
        return None;
    };
    value_to_pk_string(obj.get(field)?)
}

/// Stringify a scalar value into its primary-key byte form (mirrors the
/// `sql_value_to_string` convention used by the plain-INSERT identity path).
fn value_to_pk_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}

/// Strip the `{database_id}/` qualifier from a db-qualified collection name to
/// recover the bare name the catalog keys collections by (the DEFAULT database
/// uses the bare name unqualified).
fn bare_collection_name(database_id: DatabaseId, qualified: &str) -> String {
    if database_id == DatabaseId::DEFAULT {
        return qualified.to_string();
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified
        .strip_prefix(&prefix)
        .unwrap_or(qualified)
        .to_string()
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
