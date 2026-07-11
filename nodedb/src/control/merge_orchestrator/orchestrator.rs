// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for autocommit `MERGE`.
//!
//! `MERGE ... WHEN NOT MATCHED THEN INSERT` inserts brand-new rows into the
//! target. Every such row must receive its OWN globally-unique surrogate,
//! registered in the catalog so cross-engine search (vector / FTS / spatial)
//! can resolve a hit back to the target row's identity. Surrogate registration
//! is Control-Plane-only (WAL-durable, under the registry lock) and the Data
//! Plane never touches the catalog, so autocommit MERGE runs as a
//! Control-Plane-driven, TOCTOU-safe, atomic round trip:
//!
//! 0. **Source-ship**: the source collection's vShard can map to a DIFFERENT
//!    Data-Plane core than the target's, so the resolve/apply dispatches (which
//!    target the target core) cannot read the source from local storage. The
//!    Control Plane scans the source on its OWN core via the shared
//!    `MaterializeScan` primitive and ships the RAW stored rows into the plan's
//!    `source_rows`; the Data Plane builds the join-map from these instead of a
//!    local read. This is what makes cross-core MERGE correct.
//! 1. **Resolve** (`DocumentOp::Merge { resolve_only: true }`): the Data Plane
//!    classifies the merge against a point-in-time snapshot and returns the
//!    NOT-MATCHED insert rows as `Vec<(join_key, body)>` WITHOUT writing.
//! 2. **Assign**: for each insert row, allocate a fresh, registered surrogate
//!    keyed on the target collection's primary key exactly as a plain `INSERT`
//!    would (`assign` for a declared PK, `assign_fresh` for an auto-`_rowid`
//!    target). The source surrogate is never inherited.
//! 3. **Apply** (`DocumentOp::Merge { resolved_inserts: Some(..) }`): the Data
//!    Plane re-derives the classification, VERIFIES the recomputed insert-key
//!    set still equals the assigned keys — returning `OllpRetryRequired`
//!    WITHOUT writing on drift — and applies every arm's writes with the
//!    pre-assigned surrogates. The matched UPDATE and NOT-MATCHED INSERT arms
//!    share one redb transaction (all-or-nothing).
//!
//! ## TOCTOU
//!
//! The resolve (phase 1) and apply (phase 3) are distinct snapshots separated
//! by the surrogate-assignment round trip. A concurrent write to source/target
//! between them is caught by the apply-time verification, which returns
//! `ErrorCode::OllpRetryRequired`; this loop then re-resolves (fresh phase 1)
//! and retries — the same predict-verify-retry contract the OLLP dependent-read
//! path uses. Retries are bounded; exhaustion surfaces `OllpExhausted`.

use nodedb_types::columnar::DocumentMode;
use nodedb_types::{CollectionType, DatabaseId, Surrogate, TenantId, Value};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, scan_source_page};
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;

/// Upper bound on resolve→apply retries under concurrent source/target drift.
/// Mirrors the OLLP dependent-read retry ceiling: a merge whose matched /
/// not-matched classification keeps changing every attempt is surfaced as
/// `OllpExhausted` rather than looping forever.
const MAX_MERGE_RETRIES: u32 = 10;

/// How the target collection's primary key drives surrogate assignment for a
/// merge-inserted row (mirrors the plain-`INSERT` identity path).
enum TargetPk {
    /// Auto-generated `_rowid` (no declared PK): every inserted row gets a
    /// fresh, distinct surrogate.
    AutoRowId,
    /// A declared / built-in primary-key field: the fresh surrogate is
    /// content-addressed on this field's value so a later point-get /
    /// cross-engine resolve lands on the same identity.
    Field(String),
}

/// Bundled arguments for [`run_merge`], mirroring the fields of the intercepted
/// `DocumentOp::Merge` plan.
pub struct MergeArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
}

/// Drive an autocommit `MERGE` from the Control Plane.
///
/// Returns a `{"affected": N}` response mirroring the shape the Data Plane
/// merge handler produces, so the dispatch loops render the same command tag.
pub async fn run_merge(state: &SharedState, args: MergeArgs<'_>) -> crate::Result<Response> {
    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(args.database_id, args.target_collection);
    let target = catalog
        .get_collection(args.database_id, args.tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: args.tenant_id,
            collection: args.target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target)?;

    let mut attempt: u32 = 0;
    loop {
        // Phase 0: read the SOURCE where it lives. The source collection's
        // vShard can map to a DIFFERENT Data-Plane core than the target's, so
        // the resolve/apply dispatches (which target the target core) cannot
        // read it from local storage. Scan it on its OWN core via the shared
        // source-scan primitive (which routes by the source collection's
        // vShard) and ship the RAW stored rows into the plan. A fresh read per
        // attempt keeps each attempt's resolve and apply on one consistent
        // source snapshot; a retry picks up concurrent source mutation.
        let source_rows = read_source_rows(
            state,
            args.tenant_id,
            args.database_id,
            args.source_collection,
        )
        .await?;

        // Phase 1: resolve the NOT-MATCHED insert rows (read-only snapshot).
        let resolve_plan = merge_plan(&args, true, None, Some(source_rows.clone()));
        let resolve_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            resolve_plan,
        )
        .await?;
        if resolve_resp.status != Status::Ok {
            return Ok(resolve_resp);
        }
        let insert_rows = decode_resolve(&resolve_resp.payload)?;

        // Phase 2: assign a fresh, registered surrogate per inserted row.
        let mut resolved: Vec<(String, u32)> = Vec::with_capacity(insert_rows.len());
        for (join_key, body) in &insert_rows {
            let surrogate = assign_target_surrogate(
                state,
                args.database_id,
                args.tenant_id,
                args.target_collection,
                &target_pk,
                body,
            )?;
            resolved.push((join_key.clone(), surrogate.as_u32()));
        }

        // Phase 3: atomic apply with the pre-assigned surrogates + drift verify.
        // The apply reuses THIS attempt's source snapshot so the DP re-derives
        // the classification from the same source the resolve saw.
        let apply_plan = merge_plan(&args, false, Some(resolved), Some(source_rows));
        let apply_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            apply_plan,
        )
        .await?;

        if apply_resp.error_code == Some(ErrorCode::OllpRetryRequired) {
            attempt += 1;
            if attempt > MAX_MERGE_RETRIES {
                return Err(crate::Error::OllpExhausted {
                    retries: MAX_MERGE_RETRIES.min(u8::MAX as u32) as u8,
                });
            }
            // Concurrent drift: re-resolve (fresh phase 1) and retry. The
            // surrogates assigned this round are simply unused (harmless —
            // the counter is monotonic and gap-tolerant).
            continue;
        }

        return Ok(apply_resp);
    }
}

/// Build a `DocumentOp::Merge` physical plan for one orchestrator pass.
///
/// `source_rows` carries the RAW stored source rows scanned on the source's own
/// core (phase 0) so the Data Plane builds the join-map from the shipped bytes
/// rather than reading the source from the target core's local store.
fn merge_plan(
    args: &MergeArgs<'_>,
    resolve_only: bool,
    resolved_inserts: Option<Vec<(String, u32)>>,
    source_rows: Option<Vec<(String, Vec<u8>)>>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: args.target_collection.to_string(),
        source_collection: args.source_collection.to_string(),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        clauses: args.clauses.to_vec(),
        returning: None,
        resolve_only,
        resolved_inserts,
        source_rows,
    })
}

/// Scan the SOURCE collection to completion on its OWN Data-Plane core and
/// collect every row as `(source_doc_id, raw_stored_bytes)`.
///
/// Uses the same cursor-paginated `MaterializeScan` primitive the clone
/// materializer and `INSERT ... SELECT` use; `scan_source_page` routes by the
/// source collection's vShard, so the read lands on whichever core owns the
/// source — the whole point of source-shipping. The raw stored bytes (a Binary
/// Tuple for a strict source, MessagePack for a schemaless source) are shipped
/// unchanged; the Data Plane decodes them with the source's strict schema
/// (present on every core because `Register` is broadcast). `MERGE` has no
/// source `WHERE`/`LIMIT`, so the full source is joined.
async fn read_source_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    source_collection: &str,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let mut cursor: Vec<u8> = Vec::new();
    let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
    loop {
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            database_id,
            source_collection,
            &cursor,
            None,
        )
        .await?;
        for (doc_id, _source_surrogate, value) in entries {
            rows.push((doc_id, value));
        }
        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }
    Ok(rows)
}

/// Decode the RESOLVE pass payload into `(join_key, body_msgpack)` insert rows.
fn decode_resolve(payload: &[u8]) -> crate::Result<Vec<(String, Vec<u8>)>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("merge resolve rows: {e}"),
    })
}

/// Assign a fresh, registered surrogate for one merge-inserted row on the
/// TARGET's primary key.
fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    body: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection)
        }
        TargetPk::Field(field) => match extract_pk_value(body, field) {
            Some(pk) if !pk.is_empty() => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value: mint a fresh unique surrogate rather than
            // collapsing every keyless inserted row onto one binding.
            _ => state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection),
        },
    }
}

/// Resolve how the target collection's primary key maps an inserted row to a
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
                "MERGE target '{}' must be a document collection",
                target.name
            ),
        }),
    }
}

/// Extract a stringified primary-key value from a MessagePack insert body.
fn extract_pk_value(body: &[u8], field: &str) -> Option<String> {
    let Value::Object(obj) = nodedb_types::value_from_msgpack(body).ok()? else {
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
/// recover the bare name the catalog keys collections by.
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
