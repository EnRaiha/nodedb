// SPDX-License-Identifier: BUSL-1.1

//! Document engine source-to-target row copy.
//!
//! Drives the source `MaterializeScan` cursor to completion; for each
//! non-tombstoned, not-yet-copied surrogate, dispatches a fresh-surrogate
//! `PointInsert { if_absent: true }` against target. Calls the reaper at the
//! end to flip status to `Materialized` and clear `cloned_from`.
//!
//! Crash-restart-safe: tombstones filter deleted source rows, `clone_copyups`
//! filters CoW-copied rows, and `if_absent` skips already-written rows — the
//! per-page checkpoint is best-effort but the per-key probes cover it.

use nodedb_types::{CloneStatus, DatabaseId, Lsn, Surrogate, TenantId};

use crate::types::TxnId;

use super::dispatch::dispatch_local;
use super::reaper::{ReapParams, reap_materialized_collection};
use crate::bridge::envelope::Status;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// Rows fetched per scan round-trip. Matches the KV page size.
const SCAN_PAGE: usize = 4_096;

/// Materialize one Document clone collection.
pub(super) async fn materialize_document_collection(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let Some(ref origin) = coll.cloned_from else {
        return Ok(());
    };

    let target_qualified = db_qualified(db_id, &coll.name);
    let source_qualified = db_qualified(origin.source_database, &origin.source_collection);
    let tenant_id = TenantId::new(coll.tenant_id);

    // Flip status to `Materializing` if still `Shadowed` so concurrent
    // readers see in-progress state.
    if matches!(coll.clone_status, CloneStatus::Shadowed) {
        let mut updated = coll.clone();
        updated.clone_status = CloneStatus::Materializing {
            progress_lsn: Lsn::new(0),
            bytes_done: 0,
            bytes_total: 0,
        };
        let outcome = propose_catalog_entry(
            state,
            &CatalogEntry::PutCollection(Box::new(updated.clone())),
        )?;
        if outcome.needs_local_apply() {
            catalog.put_collection(db_id, &updated)?;
        }
    }

    // Tombstones: source surrogates deleted from the clone before materialization.
    let tombstoned = catalog.list_clone_tombstones(&target_qualified)?;

    // Convert as_of_lsn to milliseconds for the source-side scan.
    let system_as_of_ms = state.ms_to_lsn_inverse(origin.as_of_lsn);

    let mut cursor: Vec<u8> = Vec::new();
    let mut copied: u64 = 0;
    let mut total_seen: u64 = 0;

    loop {
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            origin.source_database,
            &source_qualified,
            &cursor,
            system_as_of_ms,
            None,
        )
        .await?;

        for (doc_id_hex, source_surrogate_u32, value_bytes) in entries {
            total_seen += 1;

            let source_surrogate = Surrogate::new(source_surrogate_u32);

            // Skip rows deleted from the clone (CoW tombstone).
            if tombstoned.contains(&source_surrogate_u32) {
                continue;
            }

            // Skip rows already copy-up'd into target by the CoW write path.
            if catalog
                .get_clone_copyup(&target_qualified, source_surrogate_u32)?
                .is_some()
            {
                continue;
            }

            // Recover the user-visible PK bytes from the catalog so the
            // surrogate assigner produces the same surrogate the write path
            // would allocate for this (collection, pk) pair.
            let pk_bytes = catalog
                .get_pk_for_surrogate(
                    origin.source_database,
                    tenant_id,
                    &origin.source_collection,
                    source_surrogate,
                )
                .map_err(|e| crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "get_pk_for_surrogate failed for surrogate {source_surrogate_u32} \
                         in '{source_qualified}': {e}"
                    ),
                })?
                .unwrap_or_else(|| {
                    // No PK binding (e.g. very old row): fall back to the hex doc_id
                    // as key bytes — deterministic but may differ from the write path.
                    doc_id_hex.as_bytes().to_vec()
                });

            let document_id = String::from_utf8_lossy(&pk_bytes).into_owned();

            // Allocate target surrogate using the same (collection, pk_bytes) key
            // the normal INSERT path would use.
            let target_surrogate = state
                .surrogate_assigner
                .assign(db_id, tenant_id, &target_qualified, &pk_bytes)
                .map_err(|e| crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "surrogate assign failed for doc '{doc_id_hex}' in \
                         '{target_qualified}': {e}"
                    ),
                })?;

            let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(db_id, &coll.name),
                document_id: document_id.clone(),
                value: value_bytes,
                if_absent: true,
                surrogate: target_surrogate,
                // A materializer copy answers no client, so it projects
                // nothing and needs no read gate.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            });

            let resp =
                dispatch_local(state, tenant_id, db_id, &target_qualified, plan, None).await?;
            if resp.status != Status::Ok {
                return Err(crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "document insert on target '{target_qualified}' for doc \
                         '{document_id}' returned status {:?}",
                        resp.status
                    ),
                });
            }
            copied += 1;
        }

        // Per-page progress checkpoint.
        checkpoint_progress(
            state,
            catalog,
            db_id,
            coll,
            origin.as_of_lsn,
            copied,
            total_seen,
        )?;

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }

    tracing::info!(
        db_id = db_id.as_u64(),
        collection = %coll.name,
        copied,
        skipped_tombstoned = tombstoned.len(),
        source_total = total_seen,
        "document materialize: source rows copied to target",
    );

    reap_materialized_collection(ReapParams {
        target_collection_qualified: &target_qualified,
        db_id,
        tenant_id: coll.tenant_id,
        name: &coll.name,
        state,
        catalog,
    })?;

    Ok(())
}

/// Persist a `Materializing { progress_lsn, .. }` checkpoint between scan pages.
fn checkpoint_progress(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
    as_of_lsn: Lsn,
    copied: u64,
    total_seen: u64,
) -> crate::Result<()> {
    let mut updated = coll.clone();
    updated.clone_status = CloneStatus::Materializing {
        progress_lsn: as_of_lsn,
        bytes_done: copied,
        bytes_total: total_seen,
    };
    let outcome = propose_catalog_entry(
        state,
        &CatalogEntry::PutCollection(Box::new(updated.clone())),
    )?;
    if outcome.needs_local_apply() {
        catalog.put_collection(db_id, &updated)?;
    }
    Ok(())
}

/// `(doc_id_hex, source_surrogate_u32, value_bytes)` returned by one scan page.
pub(crate) type ScanPage = (Vec<(String, u32, Vec<u8>)>, Vec<u8>);

/// Run one source-side `MaterializeScan` round-trip.
///
/// `txn_id`, when set (COMMIT-time MERGE / `UPDATE ... FROM` expanders),
/// makes the source handler fold the transaction's staging overlay in.
/// Autocommit callers pass `None` (base-only).
pub(crate) async fn scan_source_page(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
    system_as_of_ms: Option<i64>,
    txn_id: Option<TxnId>,
) -> crate::Result<ScanPage> {
    let plan = PhysicalPlan::Document(DocumentOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        system_as_of_ms,
    });
    let resp = dispatch_local(
        state,
        tenant_id,
        source_db_id,
        source_qualified,
        plan,
        txn_id,
    )
    .await?;
    if resp.status != Status::Ok {
        return Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "document materialize-scan on source '{source_qualified}' returned status {:?}",
                resp.status
            ),
        });
    }
    parse_materialize_scan_payload(resp.payload.as_ref())
}

/// Scan a source collection to completion on its OWN Data-Plane core,
/// collecting every row as `(source_doc_id, msgpack_body)`. Shared by the
/// `MERGE` / `UPDATE ... FROM` orchestrators, which ship source rows into
/// their plan so the Data Plane builds the join-map without a local read of
/// a possibly-non-resident source.
///
/// `txn_id`: `None` scans committed base storage only; `Some(txn)` (staged
/// expanders) folds the transaction's SOURCE-side staging overlay in too.
pub(crate) async fn read_all_source_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    source_collection: &str,
    txn_id: Option<TxnId>,
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
            txn_id,
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

/// Parse the msgpack payload emitted by `execute_document_materialize_scan`:
///   `[next_cursor: bin, entries: [[doc_id: str, surrogate: u32, value_bytes: bin], ...]]`.
fn parse_materialize_scan_payload(payload: &[u8]) -> crate::Result<ScanPage> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let bad = || crate::Error::Serialization {
        format: "msgpack".into(),
        detail: "document materialize-scan response: malformed payload".into(),
    };

    let (outer_len, mut off) = msgpack_scan::array_header(payload, 0).ok_or_else(bad)?;
    if outer_len != 2 {
        return Err(bad());
    }

    let next_cursor = msgpack_scan::read_bin_advance(payload, &mut off)
        .ok_or_else(bad)?
        .to_vec();

    let (entry_count, mut entry_off) = msgpack_scan::array_header(payload, off).ok_or_else(bad)?;

    // `entry_count` is attacker-controlled msgpack metadata. Grow only after
    // each complete entry has been structurally consumed from `payload`.
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        let (triple_len, mut triple_off) =
            msgpack_scan::array_header(payload, entry_off).ok_or_else(bad)?;
        if triple_len != 3 {
            return Err(bad());
        }
        let doc_id = msgpack_scan::read_str_advance(payload, &mut triple_off)
            .ok_or_else(bad)?
            .to_string();
        let surrogate = msgpack_scan::read_u32_advance(payload, &mut triple_off).ok_or_else(bad)?;
        let value = msgpack_scan::read_bin_advance(payload, &mut triple_off)
            .ok_or_else(bad)?
            .to_vec();
        entries.push((doc_id, surrogate, value));
        entry_off = triple_off;
    }

    Ok((entries, next_cursor))
}
