// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine (Plain / Timeseries / Spatial) source-to-target row copy.
//!
//! Drives the source `ColumnarOp::MaterializeScan` cursor to completion, and
//! for each non-tombstoned, not-yet-copied row dispatches a
//! `ColumnarOp::Insert { intent: InsertIfAbsent }` against target with a
//! fresh surrogate. Calls the reaper at the end to flip the collection status
//! to `Materialized` and clear `cloned_from`.
//!
//! ## Idempotency / restart-safety
//!
//! Resume after a crash works because every step is observable:
//!   - Tombstones in `clone_tombstones` filter deleted source rows (keyed on
//!     the synthetic surrogate produced by the Data Plane scan).
//!   - Copy-up entries in `clone_copyups` filter rows already written by the
//!     CoW write path.
//!   - `InsertIfAbsent` intent silently skips rows already written by a prior
//!     materializer pass.
//!
//! ## Profile coverage
//!
//! Plain columnar, Timeseries, and Spatial all share the same `MutationEngine`
//! storage layer. A single `ColumnarOp::MaterializeScan` handler (and this
//! Control Plane loop) serves all three profiles.

use nodedb_types::{CloneStatus, DatabaseId, Lsn, TenantId};

use super::dispatch::dispatch_local;
use super::reaper::{ReapParams, reap_materialized_collection};
use crate::bridge::envelope::Status;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::document::UpdateValue;
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, PhysicalPlan, TimeseriesOp,
};

/// Rows fetched per scan round-trip. Matches the KV / Document page size.
const SCAN_PAGE: usize = 4_096;

/// Materialize one columnar clone collection (Plain / Timeseries / Spatial).
pub(super) async fn materialize_columnar_collection(
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

    // Flip status to `Materializing` if still `Shadowed`.
    if matches!(coll.clone_status, CloneStatus::Shadowed) {
        let mut updated = coll.clone();
        updated.clone_status = CloneStatus::Materializing {
            progress_lsn: Lsn::new(0),
            bytes_done: 0,
            bytes_total: 0,
        };
        let proposed = propose_catalog_entry(
            state,
            &CatalogEntry::PutCollection(Box::new(updated.clone())),
        )?;
        if proposed == 0 {
            catalog.put_collection(db_id, &updated)?;
        }
    }

    // Tombstones: synthetic source surrogates deleted from the clone before
    // materialization. The Data Plane scan encodes a unique u32 per row as the
    // surrogate (segment_id in upper 16 bits, row_idx in lower 16 bits).
    let tombstoned = catalog.list_clone_tombstones(&target_qualified)?;

    // Convert as_of_lsn to milliseconds for the source-side scan.
    let system_as_of_ms = state.ms_to_lsn_inverse(origin.as_of_lsn);

    // Detect target engine profile so INSERT dispatches to the right handler.
    // Timeseries collections use `TimeseriesOp::Ingest` (msgpack array format)
    // so rows land in `columnar_memtables` — not `columnar_engines` (plain
    // columnar). Plain / Spatial use `ColumnarOp::Insert`.
    let target_is_timeseries = coll.collection_type.is_timeseries();

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
        )
        .await?;

        for (source_surrogate_u32, value_bytes) in entries {
            total_seen += 1;

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

            // Allocate target surrogate using a synthetic key derived from the
            // source surrogate bytes so allocation is deterministic across
            // retries. The source surrogate encodes (segment_id, row_idx) and
            // is unique per source row within the collection.
            let target_surrogate = state
                .surrogate_assigner
                .assign(
                    db_id,
                    tenant_id,
                    &target_qualified,
                    &source_surrogate_u32.to_be_bytes(),
                )
                .map_err(|e| crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "surrogate assign failed for source surrogate {source_surrogate_u32} in \
                         '{target_qualified}': {e}"
                    ),
                })?;

            // Wrap the value_bytes (msgpack Value::Object) in a msgpack array
            // so the Insert / Ingest handler can decode it as a row sequence.
            let payload = wrap_in_array(value_bytes)?;

            let plan = if target_is_timeseries {
                // Timeseries target: use TimeseriesOp::Ingest so rows land in
                // `columnar_memtables` (the timeseries scan path reads from
                // there, not from `columnar_engines`).
                // Format "msgpack" = msgpack array-of-maps (same layout as
                // SQL VALUES ingest produced by the planner).
                PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: target_qualified.clone(),
                    payload,
                    format: "msgpack".into(),
                    wal_lsn: None,
                    surrogates: vec![target_surrogate],
                    provenance: None,
                    rls_write_check: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                })
            } else {
                PhysicalPlan::Columnar(ColumnarOp::Insert {
                    collection: target_qualified.clone(),
                    payload,
                    format: "msgpack".into(),
                    intent: ColumnarInsertIntent::InsertIfAbsent,
                    on_conflict_updates: Vec::<(String, UpdateValue)>::new(),
                    surrogates: vec![target_surrogate],
                    schema_bytes: Vec::new(),
                    provenance: None,
                    wal_lsn: None,
                    rls_write_check: Vec::new(),
                    // Internal row copy — nothing is projected back and no
                    // caller identity's reads are being gated.
                    returning: None,
                    rls_filters: Vec::new(),
                })
            };

            let resp =
                dispatch_local(state, tenant_id, db_id, &target_qualified, plan, None).await?;
            if resp.status != Status::Ok {
                return Err(crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "columnar insert on target '{target_qualified}' for source surrogate \
                         {source_surrogate_u32} returned status {:?}",
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
        "columnar materialize: source rows copied to target",
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
    let proposed = propose_catalog_entry(
        state,
        &CatalogEntry::PutCollection(Box::new(updated.clone())),
    )?;
    if proposed == 0 {
        catalog.put_collection(db_id, &updated)?;
    }
    Ok(())
}

/// `(source_surrogate_u32, value_bytes)` returned by one scan page.
type ScanPage = (Vec<(u32, Vec<u8>)>, Vec<u8>);

/// Run one source-side `MaterializeScan` round-trip.
async fn scan_source_page(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
    system_as_of_ms: Option<i64>,
) -> crate::Result<ScanPage> {
    let plan = PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
        collection: source_qualified.to_string(),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        system_as_of_ms,
    });
    let resp = dispatch_local(state, tenant_id, source_db_id, source_qualified, plan, None).await?;
    if resp.status != Status::Ok {
        return Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "columnar materialize-scan on source '{source_qualified}' returned status {:?}",
                resp.status
            ),
        });
    }
    parse_materialize_scan_payload(resp.payload.as_ref())
}

/// Parse the msgpack payload emitted by `execute_columnar_materialize_scan`:
///   `[next_cursor: bin, entries: [[surrogate: u32, value_bytes: bin], ...]]`.
fn parse_materialize_scan_payload(payload: &[u8]) -> crate::Result<ScanPage> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let bad = || crate::Error::Serialization {
        format: "msgpack".into(),
        detail: "columnar materialize-scan response: malformed payload".into(),
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
        let (pair_len, mut pair_off) =
            msgpack_scan::array_header(payload, entry_off).ok_or_else(bad)?;
        if pair_len != 2 {
            return Err(bad());
        }
        let surrogate = msgpack_scan::read_u32_advance(payload, &mut pair_off).ok_or_else(bad)?;
        let value = msgpack_scan::read_bin_advance(payload, &mut pair_off)
            .ok_or_else(bad)?
            .to_vec();
        entries.push((surrogate, value));
        entry_off = pair_off;
    }

    Ok((entries, next_cursor))
}

/// Wrap a single msgpack Value::Object blob in a msgpack fixarray of length 1
/// so the columnar insert handler can decode it as `Vec<Value>`.
fn wrap_in_array(value_bytes: Vec<u8>) -> crate::Result<Vec<u8>> {
    // fixarray header for 1 element: 0x91
    let mut out = Vec::with_capacity(1 + value_bytes.len());
    out.push(0x91);
    out.extend_from_slice(&value_bytes);
    Ok(out)
}
