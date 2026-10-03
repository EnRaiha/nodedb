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

use nodedb_types::{DatabaseId, TenantId};

use crate::types::TxnId;

use super::dispatch::{dispatch_local, dispatch_to_owner};
use super::document_copy::RowCopy;
use super::reaper::{ReapParams, reap_materialized_collection};
use super::status::{checkpoint_progress, mark_materializing};
use crate::bridge::envelope::Status;
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
    mark_materializing(state, coll).await?;

    let target_qualified = db_qualified(db_id, &coll.name);
    // Tombstones: source surrogates deleted from the clone before materialization.
    let tombstoned = catalog.list_clone_tombstones(&target_qualified)?;
    // Convert as_of_lsn to milliseconds for the source-side scan.
    let system_as_of_ms = crate::control::clone::lsn_resolve::source_as_of_ms(
        state,
        coll.bitemporal,
        origin.as_of_lsn,
    );

    if coll.hash_chain {
        let copied = super::chained::materialize_chained_collection(
            state,
            catalog,
            db_id,
            coll,
            &tombstoned,
            system_as_of_ms,
        )
        .await?;
        checkpoint_progress(state, coll, origin.as_of_lsn, copied, copied).await?;
    } else {
        RowCopy {
            state,
            catalog,
            db_id,
            coll,
            tenant_id: TenantId::new(coll.tenant_id),
            source_db_id: origin.source_database,
            source_collection: &origin.source_collection,
            source_qualified: db_qualified(origin.source_database, &origin.source_collection),
            target_qualified: &target_qualified,
            tombstoned: &tombstoned,
            system_as_of_ms,
            as_of_lsn: origin.as_of_lsn,
        }
        .run()
        .await?;
    }

    reap_materialized_collection(ReapParams {
        db_id,
        tenant_id: coll.tenant_id,
        name: &coll.name,
        state,
        catalog,
    })
    .await?;
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
    let page = SourcePage {
        tenant_id,
        source_db_id,
        source_qualified,
        cursor,
        system_as_of_ms,
        raw_bodies: false,
    };
    scan_page(state, page, txn_id).await
}

/// One source-side scan page request.
pub(super) struct SourcePage<'a> {
    pub tenant_id: TenantId,
    pub source_db_id: DatabaseId,
    pub source_qualified: &'a str,
    pub cursor: &'a [u8],
    pub system_as_of_ms: Option<i64>,
    /// Bodies as stored, with no `id` added.
    pub raw_bodies: bool,
}

/// Run one `MaterializeScan` round-trip for `page`.
pub(super) async fn scan_page(
    state: &SharedState,
    page: SourcePage<'_>,
    txn_id: Option<TxnId>,
) -> crate::Result<ScanPage> {
    let SourcePage {
        tenant_id,
        source_db_id,
        source_qualified,
        cursor,
        system_as_of_ms,
        raw_bodies,
    } = page;
    let plan = PhysicalPlan::Document(DocumentOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        system_as_of_ms,
        raw_bodies,
    });
    // A transaction's staging overlay lives on the source shard's leader, so
    // a staged read runs there (`dispatch_local` routes it). Every other read
    // goes to the source shard's owner.
    if txn_id.is_none() {
        let payload =
            dispatch_to_owner(state, tenant_id, source_db_id, source_qualified, plan).await?;
        return parse_materialize_scan_payload(&payload);
    }
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
