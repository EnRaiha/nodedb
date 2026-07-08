// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of columnar and timeseries rows drained from the
//! snapshot before the topology split (see [`super::restore_tenant`]'s
//! doc comments on why these two engines bypass the per-node snapshot
//! install path).

use std::sync::Arc;

use crate::Error;
use crate::control::state::SharedState;
use crate::types::TenantId;

use crate::control::backup::snapshot_keys::extract_db_scoped_collection;

/// Decode and durably re-issue every restored timeseries collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `memtables` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the captured `MemtableSnapshot` wire shape); `flushed` carries the flushed
/// partition blobs keyed by the same `"{db}:{tid}:{collection}"` key. The union
/// of the two key sets is re-issued once per collection (memtable + flushed rows
/// merged into a single ingest).
pub(super) async fn reissue_timeseries_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    memtables: Vec<(String, Vec<u8>)>,
    flushed: Vec<crate::types::TsFlushedCollectionBlob>,
) -> Result<usize, Error> {
    // Timeseries segment KEK == the WAL encryption key (segments are written via
    // the same key). Absent when at-rest encryption is not configured, in which
    // case segments are plaintext and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();
    let database_id = crate::types::DatabaseId::DEFAULT;

    // Index memtable bytes and flushed blobs by their `{db}:{tid}:{collection}`
    // key so each collection is decoded + re-issued exactly once.
    let mut memtable_by_key: std::collections::HashMap<String, Vec<u8>> =
        memtables.into_iter().collect();
    let mut keys_in_order: Vec<String> = Vec::new();
    let mut flushed_by_key: std::collections::HashMap<
        String,
        crate::types::TsFlushedCollectionBlob,
    > = std::collections::HashMap::new();
    for blob in flushed {
        keys_in_order.push(blob.collection_key.clone());
        flushed_by_key.insert(blob.collection_key.clone(), blob);
    }
    for key in memtable_by_key.keys() {
        if !flushed_by_key.contains_key(key) {
            keys_in_order.push(key.clone());
        }
    }

    let empty_flushed = crate::types::TsFlushedCollectionBlob::default();
    let mut reissued = 0usize;
    for key in keys_in_order {
        let Some(collection) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed timeseries snapshot key '{key}'"),
            });
        };
        let collection = collection.to_owned();

        let memtable_bytes = memtable_by_key.remove(&key);
        let flushed_blob = flushed_by_key.get(&key).unwrap_or(&empty_flushed);

        let rows = super::super::timeseries_reissue::decode_timeseries_live_rows(
            &collection,
            memtable_bytes.as_deref(),
            flushed_blob,
            kek.as_ref(),
        )?;
        if rows.is_empty() {
            continue;
        }

        let plan =
            super::super::timeseries_reissue::build_timeseries_ingest_plan(&collection, rows)?;
        super::super::timeseries_reissue::reissue_timeseries_durably(
            state,
            TenantId::new(tenant_id),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}

/// Decode and durably re-issue every restored plain-columnar collection.
///
/// Returns the number of collections that produced at least one live row and
/// were re-issued. `entries` are `("{db}:{tid}:{collection}", msgpack)` pairs
/// (the `ColumnarEngineSnapshot` wire shape).
pub(super) async fn reissue_columnar_snapshots(
    state: &Arc<SharedState>,
    tenant_id: u64,
    entries: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    // Columnar segment KEK == the WAL encryption key (segments are written via
    // `SegmentWriter::plain().write_segment(..., kek)` with this key). Absent
    // when at-rest encryption is not configured, in which case segments are
    // plaintext NDBS and decode with `kek = None`.
    let kek = state.wal.encryption_key().cloned();
    let database_id = crate::types::DatabaseId::DEFAULT;

    let mut reissued = 0usize;
    for (key, bytes) in entries {
        let Some(collection) = extract_db_scoped_collection(&key, tenant_id) else {
            return Err(Error::Internal {
                detail: format!("restore reissue: malformed columnar snapshot key '{key}'"),
            });
        };
        let collection = collection.to_owned();

        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize ColumnarEngineSnapshot for '{collection}': {e}"
                ),
            })?;

        let decoded = super::super::columnar_reissue::decode_snapshot_live_rows(
            &collection,
            snap,
            kek.as_ref(),
        )?;
        if decoded.rows.is_empty() {
            continue;
        }

        let plan =
            super::super::columnar_reissue::build_columnar_insert_plan(&collection, decoded)?;
        super::super::columnar_reissue::reissue_columnar_durably(
            state,
            TenantId::new(tenant_id),
            database_id,
            &collection,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}
