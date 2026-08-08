// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored timeseries rows.
//!
//! The snapshot-install path lands timeseries engine state via a per-node
//! DIRECT install: `restore_timeseries` rebuilds the memtable in-memory and
//! `restore_flushed_ts_segments` writes partition directories straight to the
//! local disk. Neither emits a WAL record nor a Raft entry, so on a multi-replica
//! cluster the restored data lands on ONLY the restore-target node — querying any
//! other replica returns zero rows.
//!
//! RESTORE instead re-issues each restored timeseries collection's live rows as a
//! durable `TimeseriesOp::Ingest`, branching on cluster vs single-node exactly
//! like a normal timeseries write (and exactly like the columnar reissue):
//!
//! - Cluster (`async_raft_proposer` present): build a `ReplicatedEntry` via
//!   `to_replicated_entry` (which maps `TimeseriesOp::Ingest` →
//!   `ReplicatedWrite::TimeseriesIngest`) and propose it through Raft. Replicates
//!   to every replica; recovery via Raft-log re-apply.
//! - Single-node: WAL-append the plan, then dispatch it into the Data Plane so it
//!   is installed live (WAL makes it durable for restart replay).
//!
//! ## Identity
//!
//! Unlike columnar, the timeseries engine does NOT carry a per-row cross-engine
//! surrogate sidecar: the memtable stores only the schema columns, the flushed
//! segment files store only the schema columns, and the `"msgpack"` ingest format
//! re-derives series identity from the tag columns. There is therefore no
//! surrogate to preserve — the rows are re-issued with an empty surrogate list,
//! identical to the live ILP / msgpack ingest path. This is not a loss of
//! identity: timeseries simply has no surrogate sidecar to begin with.

use std::collections::HashMap;
use std::time::Duration;

use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::value::Value;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::control::state::SharedState;
use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, ColumnarMemtable, ColumnarMemtableConfig, MemtableSnapshot,
};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;
use crate::types::{DatabaseId, TenantId, TsFlushedCollectionBlob, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;

/// Per-collection re-issue dispatch timeout. Generous: a restored collection may
/// carry many flushed partitions' worth of rows in one ingest.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);

/// Server-stamped reserved column — re-derived by the ingest path, so it must
/// NOT be carried back into the re-issued rows (the ingest handler restamps it).
/// `_ts_valid_from` / `_ts_valid_until` ARE client-provided and preserved.
const TS_SYSTEM_COLUMN: &str = TS_SYSTEM;

/// Decode the memtable section plus every flushed partition of one timeseries
/// collection into live `Value::Object` rows (keyed by column name).
///
/// `memtable_bytes` is the msgpack `MemtableSnapshot` captured at backup time
/// (`None` when the collection had no resident memtable). `flushed` carries the
/// raw flushed partition directories. `kek` is the timeseries segment encryption
/// key (the WAL encryption key); `None` when at-rest encryption is not
/// configured (plaintext segments).
pub fn decode_timeseries_live_rows(
    collection: &str,
    memtable_bytes: Option<&[u8]>,
    flushed: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<Value>> {
    let mut rows: Vec<Value> = Vec::new();

    if let Some(bytes) = memtable_bytes {
        decode_memtable_rows(collection, bytes, &mut rows)?;
    }

    for part in &flushed.partitions {
        decode_partition_rows(collection, part, kek, &mut rows)?;
    }

    Ok(rows)
}

/// Decode the captured memtable snapshot into row objects, appending to `rows`.
fn decode_memtable_rows(
    collection: &str,
    bytes: &[u8],
    rows: &mut Vec<Value>,
) -> crate::Result<()> {
    let snap: MemtableSnapshot =
        zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: decode timeseries memtable for '{collection}': {e}"),
        })?;
    let mt = ColumnarMemtable::from_snapshot(snap, ColumnarMemtableConfig::default())?;

    let schema = mt.schema();
    let columns: Vec<(usize, String, ColumnType)> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| (i, name.clone(), *ty))
        .collect();

    for idx in 0..mt.row_count() as usize {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(columns.len());
        for (col_idx, name, ty) in &columns {
            if name == TS_SYSTEM_COLUMN {
                continue;
            }
            let cell = memtable_cell(&mt, *col_idx, *ty, idx);
            insert_non_null(&mut map, name, cell);
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

/// Extract one cell from a memtable column as a `Value`.
fn memtable_cell(mt: &ColumnarMemtable, col_idx: usize, ty: ColumnType, idx: usize) -> Value {
    match ty {
        ColumnType::Timestamp => Value::Integer(mt.column(col_idx).as_timestamps()[idx]),
        ColumnType::Int64 => Value::Integer(mt.column(col_idx).as_i64()[idx]),
        ColumnType::Float64 => {
            let v = mt.column(col_idx).as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol => match mt.column(col_idx) {
            ColumnData::Symbol(ids) => mt
                .symbol_dict(col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    }
}

/// Decode one flushed partition directory into row objects, appending to `rows`.
///
/// The partition files are materialized to a temporary directory and read with
/// the SAME `ColumnarSegmentReader` the live scan path uses, so the decode is
/// byte-faithful to what a query against the restored segment would return.
fn decode_partition_rows(
    collection: &str,
    part: &crate::types::TsFlushedPartitionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    rows: &mut Vec<Value>,
) -> crate::Result<()> {
    let tmp = tempfile::Builder::new()
        .prefix("nodedb-ts-reissue-")
        .tempdir()
        .map_err(Error::Io)?;
    let part_dir = tmp.path();
    for (filename, data) in &part.files {
        std::fs::write(part_dir.join(filename), data).map_err(Error::Io)?;
    }

    let schema = ColumnarSegmentReader::read_schema(part_dir, kek).map_err(|e| Error::Storage {
        engine: "timeseries".into(),
        detail: format!(
            "restore reissue: read schema for partition '{}' of '{collection}': {e}",
            part.dir_name
        ),
    })?;

    let requested: Vec<(String, ColumnType)> = schema.columns.clone();
    let col_data = ColumnarSegmentReader::read_columns(part_dir, &requested, kek).map_err(|e| {
        Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "restore reissue: read columns for partition '{}' of '{collection}': {e}",
                part.dir_name
            ),
        }
    })?;

    let mut sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        if *ty == ColumnType::Symbol
            && let Ok(dict) = ColumnarSegmentReader::read_symbol_dict(part_dir, name, kek)
        {
            sym_dicts.insert(i, dict);
        }
    }

    let row_count = col_data.first().map(|c| c.len()).unwrap_or(0);
    for idx in 0..row_count {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());
        for (col_i, (name, ty)) in schema.columns.iter().enumerate() {
            if name == TS_SYSTEM_COLUMN {
                continue;
            }
            let cell = partition_cell(&col_data[col_i], *ty, col_i, &sym_dicts, idx);
            insert_non_null(&mut map, name, cell);
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

/// Extract one cell from a flushed-segment column as a `Value`.
fn partition_cell(
    data: &ColumnData,
    ty: ColumnType,
    col_idx: usize,
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> Value {
    match ty {
        ColumnType::Timestamp => Value::Integer(data.as_timestamps()[idx]),
        ColumnType::Int64 => Value::Integer(data.as_i64()[idx]),
        ColumnType::Float64 => {
            let v = data.as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol => match data {
            ColumnData::Symbol(ids) => sym_dicts
                .get(&col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    }
}

/// Insert a cell, skipping nulls so a re-issued row carries only present fields
/// (mirrors the ILP / msgpack ingest contract: absent field == not written).
fn insert_non_null(map: &mut HashMap<String, Value>, name: &str, value: Value) {
    if !matches!(value, Value::Null) {
        map.insert(name.to_string(), value);
    }
}

/// Build the durable `TimeseriesOp::Ingest` plan from decoded rows.
///
/// The payload is the native-`Value` msgpack encoding of `Value::Array(rows)`
/// (array of per-row field-keyed maps) — the exact shape the `"msgpack"` ingest
/// handler decodes (`decode_msgpack_rows`). Surrogates are empty: timeseries
/// re-derives series identity from the tag columns.
pub fn build_timeseries_ingest_plan(
    collection: &str,
    rows: Vec<Value>,
) -> crate::Result<PhysicalPlan> {
    let payload =
        nodedb_types::value_to_msgpack(&Value::Array(rows)).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: encode timeseries rows for '{collection}': {e}"),
        })?;

    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: collection.to_string(),
        payload,
        format: "msgpack".into(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        rls_write_check: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Re-issue a restored timeseries collection's rows durably.
///
/// Branches identically to a normal write (and to `reissue_columnar_durably`):
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: `wal_append_if_write` then `sync_dispatch::dispatch_system`.
pub async fn reissue_timeseries_durably(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<()> {
    let vshard = VShardId::from_collection_in_database(database_id, collection);

    if let Some(proposer) = state.async_raft_proposer() {
        let entry = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard,
            &plan,
        )
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "restore reissue: timeseries plan for '{collection}' did not map to a \
                     replicated write"
            ),
        })?;
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }

    // Single-node: WAL first (durable for restart replay), then install live.
    wal_append_if_write(&state.wal, tenant_id, vshard, database_id, &plan)?;
    sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::BackupRestore,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        REISSUE_TIMEOUT,
    )
    .await?;
    Ok(())
}
