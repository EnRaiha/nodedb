// SPDX-License-Identifier: BUSL-1.1

//! Raw scan mode: emit rows from memtable + disk partitions.
//!
//! No aggregation — returns individual rows as MessagePack.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;

use super::super::columnar_filter;

/// Parameters for a timeseries raw scan (no aggregation).
pub(in crate::data::executor) struct RawScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filter_predicates: &'a [crate::bridge::scan_filter::ScanFilter],
    pub has_filters: bool,
    pub computed_columns: &'a [u8],
    /// `AS OF SYSTEM TIME NULL`: emit every `_ts_system` version ordered
    /// ascending by system time (audit-log semantics).
    pub all_versions: bool,
}

impl CoreLoop {
    /// Raw scan mode: emit rows from memtable + partitions.
    pub(in crate::data::executor) fn execute_ts_raw_scan(
        &self,
        params: RawScanParams<'_>,
    ) -> Response {
        let RawScanParams {
            task,
            tid,
            collection,
            time_range,
            limit,
            filter_predicates,
            has_filters,
            computed_columns: computed_columns_bytes,
            all_versions,
        } = params;

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid.as_u64(), collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let key = (tid, collection.to_string());
        let mut results: Vec<rmpv::Value> = Vec::new();

        // 1. Read from memtable.
        if let Some(mt) = self.columnar_memtables.get(&key)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let timestamps = mt.column(schema.timestamp_idx).as_timestamps();
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            let (filtered_indices, need_json_filter) = if has_filters {
                let row_count = mt.row_count() as usize;
                if let Some(bitmask) =
                    columnar_filter::eval_filters_bitmask(mt, filter_predicates, row_count)
                {
                    let bm_indices = nodedb_query::simd_filter::bitmask_to_indices(&bitmask);
                    (bm_indices, false)
                } else {
                    match columnar_filter::eval_filters_sparse(mt, filter_predicates, &indices) {
                        Some(mask) => (columnar_filter::apply_mask(&indices, &mask), false),
                        None => (indices, true),
                    }
                }
            } else {
                (indices, false)
            };

            let columns: Vec<_> = schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| (i, name, ty, mt.column(i)))
                .collect();
            for &idx in &filtered_indices {
                if results.len() >= limit {
                    break;
                }
                let row = emit_memtable_row(mt, &columns, idx as usize);
                if need_json_filter {
                    // Encode rmpv row to msgpack bytes for binary filter eval.
                    let mut buf = Vec::new();
                    rmpv::encode::write_value(&mut buf, &row).ok();
                    if !filter_predicates.iter().all(|f| f.matches_binary(&buf)) {
                        continue;
                    }
                }
                results.push(row);
            }
        }

        // 2. Read from disk partitions.
        if let Some(registry) = self.ts_registries.get(&key) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let entries: Vec<_> = registry.query_partitions(&query_range);

            if !entries.is_empty() {
                let data_dir = &self.data_dir;
                let partition_dirs: Vec<std::path::PathBuf> = entries
                    .iter()
                    .map(|e| data_dir.join("ts").join(collection).join(&e.dir_name))
                    .filter(|p| p.exists())
                    .collect();

                let remaining = limit.saturating_sub(results.len());
                if remaining > 0 && !partition_dirs.is_empty() {
                    let partition_rows = scan_partitions_parallel(
                        &partition_dirs,
                        time_range,
                        remaining,
                        filter_predicates,
                        has_filters,
                    );
                    results.extend(partition_rows);
                    results.truncate(limit);
                }
            }
        }

        // Apply computed columns (e.g. time_bucket) if present.
        let results = if !computed_columns_bytes.is_empty() {
            let computed_cols_result: Result<Vec<crate::bridge::expr_eval::ComputedColumn>, _> =
                zerompk::from_msgpack(computed_columns_bytes);
            let computed_cols = match computed_cols_result {
                Ok(cols) => cols,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("computed_columns deserialize failed: {e}"),
                        },
                    );
                }
            };
            if computed_cols.is_empty() {
                results
            } else {
                results
                    .into_iter()
                    .map(|row| apply_computed_columns_rmpv(row, &computed_cols))
                    .collect()
            }
        } else {
            results
        };

        // Audit-log order: ascending by system time across all versions.
        let results = if all_versions {
            let mut sorted = results;
            sorted.sort_by_key(rmpv_system_time);
            sorted
        } else {
            results
        };

        let array = rmpv::Value::Array(results);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
        self.response_with_payload(task, buf)
    }
}

// ---------------------------------------------------------------------------
// Parallel partition scan for raw mode
// ---------------------------------------------------------------------------

/// Extract the `_ts_system` value from an rmpv-encoded row for audit-log
/// ordering. Rows without the column sort first (treated as `i64::MIN`).
fn rmpv_system_time(row: &rmpv::Value) -> i64 {
    let rmpv::Value::Map(entries) = row else {
        return i64::MIN;
    };
    for (k, v) in entries {
        if let rmpv::Value::String(s) = k
            && s.as_str() == Some("_ts_system")
            && let rmpv::Value::Integer(i) = v
        {
            return i.as_i64().unwrap_or(i64::MIN);
        }
    }
    i64::MIN
}

/// Scan disk partitions in parallel, returning rmpv rows sorted by timestamp.
fn scan_partitions_parallel(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    if partition_dirs.len() <= 1 {
        return partition_dirs
            .first()
            .map(|dir| scan_one_partition(dir, time_range, limit, filter_predicates, has_filters))
            .unwrap_or_default();
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let available = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let thread_count = available.min(partition_dirs.len()).min(8);

        if thread_count <= 1 {
            return scan_partitions_sequential(
                partition_dirs,
                time_range,
                limit,
                filter_predicates,
                has_filters,
            );
        }

        let chunk_size = partition_dirs.len().div_ceil(thread_count);
        let filters_ref = filter_predicates;

        let mut thread_results: Vec<Vec<rmpv::Value>> = std::thread::scope(|s| {
            let handles: Vec<_> = partition_dirs
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        scan_partitions_sequential(
                            chunk,
                            time_range,
                            limit,
                            filters_ref,
                            has_filters,
                        )
                    })
                })
                .collect();

            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        });

        // Merge: each thread's results are already time-sorted (partitions are
        // time-ordered). Flatten, sort globally, truncate to limit.
        let total: usize = thread_results.iter().map(|v| v.len()).sum();
        let mut merged = Vec::with_capacity(total.min(limit));
        for batch in &mut thread_results {
            merged.append(batch);
        }
        // Sort by timestamp (first field in each row map).
        merged.sort_by_key(extract_timestamp);
        merged.truncate(limit);
        merged
    }

    #[cfg(target_arch = "wasm32")]
    {
        scan_partitions_sequential(
            partition_dirs,
            time_range,
            limit,
            filter_predicates,
            has_filters,
        )
    }
}

fn scan_partitions_sequential(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    let mut results = Vec::new();
    for dir in partition_dirs {
        if results.len() >= limit {
            break;
        }
        let remaining = limit - results.len();
        let rows = scan_one_partition(dir, time_range, remaining, filter_predicates, has_filters);
        results.extend(rows);
    }
    results.truncate(limit);
    results
}

/// Scan a single disk partition, returning rmpv rows.
fn scan_one_partition(
    part_dir: &std::path::Path,
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    let schema = match ColumnarSegmentReader::read_schema(part_dir, None) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    // Prefetch all column files into page cache before reading.
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
    crate::data::io::fadvise::prefetch_partition_columns(part_dir, &all_col_names);

    let col_data: Vec<Option<ColumnData>> = schema
        .columns
        .iter()
        .map(|(name, ty)| ColumnarSegmentReader::read_column(part_dir, name, *ty, None).ok())
        .collect();

    let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
        .filter_map(|(i, (name, _))| {
            ColumnarSegmentReader::read_symbol_dict(part_dir, name, None)
                .ok()
                .map(|dict| (i, dict))
        })
        .collect();

    let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
    let Some(ts_col) = ts_col else {
        return Vec::new();
    };
    let timestamps = ts_col.as_timestamps();
    let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

    let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
    let part_src = columnar_filter::PartitionColumns {
        schema: &schema_vec,
        columns: &col_data,
        sym_dicts: &sym_dicts,
    };

    let row_count = timestamps.len();
    let filtered_indices = if has_filters {
        if let Some(bitmask) =
            columnar_filter::eval_filters_bitmask(&part_src, filter_predicates, row_count)
        {
            nodedb_query::simd_filter::bitmask_to_indices(&bitmask)
        } else {
            match columnar_filter::eval_filters_sparse(&part_src, filter_predicates, &indices) {
                Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                None => indices,
            }
        }
    } else {
        indices
    };

    let mut rows = Vec::with_capacity(filtered_indices.len().min(limit));
    for &idx in &filtered_indices {
        if rows.len() >= limit {
            break;
        }
        let row = emit_partition_row(&schema_vec, &col_data, &sym_dicts, idx as usize);
        rows.push(row);
    }

    // Release page cache for this partition.
    crate::data::io::fadvise::release_partition_columns(part_dir, &all_col_names);

    rows
}

// ---------------------------------------------------------------------------
// Row emission helpers — build rmpv::Value directly
// ---------------------------------------------------------------------------

/// Emit a single row from the memtable as rmpv::Value::Map.
fn emit_memtable_row(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    columns: &[(usize, &String, &ColumnType, &ColumnData)],
    idx: usize,
) -> rmpv::Value {
    // Build raw msgpack bytes, then decode to rmpv::Value.
    let mut buf = Vec::with_capacity(columns.len() * 32);
    nodedb_query::msgpack_scan::write_map_header(&mut buf, columns.len());
    for (col_idx, col_name, col_type, col_data) in columns {
        nodedb_query::msgpack_scan::write_str(&mut buf, col_name);
        super::super::columnar_read::emit_column_value(
            &mut buf, mt, *col_idx, col_type, col_data, idx,
        );
    }
    rmpv::decode::read_value(&mut buf.as_slice()).unwrap_or(rmpv::Value::Nil)
}

/// Emit a single row from a disk partition as rmpv::Value::Map.
fn emit_partition_row(
    schema: &[(String, ColumnType)],
    col_data: &[Option<ColumnData>],
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> rmpv::Value {
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(schema.len());
    for (col_i, (col_name, col_type)) in schema.iter().enumerate() {
        let Some(data) = &col_data[col_i] else {
            continue;
        };
        let val = match col_type {
            ColumnType::Timestamp => rmpv::Value::Integer(data.as_timestamps()[idx].into()),
            ColumnType::Float64 => {
                let v = data.as_f64()[idx];
                if v.is_nan() {
                    rmpv::Value::Nil
                } else {
                    rmpv::Value::F64(v)
                }
            }
            ColumnType::Int64 => {
                if let ColumnData::Int64(vals) = data {
                    rmpv::Value::Integer(vals[idx].into())
                } else {
                    rmpv::Value::Nil
                }
            }
            ColumnType::Symbol => {
                if let ColumnData::Symbol(ids) = data {
                    sym_dicts
                        .get(&col_i)
                        .and_then(|dict| dict.get(ids[idx]))
                        .map(|s| rmpv::Value::String(s.into()))
                        .unwrap_or(rmpv::Value::Nil)
                } else {
                    rmpv::Value::Nil
                }
            }
        };
        fields.push((rmpv::Value::String(col_name.as_str().into()), val));
    }
    rmpv::Value::Map(fields)
}

/// Extract timestamp from a row (first integer field) for sort-merge.
fn extract_timestamp(row: &rmpv::Value) -> i64 {
    if let rmpv::Value::Map(fields) = row {
        for (_, v) in fields {
            if let rmpv::Value::Integer(n) = v {
                return n.as_i64().unwrap_or(0);
            }
        }
    }
    0
}

/// Apply computed column expressions to an rmpv row.
///
/// Converts the row to `nodedb_types::Value` for expression evaluation,
/// then produces a new row containing only the computed columns.
/// When computed columns are present, the output contains ONLY
/// computed columns (matching Document engine behavior for projection).
fn apply_computed_columns_rmpv(
    row: rmpv::Value,
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
) -> rmpv::Value {
    let doc = rmpv_to_nodedb_value(&row);
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(computed_cols.len());
    for cc in computed_cols {
        let result = cc.expr.eval(&doc);
        fields.push((
            rmpv::Value::String(cc.alias.as_str().into()),
            nodedb_value_to_rmpv(&result),
        ));
    }
    rmpv::Value::Map(fields)
}

/// Convert rmpv row to nodedb_types::Value for expression evaluation.
fn rmpv_to_nodedb_value(row: &rmpv::Value) -> nodedb_types::Value {
    match row {
        rmpv::Value::Map(fields) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in fields {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => continue,
                };
                let val = match v {
                    rmpv::Value::Integer(n) => {
                        nodedb_types::Value::Integer(n.as_i64().unwrap_or(0))
                    }
                    rmpv::Value::F64(f) => nodedb_types::Value::Float(*f),
                    rmpv::Value::String(s) => {
                        nodedb_types::Value::String(s.as_str().unwrap_or("").to_string())
                    }
                    rmpv::Value::Nil => nodedb_types::Value::Null,
                    rmpv::Value::Boolean(b) => nodedb_types::Value::Bool(*b),
                    _ => nodedb_types::Value::Null,
                };
                map.insert(key, val);
            }
            nodedb_types::Value::Object(map)
        }
        _ => nodedb_types::Value::Null,
    }
}

/// Convert nodedb_types::Value back to rmpv::Value for response encoding.
fn nodedb_value_to_rmpv(v: &nodedb_types::Value) -> rmpv::Value {
    match v {
        nodedb_types::Value::Integer(n) => rmpv::Value::Integer((*n).into()),
        nodedb_types::Value::Float(f) => rmpv::Value::F64(*f),
        nodedb_types::Value::String(s) => rmpv::Value::String(s.as_str().into()),
        nodedb_types::Value::Bool(b) => rmpv::Value::Boolean(*b),
        nodedb_types::Value::Null => rmpv::Value::Nil,
        _ => rmpv::Value::Nil,
    }
}
