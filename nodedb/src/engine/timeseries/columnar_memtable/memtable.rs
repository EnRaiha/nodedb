// SPDX-License-Identifier: BUSL-1.1

//! `ColumnarMemtable` — per-column ingest buffer for timeseries data.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design).

use std::collections::HashMap;

use nodedb_types::timeseries::{IngestResult, MetricSample, SeriesId, SymbolDictionary};

use super::snapshot::{MemtableSnapshot, column_to_snapshot, rebuild_columns};
use super::types::{
    ColumnData, ColumnType, ColumnValue, ColumnarDrainResult, ColumnarFlushView,
    ColumnarMemtableConfig, ColumnarSchema, max_system_ts_of,
};

/// Columnar memtable: per-column vectors instead of per-series hash maps.
///
/// Each row is a flat tuple of (timestamp, value, tag1, tag2, ...).
/// Series identity is derived from the tag columns at query time.
/// This layout is SIMD-friendly: aggregation functions operate on
/// contiguous `&[f64]` or `&[i64]` slices.
pub struct ColumnarMemtable {
    schema: ColumnarSchema,
    columns: Vec<ColumnData>,
    /// Per-series row count for quick cardinality checks.
    series_row_counts: HashMap<SeriesId, u64>,
    /// Per-tag-column symbol dictionary.
    symbol_dicts: HashMap<usize, SymbolDictionary>,
    row_count: u64,
    memory_bytes: usize,
    config: ColumnarMemtableConfig,
    min_ts: i64,
    max_ts: i64,
}

impl ColumnarMemtable {
    /// Create a new columnar memtable with the given schema.
    pub fn new(schema: ColumnarSchema, config: ColumnarMemtableConfig) -> Self {
        let columns: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|(_, ty)| ColumnData::new(*ty))
            .collect();

        // Initialize symbol dicts for tag columns.
        let mut symbol_dicts = HashMap::new();
        for (i, (_, ty)) in schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        Self {
            schema,
            columns,
            series_row_counts: HashMap::new(),
            symbol_dicts,
            row_count: 0,
            memory_bytes: 0,
            config,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }

    /// Create a simple metrics memtable (timestamp + f64 value, no tags).
    pub fn new_metric(config: ColumnarMemtableConfig) -> Self {
        Self::new(ColumnarSchema::metric_default(), config)
    }

    /// Ingest a metric sample into the default (timestamp, value) layout.
    ///
    /// For the simple 2-column schema. For multi-column schemas with tags,
    /// use `ingest_row()` instead.
    ///
    /// Does NOT enforce `hard_memory_limit`, for the same reason `ingest_row`
    /// does not (see its doc): the sole production caller is WAL replay
    /// (`replay_timeseries_payload`), and a sample reaching here belongs to a
    /// record that has ALREADY COMMITTED, so refusing it is not backpressure —
    /// it is silent loss of a durable write. Replay must take the record whole;
    /// the ceiling lives at the record boundary in the live ingest handler's
    /// admission gate, never here. NOTE: if a structured-`TimeseriesWalBatch`
    /// ingest producer is ever added, its replay must gain that same
    /// record-boundary flush, or a mid-record replay flush would stamp the
    /// partition with an LSN covering rows it does not hold.
    /// `replay_timeseries_wal` advances `ts_max_ingested_lsn` only AFTER a
    /// record is applied, which is what keeps that stamp honest.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> IngestResult {
        // Push to timestamp column.
        if let ColumnData::Timestamp(ref mut v) = self.columns[self.schema.timestamp_idx] {
            v.push(sample.timestamp_ms);
        }

        // Push to value column (assume index 1 for default schema).
        if self.columns.len() > 1
            && let ColumnData::Float64(ref mut v) = self.columns[1]
        {
            v.push(sample.value);
        }

        self.update_stats(series_id, sample.timestamp_ms, 16);
        self.check_flush_state()
    }

    /// Ingest a row with explicit column values.
    ///
    /// `values` must match the schema length. Tag string values are resolved
    /// to symbol IDs via the per-column dictionary.
    ///
    /// ## Why this does NOT enforce `hard_memory_limit`
    ///
    /// It used to: a row arriving above the ceiling came back
    /// `Ok(IngestResult::Rejected)`. But every row reaching here belongs to a
    /// WAL record that has ALREADY COMMITTED, so refusing it is not
    /// backpressure — it is silent loss of a durable write. The caller then
    /// tried to rescue the refusal by flushing and re-ingesting mid-record,
    /// which stamped the flushed partition with the PREVIOUS record's LSN
    /// while it held part of the current one; replay, gated on that stamp,
    /// re-appended the whole record on top.
    ///
    /// The ceiling now lives at the record boundary instead (the ingest
    /// handler's admission gate flushes BEFORE a record when the memtable is
    /// at or over it), so a flush always lands on a whole-record prefix and
    /// the partition's stamp is true of every row in it. Errors returned here
    /// are therefore genuine per-row data faults — bad arity, type mismatch,
    /// exhausted tag dictionary — never "come back later".
    pub fn ingest_row(
        &mut self,
        series_id: SeriesId,
        values: &[ColumnValue],
    ) -> crate::Result<IngestResult> {
        let col_types: Vec<(String, ColumnType)> = self.schema.columns.clone();

        if values.len() != col_types.len() {
            return Err(crate::Error::BadRequest {
                detail: format!("expected {} columns, got {}", col_types.len(), values.len()),
            });
        }

        let mut ts = 0i64;
        let mut row_bytes = 0usize;
        let max_card = self.config.max_tag_cardinality;

        for (i, (val, (col_name, col_type))) in values.iter().zip(col_types.iter()).enumerate() {
            match (val, col_type) {
                (ColumnValue::Timestamp(t), ColumnType::Timestamp) => {
                    if let ColumnData::Timestamp(ref mut v) = self.columns[i] {
                        v.push(*t);
                    }
                    ts = *t;
                    row_bytes += 8;
                }
                (ColumnValue::Float64(f), ColumnType::Float64) => {
                    if let ColumnData::Float64(ref mut v) = self.columns[i] {
                        v.push(*f);
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Int64(n), ColumnType::Int64) => {
                    if let ColumnData::Int64(ref mut v) = self.columns[i] {
                        v.push(*n);
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Symbol(s), ColumnType::Symbol) => {
                    let dict =
                        self.symbol_dicts
                            .get_mut(&i)
                            .ok_or_else(|| crate::Error::BadRequest {
                                detail: format!(
                                    "internal error: symbol dict missing for column {i}"
                                ),
                            })?;
                    match dict.resolve(s, max_card) {
                        Some(sym_id) => {
                            if let ColumnData::Symbol(ref mut v) = self.columns[i] {
                                v.push(sym_id);
                            }
                        }
                        None => {
                            self.rollback_partial_row(i);
                            return Err(crate::Error::BadRequest {
                                detail: format!(
                                    "tag cardinality limit ({max_card}) exceeded for column '{col_name}'"
                                ),
                            });
                        }
                    }
                    row_bytes += 4;
                }
                _ => {
                    self.rollback_partial_row(i);
                    return Err(crate::Error::BadRequest {
                        detail: format!("type mismatch at column {i}: expected {col_type:?}"),
                    });
                }
            }
        }

        self.update_stats(series_id, ts, row_bytes);
        Ok(self.check_flush_state())
    }

    /// Roll back a partially written row (called on error during `ingest_row`).
    fn rollback_partial_row(&mut self, columns_written: usize) {
        for col in self.columns.iter_mut().take(columns_written) {
            match col {
                ColumnData::Timestamp(v) => {
                    v.pop();
                }
                ColumnData::Float64(v) => {
                    v.pop();
                }
                ColumnData::Int64(v) => {
                    v.pop();
                }
                ColumnData::Symbol(v) => {
                    v.pop();
                }
                ColumnData::DictEncoded { ids, valid, .. } => {
                    ids.pop();
                    valid.pop();
                }
            }
        }
    }

    fn update_stats(&mut self, series_id: SeriesId, ts: i64, row_bytes: usize) {
        *self.series_row_counts.entry(series_id).or_insert(0) += 1;
        self.row_count += 1;
        self.memory_bytes += row_bytes;
        if ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }

    fn check_flush_state(&self) -> IngestResult {
        if self.memory_bytes >= self.config.max_memory_bytes {
            IngestResult::FlushNeeded
        } else {
            IngestResult::Ok
        }
    }

    /// Borrow this memtable's live rows as the payload a flush would write.
    ///
    /// The read-only half of a flush: a segment can be encoded and landed from
    /// this view, and only then does [`Self::drain`] take the rows out. Nothing
    /// leaves memory before it is durable, so a failed segment write leaves the
    /// memtable exactly as it was.
    pub fn flush_view(&self) -> ColumnarFlushView<'_> {
        ColumnarFlushView {
            columns: &self.columns,
            schema: &self.schema,
            symbol_dicts: &self.symbol_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts: max_system_ts_of(&self.schema, &self.columns),
        }
    }

    /// Drain all data from the memtable, resetting it for reuse.
    ///
    /// Returns the column data, schema, symbol dicts, and stats.
    ///
    /// Callers that flush must write the segment from [`Self::flush_view`] FIRST
    /// and drain only once that write has committed — the rows here have no
    /// other copy but the WAL, and the checkpoint that calls the flush is what
    /// authorises deleting it.
    pub fn drain(&mut self) -> ColumnarDrainResult {
        let mut drained_columns = Vec::with_capacity(self.columns.len());
        for col in &mut self.columns {
            // DictEncoded columns are drained by swapping in a fresh Symbol
            // placeholder. The flusher converts Symbol → DictEncoded during
            // segment encoding once it has enough cardinality data.
            let col_type = match col {
                ColumnData::Timestamp(_) => ColumnType::Timestamp,
                ColumnData::Float64(_) => ColumnType::Float64,
                ColumnData::Int64(_) => ColumnType::Int64,
                ColumnData::Symbol(_) => ColumnType::Symbol,
                ColumnData::DictEncoded { .. } => ColumnType::Symbol,
            };
            let mut empty = ColumnData::new(col_type);
            std::mem::swap(col, &mut empty);
            drained_columns.push(empty);
        }

        let drained_dicts = std::mem::take(&mut self.symbol_dicts);
        // Reinitialize symbol dicts.
        for (i, (_, ty)) in self.schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                self.symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        // Scan `_ts_system` column (if present) for retention's system-time axis.
        let max_system_ts = max_system_ts_of(&self.schema, &drained_columns);

        let result = ColumnarDrainResult {
            columns: drained_columns,
            schema: self.schema.clone(),
            symbol_dicts: drained_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts,
            series_row_counts: std::mem::take(&mut self.series_row_counts),
        };

        self.row_count = 0;
        self.memory_bytes = 0;
        self.min_ts = i64::MAX;
        self.max_ts = i64::MIN;

        result
    }

    // -- Mutators --

    /// Truncate this memtable back to `n` rows.
    ///
    /// Used during transaction rollback to reverse a `TimeseriesIngest` operation.
    /// All column vectors are truncated; aggregate stats are recomputed from the
    /// surviving rows. `series_row_counts` is rebuilt from scratch so per-series
    /// cardinality remains consistent.
    pub fn truncate_to(&mut self, n: u64) {
        if n >= self.row_count {
            return;
        }
        let n_usize = n as usize;
        let ts_idx = self.schema.timestamp_idx;
        for col in &mut self.columns {
            match col {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.truncate(n_usize),
                ColumnData::Float64(v) => v.truncate(n_usize),
                ColumnData::Symbol(v) => v.truncate(n_usize),
                ColumnData::DictEncoded { ids, valid, .. } => {
                    ids.truncate(n_usize);
                    valid.truncate(n_usize);
                }
            }
        }
        self.row_count = n;
        // Recompute ts range from surviving timestamps.
        if n == 0 {
            self.min_ts = i64::MAX;
            self.max_ts = i64::MIN;
            self.series_row_counts.clear();
        } else if let ColumnData::Timestamp(ts) = &self.columns[ts_idx] {
            self.min_ts = ts.iter().copied().min().unwrap_or(i64::MAX);
            self.max_ts = ts.iter().copied().max().unwrap_or(i64::MIN);
        }
        // Recompute memory_bytes estimate by re-summing column capacities.
        self.memory_bytes = self
            .columns
            .iter()
            .map(|c| match c {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.capacity() * 8,
                ColumnData::Float64(v) => v.capacity() * 8,
                ColumnData::Symbol(v) => v.capacity() * 4,
                ColumnData::DictEncoded {
                    ids,
                    valid,
                    dictionary,
                    ..
                } => ids.capacity() * 4 + valid.capacity() + dictionary.len() * 32,
            })
            .sum();
    }

    // -- Accessors --

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Approximate memory usage. Uses incremental tracking with periodic
    /// recomputation from column capacities for accuracy.
    pub fn memory_bytes(&self) -> usize {
        let col_bytes: usize = self.columns.iter().map(|c| c.memory_bytes()).sum();
        let dict_bytes: usize = self.symbol_dicts.len() * 256; // rough estimate
        self.memory_bytes.max(col_bytes + dict_bytes)
    }

    pub fn min_ts(&self) -> i64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.max_ts
    }

    pub fn series_count(&self) -> usize {
        self.series_row_counts.len()
    }

    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Return the immutable admission configuration used to construct this
    /// memtable. Transaction undo uses this with a snapshot so restoration
    /// preserves the original limits even if live operator tuning changed.
    pub fn config(&self) -> ColumnarMemtableConfig {
        self.config.clone()
    }

    /// Restore the pre-transaction resident-byte accounting after replacing
    /// the memtable from a logical snapshot.
    ///
    /// A snapshot preserves values and dictionaries, but rebuilding its vectors
    /// intentionally does not preserve their spare capacity. Transaction undo
    /// retains the original governor reservation, so it must also reinstate the
    /// original reported footprint rather than silently undercounting it.
    pub(crate) fn restore_memory_bytes_for_undo(&mut self, memory_bytes: usize) {
        self.memory_bytes = memory_bytes;
    }

    /// Export a lossless snapshot (carries column types + symbol dicts). INFALLIBLE.
    pub fn export_snapshot(&self) -> MemtableSnapshot {
        let columns: Vec<_> = self.columns.iter().map(column_to_snapshot).collect();

        // Stable ordering for deterministic snapshots.
        let mut symbol_dicts: Vec<(usize, SymbolDictionary)> = self
            .symbol_dicts
            .iter()
            .map(|(&idx, dict)| (idx, dict.clone()))
            .collect();
        symbol_dicts.sort_by_key(|(idx, _)| *idx);

        let mut series_row_counts: Vec<(SeriesId, u64)> = self
            .series_row_counts
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect();
        series_row_counts.sort_by_key(|(id, _)| *id);

        MemtableSnapshot {
            schema_columns: self.schema.columns.clone(),
            timestamp_idx: self.schema.timestamp_idx,
            columns,
            symbol_dicts,
            series_row_counts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
        }
    }

    /// Reconstruct a memtable from a snapshot. Returns a typed error on any
    /// schema/row-count mismatch.
    ///
    /// `config` is not carried in the snapshot — it is operator tuning, not
    /// data — so callers pass the live `ColumnarMemtableConfig::from_tuning`
    /// value. A memtable keeps its limits for its whole life, so restoring with
    /// compiled defaults would silently ignore the operator's configuration.
    pub fn from_snapshot(
        snap: MemtableSnapshot,
        config: ColumnarMemtableConfig,
    ) -> crate::Result<Self> {
        if snap.timestamp_idx >= snap.schema_columns.len() {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "snapshot timestamp_idx {} out of range for {} columns",
                    snap.timestamp_idx,
                    snap.schema_columns.len(),
                ),
            });
        }

        let columns = rebuild_columns(snap.columns, &snap.schema_columns, snap.row_count)?;
        let n = snap.schema_columns.len();
        // Codecs are ephemeral — not serialized; rebuild as Auto.
        let schema = ColumnarSchema {
            columns: snap.schema_columns,
            timestamp_idx: snap.timestamp_idx,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; n],
        };

        let symbol_dicts: HashMap<usize, SymbolDictionary> =
            snap.symbol_dicts.into_iter().collect();
        let series_row_counts: HashMap<SeriesId, u64> =
            snap.series_row_counts.into_iter().collect();
        let memory_bytes: usize =
            columns.iter().map(|c| c.memory_bytes()).sum::<usize>() + symbol_dicts.len() * 256;

        Ok(Self {
            schema,
            columns,
            series_row_counts,
            symbol_dicts,
            row_count: snap.row_count,
            memory_bytes,
            config,
            min_ts: snap.min_ts,
            max_ts: snap.max_ts,
        })
    }

    pub fn column(&self, idx: usize) -> &ColumnData {
        &self.columns[idx]
    }

    pub fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        self.symbol_dicts.get(&col_idx)
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Add a new column to the memtable schema, backfilling existing rows
    /// with NULL-equivalent values.
    ///
    /// Used for ILP schema evolution: when a new field appears in a later
    /// batch, the column is added and old rows get NaN/0/null-symbol.
    pub fn add_column(&mut self, name: String, col_type: ColumnType) {
        // Don't add duplicates.
        if self.schema.columns.iter().any(|(n, _)| n == &name) {
            return;
        }
        let existing_rows = self.row_count as usize;
        let col = match col_type {
            ColumnType::Float64 => ColumnData::Float64(vec![f64::NAN; existing_rows]),
            ColumnType::Int64 => ColumnData::Int64(vec![0; existing_rows]),
            ColumnType::Symbol => ColumnData::Symbol(vec![u32::MAX; existing_rows]),
            ColumnType::Timestamp => return, // never add a second timestamp
        };
        let idx = self.columns.len();
        self.columns.push(col);
        self.schema.columns.push((name, col_type));
        self.schema.codecs.push(nodedb_codec::ColumnCodec::Auto);
        if col_type == ColumnType::Symbol {
            self.symbol_dicts.insert(idx, SymbolDictionary::new());
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::MetricSample;

    use super::super::snapshot::ColumnSnapshot;
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn empty_memtable() {
        let mt = ColumnarMemtable::new_metric(default_config());
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
        assert_eq!(mt.series_count(), 0);
    }

    #[test]
    fn ingest_simple_metric() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        let result = mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1000,
                value: 42.5,
            },
        );
        assert_eq!(result, IngestResult::Ok);
        assert_eq!(mt.row_count(), 1);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1000);

        let ts_col = mt.column(0).as_timestamps();
        assert_eq!(ts_col, &[1000]);
        let val_col = mt.column(1).as_f64();
        assert!((val_col[0] - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ingest_multiple_metrics() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..100 {
            mt.ingest_metric(
                i % 10,
                MetricSample {
                    timestamp_ms: 1000 + i as i64,
                    value: i as f64,
                },
            );
        }
        assert_eq!(mt.row_count(), 100);
        assert_eq!(mt.series_count(), 10);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1099);
    }

    #[test]
    fn ingest_row_with_tags() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("dc".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let result = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(5000),
                ColumnValue::Float64(99.9),
                ColumnValue::Symbol("prod-1".to_string()),
                ColumnValue::Symbol("us-east".to_string()),
            ],
        );
        assert!(result.is_ok());
        assert_eq!(mt.row_count(), 1);

        // Verify symbol dictionaries were populated.
        let host_dict = mt.symbol_dict(2).unwrap();
        assert_eq!(host_dict.len(), 1);
        assert_eq!(host_dict.get(0), Some("prod-1"));

        let dc_dict = mt.symbol_dict(3).unwrap();
        assert_eq!(dc_dict.get(0), Some("us-east"));
    }

    #[test]
    fn tag_cardinality_breaker() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let config = ColumnarMemtableConfig {
            max_tag_cardinality: 5,
            ..default_config()
        };
        let mut mt = ColumnarMemtable::new(schema, config);

        // First 5 unique tags work.
        for i in 0..5 {
            let tag = format!("val-{i}");
            let r = mt.ingest_row(
                i as u64,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol(tag.clone()),
                ],
            );
            assert!(r.is_ok());
        }
        assert_eq!(mt.row_count(), 5);

        // 6th unique tag is rejected.
        let r = mt.ingest_row(
            99,
            &[
                ColumnValue::Timestamp(2000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol("one-too-many".to_string()),
            ],
        );
        assert!(r.is_err());
        // Row count didn't increase (rolled back).
        assert_eq!(mt.row_count(), 5);
    }

    #[test]
    fn drain_returns_data_and_resets() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..50 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64,
                },
            );
        }
        assert_eq!(mt.row_count(), 50);

        let result = mt.drain();
        assert_eq!(result.row_count, 50);
        assert_eq!(result.min_ts, 1000);
        assert_eq!(result.max_ts, 1049);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].len(), 50);
        assert_eq!(result.columns[1].len(), 50);

        // Memtable is reset.
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
    }

    #[test]
    fn ingest_metric_accepts_past_hard_limit() {
        // A sample reaching the memtable belongs to an already-committed WAL
        // record; refusing it would silently drop a durable write on replay.
        // So `ingest_metric` accepts every sample regardless of the ceiling —
        // it never returns `Rejected`, and the resident footprint overshoots
        // the hard limit rather than losing data.
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        for i in 0..1000 {
            let r = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i,
                    value: 1.0,
                },
            );
            assert_ne!(
                r,
                IngestResult::Rejected,
                "sample {i} must not be rejected — that would drop a durable record on replay"
            );
        }
        assert_eq!(
            mt.row_count(),
            1000,
            "every sample past the limit is retained"
        );
        assert!(
            mt.memory_bytes() >= 200,
            "the footprint is allowed to overshoot the hard limit"
        );
    }

    #[test]
    fn flush_needed_signal() {
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        let mut flush_signaled = false;
        for i in 0..100 {
            let r = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i,
                    value: 1.0,
                },
            );
            if r == IngestResult::FlushNeeded {
                flush_signaled = true;
                break;
            }
        }
        assert!(flush_signaled);
    }

    #[test]
    fn type_mismatch_rejected() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let r = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(1000),
                ColumnValue::Int64(42), // Wrong: schema says Float64
            ],
        );
        assert!(r.is_err());
        assert_eq!(mt.row_count(), 0); // Rolled back.
    }

    // -----------------------------------------------------------------------
    // Snapshot round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn snapshot_roundtrip_empty_memtable() {
        let mt = ColumnarMemtable::new_metric(default_config());
        let snap = mt.export_snapshot();
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: MemtableSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let mt2 = ColumnarMemtable::from_snapshot(snap2, default_config()).expect("from_snapshot");
        assert_eq!(mt2.row_count(), 0);
        assert!(mt2.is_empty());
        assert_eq!(mt2.schema().columns.len(), 2);
        assert_eq!(mt2.schema().timestamp_idx, 0);
    }

    #[test]
    fn snapshot_roundtrip_multi_column_with_symbol_tags() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        for i in 0..5u64 {
            mt.ingest_row(
                i,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(i as f64 * 1.5),
                    ColumnValue::Symbol(format!("host-{i}")),
                ],
            )
            .expect("ingest");
        }
        // Also re-use an existing host to verify symbol dict cardinality.
        mt.ingest_row(
            99,
            &[
                ColumnValue::Timestamp(2000),
                ColumnValue::Float64(99.0),
                ColumnValue::Symbol("host-0".to_string()),
            ],
        )
        .expect("ingest existing host");

        let expected_row_count = mt.row_count();
        let expected_min_ts = mt.min_ts();
        let expected_max_ts = mt.max_ts();
        let expected_schema_cols: Vec<(String, ColumnType)> = mt.schema().columns.clone();
        let expected_ts_idx = mt.schema().timestamp_idx;
        let expected_dict_len = mt.symbol_dict(2).map(|d| d.len()).unwrap_or(0);

        let snap = mt.export_snapshot();
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: MemtableSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let mt2 = ColumnarMemtable::from_snapshot(snap2, default_config()).expect("from_snapshot");

        assert_eq!(mt2.row_count(), expected_row_count);
        assert_eq!(mt2.min_ts(), expected_min_ts);
        assert_eq!(mt2.max_ts(), expected_max_ts);
        assert_eq!(mt2.schema().columns, expected_schema_cols);
        assert_eq!(mt2.schema().timestamp_idx, expected_ts_idx);

        // Verify symbol dict was faithfully restored.
        let dict2 = mt2.symbol_dict(2).expect("host dict present");
        assert_eq!(dict2.len(), expected_dict_len);
        assert_eq!(dict2.get(0), Some("host-0"));

        // Verify column data lengths match.
        for i in 0..3 {
            assert_eq!(mt2.column(i).len(), expected_row_count as usize);
        }
    }

    #[test]
    fn snapshot_roundtrip_dict_encoded_column_rebuilds_reverse() {
        // Construct a DictEncoded snapshot directly (bypass ingest path).
        let snap = MemtableSnapshot {
            schema_columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            columns: vec![
                ColumnSnapshot::Timestamp(vec![1000, 2000, 3000]),
                ColumnSnapshot::DictEncoded {
                    ids: vec![0, 1, 0],
                    dictionary: vec!["alpha".to_string(), "beta".to_string()],
                    valid: vec![true, true, true],
                },
            ],
            symbol_dicts: vec![],
            series_row_counts: vec![],
            row_count: 3,
            min_ts: 1000,
            max_ts: 3000,
        };

        let mt = ColumnarMemtable::from_snapshot(snap, default_config()).expect("from_snapshot");
        assert_eq!(mt.row_count(), 3);
        // Verify the DictEncoded column has correct data.
        match mt.column(1) {
            ColumnData::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => {
                assert_eq!(ids, &[0u32, 1, 0]);
                assert_eq!(dictionary, &["alpha", "beta"]);
                assert_eq!(reverse.get("alpha"), Some(&0u32));
                assert_eq!(reverse.get("beta"), Some(&1u32));
                assert_eq!(valid, &[true, true, true]);
            }
            other => panic!("expected DictEncoded, got {:?}", other),
        }
    }

    #[test]
    fn snapshot_from_invalid_column_lengths_returns_error() {
        // row_count says 3 but timestamp column has only 2 rows → mismatch.
        let snap = MemtableSnapshot {
            schema_columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            columns: vec![
                ColumnSnapshot::Timestamp(vec![1000, 2000]),  // 2 rows
                ColumnSnapshot::Float64(vec![1.0, 2.0, 3.0]), // 3 rows — mismatch
            ],
            symbol_dicts: vec![],
            series_row_counts: vec![],
            row_count: 3,
            min_ts: 1000,
            max_ts: 2000,
        };
        let result = ColumnarMemtable::from_snapshot(snap, default_config());
        assert!(
            matches!(result, Err(crate::Error::BadRequest { .. })),
            "expected BadRequest error on length mismatch"
        );
    }
}
