// SPDX-License-Identifier: Apache-2.0

//! Timeseries engine operations dispatched to the Data Plane.

use nodedb_types::{Surrogate, SystemTimeScope};

use crate::physical_plan::document::ReturningSpec;

/// An unconstrained `(min_ts_ms, max_ts_ms)` envelope.
///
/// The Control Plane always plans a timeseries scan unbounded: narrowing it
/// requires knowing which column is the collection's declared `TIME_KEY`, and
/// that is resolved in the Data Plane where the collection's registered schema
/// lives. Internal callers that already know an exact window (retention,
/// continuous-aggregate refresh) pass their own bounds instead.
pub const UNBOUNDED_TIME_RANGE: (i64, i64) = (i64::MIN, i64::MAX);

/// Timeseries engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TimeseriesOp {
    /// Columnar partition scan with time-range pruning.
    ///
    /// Universal timeseries query path: handles raw scans, time-bucket
    /// aggregation, and generic GROUP BY. Reads from both the active
    /// memtable and sealed disk partitions.
    Scan {
        collection: String,
        /// `(min_ts_ms, max_ts_ms)` pruning envelope. The Data Plane narrows
        /// it further using the query's bounds on the declared `TIME_KEY`;
        /// see [`UNBOUNDED_TIME_RANGE`].
        time_range: (i64, i64),
        projection: Vec<String>,
        limit: usize,
        filters: Vec<u8>,
        /// `ORDER BY` terms, each an expression, in significance order.
        /// Empty = the engine's natural order. Applied to the materialized
        /// result before `limit` is enforced, so an ordered query returns the
        /// first `limit` rows of the ordering the client asked for.
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// time_bucket interval in milliseconds. 0 = no bucketing.
        bucket_interval_ms: i64,
        /// GROUP BY column names (empty = no grouping or whole-table agg).
        group_by: Vec<String>,
        /// Aggregate expressions: `(op, field)` e.g. `("count","*")`, `("avg","elapsed_ms")`.
        /// Empty = raw scan (no aggregation).
        aggregates: Vec<(String, String)>,
        /// Gap-fill strategy for time-bucket aggregation.
        /// Empty = no gap-fill. Otherwise: "null", "prev", "linear", or literal value.
        /// Only applied when `bucket_interval_ms > 0`.
        gap_fill: String,
        /// Serialized `Vec<ComputedColumn>` for scalar projection expressions
        /// (e.g. `time_bucket('1h', timestamp)`). Applied per-row in raw scan mode.
        computed_columns: Vec<u8>,
        /// RLS post-scan filters (applied after time-range pruning).
        rls_filters: Vec<u8>,
        /// System-time selection. `Current` = current state; `AsOf(ms)` =
        /// block-skip + post-filter to rows written ≤ ms; `AllVersions` =
        /// every `_ts_system` row ordered ascending (audit log), system-time
        /// column projected. Only meaningful for timeseries collections
        /// created `WITH BITEMPORAL`.
        #[serde(default)]
        system_time: SystemTimeScope,
        /// Bitemporal valid-time point. When `Some`, only rows whose
        /// `[_ts_valid_from, _ts_valid_until)` interval contains this
        /// point are returned.
        #[serde(default)]
        valid_at_ms: Option<i64>,
    },

    /// Write a batch of samples to the columnar memtable.
    Ingest {
        collection: String,
        payload: Vec<u8>,
        /// "ilp" for InfluxDB Line Protocol, "samples" for structured.
        format: String,
        /// WAL record LSN for deduplication. Set by the WAL catch-up task
        /// so the Data Plane can skip records that have already been ingested
        /// or flushed to disk. `None` for live ingest (always accepted).
        #[serde(default)]
        wal_lsn: Option<u64>,
        /// Reserved for per-row cross-engine `Surrogate` identities. The
        /// timeseries ingest handler does NOT consume this field: timeseries
        /// rows are identified internally by `series_id` (a deterministic hash
        /// of measurement + tags, computed identically on every replica), and
        /// timeseries does not participate in cross-engine bitmap joins, so no
        /// cross-engine surrogate binding is required. Almost always `vec![]`;
        /// retained for plan-shape uniformity with the columnar `Insert` op.
        #[serde(default)]
        surrogates: Vec<Surrogate>,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        /// Compiled row-level-security WRITE predicate (`Vec<ScanFilter>` as
        /// MessagePack), evaluated in the Data Plane against every parsed row
        /// before it reaches the memtable. Every ingest format normalizes into
        /// ILP inside the handler, so one gate there covers all of them —
        /// including the raw ILP listener, which builds its tasks outside the
        /// SQL planner. Empty means no write policy restricts this identity
        /// here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// When `Some`, return the STORED post-image of each ingested point —
        /// the row as it exists after time-key normalization, tag/field
        /// splitting and schema resolution, read back through the ordinary scan
        /// projection so it matches what `SELECT` shows. Never the submitted
        /// line: every format is rewritten into ILP before a point is built, so
        /// echoing the request would report values the collection does not hold.
        ///
        /// A batch that rejects any row FAILS when this is set, rather than
        /// answering with a short row set: the count response has a `rejected`
        /// field to report the loss and a row set has nowhere to put it.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. Distinct from
        /// `rls_write_check` above, which decides whether the write happens at
        /// all: this bounds what may be shown back, so a `RETURNING` row set
        /// never exceeds a `SELECT` by the same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },
}
