// SPDX-License-Identifier: Apache-2.0

//! Shared timeseries types for multi-model database engines.
//!
//! Used by both `nodedb` (server) and `nodedb-lite` (embedded) for
//! timeseries ingest, storage, and query.

pub mod config;
pub mod continuous_agg;
pub mod ingest;
pub mod partition;
pub mod series;
pub mod sync;

// Public API surface — flattened re-exports for callers that don't need the sub-module structure.
pub use config::{ArchiveCompression, ConfigValidationError, TieredPartitionConfig};
pub use continuous_agg::{AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy};
pub use ingest::{IngestResult, LogEntry, MetricSample, SymbolDictionary, TimeRange};
pub use partition::{
    FlushedKind, FlushedSeries, IntervalParseError, PartitionInterval, PartitionMeta,
    PartitionState, SegmentKind, SegmentRef,
};
pub use series::{BatteryState, LiteId, ResolvedSeries, SeriesCatalog, SeriesId, SeriesKey};
pub use sync::{LogWalBatch, TimeseriesDelta, TimeseriesWalBatch};
