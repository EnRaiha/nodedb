// SPDX-License-Identifier: Apache-2.0

//! Sync delta and WAL payload types.

use serde::{Deserialize, Serialize};

use super::series::{LiteId, SeriesId, SeriesKey};

/// Wire format for Lite→Origin timeseries delta exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesDelta {
    /// Source Lite instance ID (CUID2).
    pub source_id: LiteId,
    /// Series identifier (metric + tags hash).
    pub series_id: SeriesId,
    /// Canonical series key for the source.
    pub series_key: SeriesKey,
    /// Minimum timestamp in this block.
    pub min_ts: i64,
    /// Maximum timestamp in this block.
    pub max_ts: i64,
    /// Gorilla-encoded compressed samples.
    pub encoded_block: Vec<u8>,
    /// Number of samples in the block.
    pub sample_count: u64,
}

/// WAL record payload for a timeseries metric batch.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TimeseriesWalBatch {
    /// Collection name.
    pub collection: String,
    /// Batch of metric samples: (series_id, timestamp_ms, value).
    pub samples: Vec<(SeriesId, i64, f64)>,
    /// Sync provenance for idempotent WAL replay across replicas.
    #[serde(default)]
    pub provenance: Option<crate::sync::wire::SyncProvenance>,
}

/// WAL record payload for a timeseries log batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogWalBatch {
    /// Collection name.
    pub collection: String,
    /// Batch of log entries: (series_id, timestamp_ms, data).
    pub entries: Vec<(SeriesId, i64, Vec<u8>)>,
}
