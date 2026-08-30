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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeseries_delta_serialization() {
        let delta = TimeseriesDelta {
            source_id: "clxyz1234test".into(),
            series_id: 12345,
            series_key: SeriesKey::new("cpu", vec![("host".into(), "prod".into())]),
            min_ts: 1000,
            max_ts: 2000,
            encoded_block: vec![1, 2, 3, 4],
            sample_count: 100,
        };
        let json = sonic_rs::to_string(&delta).unwrap();
        let back: TimeseriesDelta = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back.source_id, "clxyz1234test");
        assert_eq!(back.series_id, 12345);
        assert_eq!(back.sample_count, 100);
    }
}
