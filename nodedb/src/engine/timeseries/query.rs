// SPDX-License-Identifier: BUSL-1.1

//! Query execution engine for timeseries data.
//!
//! Provides scan, aggregation, and downsample operations over L1 segments.
//! Uses the `SegmentIndex` to locate relevant segments, then reads and
//! decodes them via the `reader` module.
//!
//! ## Query Flow
//!
//! 1. Caller provides `(series_id, time_range, operation)`.
//! 2. `SegmentIndex::query()` returns segment references overlapping the range.
//! 3. Each segment is read from L1 disk and decompressed.
//! 4. Samples are filtered to the exact time range.
//! 5. Aggregation/downsample is applied if requested.
//! 6. Results are returned.

use std::path::Path;

use super::compress::DictionaryRegistry;
use nodedb_types::timeseries::{SegmentKind, SeriesId, TimeRange};

use super::reader::{self, MetricAggregation, SegmentReadError};
use super::segment_index::SegmentIndex;

/// Query result for metric data.
#[derive(Debug)]
pub enum MetricQueryResult {
    /// Raw samples within the time range.
    Samples(Vec<(i64, f64)>),
    /// Aggregated result.
    Aggregation(MetricAggregation),
    /// Downsampled result (window_start_ts, avg_value).
    Downsampled(Vec<(i64, f64)>),
}

/// Query result for log data.
#[derive(Debug)]
pub struct LogQueryResult {
    pub entries: Vec<nodedb_types::timeseries::LogEntry>,
}

/// Errors from query execution.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("segment read error: {0}")]
    SegmentRead(#[from] SegmentReadError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no data found for series {series_id}")]
    NoData { series_id: SeriesId },
}

/// Timeseries query executor.
///
/// Reads L1 segments and applies operations (scan, aggregate, downsample).
/// NOT thread-safe — lives on a single Data Plane core.
pub struct TimeseriesQueryEngine<'a> {
    /// Segment index for locating relevant segments.
    segment_index: &'a SegmentIndex,
    /// Base directory for L1 segment files.
    l1_dir: &'a Path,
    /// Dictionary registry for log decompression.
    log_registry: &'a DictionaryRegistry,
}

impl<'a> TimeseriesQueryEngine<'a> {
    pub fn new(
        segment_index: &'a SegmentIndex,
        l1_dir: &'a Path,
        log_registry: &'a DictionaryRegistry,
    ) -> Self {
        Self {
            segment_index,
            l1_dir,
            log_registry,
        }
    }

    /// Scan all metric samples for a series within a time range.
    pub fn scan_metrics(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
    ) -> Result<Vec<(i64, f64)>, QueryError> {
        let segments = self.segment_index.query(series_id, range);
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_samples = Vec::new();

        for seg in &segments {
            if seg.kind != SegmentKind::Metric {
                continue;
            }

            let path = self.l1_dir.join(&seg.path);
            let data = reader::read_metric_segment(&path)?;

            // Filter to exact time range.
            for (ts, val) in data.samples {
                if range.contains(ts) {
                    all_samples.push((ts, val));
                }
            }
        }

        // Sort by timestamp for ordered output.
        all_samples.sort_by_key(|&(ts, _)| ts);
        Ok(all_samples)
    }

    /// Aggregate metrics for a series within a time range.
    pub fn aggregate_metrics(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
    ) -> Result<Option<MetricAggregation>, QueryError> {
        let samples = self.scan_metrics(series_id, range)?;
        Ok(MetricAggregation::compute(&samples))
    }

    /// Downsample metrics into fixed time windows.
    pub fn downsample_metrics(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
        window_ms: i64,
    ) -> Result<Vec<(i64, f64)>, QueryError> {
        let samples = self.scan_metrics(series_id, range)?;
        Ok(reader::downsample(&samples, window_ms))
    }

    /// Scan all log entries for a series within a time range.
    pub fn scan_logs(
        &self,
        series_id: SeriesId,
        range: &TimeRange,
    ) -> Result<Vec<nodedb_types::timeseries::LogEntry>, QueryError> {
        let segments = self.segment_index.query(series_id, range);
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_entries = Vec::new();

        for seg in &segments {
            if seg.kind != SegmentKind::Log {
                continue;
            }

            let path = self.l1_dir.join(&seg.path);
            let data = reader::read_log_segment(&path, self.log_registry)?;

            for entry in data.entries {
                if range.contains(entry.timestamp_ms) {
                    all_entries.push(entry);
                }
            }
        }

        all_entries.sort_by_key(|e| e.timestamp_ms);
        Ok(all_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::gorilla::GorillaEncoder;
    use crate::engine::timeseries::reader::encode_tseg_header;
    use crate::engine::timeseries::segment_index::SegmentIndex;
    use nodedb_types::timeseries::SegmentRef;
    use tempfile::TempDir;

    /// Write one metric segment in the on-disk TSEG layout the reader expects:
    /// header, kind byte, sample count, block length, Gorilla block.
    fn write_metric_segment(l1_dir: &Path, filename: &str, block: &[u8], sample_count: u64) -> u64 {
        let mut buf = Vec::new();
        encode_tseg_header(&mut buf);
        buf.push(0x01); // kind = metric
        buf.extend_from_slice(&sample_count.to_le_bytes());
        buf.extend_from_slice(&(block.len() as u32).to_le_bytes());
        buf.extend_from_slice(block);

        std::fs::create_dir_all(l1_dir).unwrap();
        std::fs::write(l1_dir.join(filename), &buf).unwrap();
        buf.len() as u64
    }

    fn setup_test_data(dir: &TempDir) -> (SegmentIndex, DictionaryRegistry) {
        let l1_dir = dir.path().join("l1");

        // Two metric segments for series 1, covering adjacent time ranges.
        let mut enc1 = GorillaEncoder::new();
        for i in 0..50 {
            enc1.encode(1000 + i * 100, 10.0 + i as f64 * 0.5);
        }
        let mut enc2 = GorillaEncoder::new();
        for i in 0..50 {
            enc2.encode(6000 + i * 100, 50.0 + i as f64 * 0.3);
        }

        let mut idx = SegmentIndex::new();
        for (filename, block, min_ts, max_ts) in [
            ("ts-seg-1.seg", enc1.finish(), 1000i64, 5900i64),
            ("ts-seg-2.seg", enc2.finish(), 6000, 10900),
        ] {
            let size_bytes = write_metric_segment(&l1_dir, filename, &block, 50);
            idx.add(
                1,
                SegmentRef {
                    path: filename.to_string(),
                    min_ts,
                    max_ts,
                    kind: SegmentKind::Metric,
                    size_bytes,
                    created_at_ms: 0,
                },
            );
        }

        (idx, DictionaryRegistry::new())
    }

    #[test]
    fn scan_metrics_full_range() {
        let dir = TempDir::new().unwrap();
        let (idx, registry) = setup_test_data(&dir);
        let l1_dir = dir.path().join("l1");

        let engine = TimeseriesQueryEngine::new(&idx, &l1_dir, &registry);

        let samples = engine.scan_metrics(1, &TimeRange::new(0, 20000)).unwrap();
        assert_eq!(samples.len(), 100); // 50 + 50
        // Verify sorted by timestamp.
        for w in samples.windows(2) {
            assert!(w[0].0 <= w[1].0);
        }
    }

    #[test]
    fn scan_metrics_partial_range() {
        let dir = TempDir::new().unwrap();
        let (idx, registry) = setup_test_data(&dir);
        let l1_dir = dir.path().join("l1");

        let engine = TimeseriesQueryEngine::new(&idx, &l1_dir, &registry);

        // Only query the first segment's range.
        let samples = engine.scan_metrics(1, &TimeRange::new(2000, 4000)).unwrap();
        // Should get samples between ts 2000 and 4000 from the first segment.
        assert!(!samples.is_empty());
        for &(ts, _) in &samples {
            assert!((2000..=4000).contains(&ts));
        }
    }

    #[test]
    fn aggregate_metrics_works() {
        let dir = TempDir::new().unwrap();
        let (idx, registry) = setup_test_data(&dir);
        let l1_dir = dir.path().join("l1");

        let engine = TimeseriesQueryEngine::new(&idx, &l1_dir, &registry);

        let agg = engine
            .aggregate_metrics(1, &TimeRange::new(0, 20000))
            .unwrap()
            .unwrap();
        assert_eq!(agg.count, 100);
        assert!(agg.min < agg.max);
    }

    #[test]
    fn downsample_reduces_points() {
        let dir = TempDir::new().unwrap();
        let (idx, registry) = setup_test_data(&dir);
        let l1_dir = dir.path().join("l1");

        let engine = TimeseriesQueryEngine::new(&idx, &l1_dir, &registry);

        let full = engine.scan_metrics(1, &TimeRange::new(0, 20000)).unwrap();
        let downsampled = engine
            .downsample_metrics(1, &TimeRange::new(0, 20000), 5000)
            .unwrap();

        assert!(downsampled.len() < full.len());
        assert!(!downsampled.is_empty());
    }

    #[test]
    fn empty_series_returns_empty() {
        let dir = TempDir::new().unwrap();
        let idx = SegmentIndex::new();
        let registry = DictionaryRegistry::new();
        let l1_dir = dir.path().join("l1");

        let engine = TimeseriesQueryEngine::new(&idx, &l1_dir, &registry);

        let samples = engine
            .scan_metrics(999, &TimeRange::new(0, 100000))
            .unwrap();
        assert!(samples.is_empty());
    }
}
