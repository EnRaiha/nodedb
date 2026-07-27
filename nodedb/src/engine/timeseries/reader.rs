// SPDX-License-Identifier: BUSL-1.1

//! Segment reader for L1 timeseries data.
//!
//! Reads and decodes L1 segment files (Gorilla-compressed metrics,
//! Zstd-compressed logs) for the query path.
//!
//! ## Segment Formats
//!
//! ### TSEG header (12 bytes at offset 0)
//! ```text
//! [magic: b"TSEG" 4B][version: u16 LE = 1, 2B][reserved: 2B zero][crc32c: u32 LE over bytes 0..8, 4B]
//! ```
//!
//! ### Metric Segment (after header)
//! ```text
//! [kind: 1B = 0x01][sample_count: 8B][block_len: 4B][gorilla_block: N]
//! ```
//!
//! ### Log Segment (after header)
//! ```text
//! [kind: 1B = 0x02][entry_count: 4B][compressed_len: 4B][compressed_block: N]
//! ```

use std::path::Path;

use super::compress::{DictionaryRegistry, decompress_log};
use super::gorilla::GorillaDecoder;
use nodedb_types::timeseries::LogEntry;

pub const SEGMENT_MAGIC: [u8; 4] = *b"TSEG";
pub const TSEG_HEADER_VERSION: u16 = 1;
/// Total byte length of the TSEG header prefix.
pub const TSEG_HEADER_SIZE: usize = 12;

const KIND_METRIC: u8 = 0x01;
const KIND_LOG: u8 = 0x02;

// ── Cursor helper ────────────────────────────────────────────────────────────

/// Minimal cursor over a byte slice for safe, offset-free parsing.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.remaining() < n {
            return None;
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Some(slice)
    }

    fn read_u8(&mut self) -> Option<u8> {
        let b = self.read_bytes(1)?;
        Some(b[0])
    }

    fn read_u16_le(&mut self) -> Option<u16> {
        let b = self.read_bytes(2)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_u32_le(&mut self) -> Option<u32> {
        let b = self.read_bytes(4)?;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64_le(&mut self) -> Option<u64> {
        let b = self.read_bytes(8)?;
        Some(u64::from_le_bytes(b.try_into().ok()?))
    }
}

// ── TSEG header encode/decode ─────────────────────────────────────────────────

/// Encode the 12-byte TSEG header into `out`.
pub fn encode_tseg_header(out: &mut Vec<u8>) {
    let base = out.len();
    out.extend_from_slice(&SEGMENT_MAGIC);
    out.extend_from_slice(&TSEG_HEADER_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    let crc = crc32c::crc32c(&out[base..base + 8]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Validate the TSEG header at the start of `data` and return
/// the number of bytes consumed (always [`TSEG_HEADER_SIZE`] on success).
pub fn decode_tseg_header(data: &[u8]) -> Result<usize, SegmentReadError> {
    if data.len() < TSEG_HEADER_SIZE {
        return Err(SegmentReadError::TooSmall { size: data.len() });
    }

    let mut cur = Cursor::new(data);

    let magic = cur
        .read_bytes(4)
        .expect("invariant: TSEG_HEADER_SIZE guard at line 99 ensures header bytes available");
    if magic != SEGMENT_MAGIC {
        return Err(SegmentReadError::InvalidMagic);
    }

    let version = cur
        .read_u16_le()
        .expect("invariant: TSEG_HEADER_SIZE guard at line 99 ensures 2 bytes for version field");
    let _reserved = cur
        .read_u16_le()
        .expect("invariant: TSEG_HEADER_SIZE guard at line 99 ensures 2 bytes for reserved field");
    let crc_stored = cur
        .read_u32_le()
        .expect("invariant: TSEG_HEADER_SIZE guard at line 99 ensures 4 bytes for crc field");

    if version != TSEG_HEADER_VERSION {
        return Err(SegmentReadError::UnsupportedVersion { version });
    }

    // CRC covers bytes 0..8 (magic + version + reserved).
    let crc_calc = crc32c::crc32c(&data[..8]);
    if crc_stored != crc_calc {
        return Err(SegmentReadError::InvalidHeaderCrc {
            stored: crc_stored,
            calc: crc_calc,
        });
    }

    Ok(TSEG_HEADER_SIZE)
}

// ── Public types ──────────────────────────────────────────────────────────────

/// A decoded metric segment: all samples decompressed.
#[derive(Debug)]
pub struct MetricSegmentData {
    /// Decompressed (timestamp_ms, value) pairs, in order.
    pub samples: Vec<(i64, f64)>,
}

/// A decoded log segment: all entries decompressed.
#[derive(Debug)]
pub struct LogSegmentData {
    /// Decompressed log entries, in order.
    pub entries: Vec<LogEntry>,
}

/// What kind of data a segment contains.
#[derive(Debug)]
pub enum SegmentData {
    Metric(MetricSegmentData),
    Log(LogSegmentData),
}

/// Errors from segment reading.
#[derive(Debug, thiserror::Error)]
pub enum SegmentReadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("segment too small: {size} bytes")]
    TooSmall { size: usize },
    #[error("invalid segment magic")]
    InvalidMagic,
    #[error("unknown segment kind: {kind:#x}")]
    UnknownKind { kind: u8 },
    #[error("decompression error: {detail}")]
    Decompression { detail: String },
    #[error("unsupported segment format version: {version}")]
    UnsupportedVersion { version: u16 },
    #[error("segment header CRC mismatch: stored={stored:#010x} calc={calc:#010x}")]
    InvalidHeaderCrc { stored: u32, calc: u32 },
}

// ── Readers ───────────────────────────────────────────────────────────────────

/// Read and decode a metric segment from disk.
///
/// Returns all (timestamp_ms, value) pairs.
pub fn read_metric_segment(path: &Path) -> Result<MetricSegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;
    read_metric_segment_from_bytes(&data)
}

/// Read and decode a metric segment from in-memory bytes.
pub fn read_metric_segment_from_bytes(data: &[u8]) -> Result<MetricSegmentData, SegmentReadError> {
    let header_end = decode_tseg_header(data)?;
    let mut cur = Cursor::new(&data[header_end..]);

    let kind = cur
        .read_u8()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;
    if kind != KIND_METRIC {
        return Err(SegmentReadError::UnknownKind { kind });
    }

    let _sample_count = cur
        .read_u64_le()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;
    let block_len = cur
        .read_u32_le()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })? as usize;

    let gorilla_block = cur
        .read_bytes(block_len)
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;

    let mut decoder = GorillaDecoder::new(gorilla_block);
    let samples = decoder.decode_all();

    Ok(MetricSegmentData { samples })
}

/// Read and decode a log segment from disk.
///
/// Returns all log entries. Requires a dictionary registry for
/// Zstd dictionary decompression.
pub fn read_log_segment(
    path: &Path,
    registry: &DictionaryRegistry,
) -> Result<LogSegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;
    read_log_segment_from_bytes(&data, registry)
}

/// Read and decode a log segment from in-memory bytes.
pub fn read_log_segment_from_bytes(
    data: &[u8],
    registry: &DictionaryRegistry,
) -> Result<LogSegmentData, SegmentReadError> {
    let header_end = decode_tseg_header(data)?;
    let mut cur = Cursor::new(&data[header_end..]);

    let kind = cur
        .read_u8()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;
    if kind != KIND_LOG {
        return Err(SegmentReadError::UnknownKind { kind });
    }

    let entry_count = cur
        .read_u32_le()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })? as usize;
    let compressed_len = cur
        .read_u32_le()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })? as usize;

    let compressed_block = cur
        .read_bytes(compressed_len)
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;

    let raw = decompress_log(compressed_block, registry).map_err(|e| {
        SegmentReadError::Decompression {
            detail: e.to_string(),
        }
    })?;

    // Parse raw bytes back into log entries.
    // Format per entry: [timestamp_ms:8] [data_len:4] [data:N]
    // The header count is decoded from segment bytes. Allocate only after a
    // full entry has been consumed from the decompressed block.
    let mut entries = Vec::new();
    let mut raw_cur = Cursor::new(&raw);

    while raw_cur.remaining() >= 12 && entries.len() < entry_count {
        let timestamp_ms = raw_cur.read_u64_le().expect(
            "invariant: 'while raw_cur.remaining() >= 12' loop guard ensures 8 bytes for timestamp",
        ) as i64;
        let data_len = raw_cur
            .read_u32_le()
            .expect("invariant: remaining >= 12, u64 consumed 8, leaving 4 for data_len")
            as usize;

        let entry_data = match raw_cur.read_bytes(data_len) {
            Some(b) => b.to_vec(),
            None => break,
        };

        entries.push(LogEntry {
            timestamp_ms,
            data: entry_data,
        });
    }

    Ok(LogSegmentData { entries })
}

/// Read a segment file, auto-detecting its type.
pub fn read_segment(
    path: &Path,
    registry: &DictionaryRegistry,
) -> Result<SegmentData, SegmentReadError> {
    let data = std::fs::read(path)?;

    let header_end = decode_tseg_header(&data)?;
    let kind = data
        .get(header_end)
        .copied()
        .ok_or(SegmentReadError::TooSmall { size: data.len() })?;

    match kind {
        KIND_METRIC => read_metric_segment_from_bytes(&data).map(SegmentData::Metric),
        KIND_LOG => read_log_segment_from_bytes(&data, registry).map(SegmentData::Log),
        k => Err(SegmentReadError::UnknownKind { kind: k }),
    }
}

// ── Aggregation helpers ───────────────────────────────────────────────────────

/// Aggregation functions for metric samples.
#[derive(Debug, Clone, Copy)]
pub struct MetricAggregation {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub last_ts: i64,
}

impl MetricAggregation {
    /// Compute aggregation over a slice of (timestamp, value) pairs.
    pub fn compute(samples: &[(i64, f64)]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        let mut agg = Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first_ts: samples[0].0,
            last_ts: samples[0].0,
        };

        for &(ts, val) in samples {
            agg.count += 1;
            agg.sum += val;
            if val < agg.min {
                agg.min = val;
            }
            if val > agg.max {
                agg.max = val;
            }
            if ts < agg.first_ts {
                agg.first_ts = ts;
            }
            if ts > agg.last_ts {
                agg.last_ts = ts;
            }
        }

        Some(agg)
    }

    /// Average value.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Merge two aggregations (for combining results from multiple segments).
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            count: self.count + other.count,
            sum: self.sum + other.sum,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            first_ts: self.first_ts.min(other.first_ts),
            last_ts: self.last_ts.max(other.last_ts),
        }
    }
}

/// Downsample metric samples by averaging within fixed time windows.
///
/// Given samples sorted by timestamp and a window size (in ms), returns
/// one (timestamp, avg_value) per window. The timestamp is the start
/// of the window.
pub fn downsample(samples: &[(i64, f64)], window_ms: i64) -> Vec<(i64, f64)> {
    if samples.is_empty() || window_ms <= 0 {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut window_start = (samples[0].0 / window_ms) * window_ms;
    let mut window_sum = 0.0;
    let mut window_count = 0u64;

    for &(ts, val) in samples {
        let this_window = (ts / window_ms) * window_ms;
        if this_window != window_start {
            if window_count > 0 {
                result.push((window_start, window_sum / window_count as f64));
            }
            window_start = this_window;
            window_sum = 0.0;
            window_count = 0;
        }
        window_sum += val;
        window_count += 1;
    }

    if window_count > 0 {
        result.push((window_start, window_sum / window_count as f64));
    }

    result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::gorilla::GorillaEncoder;
    use tempfile::TempDir;

    /// Build a valid TSEG header buffer.
    fn make_tseg_header() -> Vec<u8> {
        let mut buf = Vec::new();
        encode_tseg_header(&mut buf);
        buf
    }

    fn write_test_metric_segment(dir: &Path, samples: &[(i64, f64)]) -> std::path::PathBuf {
        let mut encoder = GorillaEncoder::new();
        for &(ts, val) in samples {
            encoder.encode(ts, val);
        }
        let gorilla_block = encoder.finish();

        let path = dir.join("test-metric.seg");
        let mut buf = make_tseg_header();
        buf.push(KIND_METRIC);
        buf.extend_from_slice(&(samples.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(gorilla_block.len() as u32).to_le_bytes());
        buf.extend_from_slice(&gorilla_block);
        std::fs::write(&path, &buf).unwrap();
        path
    }

    #[test]
    fn read_metric_segment_roundtrip() {
        let dir = TempDir::new().unwrap();
        let samples = vec![(1000i64, 42.0f64), (2000, 43.5), (3000, 41.0), (4000, 44.2)];
        let path = write_test_metric_segment(dir.path(), &samples);

        let data = read_metric_segment(&path).unwrap();
        assert_eq!(data.samples.len(), 4);
        assert_eq!(data.samples[0].0, 1000);
        assert!((data.samples[0].1 - 42.0).abs() < f64::EPSILON);
        assert_eq!(data.samples[3].0, 4000);
    }

    #[test]
    fn aggregation_basic() {
        let samples = vec![(1000i64, 10.0f64), (2000, 20.0), (3000, 30.0), (4000, 40.0)];
        let agg = MetricAggregation::compute(&samples).unwrap();
        assert_eq!(agg.count, 4);
        assert!((agg.sum - 100.0).abs() < f64::EPSILON);
        assert!((agg.avg() - 25.0).abs() < f64::EPSILON);
        assert!((agg.min - 10.0).abs() < f64::EPSILON);
        assert!((agg.max - 40.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregation_empty() {
        assert!(MetricAggregation::compute(&[]).is_none());
    }

    #[test]
    fn aggregation_merge() {
        let a = MetricAggregation {
            count: 2,
            sum: 30.0,
            min: 10.0,
            max: 20.0,
            first_ts: 1000,
            last_ts: 2000,
        };
        let b = MetricAggregation {
            count: 2,
            sum: 70.0,
            min: 30.0,
            max: 40.0,
            first_ts: 3000,
            last_ts: 4000,
        };
        let merged = a.merge(&b);
        assert_eq!(merged.count, 4);
        assert!((merged.sum - 100.0).abs() < f64::EPSILON);
        assert!((merged.min - 10.0).abs() < f64::EPSILON);
        assert!((merged.max - 40.0).abs() < f64::EPSILON);
        assert_eq!(merged.first_ts, 1000);
        assert_eq!(merged.last_ts, 4000);
    }

    #[test]
    fn downsample_basic() {
        let samples: Vec<(i64, f64)> = (0..100).map(|i| (i * 100, i as f64)).collect();
        let downsampled = downsample(&samples, 1000);
        assert_eq!(downsampled.len(), 10);
        assert_eq!(downsampled[0].0, 0);
        assert!((downsampled[0].1 - 4.5).abs() < f64::EPSILON);
        assert_eq!(downsampled[9].0, 9000);
        assert!((downsampled[9].1 - 94.5).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_empty() {
        assert!(downsample(&[], 1000).is_empty());
    }

    #[test]
    fn invalid_segment_errors() {
        let dir = TempDir::new().unwrap();

        // Too small.
        let path = dir.path().join("tiny.seg");
        std::fs::write(&path, [0u8; 3]).unwrap();
        assert!(matches!(
            read_metric_segment(&path),
            Err(SegmentReadError::TooSmall { .. })
        ));

        // Bad magic.
        let path = dir.path().join("bad_magic.seg");
        let mut buf = vec![0u8; TSEG_HEADER_SIZE + 13];
        buf[0] = b'X'; // corrupt magic
        std::fs::write(&path, &buf).unwrap();
        assert!(matches!(
            read_metric_segment(&path),
            Err(SegmentReadError::InvalidMagic)
        ));
    }

    // ── TSEG golden tests ───────────────────────────────────────────────

    /// Verify the exact byte layout of the TSEG header.
    #[test]
    fn tseg_golden_header_bytes() {
        let mut buf = Vec::new();
        encode_tseg_header(&mut buf);
        assert_eq!(buf.len(), TSEG_HEADER_SIZE);

        // Bytes 0..4: magic
        assert_eq!(&buf[0..4], b"TSEG");

        // Bytes 4..6: version = 1 LE
        assert_eq!(u16::from_le_bytes([buf[4], buf[5]]), TSEG_HEADER_VERSION);

        // Bytes 6..8: reserved = 0
        assert_eq!(&buf[6..8], &[0, 0]);

        // Bytes 8..12: CRC32C over bytes 0..8
        let expected_crc = crc32c::crc32c(&buf[0..8]);
        let stored_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        assert_eq!(stored_crc, expected_crc);
    }

    /// Version != 1 must be rejected.
    #[test]
    fn tseg_rejects_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"TSEG");
        buf.extend_from_slice(&2u16.to_le_bytes()); // version = 2
        buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
        let crc = crc32c::crc32c(&buf[..8]);
        buf.extend_from_slice(&crc.to_le_bytes());

        let err = decode_tseg_header(&buf).unwrap_err();
        assert!(
            matches!(err, SegmentReadError::UnsupportedVersion { version: 2 }),
            "expected UnsupportedVersion {{version: 2}}, got {err:?}"
        );
    }

    /// Corrupt CRC must be rejected.
    #[test]
    fn tseg_rejects_bad_crc() {
        let mut buf = make_tseg_header();
        buf[8] ^= 0xFF; // flip first CRC byte
        let err = decode_tseg_header(&buf).unwrap_err();
        assert!(
            matches!(err, SegmentReadError::InvalidHeaderCrc { .. }),
            "expected InvalidHeaderCrc, got {err:?}"
        );
    }
}
