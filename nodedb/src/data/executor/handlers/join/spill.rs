// SPDX-License-Identifier: BUSL-1.1

//! Framed partition spill writer and reader for the grace-hash join.
//!
//! Wraps [`UringWriter`] with a simple length-prefixed framing layer so that
//! many discrete msgpack rows can be stored in a single spill file and read
//! back as individual `&[u8]` slices.
//!
//! ## Framing format
//!
//! Each row is written as:
//!
//! ```text
//! [ 4 bytes: row length as u32 little-endian ][ row_len bytes: msgpack payload ]
//! ```
//!
//! The frame header is always 4 bytes; zero-length rows are legal (the 4-byte
//! header is written but the body is empty).
//!
//! ## Panic safety
//!
//! [`parse_framed_rows`] / [`FramedRows`] never panic or index out of bounds.
//! A truncated or malformed tail (< 4 bytes remaining, or a declared length
//! that exceeds remaining bytes) causes the iterator to stop cleanly.
//!
//! ## Consumer
//!
//! The grace-hash join build/probe pipeline (`hash.rs`, future
//! `grace_hash.rs`) is the intended consumer of these types.

use std::path::{Path, PathBuf};

use crate::data::io::uring_writer::UringWriter;

// ── Writer ────────────────────────────────────────────────────────────────────

/// Framing layer over [`UringWriter`] that stores many discrete msgpack rows
/// in a single spill file.
///
/// Not `Send` — delegates to [`UringWriter`] which is `!Send` / TPC-owned.
///
/// # Usage
///
/// ```ignore
/// let mut w = SpillPartitionWriter::create(&path)?;
/// w.append_row(row_bytes)?;
/// let path = w.finish()?;
/// ```
pub(super) struct SpillPartitionWriter {
    writer: UringWriter,
}

impl SpillPartitionWriter {
    /// Create a new partition spill file at `path`.
    ///
    /// Returns `None` if io_uring is unavailable or the file cannot be
    /// created, mirroring [`UringWriter::new`]'s fallback convention.
    pub(super) fn create(path: &Path) -> Option<Self> {
        let writer = UringWriter::new(path)?;
        Some(Self { writer })
    }

    /// Write one msgpack row into the spill file with a 4-byte LE length prefix.
    ///
    /// Two `append` calls are issued: the 4-byte header, then the body.
    /// Both must succeed atomically from the caller's perspective — any error
    /// leaves the file in an invalid state (truncated frame), but that is
    /// acceptable: spill files are temporary and never partially re-used.
    ///
    /// Returns a typed [`crate::Error`] if `row.len()` exceeds `u32::MAX` or
    /// if either write fails.
    pub(super) fn append_row(&mut self, row: &[u8]) -> crate::Result<()> {
        // Guard: length must fit in a u32 frame header.
        if row.len() > u32::MAX as usize {
            return Err(crate::Error::Storage {
                engine: "spill".into(),
                detail: format!(
                    "spill row length {} exceeds u32::MAX ({}); row cannot be framed",
                    row.len(),
                    u32::MAX
                ),
            });
        }

        // Write 4-byte little-endian length prefix.
        let len_bytes = (row.len() as u32).to_le_bytes();
        self.writer.append(&len_bytes)?;

        // Write the row body (zero-length rows are legal; UringWriter::append
        // returns Ok(()) immediately for empty slices).
        self.writer.append(row)?;

        Ok(())
    }

    /// Flush and close the writer, returning the spill file path.
    pub(super) fn finish(self) -> crate::Result<PathBuf> {
        self.writer.finish()
    }
}

// ── Reader (zero-copy iterator) ───────────────────────────────────────────────

/// Parse a flat byte buffer produced by [`SpillPartitionWriter`] into an
/// iterator of individual msgpack row slices.
///
/// The buffer is typically the concatenated output of
/// `UringReader::read_files(&[&spill_path])`.
///
/// Yields `&buf[frame_body_start..frame_body_end]` for each valid frame.
/// On a truncated or malformed tail the iterator stops cleanly; it never
/// panics and never indexes out of bounds.
pub(super) fn parse_framed_rows(buf: &[u8]) -> FramedRows<'_> {
    FramedRows { buf, pos: 0 }
}

/// Zero-copy iterator over length-prefixed msgpack rows in a spill buffer.
///
/// Produced by [`parse_framed_rows`].
pub(super) struct FramedRows<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for FramedRows<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least 4 bytes for the length header.
        let header = self.buf.get(self.pos..self.pos + 4)?;

        // SAFETY: `header` is exactly 4 bytes — the `get` above guarantees this.
        let row_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;

        let body_start = self.pos + 4;
        let body_end = body_start.checked_add(row_len)?;

        // Ensure the full body is present (checked slice — no panic).
        let row = self.buf.get(body_start..body_end)?;

        self.pos = body_end;
        Some(row)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::parse_framed_rows;

    // ── parse_framed_rows pure unit tests (no IO) ──────────────────────────

    /// Build a framed buffer manually and assert the iterator splits correctly.
    #[test]
    fn parse_framed_rows_correct_split() {
        let rows: &[&[u8]] = &[b"hello", b"world", b"", b"\xDE\xAD\xBE\xEF"];
        let mut buf = Vec::new();
        for row in rows {
            buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
            buf.extend_from_slice(row);
        }

        let parsed: Vec<&[u8]> = parse_framed_rows(&buf).collect();
        assert_eq!(parsed.len(), rows.len(), "row count mismatch");
        for (i, (&expected, &got)) in rows.iter().zip(parsed.iter()).enumerate() {
            assert_eq!(got, expected, "row {i} content mismatch");
        }
    }

    /// Truncated tail (incomplete length header) stops cleanly — no panic.
    #[test]
    fn parse_framed_rows_truncated_header_stops_cleanly() {
        // One valid frame followed by a 3-byte stub that cannot be a header.
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(b"hello");
        // Truncated header: only 3 bytes instead of 4.
        buf.extend_from_slice(&[0x01, 0x00, 0x00]);

        let parsed: Vec<&[u8]> = parse_framed_rows(&buf).collect();
        assert_eq!(parsed.len(), 1, "should yield exactly one valid frame");
        assert_eq!(parsed[0], b"hello");
    }

    /// Truncated body (header present but body shorter than declared) stops cleanly.
    #[test]
    fn parse_framed_rows_truncated_body_stops_cleanly() {
        let mut buf = Vec::new();
        // Frame 1: valid
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(b"abc");
        // Frame 2: declares 100 bytes but only 5 follow.
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(b"short");

        let parsed: Vec<&[u8]> = parse_framed_rows(&buf).collect();
        assert_eq!(
            parsed.len(),
            1,
            "only the valid first frame should be yielded"
        );
        assert_eq!(parsed[0], b"abc");
    }

    /// Empty buffer yields no rows.
    #[test]
    fn parse_framed_rows_empty_buffer() {
        let parsed: Vec<&[u8]> = parse_framed_rows(&[]).collect();
        assert!(parsed.is_empty());
    }

    /// Zero-length row (header = 0x00000000, no body) is a valid frame.
    #[test]
    fn parse_framed_rows_zero_length_row() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // zero-length frame
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(b"data");

        let parsed: Vec<&[u8]> = parse_framed_rows(&buf).collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], b"");
        assert_eq!(parsed[1], b"data");
    }

    // ── Round-trip tests (Linux + io_uring) ───────────────────────────────

    #[cfg(target_os = "linux")]
    mod io_tests {
        use super::super::{SpillPartitionWriter, parse_framed_rows};
        use crate::data::io::uring_reader::UringReader;

        fn make_row(size: usize, seed: u8) -> Vec<u8> {
            (0..size)
                .map(|i| ((i as u64 + seed as u64) % 256) as u8)
                .collect()
        }

        /// Round-trip: write several rows, read back via UringReader, parse
        /// with parse_framed_rows, assert exact equality in order.
        #[test]
        fn round_trip_varied_rows() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("partition_0.spill");

            let rows: Vec<Vec<u8>> = vec![
                make_row(0, 0),               // empty row
                make_row(17, 1),              // small row
                make_row(4096, 2),            // page-sized row
                make_row(1024 * 1024 + 1, 3), // just over 1 MiB
                make_row(255, 200),           // another small
            ];

            {
                let mut w = SpillPartitionWriter::create(&path)
                    .expect("UringWriter should be available on Linux");
                for row in &rows {
                    w.append_row(row).expect("append_row must succeed");
                }
                w.finish().expect("finish must succeed");
            }

            // Read back via UringReader.
            let mut reader = UringReader::with_config(8, 4, 8 * 1024 * 1024)
                .expect("UringReader should be available on Linux");
            let bufs = reader.read_files(&[path.as_path()]);
            assert_eq!(bufs.len(), 1, "expected one result for one path");

            let parsed: Vec<&[u8]> = parse_framed_rows(&bufs[0]).collect();
            assert_eq!(parsed.len(), rows.len(), "row count must match");
            for (i, (expected, got)) in rows.iter().zip(parsed.iter()).enumerate() {
                assert_eq!(
                    *got,
                    expected.as_slice(),
                    "row {i} content mismatch (len expected={}, got={})",
                    expected.len(),
                    got.len()
                );
            }
        }

        /// A single large row round-trips correctly.
        #[test]
        fn round_trip_single_large_row() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("large.spill");

            let row = make_row(3 * 1024 * 1024, 7); // 3 MiB
            {
                let mut w = SpillPartitionWriter::create(&path).unwrap();
                w.append_row(&row).unwrap();
                w.finish().unwrap();
            }

            let mut reader = UringReader::with_config(8, 4, 8 * 1024 * 1024).unwrap();
            let bufs = reader.read_files(&[path.as_path()]);
            let parsed: Vec<&[u8]> = parse_framed_rows(&bufs[0]).collect();

            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0], row.as_slice());
        }

        /// An empty spill file (no rows written) produces an empty iterator.
        #[test]
        fn round_trip_empty_partition() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("empty.spill");

            {
                let w = SpillPartitionWriter::create(&path).unwrap();
                w.finish().unwrap();
            }

            // UringReader treats a zero-size file as failed read (returns empty Vec).
            let mut reader = UringReader::with_config(8, 4, 4096).unwrap();
            let bufs = reader.read_files(&[path.as_path()]);
            let parsed: Vec<&[u8]> = parse_framed_rows(&bufs[0]).collect();
            assert!(
                parsed.is_empty(),
                "no rows should be yielded for an empty partition"
            );
        }
    }
}
