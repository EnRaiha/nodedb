// SPDX-License-Identifier: BUSL-1.1

//! Framed partition spill writer for the grace-hash join.
//!
//! Wraps [`UringWriter`] with a simple length-prefixed framing layer so that
//! many discrete msgpack rows can be stored in a single spill file.
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
//! ## Consumer
//!
//! [`super::grace_spill::PartitionedSpiller`] writes partition spill files via
//! [`SpillPartitionWriter`]; they are read back STREAMING (one row resident at a
//! time) by [`super::grace_repartition::FrameStreamReader`], never as a whole
//! buffer.

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

// ── Tests ─────────────────────────────────────────────────────────────────────

// This writer's framing is exercised end-to-end by the `grace_spill.rs`
// `io_tests`: those push rows through `PartitionedSpiller` (which writes via
// `SpillPartitionWriter`) and read them back through the streaming
// `FrameStreamReader` in `grace_repartition.rs` — the sole reader of these
// spill files. No standalone reader exists to unit-test in isolation here.
