// SPDX-License-Identifier: BUSL-1.1

//! Recursive re-partitioning for the grace-hash join spiller.
//!
//! When [`super::grace_spill::PartitionedSpiller::finish_and_probe`] finds that
//! one spilled partition's BUILD side is larger than `per_partition_budget`,
//! materializing it whole (the old behavior) would re-introduce the very OOM the
//! spiller exists to prevent: under heavy join-key skew one partition can hold
//! most of the build side. Instead, that partition is RE-PARTITIONED — its
//! spill file is read back STREAMING (frame-by-frame, one row resident at a
//! time, never the whole file) and re-hashed with a FRESH seed into `SUB_P`
//! sub-partitions, each written straight to a new spill file. The probe side is
//! re-partitioned with the SAME seed so matching rows co-locate. The sub-
//! partitions are then processed recursively.
//!
//! Distinct keys spread across the sub-partitions on each re-seed, so a skewed-
//! but-diverse build side shrinks geometrically until every sub-partition fits.
//! IDENTICAL-key skew (every row the same join key) is unsplittable at any seed:
//! all rows always collide into one sub-partition. That case is bounded by a
//! depth cap in `grace_spill` which returns a deterministic
//! [`crate::Error::MemoryExhausted`] — never an OOM, never a panic.
//!
//! ## Streaming, not whole-file
//!
//! The re-partition read uses [`FrameStreamReader`], which mirrors the document
//! external-sort [`super::super::document::sort::RunReader`] pattern: io_uring
//! streaming via [`UringSeqReader`] on Linux, a `std::io::BufReader` fallback
//! when io_uring is unavailable. Peak read memory is one refill buffer plus one
//! row — NOT the partition. A truncated/corrupt frame on re-read is a HARD error
//! (mirrors `RunReader::next_row`), never a silent row drop.
//!
//! ## Spill framing
//!
//! Spill files use [`super::spill::SpillPartitionWriter`]'s framing:
//! `[u32 LE len][len bytes]` per row, with NO leading count header (unlike the
//! sort run format). [`FrameStreamReader::next_row`] therefore reads until a
//! clean EOF on the frame boundary rather than counting down a header.

use std::io::{BufReader, Read as _};
use std::path::{Path, PathBuf};

use super::grace_partitioner::partition_hash_seeded;
use super::spill::SpillPartitionWriter;
use crate::data::io::uring_seq_reader::UringSeqReader;

/// Read backend for a spilled partition: io_uring streaming on Linux, blocking
/// `std::fs` (`BufReader`) when io_uring is unavailable. Mirrors `sort.rs`'s
/// `RunBackend`.
enum FrameBackend {
    // Boxed: `UringSeqReader` carries an io_uring ring + chunk buffer and is far
    // larger than the `BufReader` variant; box it to keep the enum compact.
    Uring(Box<UringSeqReader>),
    Std(BufReader<std::fs::File>),
}

/// Streaming reader over a [`SpillPartitionWriter`] spill file
/// (`[u32 LE len][bytes]` frames, no count header).
///
/// One row is resident at a time; the file is never read whole — this is the
/// property that keeps re-partitioning memory-bounded.
pub(super) struct FrameStreamReader {
    backend: FrameBackend,
}

impl FrameStreamReader {
    /// Open `path` for streaming frame reads.
    pub(super) fn open(path: &Path) -> crate::Result<Self> {
        let backend = match UringSeqReader::open_default(path) {
            Some(r) => FrameBackend::Uring(Box::new(r)),
            None => FrameBackend::Std(BufReader::new(std::fs::File::open(path).map_err(|e| {
                crate::Error::Storage {
                    engine: "grace-repartition".into(),
                    detail: format!("spill frame reader open {}: {e}", path.display()),
                }
            })?)),
        };
        Ok(Self { backend })
    }

    /// Read exactly `dst.len()` bytes. `Ok(true)` = filled; `Ok(false)` = clean
    /// EOF before any byte of `dst` (or partway) was filled; `Err` = io failure.
    /// Bridges the two backends to one uniform contract (mirrors `RunReader`).
    fn read_full(backend: &mut FrameBackend, dst: &mut [u8]) -> crate::Result<bool> {
        match backend {
            FrameBackend::Uring(r) => r.read_exact(dst),
            FrameBackend::Std(r) => match r.read_exact(dst) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(e) => Err(crate::Error::Io(e)),
            },
        }
    }

    /// Yield the next framed row, or `Ok(None)` at a clean frame-boundary EOF.
    ///
    /// A frame that begins but does not complete (header present but body short,
    /// or a partial header after the first byte) is CORRUPTION and returns
    /// `Err` — never a silent drop (mirrors `RunReader::next_row`).
    pub(super) fn next_row(&mut self) -> crate::Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        // Clean EOF exactly on a frame boundary → no more rows.
        if !Self::read_full(&mut self.backend, &mut len_buf)? {
            return Ok(None);
        }
        let row_len = u32::from_le_bytes(len_buf) as usize;

        let mut body = vec![0u8; row_len];
        if !Self::read_full(&mut self.backend, &mut body)? {
            return Err(crate::Error::Storage {
                engine: "grace-repartition".into(),
                detail: "spill partition truncated: frame body shorter than declared length".into(),
            });
        }
        Ok(Some(body))
    }
}

/// One side of a (sub-)partition awaiting processing: either rows already
/// resident in RAM (top-level non-spilled partitions, which are ≤budget by
/// construction) or a spilled file on disk that must be size-checked and, if
/// oversized, re-partitioned by streaming.
pub(super) enum PartitionSource {
    /// In-memory rows: `(empty id, value bytes)`. Already ≤budget.
    InMemory(Vec<(String, Vec<u8>)>),
    /// A spilled file path whose size is not yet known to fit the budget.
    Spilled(PathBuf),
}

impl PartitionSource {
    /// Build-side byte size used to decide whether this source fits the budget.
    ///
    /// In-memory: the sum of value bytes (the same quantity the spiller tracks
    /// against the budget). Spilled: the on-disk file length (an upper bound that
    /// includes the 4-byte-per-row framing overhead — conservative, never an
    /// under-count, so it can only over-trigger re-partitioning, never OOM).
    pub(super) fn size_bytes(&self) -> crate::Result<usize> {
        match self {
            PartitionSource::InMemory(rows) => Ok(rows.iter().map(|(_, v)| v.len()).sum()),
            PartitionSource::Spilled(path) => {
                let meta = std::fs::metadata(path).map_err(|e| crate::Error::Storage {
                    engine: "grace-repartition".into(),
                    detail: format!("stat spilled partition {}: {e}", path.display()),
                })?;
                Ok(meta.len() as usize)
            }
        }
    }

    /// Materialize this source's rows into RAM. Only called once the source is
    /// known to fit the budget, so this is bounded.
    ///
    /// In-memory: returned by move. Spilled: streamed back frame-by-frame (one
    /// row resident at a time during the read); the id is always empty (the join
    /// output never reads the id — see `grace_spill` module docs).
    pub(super) fn materialize(self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        match self {
            PartitionSource::InMemory(rows) => Ok(rows),
            PartitionSource::Spilled(path) => {
                let mut reader = FrameStreamReader::open(&path)?;
                let mut out = Vec::new();
                while let Some(row) = reader.next_row()? {
                    out.push((String::new(), row));
                }
                Ok(out)
            }
        }
    }
}

/// Stream `src`'s rows frame-by-frame and re-hash each into one of `sub_p`
/// new spill files under `sub_dir`, keyed by `partition_hash_seeded(row, keys,
/// seed) % sub_p`. Returns the per-sub-partition spill paths (length `sub_p`).
///
/// Each output is a [`SpillPartitionWriter`]; the row is appended straight to
/// disk so no sub-partition is ever fully resident. The reader holds at most one
/// row in RAM at a time.
///
/// # io_uring fallback
///
/// `src` reaches this function ONLY when it is a [`PartitionSource::Spilled`]
/// file, which means a `SpillPartitionWriter` was successfully created on the
/// write path — so io_uring (or its `std::fs` fallback) is available and the
/// sub-partition writers below can likewise be created. If a writer cannot be
/// created we treat it as a hard error (`Storage`) rather than silently dropping
/// rows, because dropping rows would corrupt the join result.
pub(super) fn repartition_side<S: AsRef<str>>(
    src: PartitionSource,
    keys: &[S],
    seed: u64,
    sub_p: usize,
    sub_dir: &Path,
    side_tag: &str,
) -> crate::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(sub_dir).map_err(|e| crate::Error::Storage {
        engine: "grace-repartition".into(),
        detail: format!(
            "failed to create re-partition sub-dir {}: {e}",
            sub_dir.display()
        ),
    })?;

    // Create one writer per sub-partition up front. `SpillPartitionWriter` is not
    // `Clone`, so build the Vec with an iterator.
    let mut writers: Vec<SpillPartitionWriter> = Vec::with_capacity(sub_p);
    for sp in 0..sub_p {
        let path = sub_dir.join(format!("sp{sp}.{side_tag}.spill"));
        let w = SpillPartitionWriter::create(&path).ok_or_else(|| crate::Error::Storage {
            engine: "grace-repartition".into(),
            detail: format!(
                "failed to create sub-partition spill writer {}",
                path.display()
            ),
        })?;
        writers.push(w);
    }

    // Stream the source and route each row by the SEEDED hash.
    match src {
        PartitionSource::InMemory(rows) => {
            for (_, value) in rows {
                let sp = (partition_hash_seeded(&value, keys, seed) % sub_p as u64) as usize;
                // `sp < sub_p` by construction of the modulo; index is safe but
                // use `get_mut` to avoid any panic path in lib code.
                let w = writers
                    .get_mut(sp)
                    .ok_or_else(|| sub_index_error(sp, sub_p))?;
                w.append_row(&value)?;
            }
        }
        PartitionSource::Spilled(path) => {
            let mut reader = FrameStreamReader::open(&path)?;
            while let Some(value) = reader.next_row()? {
                let sp = (partition_hash_seeded(&value, keys, seed) % sub_p as u64) as usize;
                let w = writers
                    .get_mut(sp)
                    .ok_or_else(|| sub_index_error(sp, sub_p))?;
                w.append_row(&value)?;
            }
        }
    }

    // Finish each writer, collecting the sub-partition spill paths in order.
    let mut paths = Vec::with_capacity(sub_p);
    for w in writers {
        paths.push(w.finish()?);
    }
    Ok(paths)
}

/// Construct the (unreachable-in-practice) sub-partition index error without
/// panicking — `sp` is always `< sub_p` because it is a modulo result.
fn sub_index_error(sp: usize, sub_p: usize) -> crate::Error {
    crate::Error::Storage {
        engine: "grace-repartition".into(),
        detail: format!("sub-partition index {sp} out of range (sub_p={sub_p})"),
    }
}
