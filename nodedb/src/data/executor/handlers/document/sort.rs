// SPDX-License-Identifier: BUSL-1.1

//! External sort infrastructure: sort helpers, run files, and k-way merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, Read as _};
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::io::uring_seq_reader::UringSeqReader;
use crate::data::io::uring_writer::UringWriter;

use nodedb_query::msgpack_scan;

impl CoreLoop {
    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a named per-run file written via io_uring, then k-way merge to
    /// produce the final sorted output.
    ///
    /// Spill files are named (`run-N.spill`) and written through [`UringWriter`]
    /// so the per-core io_uring reactor is never stalled by blocking `std::fs`
    /// content writes. They are unlinked by [`SortSpillCleanup`] (a Drop guard),
    /// not by tempfile auto-delete. The merge reads each run back incrementally
    /// via [`UringSeqReader`] — one row at a time — so peak read memory is one
    /// refill buffer per run, not the whole run.
    pub(super) fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[(String, bool)],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // Spill directory for the named sort run files. `create_dir_all` is a
        // bounded metadata op (not bulk content I/O), so it stays `std::fs`.
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("failed to create sort spill dir: {e}"),
        })?;

        let total_rows = rows.len();

        // Declared FIRST so it Drops LAST — after the readers below close their
        // fds — guaranteeing the spill files are unlinked only once no reader
        // still holds them open.
        let mut cleanup = SortSpillCleanup {
            dir: spill_dir.clone(),
            paths: Vec::new(),
        };

        for (run_idx, chunk) in rows.chunks(self.query_tuning.sort_run_size).enumerate() {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys);

            // Build the framed run into one buffer and write it in a single
            // pass. Writing each tiny frame field separately would be hundreds
            // of thousands of micro io_uring writes.
            let mut framed = Vec::new();
            framed.extend_from_slice(&(run.len() as u32).to_le_bytes());
            for (id, val) in &run {
                let id_bytes = id.as_bytes();
                framed.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
                framed.extend_from_slice(id_bytes);
                framed.extend_from_slice(&(val.len() as u32).to_le_bytes());
                framed.extend_from_slice(val);
            }

            let run_path = spill_dir.join(format!("run-{run_idx}.spill"));
            write_sort_run(&run_path, &framed)?;
            cleanup.paths.push(run_path);
        }

        debug!(
            core = self.core_id,
            runs = cleanup.paths.len(),
            total_rows,
            "external sort: spilled runs"
        );

        // Build readers propagating errors — a run whose reader fails to init is
        // a hard error, never a silently dropped run.
        let mut readers: Vec<RunReader> = Vec::with_capacity(cleanup.paths.len());
        for (idx, path) in cleanup.paths.iter().enumerate() {
            readers.push(RunReader::open(path, idx)?);
        }

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(row) = reader.next_row()? {
                heap.push(Reverse(MergeEntry {
                    row,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        let mut result = Vec::with_capacity(output_limit.min(total_rows));
        while let Some(Reverse(entry)) = heap.pop() {
            result.push(entry.row);
            if result.len() >= output_limit {
                break;
            }
            if let Some(next_row) = readers[entry.run_idx].next_row()? {
                heap.push(Reverse(MergeEntry {
                    row: next_row,
                    run_idx: entry.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        Ok(result)
    }
}

/// Drop guard that unlinks named sort spill files (and their directory).
///
/// Named spill files do not auto-unlink (unlike tempfile handles), so each is
/// removed explicitly. Declared before the [`RunReader`]s in `external_sort` so
/// it Drops last — after the readers' fds close. Unlink is a bounded metadata
/// op, not bulk content I/O, so it stays plain `std::fs`.
struct SortSpillCleanup {
    dir: PathBuf,
    paths: Vec<PathBuf>,
}

impl Drop for SortSpillCleanup {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
        let _ = std::fs::remove_dir(&self.dir);
    }
}

/// Write one framed sort-run blob to `path`.
///
/// Uses [`UringWriter`] when io_uring is available; otherwise falls back to a
/// blocking `std::fs::write` (on a non-io_uring platform there is no per-core
/// reactor to stall, so the blocking call is plane-safe).
fn write_sort_run(path: &Path, bytes: &[u8]) -> crate::Result<()> {
    match UringWriter::new(path) {
        Some(mut w) => {
            w.append(bytes)?;
            w.finish()?;
            Ok(())
        }
        None => std::fs::write(path, bytes).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("sort spill write error: {e}"),
        }),
    }
}

/// Compare two raw msgpack documents by a list of sort keys.
///
/// Uses binary field extraction — no decode. Used by both in-memory
/// sort and external merge sort for consistent ordering.
pub(super) fn compare_docs_by_keys_binary(
    a_bytes: &[u8],
    b_bytes: &[u8],
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (field, asc) in sort_keys {
        let a_range = msgpack_scan::extract_field(a_bytes, 0, field);
        let b_range = msgpack_scan::extract_field(b_bytes, 0, field);

        let cmp = match (a_range, b_range) {
            (Some(ar), Some(br)) => msgpack_scan::compare_field_bytes(a_bytes, ar, b_bytes, br),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };

        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

/// Pre-extracted sort key offsets for a single row.
/// Each entry is `Option<(usize, usize)>` — byte range of the sort key value.
type SortKeyOffsets = Vec<Option<(usize, usize)>>;

pub(in crate::data::executor) fn sort_rows(
    rows: &mut [(String, Vec<u8>)],
    sort_keys: &[(String, bool)],
) {
    if sort_keys.is_empty() {
        return;
    }

    // Pre-extract sort key offsets for all rows — one scan per row instead
    // of O(N log N) scans during comparisons.
    let key_offsets: Vec<SortKeyOffsets> = rows
        .iter()
        .map(|(_, bytes)| {
            sort_keys
                .iter()
                .map(|(field, _)| msgpack_scan::extract_field(bytes, 0, field))
                .collect()
        })
        .collect();

    // Sort indices using pre-extracted offsets.
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&ai, &bi| {
        compare_with_preextracted(
            &rows[ai].1,
            &key_offsets[ai],
            &rows[bi].1,
            &key_offsets[bi],
            sort_keys,
        )
    });

    // Apply permutation in-place. `key_offsets` is no longer needed after
    // sorting the index; it is dropped here.
    drop(key_offsets);
    apply_permutation(rows, indices);
}

/// Compare two docs using pre-extracted sort key offsets.
fn compare_with_preextracted(
    a_bytes: &[u8],
    a_offsets: &[Option<(usize, usize)>],
    b_bytes: &[u8],
    b_offsets: &[Option<(usize, usize)>],
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (i, (_, asc)) in sort_keys.iter().enumerate() {
        let cmp = match (a_offsets[i], b_offsets[i]) {
            (Some(ar), Some(br)) => msgpack_scan::compare_field_bytes(a_bytes, ar, b_bytes, br),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };
        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

/// Apply a permutation to rows using the sorted index order.
///
/// `indices[i]` = the original row index that should appear at position `i`.
fn apply_permutation(rows: &mut [(String, Vec<u8>)], indices: Vec<usize>) {
    // Wrap each row in `Option` so we can move individual elements out by
    // index without cloning. Each slot is taken exactly once during the
    // scatter, so no element is ever double-moved.
    let mut src: Vec<Option<(String, Vec<u8>)>> = rows
        .iter_mut()
        .map(|r| Some(std::mem::replace(r, (String::new(), Vec::new()))))
        .collect();
    for (target_pos, &src_idx) in indices.iter().enumerate() {
        // `indices` is always a permutation of `0..rows.len()`, so every slot
        // is taken exactly once. The `None` arm is unreachable in practice;
        // the debug assert catches logic regressions in tests.
        debug_assert!(
            src[src_idx].is_some(),
            "apply_permutation: duplicate index {src_idx}"
        );
        rows[target_pos] = src[src_idx].take().unwrap_or_default();
    }
}

/// Read backend for a sort run: io_uring streaming on Linux, blocking
/// `std::fs` (`BufReader`) when io_uring is unavailable.
enum RunBackend {
    Uring(UringSeqReader),
    Std(BufReader<std::fs::File>),
}

pub(super) struct RunReader {
    backend: RunBackend,
    remaining: u32,
    pub(super) run_idx: usize,
}

impl RunReader {
    pub(super) fn open(path: &Path, run_idx: usize) -> crate::Result<Self> {
        let mut backend = match UringSeqReader::open_default(path) {
            Some(r) => RunBackend::Uring(r),
            None => RunBackend::Std(BufReader::new(std::fs::File::open(path).map_err(|e| {
                crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("run reader open: {e}"),
                }
            })?)),
        };

        let mut buf4 = [0u8; 4];
        if !Self::read_full(&mut backend, &mut buf4)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: missing count header".into(),
            });
        }
        let count = u32::from_le_bytes(buf4);

        Ok(Self {
            backend,
            remaining: count,
            run_idx,
        })
    }

    /// Read exactly `dst.len()` bytes. `Ok(true)` = filled; `Ok(false)` = clean
    /// EOF before fill; `Err` = io failure. Bridges the two backends to one
    /// uniform contract.
    fn read_full(backend: &mut RunBackend, dst: &mut [u8]) -> crate::Result<bool> {
        match backend {
            RunBackend::Uring(r) => r.read_exact(dst),
            RunBackend::Std(r) => match r.read_exact(dst) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(e) => Err(crate::Error::Io(e)),
            },
        }
    }

    pub(super) fn next_row(&mut self) -> crate::Result<Option<(String, Vec<u8>)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;

        let mut buf4 = [0u8; 4];

        // A run that ends before `remaining` rows have been read is corruption —
        // error, never silently drop rows.
        if !Self::read_full(&mut self.backend, &mut buf4)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }
        let id_len = u32::from_le_bytes(buf4) as usize;
        let mut id_buf = vec![0u8; id_len];
        if !Self::read_full(&mut self.backend, &mut id_buf)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }
        let id = String::from_utf8(id_buf).map_err(|_| crate::Error::Storage {
            engine: "sort".into(),
            detail: "sort run corrupt: id not valid utf-8".into(),
        })?;

        if !Self::read_full(&mut self.backend, &mut buf4)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }
        let val_len = u32::from_le_bytes(buf4) as usize;
        let mut val_buf = vec![0u8; val_len];
        if !Self::read_full(&mut self.backend, &mut val_buf)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }

        Ok(Some((id, val_buf)))
    }
}

pub(super) struct MergeEntry {
    pub(super) row: (String, Vec<u8>),
    pub(super) run_idx: usize,
    pub(super) sort_keys: Vec<(String, bool)>,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_docs_by_keys_binary(&self.row.1, &other.row.1, &self.sort_keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn sort_by_int_field_asc() {
        let mut rows = vec![
            (
                "a".into(),
                encode(&serde_json::json!({"id": "a", "val": 30})),
            ),
            (
                "b".into(),
                encode(&serde_json::json!({"id": "b", "val": 10})),
            ),
            (
                "c".into(),
                encode(&serde_json::json!({"id": "c", "val": 20})),
            ),
        ];
        sort_rows(&mut rows, &[("val".into(), true)]);
        let order: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(order, vec!["b", "c", "a"], "ASC by val: 10, 20, 30");
    }

    #[test]
    fn sort_by_int_field_desc() {
        let mut rows = vec![
            (
                "a".into(),
                encode(&serde_json::json!({"id": "a", "val": 30})),
            ),
            (
                "b".into(),
                encode(&serde_json::json!({"id": "b", "val": 10})),
            ),
            (
                "c".into(),
                encode(&serde_json::json!({"id": "c", "val": 20})),
            ),
        ];
        sort_rows(&mut rows, &[("val".into(), false)]);
        assert_eq!(rows[0].0, "a", "DESC first should be a (val=30)");
        assert_eq!(rows[1].0, "c", "DESC second should be c (val=20)");
        assert_eq!(rows[2].0, "b", "DESC third should be b (val=10)");
    }

    #[test]
    fn sort_by_string_field_asc() {
        let mut rows = vec![
            (
                "1".into(),
                encode(&serde_json::json!({"id": "1", "name": "Charlie"})),
            ),
            (
                "2".into(),
                encode(&serde_json::json!({"id": "2", "name": "Alice"})),
            ),
            (
                "3".into(),
                encode(&serde_json::json!({"id": "3", "name": "Bob"})),
            ),
        ];
        sort_rows(&mut rows, &[("name".into(), true)]);
        assert_eq!(rows[0].0, "2", "first should be Alice");
        assert_eq!(rows[2].0, "1", "last should be Charlie");
    }
}

/// End-to-end spill+merge coverage exercising the real io_uring spill write
/// (`write_sort_run`) and streaming read (`RunReader`) path.
///
/// Tested at the primitive level (write_sort_run + RunReader + manual k-way
/// heap merge) rather than via `CoreLoop::external_sort`, because constructing
/// a `CoreLoop` requires a full Data-Plane core bring-up; the merge logic here
/// is a faithful copy of `external_sort`'s loop so it covers the same path.
#[cfg(all(test, target_os = "linux"))]
mod spill_merge_tests {
    use super::*;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    /// Build a framed run blob (count header + per-row frames) byte-identical to
    /// `external_sort`'s spill layout.
    fn frame(rows: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for (id, val) in rows {
            let idb = id.as_bytes();
            out.extend_from_slice(&(idb.len() as u32).to_le_bytes());
            out.extend_from_slice(idb);
            out.extend_from_slice(&(val.len() as u32).to_le_bytes());
            out.extend_from_slice(val);
        }
        out
    }

    fn row(id: &str, val: i64) -> (String, Vec<u8>) {
        (
            id.to_string(),
            encode(&serde_json::json!({"id": id, "val": val})),
        )
    }

    /// Write several internally-sorted runs, open them via `RunReader`, drive
    /// the same heap merge `external_sort` uses, and assert the output is
    /// globally sorted and contains exactly every row (no drops).
    #[test]
    fn spill_then_kway_merge_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sort_keys = vec![("val".to_string(), true)];

        // Three runs, each internally sorted ascending by `val`.
        let runs = [
            vec![row("a", 1), row("d", 4), row("g", 7)],
            vec![row("b", 2), row("e", 5), row("h", 8)],
            vec![row("c", 3), row("f", 6), row("i", 9)],
        ];

        let mut readers: Vec<RunReader> = Vec::new();
        for (idx, run) in runs.iter().enumerate() {
            let path = dir.path().join(format!("run-{idx}.spill"));
            write_sort_run(&path, &frame(run)).unwrap();
            readers.push(RunReader::open(&path, idx).unwrap());
        }

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(r) = reader.next_row().unwrap() {
                heap.push(Reverse(MergeEntry {
                    row: r,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.clone(),
                }));
            }
        }

        let mut out: Vec<String> = Vec::new();
        while let Some(Reverse(entry)) = heap.pop() {
            out.push(entry.row.0.clone());
            if let Some(next) = readers[entry.run_idx].next_row().unwrap() {
                heap.push(Reverse(MergeEntry {
                    row: next,
                    run_idx: entry.run_idx,
                    sort_keys: sort_keys.clone(),
                }));
            }
        }

        // Globally sorted by val: a..i, and every row present exactly once.
        assert_eq!(out, vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
    }

    /// A run whose count header claims more rows than its bytes provide must
    /// surface an `Err` from `next_row` — never silently return fewer rows.
    #[test]
    fn truncated_run_errors_not_silent_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trunc.spill");

        // Header says 3 rows, but only 1 row of frame bytes follows.
        let one = vec![row("x", 1)];
        let mut bytes = frame(&one);
        // Overwrite the count header (first 4 bytes) with 3.
        bytes[0..4].copy_from_slice(&3u32.to_le_bytes());
        write_sort_run(&path, &bytes).unwrap();

        let mut reader = RunReader::open(&path, 0).unwrap();
        // First row reads back fine.
        assert!(reader.next_row().unwrap().is_some());
        // Second row: bytes exhausted but remaining > 0 → must error.
        assert!(
            reader.next_row().is_err(),
            "truncated run must error, not silently drop rows"
        );
    }
}
