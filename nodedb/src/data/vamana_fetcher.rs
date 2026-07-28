// SPDX-License-Identifier: BUSL-1.1

//! io_uring-backed `NodeFetcher` for disk-resident Vamana indexes.
//!
//! `IoUringNodeFetcher` implements `nodedb_vector::vamana::NodeFetcher` using
//! the Data Plane's `UringReader` infrastructure.  It is intentionally
//! `!Send` — one instance lives on a single TPC core alongside the
//! `UringReader` it wraps.
//!
//! ## Pre-fetch model
//!
//! `prefetch_batch` submits `IORING_OP_READ` SQEs for a set of node indices
//! in a single batch, then returns immediately without waiting.  The in-flight
//! reads land asynchronously while the beam-search loop evaluates distances
//! for the *current* frontier.  When `fetch_fp32` is later called for those
//! same indices it collects completions from the ring without re-submitting.
//!
//! If `fetch_fp32` is called for an index that was never pre-fetched (e.g. the
//! entry-point seed), it falls back to a synchronous read via a dedicated
//! single-op submission.
//!
//! ## TPC Page Fault Hazard compliance
//!
//! All reads go through `IORING_OP_READ` (not mmap), so the TPC reactor never
//! takes a major page fault on the vectors block.

use std::collections::HashMap;
use std::os::unix::io::AsRawFd;

use nodedb_vector::vamana::node_fetcher::NodeFetcher;
use nodedb_vector::vamana::storage::{VamanaStorageLayout, vector_offset};

use crate::data::io::aligned_buf::{ALIGNMENT, AlignedBuf};

/// Queue depth for the dedicated Vamana io_uring instance.
const VAMANA_QUEUE_DEPTH: u32 = 128;

/// io_uring-backed node fetcher for a single Vamana index file.
///
/// `!Send` — lives on one TPC Data Plane core.
pub struct IoUringNodeFetcher {
    ring: io_uring::IoUring,
    file: std::fs::File,
    layout: VamanaStorageLayout,
    /// Completed reads indexed by node index.  Populated by
    /// `drain_completions`; consumed (and cleared) by `fetch_fp32`.
    completed: HashMap<u32, Vec<f32>>,
    /// Number of SQEs currently in-flight (submitted but not yet drained).
    in_flight: u32,
    /// Per-fetch aligned buffer pool.  Recycled across prefetch batches.
    buf_pool: Vec<AlignedBuf>,
    /// Free slots in `buf_pool`.
    free_bufs: Vec<usize>,
    /// Mapping from SQE user_data (node index) → pool slot, so completions
    /// know which buffer to decode.
    pending: HashMap<u32, usize>,
}

impl IoUringNodeFetcher {
    /// Open a Vamana index file for io_uring-backed vector fetching.
    ///
    /// `pool_size` is the number of pre-allocated aligned read buffers.
    /// A good default is `2 * beam_width` (e.g. 128 for `l_search = 64`).
    pub fn open(
        path: &std::path::Path,
        layout: VamanaStorageLayout,
        pool_size: usize,
    ) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let ring = io_uring::IoUring::new(VAMANA_QUEUE_DEPTH).map_err(std::io::Error::other)?;

        let vector_bytes = layout.dim as usize * 4; // f32 = 4 bytes
        let buf_size = align_up(vector_bytes, ALIGNMENT);

        let mut buf_pool = Vec::with_capacity(pool_size);
        let mut free_bufs = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            match AlignedBuf::new(buf_size) {
                Ok(buf) => {
                    buf_pool.push(buf);
                    free_bufs.push(i);
                }
                Err(_) => break,
            }
        }

        if buf_pool.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "IoUringNodeFetcher: failed to allocate buffer pool",
            ));
        }

        Ok(Self {
            ring,
            file,
            layout,
            completed: HashMap::new(),
            in_flight: 0,
            buf_pool,
            free_bufs,
            pending: HashMap::new(),
        })
    }

    /// Drain all pending completions into `self.completed`.
    ///
    /// Decodes each buffer as `dim` little-endian `f32` values and stores
    /// the result keyed by node index.  Pool buffers are returned to
    /// `free_bufs`.
    fn drain_completions(&mut self) {
        if self.in_flight == 0 {
            return;
        }

        // Collect all available CQEs.
        let mut drained = 0u32;
        loop {
            // Drain whatever is available without blocking.
            let _ = self.ring.submit();
            let Some(cqe) = self.ring.completion().next() else {
                break;
            };

            let node_idx = cqe.user_data() as u32;
            let result = cqe.result();
            drained += 1;

            let Some(slot) = self.pending.remove(&node_idx) else {
                continue;
            };

            if result >= 0 {
                let expected = self.layout.dim as usize * 4;
                let actual = result as usize;
                if actual >= expected {
                    let floats = decode_f32_le(
                        // SAFETY: io_uring wrote `actual` bytes into buf_pool[slot].
                        // We only decode `expected` bytes (dim * 4).
                        unsafe { self.buf_pool[slot].as_slice(expected) },
                        self.layout.dim as usize,
                    );
                    self.completed.insert(node_idx, floats);
                }
            }
            // Return pool slot regardless of success/failure.
            self.free_bufs.push(slot);
        }

        self.in_flight = self.in_flight.saturating_sub(drained);
    }

    /// Submit a single synchronous read for `node_idx` and wait for it.
    ///
    /// Used as a fallback when `fetch_fp32` is called for a node that was
    /// never pre-fetched (e.g. the entry-point seed on first query).
    fn read_sync(&mut self, node_idx: u32) -> Option<Vec<f32>> {
        let off = vector_offset(&self.layout, node_idx as u64);
        let dim = self.layout.dim as usize;
        let needed = dim * 4;
        let buf_size = align_up(needed, ALIGNMENT);

        let mut buf = AlignedBuf::new(buf_size).ok()?;
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let op = io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), buf_size as u32)
            .offset(off)
            .build()
            .user_data(node_idx as u64);

        // SAFETY: `buf` outlives the SQE submission and the wait.
        unsafe {
            self.ring.submission().push(&op).ok()?;
        }
        self.ring.submit_and_wait(1).ok()?;

        let cqe = self.ring.completion().next()?;
        if cqe.result() < needed as i32 {
            return None;
        }

        // SAFETY: io_uring wrote at least `needed` bytes into `buf`.
        let floats = decode_f32_le(unsafe { buf.as_slice(needed) }, dim);
        Some(floats)
    }
}

impl NodeFetcher for IoUringNodeFetcher {
    fn dim(&self) -> usize {
        self.layout.dim as usize
    }

    /// Submit `IORING_OP_READ` SQEs for each index in `indices`.
    ///
    /// Indices that are already completed (from a prior prefetch), already
    /// in-flight, or cannot get a pool buffer are silently skipped —
    /// `fetch_fp32` will handle them via a sync fallback.
    fn prefetch_batch(&mut self, indices: &[u32]) {
        // Drain any already-completed CQEs to free pool slots.
        self.drain_completions();

        let dim = self.layout.dim as usize;
        let needed = dim * 4;
        let buf_size = align_up(needed, ALIGNMENT);
        let fd = io_uring::types::Fd(self.file.as_raw_fd());

        for &node_idx in indices {
            // Skip: already completed, already in-flight.
            if self.completed.contains_key(&node_idx) || self.pending.contains_key(&node_idx) {
                continue;
            }
            // Skip: no free buffer.
            let Some(slot) = self.free_bufs.pop() else {
                continue;
            };
            // Grow pool buffer to required size if needed (shouldn't happen
            // since pool is sized at open, but guard against it).
            if self.buf_pool[slot].capacity() < buf_size {
                self.free_bufs.push(slot);
                continue;
            }

            let off = vector_offset(&self.layout, node_idx as u64);
            let op =
                io_uring::opcode::Read::new(fd, self.buf_pool[slot].as_mut_ptr(), buf_size as u32)
                    .offset(off)
                    .build()
                    .user_data(node_idx as u64);

            // SAFETY: buf_pool[slot] outlives the SQE and completion.
            // `self.pending` keeps the slot reserved until drain.
            let pushed = unsafe { self.ring.submission().push(&op).is_ok() };
            if pushed {
                self.pending.insert(node_idx, slot);
                self.in_flight += 1;
            } else {
                // SQ full — return the slot.
                self.free_bufs.push(slot);
            }
        }

        // Flush the submission queue to the kernel without waiting.
        let _ = self.ring.submit();
    }

    fn fetch_fp32(&mut self, idx: u32) -> Option<Vec<f32>> {
        // Drain any completions that arrived since last prefetch/fetch.
        self.drain_completions();

        // Happy path: pre-fetch already landed.
        if let Some(v) = self.completed.remove(&idx) {
            return Some(v);
        }

        // If still in-flight, wait for it specifically.
        if self.pending.contains_key(&idx) {
            // Wait until at least one completion arrives and drain.
            let _ = self.ring.submit_and_wait(1);
            self.drain_completions();
            if let Some(v) = self.completed.remove(&idx) {
                return Some(v);
            }
        }

        // Fallback: index was never pre-fetched (entry-point seed).
        if idx < self.layout.num_nodes as u32 {
            return self.read_sync(idx);
        }

        None
    }
}

/// Round `value` up to the next multiple of `align` (must be a power of two).
#[inline]
fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Decode `dim` little-endian `f32` values from a byte slice.
///
/// `bytes` must be at least `dim * 4` bytes long.
fn decode_f32_le(bytes: &[u8], dim: usize) -> Vec<f32> {
    // `dim` originates in the validated Vamana layout; the byte slice remains
    // the direct proof for this allocation bound.
    let mut out = Vec::with_capacity(bytes.len() / 4);
    for i in 0..dim {
        let start = i * 4;
        let val = f32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
        out.push(val);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use nodedb_vector::vamana::storage::VamanaStorageLayout;

    use super::*;

    /// Write a minimal Vamana vectors block (no adjacency, no header) to a
    /// temp file and verify that `IoUringNodeFetcher` reads it correctly.
    fn write_vectors_file(path: &std::path::Path, vectors: &[Vec<f32>]) {
        let mut f = std::fs::File::create(path).unwrap();
        for vec in vectors {
            for &x in vec {
                f.write_all(&x.to_le_bytes()).unwrap();
            }
        }
        f.sync_all().unwrap();
    }

    #[test]
    fn fetch_single_node_no_prefetch() {
        let dir = tempfile::tempdir().unwrap();
        let vecs = vec![
            vec![1.0_f32, 2.0, 3.0, 4.0],
            vec![5.0_f32, 6.0, 7.0, 8.0],
            vec![9.0_f32, 10.0, 11.0, 12.0],
        ];
        let dim = 4u32;
        let n = vecs.len() as u64;

        // Build a layout with adjacency_offset = HEADER_BYTES = 64, r = 0.
        // vectors_offset = 64 + 0 * 4 * 3 = 64.
        // We write the vectors block starting at byte 0 of our test file,
        // so we create a layout where vectors_offset = 0.
        let layout = VamanaStorageLayout {
            dim,
            r: 0,
            alpha: 1.2,
            num_nodes: n,
            entry: 0,
            adjacency_offset: 0,
            vectors_offset: 0,
        };

        let path = dir.path().join("vamana.vec");
        write_vectors_file(&path, &vecs);

        let mut fetcher = IoUringNodeFetcher::open(&path, layout, 8).unwrap();

        let got = fetcher.fetch_fp32(1).unwrap();
        assert_eq!(got, vecs[1], "node 1 vector mismatch");
    }

    #[test]
    fn prefetch_batch_then_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let vecs: Vec<Vec<f32>> = (0..8u32)
            .map(|i| vec![i as f32, (i + 1) as f32, (i + 2) as f32, (i + 3) as f32])
            .collect();
        let dim = 4u32;
        let n = vecs.len() as u64;

        let layout = VamanaStorageLayout {
            dim,
            r: 0,
            alpha: 1.2,
            num_nodes: n,
            entry: 0,
            adjacency_offset: 0,
            vectors_offset: 0,
        };

        let path = dir.path().join("vamana.vec");
        write_vectors_file(&path, &vecs);

        let mut fetcher = IoUringNodeFetcher::open(&path, layout, 8).unwrap();

        // Pre-fetch nodes 0..4 in one batch.
        fetcher.prefetch_batch(&[0, 1, 2, 3]);

        for i in 0u32..4 {
            let got = fetcher.fetch_fp32(i).unwrap();
            assert_eq!(
                got, vecs[i as usize],
                "node {i} vector mismatch after prefetch"
            );
        }
    }

    #[test]
    fn out_of_range_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let vecs = vec![vec![1.0_f32, 2.0]];
        let layout = VamanaStorageLayout {
            dim: 2,
            r: 0,
            alpha: 1.2,
            num_nodes: 1,
            entry: 0,
            adjacency_offset: 0,
            vectors_offset: 0,
        };
        let path = dir.path().join("v.vec");
        write_vectors_file(&path, &vecs);

        let mut fetcher = IoUringNodeFetcher::open(&path, layout, 4).unwrap();
        assert!(fetcher.fetch_fp32(99).is_none());
    }

    #[test]
    fn decode_f32_le_roundtrip() {
        let values = vec![1.5_f32, -3.25, 0.0, f32::MAX];
        let bytes: Vec<u8> = values.iter().flat_map(|x| x.to_le_bytes()).collect();
        let decoded = decode_f32_le(&bytes, values.len());
        assert_eq!(decoded, values);
    }
}
