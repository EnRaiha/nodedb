// SPDX-License-Identifier: BUSL-1.1

//! Batched io_uring reader for columnar segment files.
//!
//! Submits multiple file reads as io_uring SQEs in a single batch, then
//! waits for all completions. The kernel processes reads in parallel
//! internally, giving ~2-4x throughput vs sequential `std::fs::read()`.
//!
//! ## Design
//!
//! - One `UringReader` per TPC core (stored in `CoreLoop`, `!Send`)
//! - Reusable aligned buffer pool (pre-allocated, avoids per-read allocation)
//! - `read_files()` opens files, submits reads, waits for all completions
//! - Falls back gracefully if io_uring setup fails (old kernels)
//!
//! ## Integration
//!
//! ```text
//! aggregate_partition(dir, ..., uring_reader)
//!   → uring_reader.read_files([timestamp.col, value.col, qtype.col])
//!   → submit 3 IORING_OP_READ SQEs
//!   → submit_and_wait(3)
//!   → kernel reads 3 files in parallel from NVMe
//!   → return [Vec<u8>, Vec<u8>, Vec<u8>]
//! ```

use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "linux")]
use std::time::Instant;

#[cfg(target_os = "linux")]
use super::aligned_buf::{ALIGNMENT, AlignedBuf};
use super::io_metrics::IoMetrics;
#[cfg(target_os = "linux")]
use super::io_metrics::{TIER_CRITICAL, TIER_HIGH, TIER_LOW};
use crate::bridge::envelope::Priority;

/// Queue depth for the io_uring instance.
#[cfg(target_os = "linux")]
const QUEUE_DEPTH: u32 = 64;

/// Maximum number of pre-allocated buffers in the pool.
#[cfg(target_os = "linux")]
const POOL_SIZE: usize = 32;

/// Default buffer size (4 MiB — fits most column files).
#[cfg(target_os = "linux")]
const DEFAULT_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Consecutive completion waits that make no progress before giving up.
///
/// Bounds the drain loop when the ring stops delivering CQEs, instead of
/// spinning forever on a wait that never returns new completions.
#[cfg(target_os = "linux")]
const MAX_STALLED_WAITS: u32 = 8;

/// Per-core batched io_uring reader.
///
/// Not `Send` — owned by a single Data Plane core.
#[cfg(target_os = "linux")]
pub struct UringReader {
    ring: io_uring::IoUring,
    /// Pre-allocated aligned buffer pool.
    pool: Vec<AlignedBuf>,
    /// Indices of available buffers in the pool.
    free: Vec<usize>,
    /// Buffer size for each pool slot.
    buf_size: usize,
    /// Submission queue depth the ring was created with.
    queue_depth: u32,
}

#[cfg(not(target_os = "linux"))]
pub struct UringReader;

#[cfg(target_os = "linux")]
impl UringReader {
    /// Create a new io_uring reader with a pre-allocated buffer pool.
    ///
    /// Returns `None` if io_uring is not available (old kernel, WASM).
    pub fn new() -> Option<Self> {
        Self::with_config(QUEUE_DEPTH, POOL_SIZE, DEFAULT_BUF_SIZE)
    }

    /// Create with custom configuration.
    pub fn with_config(queue_depth: u32, pool_size: usize, buf_size: usize) -> Option<Self> {
        let ring = io_uring::IoUring::new(queue_depth).ok()?;

        let mut pool = Vec::with_capacity(pool_size);
        let mut free = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            match AlignedBuf::new(buf_size) {
                Ok(buf) => {
                    pool.push(buf);
                    free.push(i);
                }
                Err(_) => break,
            }
        }

        if pool.is_empty() {
            return None;
        }

        Some(Self {
            ring,
            pool,
            free,
            buf_size,
            queue_depth: queue_depth.max(1),
        })
    }

    /// Read multiple files in a single batched io_uring submission.
    ///
    /// Opens each file, submits `IORING_OP_READ` SQEs for all, then
    /// waits for all completions. Returns file contents in the same
    /// order as `paths`.
    ///
    /// Batches larger than the ring depth are submitted in several rounds.
    ///
    /// A file that cannot be opened, cannot be queued, or whose read returns a
    /// kernel error is returned as an empty `Vec<u8>`.
    pub fn read_files(&mut self, paths: &[&Path]) -> Vec<Vec<u8>> {
        if paths.is_empty() {
            return Vec::new();
        }

        // Open files, determine sizes, assign buffers.
        let mut reads: Vec<PendingRead> = Vec::with_capacity(paths.len());
        let mut oversized: Vec<AlignedBuf> = Vec::new();

        for (i, path) in paths.iter().enumerate() {
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(_) => {
                    reads.push(PendingRead::failed(i));
                    continue;
                }
            };
            let size = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            if size == 0 {
                reads.push(PendingRead::failed(i));
                continue;
            }

            let buf_source = if size <= self.buf_size {
                if let Some(slot) = self.free.pop() {
                    BufSource::Pool(slot)
                } else {
                    // Pool exhausted — allocate dedicated.
                    match AlignedBuf::new(size) {
                        Ok(buf) => {
                            let idx = oversized.len();
                            oversized.push(buf);
                            BufSource::Oversized(idx)
                        }
                        Err(_) => {
                            reads.push(PendingRead::failed(i));
                            continue;
                        }
                    }
                }
            } else {
                match AlignedBuf::new(size) {
                    Ok(buf) => {
                        let idx = oversized.len();
                        oversized.push(buf);
                        BufSource::Oversized(idx)
                    }
                    Err(_) => {
                        reads.push(PendingRead::failed(i));
                        continue;
                    }
                }
            };

            reads.push(PendingRead {
                index: i,
                file: Some(file),
                size,
                buf_source,
            });
        }

        // Submit SQEs, refilling the ring whenever it fills up.
        //
        // A full submission queue means "submit what is queued and continue",
        // not "drop this read": the queued entries go to the kernel, ready
        // completions are reaped, and the push is retried. A read that still
        // cannot be queued stays `NotSubmitted` and takes the same result path
        // as a kernel read error.
        let mut states = vec![ReadState::NotSubmitted; paths.len()];
        let mut queued = 0u32;
        let mut in_flight = 0u32;

        for read in &reads {
            let Some(ref file) = read.file else {
                continue;
            };

            let (buf_ptr, buf_cap) = match read.buf_source {
                BufSource::Pool(slot) => (self.pool[slot].as_mut_ptr(), self.pool[slot].capacity()),
                BufSource::Oversized(idx) => {
                    (oversized[idx].as_mut_ptr(), oversized[idx].capacity())
                }
                BufSource::None => continue,
            };

            // Cap outstanding work at the ring depth so the completion queue
            // can never overflow and silently drop a CQE.
            if queued + in_flight >= self.queue_depth {
                self.submit_queued(&mut queued, &mut in_flight, &mut states);
                self.drain_ready(&mut in_flight, &mut states);
                if queued + in_flight >= self.queue_depth {
                    self.wait_for_completions(1, &mut queued, &mut in_flight, &mut states);
                }
            }

            let read_len = round_up_read(read.size).min(buf_cap) as u32;
            let fd = io_uring::types::Fd(file.as_raw_fd());
            let read_op = io_uring::opcode::Read::new(fd, buf_ptr, read_len)
                .offset(0)
                .build()
                .user_data(read.index as u64);

            // SAFETY: buf_ptr points to a buffer that outlives the SQE and is
            // never moved while the kernel owns it. Pool buffers live in
            // self.pool, oversized buffers in the local `oversized` Vec; both
            // stay put for the whole call, and neither is handed back to the
            // pool nor read until its completion is reaped. The intermediate
            // submit/reap calls below only advance ring state, so a buffer
            // whose SQE is already in flight is untouched by them. File fds
            // stay valid until `reads` is dropped at the end of the function.
            let mut pushed = unsafe { self.ring.submission().push(&read_op).is_ok() };
            if !pushed {
                self.submit_queued(&mut queued, &mut in_flight, &mut states);
                self.drain_ready(&mut in_flight, &mut states);
                // SAFETY: same buffer and fd as the push above, still alive.
                pushed = unsafe { self.ring.submission().push(&read_op).is_ok() };
            }

            if pushed {
                states[read.index] = ReadState::InFlight;
                queued += 1;
            }
        }

        // Submit the remainder and reap every outstanding completion.
        let mut stalled = 0u32;
        while queued + in_flight > 0 {
            let outstanding = queued + in_flight;
            self.wait_for_completions(
                outstanding as usize,
                &mut queued,
                &mut in_flight,
                &mut states,
            );
            if queued + in_flight == outstanding {
                stalled += 1;
                if stalled >= MAX_STALLED_WAITS {
                    break;
                }
            } else {
                stalled = 0;
            }
        }

        // Extract results and return pool buffers.
        let mut results = vec![Vec::new(); paths.len()];
        for read in &reads {
            if read.file.is_none() || read.size == 0 {
                continue;
            }

            match states[read.index] {
                ReadState::Done => {
                    results[read.index] = match read.buf_source {
                        BufSource::Pool(slot) => {
                            // SAFETY: io_uring wrote read.size bytes into pool[slot].
                            let v = unsafe { self.pool[slot].to_vec(read.size) };
                            self.free.push(slot);
                            v
                        }
                        BufSource::Oversized(idx) => {
                            // SAFETY: io_uring wrote read.size bytes into oversized[idx].
                            unsafe { oversized[idx].to_vec(read.size) }
                        }
                        BufSource::None => Vec::new(),
                    };
                }
                ReadState::Failed | ReadState::NotSubmitted => {
                    // The kernel is finished with this buffer, or never had it,
                    // so the pool slot goes back for reuse and the result stays
                    // an empty Vec.
                    if let BufSource::Pool(slot) = read.buf_source {
                        self.free.push(slot);
                    }
                }
                ReadState::InFlight => {
                    // The ring stopped reporting completions, so the kernel may
                    // still write into this buffer. The pool slot is withheld
                    // rather than handed to a later read.
                }
            }
        }

        results
    }

    /// Hand every queued SQE to the kernel, moving them to in-flight.
    ///
    /// A full completion queue can reject the submission, so ready CQEs are
    /// reaped to free space and the submit is retried once.
    fn submit_queued(&mut self, queued: &mut u32, in_flight: &mut u32, states: &mut [ReadState]) {
        if *queued == 0 {
            return;
        }
        if let Ok(n) = self.ring.submit() {
            let n = (n as u32).min(*queued);
            *queued -= n;
            *in_flight += n;
            return;
        }
        self.drain_ready(in_flight, states);
        if let Ok(n) = self.ring.submit() {
            let n = (n as u32).min(*queued);
            *queued -= n;
            *in_flight += n;
        }
    }

    /// Reap every completion already available, without blocking.
    fn drain_ready(&mut self, in_flight: &mut u32, states: &mut [ReadState]) {
        let mut reaped = 0u32;
        for cqe in self.ring.completion() {
            let index = cqe.user_data() as usize;
            if let Some(state) = states.get_mut(index) {
                *state = if cqe.result() < 0 {
                    ReadState::Failed
                } else {
                    ReadState::Done
                };
            }
            reaped += 1;
        }
        *in_flight = in_flight.saturating_sub(reaped);
    }

    /// Submit anything queued, block until `want` completions are available,
    /// then reap them.
    fn wait_for_completions(
        &mut self,
        want: usize,
        queued: &mut u32,
        in_flight: &mut u32,
        states: &mut [ReadState],
    ) {
        if let Ok(n) = self.ring.submit_and_wait(want) {
            let n = (n as u32).min(*queued);
            *queued -= n;
            *in_flight += n;
        }
        self.drain_ready(in_flight, states);
    }

    /// Priority-aware variant of [`read_files`].
    ///
    /// Identical to `read_files` but records IO wait time in `metrics`
    /// under the bucket corresponding to `priority`:
    ///
    /// - `Background` / `Normal` → `TIER_LOW`
    /// - `High`                  → `TIER_HIGH`
    /// - `Critical`              → `TIER_CRITICAL`
    ///
    /// The wait clock starts just before the first SQE submission and stops
    /// after all CQEs are reaped.  File open + buffer setup time is included
    /// because it reflects the total latency seen by the caller.
    pub fn read_files_with_priority(
        &mut self,
        paths: &[&Path],
        priority: Priority,
        metrics: &IoMetrics,
    ) -> Vec<Vec<u8>> {
        let tier = match priority {
            Priority::Background | Priority::Normal => TIER_LOW,
            Priority::High => TIER_HIGH,
            Priority::Critical => TIER_CRITICAL,
        };
        let t0 = Instant::now();
        let result = self.read_files(paths);
        let wait_ns = t0.elapsed().as_nanos() as u64;
        metrics.record_wait(tier, wait_ns);
        result
    }
}

#[cfg(not(target_os = "linux"))]
impl UringReader {
    /// Create a reader on io_uring-capable Linux hosts.
    ///
    /// Non-Linux builds return `None` so callers use their existing
    /// sequential-read fallback path.
    pub fn new() -> Option<Self> {
        None
    }

    /// Create with custom configuration.
    pub fn with_config(_queue_depth: u32, _pool_size: usize, _buf_size: usize) -> Option<Self> {
        None
    }

    /// Non-Linux builds should not have a concrete reader, but keep this method
    /// available so generic call sites compile.
    pub fn read_files(&mut self, paths: &[&Path]) -> Vec<Vec<u8>> {
        vec![Vec::new(); paths.len()]
    }

    /// Priority-aware variant of [`read_files`].
    pub fn read_files_with_priority(
        &mut self,
        paths: &[&Path],
        _priority: Priority,
        _metrics: &IoMetrics,
    ) -> Vec<Vec<u8>> {
        self.read_files(paths)
    }
}

/// Progress of one read through the submit/complete cycle.
#[derive(Clone, Copy)]
#[cfg(target_os = "linux")]
enum ReadState {
    /// Never accepted by the submission queue — the kernel never saw the buffer.
    NotSubmitted,
    /// Handed to the kernel, completion not yet reaped.
    InFlight,
    /// Completed successfully.
    Done,
    /// Completed with a kernel error.
    Failed,
}

/// Tracks which buffer a read uses.
#[derive(Clone, Copy)]
#[cfg(target_os = "linux")]
enum BufSource {
    Pool(usize),
    Oversized(usize),
    None,
}

/// A pending read operation.
#[cfg(target_os = "linux")]
struct PendingRead {
    index: usize,
    file: Option<std::fs::File>,
    size: usize,
    buf_source: BufSource,
}

#[cfg(target_os = "linux")]
impl PendingRead {
    fn failed(index: usize) -> Self {
        Self {
            index,
            file: None,
            size: 0,
            buf_source: BufSource::None,
        }
    }
}

/// Round a read size up to ALIGNMENT.
#[inline]
#[cfg(target_os = "linux")]
fn round_up_read(size: usize) -> usize {
    super::aligned_buf::round_up(size, ALIGNMENT)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_file(dir: &Path, name: &str, size: usize) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        f.write_all(&data).unwrap();
        f.sync_all().unwrap();
        path
    }

    /// Create a test file of `size` bytes where every byte equals `mark`.
    ///
    /// Used to distinguish per-file content in batched-read tests: a wrong
    /// buffer/index mapping shows up as either a length mismatch or a byte
    /// value that doesn't match the expected `mark` for that file.
    fn create_marked_test_file(
        dir: &Path,
        name: &str,
        size: usize,
        mark: u8,
    ) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        let data: Vec<u8> = vec![mark; size];
        f.write_all(&data).unwrap();
        f.sync_all().unwrap();
        path
    }

    #[test]
    fn read_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_file(dir.path(), "test.col", 8192);

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[path.as_path()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 8192);
        for (i, &b) in results[0].iter().enumerate() {
            assert_eq!(b, (i % 256) as u8, "mismatch at byte {i}");
        }
    }

    #[test]
    fn read_multiple_files_batched() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = create_test_file(dir.path(), "a.col", 4096);
        let p2 = create_test_file(dir.path(), "b.col", 8192);
        let p3 = create_test_file(dir.path(), "c.col", 1024);

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[p1.as_path(), p2.as_path(), p3.as_path()]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].len(), 4096);
        assert_eq!(results[1].len(), 8192);
        assert_eq!(results[2].len(), 1024);
    }

    #[test]
    fn read_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let existing = create_test_file(dir.path(), "exists.col", 4096);
        let missing = dir.path().join("missing.col");

        let mut reader = UringReader::with_config(8, 4, 16384).unwrap();
        let results = reader.read_files(&[existing.as_path(), missing.as_path()]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].len(), 4096);
        assert_eq!(results[1].len(), 0);
    }

    #[test]
    fn read_oversized_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_file(dir.path(), "big.col", 16384);

        let mut reader = UringReader::with_config(8, 4, 4096).unwrap();
        let results = reader.read_files(&[path.as_path()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 16384);
    }

    #[test]
    fn read_empty_paths() {
        let mut reader = UringReader::with_config(8, 4, 4096).unwrap();
        let results = reader.read_files(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn pool_buffers_are_reused() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = create_test_file(dir.path(), "a.col", 1024);
        let p2 = create_test_file(dir.path(), "b.col", 2048);

        let mut reader = UringReader::with_config(8, 2, 4096).unwrap();

        let r1 = reader.read_files(&[p1.as_path(), p2.as_path()]);
        assert_eq!(r1[0].len(), 1024);
        assert_eq!(r1[1].len(), 2048);
        assert_eq!(reader.free.len(), 2);

        let r2 = reader.read_files(&[p1.as_path()]);
        assert_eq!(r2[0].len(), 1024);
        assert_eq!(reader.free.len(), 2);
    }

    /// A batch larger than `QUEUE_DEPTH` must still return correct content
    /// for every file, including the ones past the submission-queue depth.
    ///
    /// Today, `read_files` Phase 2 silently drops any read whose
    /// `submission().push()` call fails because the queue is full — the read
    /// is never submitted, `submitted` is not incremented for it, and Phase 4
    /// returns an empty `Vec<u8>` for that index, indistinguishable from an
    /// empty file. This test fails today: for indices at/after `QUEUE_DEPTH`,
    /// `results[i].len()` comes back `0` instead of the expected file size.
    #[test]
    fn read_batch_larger_than_queue_depth() {
        let dir = tempfile::tempdir().unwrap();
        let n = (QUEUE_DEPTH as usize) * 2 + 3;

        let mut paths = Vec::with_capacity(n);
        let mut marks = Vec::with_capacity(n);
        let mut sizes = Vec::with_capacity(n);
        for i in 0..n {
            let mark = (i % 251) as u8;
            let size = 512 + (i % 8) * 64;
            let name = format!("file_{i:04}.col");
            let path = create_marked_test_file(dir.path(), &name, size, mark);
            paths.push(path);
            marks.push(mark);
            sizes.push(size);
        }

        let mut reader = UringReader::with_config(QUEUE_DEPTH, 16, 65536).unwrap();
        let path_refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        let results = reader.read_files(&path_refs);

        assert_eq!(results.len(), n);
        for i in 0..n {
            assert_eq!(
                results[i].len(),
                sizes[i],
                "file at index {i} returned {} bytes, expected {} (path: {})",
                results[i].len(),
                sizes[i],
                paths[i].display()
            );
            for (byte_pos, &b) in results[i].iter().enumerate() {
                assert_eq!(
                    b,
                    marks[i],
                    "file at index {i} byte {byte_pos} mismatch: got {b}, expected {} (path: {})",
                    marks[i],
                    paths[i].display()
                );
            }
        }
    }

    /// A batch exactly at `QUEUE_DEPTH` must read every file correctly —
    /// pins the boundary just below where the queue-full failure begins.
    #[test]
    fn read_batch_exactly_at_queue_depth() {
        let dir = tempfile::tempdir().unwrap();
        let n = QUEUE_DEPTH as usize;

        let mut paths = Vec::with_capacity(n);
        let mut marks = Vec::with_capacity(n);
        let mut sizes = Vec::with_capacity(n);
        for i in 0..n {
            let mark = (i % 251) as u8;
            let size = 512 + (i % 8) * 64;
            let name = format!("file_{i:04}.col");
            let path = create_marked_test_file(dir.path(), &name, size, mark);
            paths.push(path);
            marks.push(mark);
            sizes.push(size);
        }

        let mut reader = UringReader::with_config(QUEUE_DEPTH, 16, 65536).unwrap();
        let path_refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        let results = reader.read_files(&path_refs);

        assert_eq!(results.len(), n);
        for i in 0..n {
            assert_eq!(
                results[i].len(),
                sizes[i],
                "file at index {i} returned {} bytes, expected {} (path: {})",
                results[i].len(),
                sizes[i],
                paths[i].display()
            );
            assert!(
                results[i].iter().all(|&b| b == marks[i]),
                "file at index {i} has a byte not equal to expected mark {} (path: {})",
                marks[i],
                paths[i].display()
            );
        }
    }

    /// A batch of exactly one file reads correctly — lower boundary above zero.
    #[test]
    fn read_batch_of_one_file_at_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_marked_test_file(dir.path(), "solo.col", 4096, 0x5A);

        let mut reader = UringReader::with_config(QUEUE_DEPTH, 16, 65536).unwrap();
        let results = reader.read_files(&[path.as_path()]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 4096);
        assert!(
            results[0].iter().all(|&b| b == 0x5A),
            "single-file batch returned unexpected byte value"
        );
    }

    /// A batch of zero files returns an empty result vector — lower boundary.
    #[test]
    fn read_batch_of_zero_files_at_boundary() {
        let mut reader = UringReader::with_config(QUEUE_DEPTH, 16, 65536).unwrap();
        let results = reader.read_files(&[]);
        assert!(results.is_empty());
    }

    /// Mixed small (pool-buffer) and large (oversized-buffer) files inside a
    /// single batch that also exceeds `QUEUE_DEPTH`. Verifies both `BufSource`
    /// paths return correct content when combined with the queue-depth defect
    /// above: `buf_size` is set to 4096, so files sized 2048 take the `Pool`
    /// path (`size <= self.buf_size`) and files sized 8192 take the
    /// `Oversized` path (`size > self.buf_size`).
    #[test]
    fn read_mixed_pool_and_oversized_in_oversized_batch() {
        let dir = tempfile::tempdir().unwrap();
        let buf_size = 4096;
        let small_size = 2048;
        let large_size = 8192;
        let n = (QUEUE_DEPTH as usize) + 5;

        let mut paths = Vec::with_capacity(n);
        let mut marks = Vec::with_capacity(n);
        let mut sizes = Vec::with_capacity(n);
        for i in 0..n {
            let mark = (i % 251) as u8;
            let size = if i % 10 == 0 { large_size } else { small_size };
            let name = format!("mixed_{i:04}.col");
            let path = create_marked_test_file(dir.path(), &name, size, mark);
            paths.push(path);
            marks.push(mark);
            sizes.push(size);
        }

        let mut reader = UringReader::with_config(QUEUE_DEPTH, 8, buf_size).unwrap();
        let path_refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        let results = reader.read_files(&path_refs);

        assert_eq!(results.len(), n);
        for i in 0..n {
            assert_eq!(
                results[i].len(),
                sizes[i],
                "file at index {i} (size {}) returned {} bytes (path: {})",
                sizes[i],
                results[i].len(),
                paths[i].display()
            );
            assert!(
                results[i].iter().all(|&b| b == marks[i]),
                "file at index {i} (size {}) has a byte not equal to expected mark {} (path: {})",
                sizes[i],
                marks[i],
                paths[i].display()
            );
        }
    }
}
