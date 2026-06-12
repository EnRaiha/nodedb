// SPDX-License-Identifier: Apache-2.0

//! Group commit coordinator.
//!
//! Batches concurrent WAL write requests into a single fsync for maximum NVMe IOPS.
//!
//! ## How it works
//!
//! 1. Multiple threads submit `PendingWrite` to the commit queue.
//! 2. One thread becomes the "commit leader" (acquires the commit lock).
//! 3. The leader drains all pending writes, appends them to the WAL writer,
//!    issues a single `fsync`, and advances `durable_lsn`.
//! 4. Non-leader threads discover their write was already committed when the
//!    pending queue is empty after acquiring the commit lock. They verify the
//!    commit succeeded via `last_commit_failed` before returning `durable: true`.
//!
//! This converts N fsyncs into 1 fsync, which is critical for NVMe performance.
//!
//! ## Safety invariants
//!
//! - `durable_lsn` is updated atomically **only after** a successful fsync.
//! - If fsync fails, `last_commit_failed` is set so non-leader threads whose
//!   writes were in the failed batch propagate the error instead of falsely
//!   reporting `durable: true`.
//! - Fsync failure is treated as **fatal for the batch** — the data may or may not
//!   be on disk, and the caller must handle the error (retry, abort, etc.).

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{Result, WalError};
use crate::writer::WalWriter;

/// A pending write request waiting to be committed.
#[derive(Debug)]
pub struct PendingWrite {
    /// Record type discriminant.
    pub record_type: u32,
    /// Tenant ID.
    pub tenant_id: u64,
    /// Virtual shard ID.
    pub vshard_id: u32,
    /// Database identifier (raw u64). Zero maps to the default database.
    pub database_id: u64,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

/// Result delivered to each waiter after group commit completes.
#[derive(Debug, Clone, Copy)]
pub struct CommitResult {
    /// The LSN assigned to this record.
    pub lsn: u64,
    /// Whether the fsync succeeded (record is durable).
    pub durable: bool,
}

/// Thread-safe group commit coordinator.
///
/// Multiple threads call `submit()` which blocks until the record is durable.
/// Internally, one thread becomes the commit leader and batches all pending
/// writes into a single WAL flush.
///
/// ## Safety invariants
///
/// - A non-leader thread only returns `durable: true` if it confirms its
///   write was drained from the pending queue **and** the leader's fsync
///   succeeded (checked via `last_commit_failed`).
/// - If the leader's fsync fails, all non-leader threads whose writes were
///   in the failed batch receive an error.
/// - `durable_lsn` is updated **only after** a successful fsync.
pub struct GroupCommitter {
    /// Pending writes queue.
    pending: Mutex<Vec<PendingWrite>>,

    /// The durable LSN — all records with LSN <= this value are on disk.
    durable_lsn: AtomicU64,

    /// Set to `true` if the most recent commit batch failed (fsync error).
    /// Non-leader threads whose writes were part of the failed batch check
    /// this flag and propagate the error instead of returning `durable: true`.
    /// Cleared at the start of each successful commit.
    last_commit_failed: AtomicBool,

    /// Serializes commit operations (drain + write + fsync).
    /// Separate from `pending` so enqueue doesn't block on I/O.
    commit_lock: Mutex<()>,
}

impl GroupCommitter {
    /// Create a new group committer.
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(1024)),
            durable_lsn: AtomicU64::new(0),
            last_commit_failed: AtomicBool::new(false),
            commit_lock: Mutex::new(()),
        }
    }

    /// Submit a write and block until it's durable.
    ///
    /// Returns the assigned LSN once the batch containing this write has been
    /// fsynced to disk. If fsync fails, the error is propagated to all threads
    /// whose writes were in the failed batch.
    pub fn submit(&self, writer: &Mutex<WalWriter>, write: PendingWrite) -> Result<CommitResult> {
        // Enqueue the write.
        {
            let mut pending = self.pending.lock().map_err(|_| WalError::LockPoisoned {
                context: "pending queue",
            })?;
            pending.push(write);
        }

        // Try to become the commit leader. If another thread holds the commit
        // lock, we block here until it finishes — then we check if our write
        // was already committed by that leader.
        let _commit_guard = self
            .commit_lock
            .lock()
            .map_err(|_| WalError::LockPoisoned {
                context: "commit lock",
            })?;

        // Drain pending writes. If the previous leader already committed our
        // write, the pending queue will be empty (our write was drained by
        // that leader). If the queue is non-empty, we are the new leader.
        let batch: Vec<PendingWrite> = {
            let mut pending = self.pending.lock().map_err(|_| WalError::LockPoisoned {
                context: "pending queue (drain)",
            })?;
            std::mem::take(&mut *pending)
        };

        if batch.is_empty() {
            // Previous leader drained our write. Check if that commit
            // succeeded or failed. This is the critical fix: without this
            // check, a non-leader would return durable:true even if the
            // leader's fsync failed.
            if self.last_commit_failed.load(Ordering::Acquire) {
                return Err(WalError::Io(std::io::Error::other(
                    "WAL fsync failed in previous group commit batch",
                )));
            }
            let lsn = self.durable_lsn.load(Ordering::Acquire);
            return Ok(CommitResult { lsn, durable: true });
        }

        // We are the leader — append and fsync.
        let mut wal = writer.lock().map_err(|_| WalError::LockPoisoned {
            context: "WAL writer",
        })?;
        let mut last_lsn = 0;

        for w in &batch {
            last_lsn = wal.append(
                w.record_type,
                w.tenant_id,
                w.vshard_id,
                w.database_id,
                &w.payload,
            )?;
        }

        let sync_result = wal.sync();
        drop(wal);

        match sync_result {
            Ok(()) => {
                // Order matters: clear error flag before advancing durable_lsn.
                // Non-leaders check error flag AFTER seeing empty batch, so
                // the flag must be cleared before they can observe the new LSN.
                self.last_commit_failed.store(false, Ordering::Release);
                self.durable_lsn.store(last_lsn, Ordering::Release);
                Ok(CommitResult {
                    lsn: last_lsn,
                    durable: true,
                })
            }
            Err(e) => {
                // Fsync failed. Mark the error so non-leader threads whose
                // writes were in this batch (drained from pending) know not
                // to report success. Do NOT update durable_lsn.
                self.last_commit_failed.store(true, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Current durable LSN (all records <= this are on disk).
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }
}

impl Default for GroupCommitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::WalReader;
    use crate::record::RecordType;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_thread_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = Mutex::new(WalWriter::open_without_direct_io(&path).unwrap());
        let gc = GroupCommitter::new();

        let result = gc
            .submit(
                &writer,
                PendingWrite {
                    record_type: RecordType::Put as u32,
                    tenant_id: 1,
                    vshard_id: 0,
                    database_id: 0,
                    payload: b"hello".to_vec(),
                },
            )
            .unwrap();

        assert!(result.durable);
        assert_eq!(result.lsn, 1);
        assert_eq!(gc.durable_lsn(), 1);

        // Verify the record is readable.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"hello");
    }

    #[test]
    fn concurrent_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let writer = Arc::new(Mutex::new(
            WalWriter::open_without_direct_io(&path).unwrap(),
        ));
        let gc = Arc::new(GroupCommitter::new());

        let mut handles = Vec::new();

        for i in 0..10 {
            let w = Arc::clone(&writer);
            let g = Arc::clone(&gc);
            handles.push(thread::spawn(move || {
                let payload = format!("record-{i}");
                let result = g
                    .submit(
                        &w,
                        PendingWrite {
                            record_type: RecordType::Put as u32,
                            tenant_id: 1,
                            vshard_id: 0,
                            database_id: 0,
                            payload: payload.into_bytes(),
                        },
                    )
                    .unwrap();
                assert!(result.durable);
                result.lsn
            }));
        }

        let lsns: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All should have gotten durable results.
        assert!(lsns.iter().all(|l| *l > 0));

        // All 10 records should be in the WAL.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 10);
    }
}
