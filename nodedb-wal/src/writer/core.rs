// SPDX-License-Identifier: Apache-2.0

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt as _;

use crate::align::AlignedBuf;
use crate::error::{Result, WalError};
use crate::preamble::SegmentPreamble;
use crate::record::{HEADER_SIZE, MIN_PADDING_RECORD_SIZE, WalRecord, WalRecordArgs};

use super::config::{WalWriterConfig, open_dwb_for, resume_offset};

/// Append-only WAL writer.
pub struct WalWriter {
    /// The WAL file handle (opened with O_DIRECT if configured).
    pub(super) file: File,

    /// Aligned write buffer for batching records before flush.
    pub(super) buffer: AlignedBuf,

    /// Current file write offset (always aligned).
    pub(super) file_offset: u64,

    /// Next LSN to assign.
    next_lsn: AtomicU64,

    /// Whether the writer has been sealed (no more writes accepted).
    sealed: bool,

    /// Configuration.
    pub(super) config: WalWriterConfig,

    /// Optional key ring for payload encryption (supports key rotation).
    encryption_ring: Option<crate::crypto::KeyRing>,

    /// Preamble written at the start of this segment (when encryption is active).
    /// Its 16 bytes are included as part of the AAD on every encrypted record,
    /// binding ciphertext to the segment it was written in.
    segment_preamble: Option<SegmentPreamble>,

    /// Optional double-write buffer for torn write protection.
    /// Records are written here before the WAL for crash recovery.
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
}

impl WalWriter {
    /// Open or create a WAL file at the given path.
    pub fn open(path: &Path, config: WalWriterConfig) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).append(false);

        #[cfg(target_os = "linux")]
        if config.use_direct_io {
            // O_DIRECT: bypass page cache.
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;

        let buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        // Scan existing WAL for recovery if the file has data.
        let (file_offset, next_lsn) = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let info = crate::recovery::recover(path)?;
            (
                resume_offset(
                    info.end_offset,
                    config.use_direct_io,
                    config.alignment,
                    path,
                ),
                info.next_lsn(),
            )
        } else {
            (0, 1)
        };

        let double_write = open_dwb_for(&config, path);

        Ok(Self {
            file,
            buffer,
            file_offset,
            next_lsn: AtomicU64::new(next_lsn),
            sealed: false,
            config,
            encryption_ring: None,
            segment_preamble: None,
            double_write,
        })
    }

    /// Resume appending to an existing segment whose filename declares
    /// `declared_first_lsn` as the lowest LSN it may hold.
    ///
    /// [`Self::open`] derives the next LSN from the records it finds, and a
    /// segment that holds none — created by a rollover that was never written
    /// to, or one whose every record was torn away — would restart the
    /// sequence at 1. Those LSNs are already spoken for by the earlier
    /// segments, so replay would see duplicates and truncation would delete
    /// the originals. The filename is the authority on where this segment's
    /// range begins, so the resumed sequence never drops below it.
    pub fn open_resuming(
        path: &Path,
        config: WalWriterConfig,
        declared_first_lsn: u64,
    ) -> Result<Self> {
        let writer = Self::open(path, config)?;
        let resumed = writer.next_lsn().max(declared_first_lsn);
        writer.next_lsn.store(resumed, Ordering::Relaxed);
        Ok(writer)
    }

    pub(crate) fn can_set_encryption_ring(&self) -> bool {
        self.file_offset == 0 && self.buffer.is_empty()
    }

    /// Set the encryption key. When set, all subsequent records will have
    /// their payloads encrypted with AES-256-GCM.
    ///
    /// Writes the 16-byte WAL segment preamble at the current file offset.
    /// Must be called before the first `append`. Calling it after records
    /// have already been written to this file returns an error.
    pub fn set_encryption_key(&mut self, key: crate::crypto::WalEncryptionKey) -> Result<()> {
        self.set_encryption_ring(crate::crypto::KeyRing::new(key))
    }

    /// Set the key ring directly (for key rotation with dual-key reads).
    ///
    /// Writes the 16-byte WAL segment preamble at the current file offset.
    /// Must be called before the first `append`. Calling it after records
    /// have already been written to this file returns an error.
    pub fn set_encryption_ring(&mut self, ring: crate::crypto::KeyRing) -> Result<()> {
        if self.file_offset != 0 || !self.buffer.is_empty() {
            return Err(WalError::EncryptionError {
                detail: "set_encryption_ring must be called before writing any records".into(),
            });
        }
        let epoch = *ring.current().epoch();
        let preamble = SegmentPreamble::new_wal(epoch);
        let preamble_bytes = preamble.to_bytes();

        // Write preamble into the buffer so it gets flushed with the first
        // record batch (or on the next sync).
        self.buffer.write(&preamble_bytes);

        self.encryption_ring = Some(ring);
        self.segment_preamble = Some(preamble);
        Ok(())
    }

    /// Access the key ring (for decryption during replay).
    pub fn encryption_ring(&self) -> Option<&crate::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// The preamble for this segment, if encryption was enabled.
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }

    /// Open a new WAL segment file with a specific starting LSN.
    ///
    /// Used by `SegmentedWal` when rolling to a new segment. The file must
    /// not already exist (or be empty). The writer will assign LSNs starting
    /// from `start_lsn`.
    pub fn open_with_start_lsn(
        path: &Path,
        config: WalWriterConfig,
        start_lsn: u64,
    ) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).append(false);

        #[cfg(target_os = "linux")]
        if config.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        let double_write = open_dwb_for(&config, path);

        Ok(Self {
            file,
            buffer,
            file_offset: 0,
            next_lsn: AtomicU64::new(start_lsn),
            sealed: false,
            config,
            encryption_ring: None,
            segment_preamble: None,
            double_write,
        })
    }

    /// Open a WAL writer with O_DIRECT disabled (for testing on tmpfs, etc.).
    pub fn open_without_direct_io(path: &Path) -> Result<Self> {
        Self::open(
            path,
            WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        )
    }

    /// Bytes reserved at the tail of the write buffer for the alignment
    /// padding record `flush_buffer` appends under O_DIRECT.
    ///
    /// The worst case is a batch that ends one byte short of a boundary: the
    /// remaining gap cannot hold a header, so the padding record borrows a
    /// whole extra block.
    pub(super) fn padding_reserve(&self) -> usize {
        if self.config.use_direct_io {
            self.config.alignment + MIN_PADDING_RECORD_SIZE
        } else {
            0
        }
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    ///
    /// The record is written to the in-memory buffer. Call `sync()` to
    /// flush to disk and make the write durable.
    ///
    /// `database_id` is stored in header bytes 34-41. Pass `0` for the
    /// default database (backward-compatible with pre-existing records).
    ///
    /// The LSN is committed only once the record is in the write buffer. An
    /// append that fails — a full device, an oversized payload — leaves the
    /// LSN sequence untouched, so the next append reuses it rather than
    /// leaving a hole that replay would have to reason about.
    pub fn append(
        &mut self,
        record_type: u32,
        tenant_id: u64,
        vshard_id: u32,
        database_id: u64,
        payload: &[u8],
    ) -> Result<u64> {
        if self.sealed {
            return Err(WalError::Sealed);
        }

        let lsn = self.next_lsn.load(Ordering::Relaxed);
        let preamble_bytes = self.segment_preamble.as_ref().map(|p| p.to_bytes());
        let record = WalRecord::new(WalRecordArgs {
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            database_id,
            payload: payload.to_vec(),
            encryption_key: self.encryption_ring.as_ref().map(|r| r.current()),
            preamble_bytes: preamble_bytes.as_ref(),
        })?;

        let header_bytes = record.header.to_bytes();
        let total_size = HEADER_SIZE + record.payload.len();
        let reserve = self.padding_reserve();

        // If the record cannot fit in an empty buffer alongside its padding,
        // no amount of flushing will make room.
        let usable = self.buffer.capacity().saturating_sub(reserve);
        if total_size > usable {
            return Err(WalError::PayloadTooLarge {
                size: record.payload.len(),
                max: usable.saturating_sub(HEADER_SIZE),
            });
        }

        // If this record doesn't fit in the remaining buffer, flush first.
        // A failed flush propagates before the LSN is committed.
        if self.buffer.remaining() < total_size + reserve {
            self.flush_buffer()?;
        }

        self.buffer.write(&header_bytes);
        self.buffer.write(&record.payload);

        // Mirror into the double-write buffer (deferred — no fsync yet) only
        // once the record is committed to the write buffer. The DWB is keyed
        // by LSN, so a record that failed to land in the WAL must not leave a
        // phantom entry behind: this LSN goes to the next append, and torn-
        // write recovery would then resurrect the wrong record for it.
        //
        // The DWB is fsynced in batch during `sync()`, before the WAL fsync.
        // This amortizes DWB fsync cost across the entire group commit batch.
        //
        // DWB failure is non-fatal for the write itself (the WAL is the
        // authoritative store), but we log a warning because it means
        // torn-write recovery is degraded. If the DWB is persistently
        // broken, we detach it to avoid repeated error noise.
        if let Some(dwb) = &mut self.double_write
            && let Err(e) = dwb.write_record_deferred(&record)
        {
            tracing::warn!(
                lsn = lsn,
                error = %e,
                "DWB write failed — torn-write protection degraded, detaching DWB"
            );
            self.double_write = None;
        }

        self.next_lsn.store(lsn + 1, Ordering::Relaxed);

        Ok(lsn)
    }

    /// Flush the write buffer to disk (group commit).
    ///
    /// This issues a single write + fsync for all records accumulated
    /// since the last flush. The DWB is also fsynced (one fsync for all
    /// deferred DWB writes in this batch).
    pub fn sync(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Crash injection: die before the DWB is made durable. Recovery must
        // still produce the committed prefix — the DWB is a torn-write side
        // channel, never a source of records the WAL itself lacks.
        nodedb_types::fail_point_err!("wal::before_dwb_flush", |detail: String| WalError::Io(
            std::io::Error::other(format!("failpoint wal::before_dwb_flush: {detail}"))
        ));

        // Flush DWB first — records must be durable in DWB before WAL.
        // If DWB flush fails, torn-write protection is lost for this batch.
        // We log a warning and detach the DWB rather than silently continuing
        // as if torn-write protection is active.
        if let Some(dwb) = &mut self.double_write
            && let Err(e) = dwb.flush()
        {
            tracing::warn!(
                error = %e,
                "DWB flush failed — torn-write protection lost for this batch, detaching DWB"
            );
            self.double_write = None;
        }
        self.flush_buffer()?;

        // Crash injection: die between the DWB fsync and the WAL fsync — the
        // window where the DWB holds records the WAL has not yet committed.
        nodedb_types::fail_point_err!("wal::before_wal_fsync", |detail: String| WalError::Io(
            std::io::Error::other(format!("failpoint wal::before_wal_fsync: {detail}"))
        ));

        self.file.sync_all()?;
        Ok(())
    }

    /// Seal the WAL — no more writes will be accepted.
    ///
    /// Flushes any buffered data before sealing.
    pub fn seal(&mut self) -> Result<()> {
        self.sync()?;
        self.sealed = true;
        Ok(())
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Relaxed)
    }

    /// Current file size (bytes written to disk).
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    #[test]
    fn write_and_sync_single_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        let lsn = writer
            .append(RecordType::Put as u32, 1, 0, 0, b"hello")
            .unwrap();
        assert_eq!(lsn, 1);

        writer.sync().unwrap();
        assert!(writer.file_offset() > 0);
    }

    #[test]
    fn lsn_increments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();

        let lsn1 = writer
            .append(RecordType::Put as u32, 1, 0, 0, b"first")
            .unwrap();
        let lsn2 = writer
            .append(RecordType::Put as u32, 1, 0, 0, b"second")
            .unwrap();
        let lsn3 = writer
            .append(RecordType::Put as u32, 1, 0, 0, b"third")
            .unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
    }

    #[test]
    fn sealed_writer_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        writer.seal().unwrap();

        assert!(matches!(
            writer.append(RecordType::Put as u32, 1, 0, 0, b"rejected"),
            Err(WalError::Sealed)
        ));
    }
}
