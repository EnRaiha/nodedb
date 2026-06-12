// SPDX-License-Identifier: Apache-2.0

//! Lazy WAL reader: reads headers without payload for selective replay.
//!
//! Unlike `WalReader` which reads every payload into a `Vec<u8>`, this
//! reader reads only the 30-byte header first. The caller inspects the
//! header (record_type, vshard_id, lsn) and decides whether to read or
//! skip the payload.
//!
//! This is critical for startup replay performance: with 100M timeseries
//! records, a vector core can skip TS payloads (potentially GBs) by
//! seeking forward instead of allocating and reading.
//!
//! ## Usage
//!
//! ```text
//! let mut reader = LazyWalReader::open(path)?;
//! while let Some(header) = reader.next_header()? {
//!     if header.record_type == RecordType::VectorPut as u32 {
//!         let payload = reader.read_payload(&header)?;
//!         // process vector record
//!     } else {
//!         reader.skip_payload(&header)?;
//!     }
//! }
//! ```

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, RecordHeader, WalRecord};

/// Lazy WAL reader that separates header reading from payload reading.
pub struct LazyWalReader {
    file: File,
    offset: u64,
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
}

impl LazyWalReader {
    /// Open a WAL file for lazy reading.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let dwb_path = path.with_extension("dwb");
        let double_write = if dwb_path.exists() {
            crate::double_write::DoubleWriteBuffer::open(
                &dwb_path,
                crate::double_write::DwbMode::Buffered,
            )
            .ok()
        } else {
            None
        };
        Ok(Self {
            file,
            offset: 0,
            double_write,
        })
    }

    /// Read the next record header (30 bytes) without reading the payload.
    ///
    /// Returns `None` at EOF or first corruption. After this call, use
    /// either `read_payload()` to get the payload or `skip_payload()` to
    /// seek past it.
    pub fn next_header(&mut self) -> Result<Option<RecordHeader>> {
        let mut header_buf = [0u8; HEADER_SIZE];
        match self.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }

        let header = RecordHeader::from_bytes(&header_buf);

        if header.validate(self.offset - HEADER_SIZE as u64).is_err() {
            return Ok(None);
        }

        // Check for unknown required record types.
        let logical_type = header.logical_record_type();
        if crate::record::RecordType::from_raw(logical_type).is_none()
            && crate::record::RecordType::is_required(logical_type)
        {
            return Err(WalError::UnknownRequiredRecordType {
                record_type: header.record_type,
                lsn: header.lsn,
            });
        }

        Ok(Some(header))
    }

    /// Read the payload for a header that was just returned by `next_header()`.
    ///
    /// Must be called exactly once after `next_header()` returns `Some`,
    /// and before calling `next_header()` again (unless `skip_payload()`
    /// was called instead).
    pub fn read_payload(&mut self, header: &RecordHeader) -> Result<Vec<u8>> {
        let mut payload = vec![0u8; header.payload_len as usize];
        if !payload.is_empty() {
            match self.read_exact(&mut payload) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "torn write: incomplete payload",
                    )));
                }
                Err(e) => return Err(e),
            }
        }

        // Verify checksum.
        let record = WalRecord {
            header: *header,
            payload: payload.clone(),
        };
        if record.verify_checksum().is_err() {
            // Try double-write buffer recovery.
            if let Some(dwb) = &mut self.double_write
                && let Ok(Some(recovered)) = dwb.recover_record(header.lsn)
            {
                return Ok(recovered.payload);
            }
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch",
            )));
        }

        Ok(payload)
    }

    /// Read the payload and return a full WalRecord.
    pub fn read_record(&mut self, header: &RecordHeader) -> Result<WalRecord> {
        let payload = self.read_payload(header)?;
        Ok(WalRecord {
            header: *header,
            payload,
        })
    }

    /// Skip the payload for a header, seeking forward without reading.
    ///
    /// This is the key optimization: non-matching records skip I/O entirely.
    pub fn skip_payload(&mut self, header: &RecordHeader) -> Result<()> {
        let len = header.payload_len as u64;
        if len > 0 {
            self.file
                .seek(SeekFrom::Current(len as i64))
                .map_err(WalError::Io)?;
            self.offset += len;
        }
        Ok(())
    }

    /// Current file offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

/// Open a WAL segment for lazy reading and iterate with a callback.
///
/// Convenience function for single-pass replay: the callback receives each
/// header and decides whether to read or skip the payload.
pub fn replay_segment_lazy<F>(path: &Path, mut handler: F) -> Result<()>
where
    F: FnMut(&mut LazyWalReader, &RecordHeader) -> Result<()>,
{
    let mut reader = LazyWalReader::open(path)?;
    while let Some(header) = reader.next_header()? {
        handler(&mut reader, &header)?;
    }
    Ok(())
}

/// Replay all WAL segments in a directory with lazy reading.
///
/// Segments are read in LSN order. The callback decides per-record whether
/// to read or skip the payload.
pub fn replay_all_segments_lazy<F>(wal_dir: &Path, mut handler: F) -> Result<()>
where
    F: FnMut(&mut LazyWalReader, &RecordHeader) -> Result<()>,
{
    let segments = crate::segment::discover_segments(wal_dir)?;
    for seg in &segments {
        replay_segment_lazy(&seg.path, &mut handler)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;

    #[test]
    fn lazy_read_all_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"hello").unwrap();
            w.append(RecordType::VectorPut as u32, 1, 0, 0, b"vector-data")
                .unwrap();
            w.append(RecordType::Put as u32, 2, 1, 0, b"world").unwrap();
            w.sync().unwrap();
        }

        let mut reader = LazyWalReader::open(&path).unwrap();
        let mut records = Vec::new();
        while let Some(header) = reader.next_header().unwrap() {
            let payload = reader.read_payload(&header).unwrap();
            records.push((header, payload));
        }
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, b"hello");
        assert_eq!(records[1].1, b"vector-data");
        assert_eq!(records[2].1, b"world");
    }

    #[test]
    fn lazy_skip_non_matching() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            // 3 big TS records, 1 small vector record.
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.append(RecordType::VectorPut as u32, 1, 0, 0, b"small-vec")
                .unwrap();
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.sync().unwrap();
        }

        // A "vector core" reads only VectorPut, skips TimeseriesBatch.
        let mut reader = LazyWalReader::open(&path).unwrap();
        let mut vector_payloads = Vec::new();
        let mut skipped = 0;

        while let Some(header) = reader.next_header().unwrap() {
            let rt = RecordType::from_raw(header.record_type);
            if rt == Some(RecordType::VectorPut) {
                let payload = reader.read_payload(&header).unwrap();
                vector_payloads.push(payload);
            } else {
                reader.skip_payload(&header).unwrap();
                skipped += 1;
            }
        }

        assert_eq!(vector_payloads.len(), 1);
        assert_eq!(vector_payloads[0], b"small-vec");
        assert_eq!(skipped, 3);
    }

    #[test]
    fn replay_all_segments_lazy_works() {
        let dir = tempfile::tempdir().unwrap();
        // Use proper segment filename pattern: wal-{lsn:020}.seg
        let path = dir.path().join("wal-00000000000000000001.seg");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"a").unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"b").unwrap();
            w.sync().unwrap();
        }

        let mut count = 0;
        replay_all_segments_lazy(dir.path(), |reader, header| {
            reader.skip_payload(header)?;
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn empty_wal_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.sync().unwrap();
        }

        let mut reader = LazyWalReader::open(&path).unwrap();
        assert!(reader.next_header().unwrap().is_none());
    }
}
