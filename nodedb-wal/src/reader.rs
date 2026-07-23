// SPDX-License-Identifier: Apache-2.0

//! WAL reader for crash recovery and replay.
//!
//! Reads records sequentially from a WAL file, validating checksums and
//! magic numbers. Stops at the first corruption point — everything before
//! that point is the committed prefix.
//!
//! ## Replay invariants
//!
//! - Replay is **deterministic**: the same WAL file always produces the
//!   same sequence of records.
//! - Replay is **idempotent**: replaying the same record twice has the
//!   same effect as replaying it once.
//! - Unknown optional record types (bit 15 clear) are skipped.
//! - Unknown required record types (bit 15 set) cause a replay failure.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::{Result, WalError};
use crate::preamble::{PREAMBLE_SIZE, SegmentPreamble, WAL_PREAMBLE_MAGIC};
use crate::record::{HEADER_SIZE, RecordHeader, RecordType, WalRecord};

fn checked_offset_add(offset: u64, len: usize) -> Result<u64> {
    let len = u64::try_from(len).map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL read length does not fit u64",
        ))
    })?;
    offset.checked_add(len).ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL read offset overflow",
        ))
    })
}

/// Sequential WAL reader.
pub struct WalReader {
    file: File,
    offset: u64,
    /// Preamble read from offset 0 of this segment (present when encryption
    /// is active). The epoch is used as part of the AAD for decryption.
    segment_preamble: Option<SegmentPreamble>,
    /// Optional double-write buffer for torn write recovery.
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
}

impl WalReader {
    /// Open a WAL file for reading.
    ///
    /// If the file begins with a valid `WALP` preamble (16 bytes), it is
    /// consumed and stored for use as AAD during decryption. Files without a
    /// preamble (unencrypted segments) start reading from offset 0 directly.
    ///
    /// Automatically opens the companion double-write buffer file
    /// (`*.dwb`) if it exists alongside the WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
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

        // Attempt to read the preamble at offset 0.
        // If the first 4 bytes match WAL_PREAMBLE_MAGIC, consume the full
        // 16-byte preamble and validate it. Otherwise rewind to 0.
        let (segment_preamble, start_offset) = try_read_preamble(&mut file)?;

        Ok(Self {
            file,
            offset: start_offset,
            segment_preamble,
            double_write,
        })
    }

    /// The preamble read from this segment file, if present.
    ///
    /// Returns `None` for unencrypted segments (no preamble written).
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }

    /// Read the next record from the WAL.
    ///
    /// Returns `None` at EOF (clean end) or at the first corruption point.
    /// Returns `Err` only for I/O errors or unknown required record types.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        loop {
            // Read header.
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None); // Clean EOF.
                }
                Err(e) => return Err(e),
            }

            let header = RecordHeader::from_bytes(&header_buf);

            // Validate before using the attacker-controlled payload length for
            // allocation. Malformed oversized declarations are explicit errors;
            // ordinary corruption remains the end of the committed prefix.
            let header_offset = self
                .offset
                .checked_sub(u64::try_from(HEADER_SIZE).map_err(|_| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "WAL header size does not fit u64",
                    ))
                })?)
                .ok_or_else(|| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "WAL header offset underflow",
                    ))
                })?;
            match header.validate(header_offset) {
                Ok(()) => {}
                Err(error @ WalError::PayloadTooLarge { .. }) => return Err(error),
                Err(_) => return Ok(None),
            }

            // Read payload only after the bounded header validation above.
            let payload_len =
                usize::try_from(header.payload_len).map_err(|_| WalError::PayloadTooLarge {
                    size: usize::MAX,
                    max: crate::record::MAX_WAL_PAYLOAD_SIZE,
                })?;
            let mut payload = vec![0u8; payload_len];
            if !payload.is_empty() {
                match self.read_exact(&mut payload) {
                    Ok(()) => {}
                    Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                }
            }

            let record = WalRecord { header, payload };

            // Verify checksum.
            if record.verify_checksum().is_err() {
                if let Some(dwb) = &mut self.double_write
                    && let Ok(Some(recovered)) = dwb.recover_record(header.lsn)
                {
                    tracing::info!(
                        lsn = header.lsn,
                        "recovered torn write from double-write buffer"
                    );
                    self.offset = checked_offset_add(self.offset, recovered.payload.len())?;
                    return Ok(Some(recovered));
                }
                return Ok(None);
            }

            // Check if the record type is known (strip encrypted flag for lookup).
            let logical_type = record.logical_record_type();
            if RecordType::from_raw(logical_type).is_none() {
                if RecordType::is_required(logical_type) {
                    return Err(WalError::UnknownRequiredRecordType {
                        record_type: header.record_type,
                        lsn: header.lsn,
                    });
                }
                // Unknown optional record — skip and continue loop.
                continue;
            }

            return Ok(Some(record));
        }
    }

    /// Iterator over all valid records in the WAL.
    pub fn records(self) -> WalRecordIter {
        WalRecordIter { reader: self }
    }

    /// Current read offset in the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let next_offset = checked_offset_add(self.offset, buf.len())?;
        self.file.read_exact(buf)?;
        self.offset = next_offset;
        Ok(())
    }
}

/// Attempt to read a WAL segment preamble from the start of a file.
///
/// Returns `(Some(preamble), PREAMBLE_SIZE)` if a valid `WALP` preamble is
/// found, or `(None, 0)` if the file does not start with the preamble magic
/// (unencrypted segment — seek back to 0).
fn try_read_preamble(file: &mut File) -> Result<(Option<SegmentPreamble>, u64)> {
    use std::io::Seek;

    let mut buf = [0u8; PREAMBLE_SIZE];
    match file.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // File is too short to hold a preamble — no preamble present.
            file.seek(std::io::SeekFrom::Start(0))?;
            return Ok((None, 0));
        }
        Err(e) => return Err(WalError::Io(e)),
    }

    if buf[0..4] == WAL_PREAMBLE_MAGIC {
        // Parse and validate the preamble. An unsupported version is a hard
        // error — do not silently fall through to record scanning.
        let preamble = SegmentPreamble::from_bytes(&buf, &WAL_PREAMBLE_MAGIC)?;
        let preamble_size = u64::try_from(PREAMBLE_SIZE).map_err(|_| {
            WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL preamble size does not fit u64",
            ))
        })?;
        Ok((Some(preamble), preamble_size))
    } else {
        // First bytes are not the preamble magic — rewind and read as records.
        file.seek(std::io::SeekFrom::Start(0))?;
        Ok((None, 0))
    }
}

/// Iterator over WAL records.
pub struct WalRecordIter {
    reader: WalReader,
}

impl Iterator for WalRecordIter {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;

    #[test]
    fn write_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write records.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 2, 1, 0, b"second")
                .unwrap();
            writer
                .append(RecordType::Delete as u32, 1, 0, 0, b"third")
                .unwrap();
            writer.sync().unwrap();
        }

        // Read them back.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].header.lsn, 1);
        assert_eq!(records[0].header.tenant_id, 1);
        assert_eq!(records[0].payload, b"first");

        assert_eq!(records[1].header.lsn, 2);
        assert_eq!(records[1].header.tenant_id, 2);
        assert_eq!(records[1].header.vshard_id, 1);
        assert_eq!(records[1].payload, b"second");

        assert_eq!(records[2].header.lsn, 3);
        assert_eq!(records[2].header.record_type, RecordType::Delete as u32);
        assert_eq!(records[2].payload, b"third");
    }

    #[test]
    fn empty_wal_yields_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        // Create an empty file.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer.sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn truncated_file_stops_at_committed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.wal");

        // Write records.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"good-record")
                .unwrap();
            writer.sync().unwrap();
        }

        // Append garbage (simulating a torn write).
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"GARBAGE_PARTIAL_RECORD").unwrap();
        }

        // Reader should return only the valid record.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"good-record");
    }

    #[test]
    fn oversized_payload_header_is_a_typed_error_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.wal");
        let header = RecordHeader {
            magic: crate::record::WAL_MAGIC,
            format_version: crate::record::WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: (crate::record::MAX_WAL_PAYLOAD_SIZE + 1) as u32,
            database_id: 0,
            reserved: [0; 8],
            crc32c: 0,
        };
        std::fs::write(&path, header.to_bytes()).unwrap();

        let mut reader = WalReader::open(&path).unwrap();
        assert!(matches!(
            reader.next_record(),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn checked_offset_add_rejects_overflow() {
        assert!(checked_offset_add(u64::MAX, 1).is_err());
    }

    #[test]
    fn skip_many_unknown_optional_records_is_iterative() {
        // Record type 99 has bit 15 clear (99 & 0x8000 == 0) and is not a
        // known variant, so the reader must skip it as an unknown optional.
        // With the current recursive implementation (line 118: `return
        // self.next_record()`), 50 000 consecutive unknown optional records
        // exhaust the stack and panic. After the fix converts the skip to a
        // loop, all 50 000 are skipped without overflow and the one valid
        // record at the end is returned.
        const UNKNOWN_OPTIONAL: u32 = 99; // no 0x8000 bit → optional, not in enum
        const SKIP_COUNT: usize = 50_000;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many_unknown.wal");

        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            for _ in 0..SKIP_COUNT {
                writer
                    .append(UNKNOWN_OPTIONAL, 1, 0, 0, b"skip-me")
                    .unwrap();
            }
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"keep-me")
                .unwrap();
            writer.sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();

        // Only the single known Put record survives; all unknown optional
        // records are silently discarded.
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"keep-me");
    }
}
