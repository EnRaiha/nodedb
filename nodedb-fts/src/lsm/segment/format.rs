// SPDX-License-Identifier: Apache-2.0

//! Immutable segment file format for the FTS LSM engine.
//!
//! Layout:
//! ```text
//! [magic: 4 bytes "FTSS"]
//! [version: u16 LE]
//! [num_terms: u32 LE]
//! [term_dict_offset: u64 LE]  — byte offset to term dictionary
//! [posting_data_offset: u64 LE]
//! [... posting block data ...]
//! [... term dictionary entries ...]
//! [footer_crc: u32 LE]  — CRC32C of all preceding bytes
//! ```
//!
//! Term dictionary entry:
//! ```text
//! [term_len: u16 LE][term_bytes][posting_offset: u64 LE][posting_len: u32 LE][df: u32 LE]
//! ```

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;

use super::error::SegmentError;

/// Segment file magic bytes.
pub const MAGIC: &[u8; 4] = b"FTSS";

/// Current segment format version.
pub const VERSION: u16 = 1;

/// Maximum term byte length that can be stored (u16::MAX).
pub const MAX_TERM_LEN: usize = u16::MAX as usize;

/// Size of the fixed header (magic + version + num_terms + offsets).
pub const HEADER_SIZE: usize = 4 + 2 + 4 + 8 + 8; // 26 bytes

/// Size of the footer CRC field appended after all other data.
pub const FOOTER_SIZE: usize = 4;

/// Segment header parsed from raw bytes.
#[derive(Debug, Clone, Copy)]
pub struct SegmentHeader {
    pub version: u16,
    pub num_terms: u32,
    pub term_dict_offset: u64,
    pub posting_data_offset: u64,
}

/// A term dictionary entry pointing into the posting data.
#[derive(Debug, Clone)]
pub struct TermDictEntry {
    pub term: String,
    /// Byte offset into the posting data section.
    pub posting_offset: u64,
    /// Length of the posting data in bytes.
    pub posting_len: u32,
    /// Document frequency (number of postings for this term).
    pub df: u32,
}

/// Write the segment header.
pub fn write_header(
    buf: &mut Vec<u8>,
    num_terms: u32,
    term_dict_offset: u64,
    posting_data_offset: u64,
) {
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&num_terms.to_le_bytes());
    buf.extend_from_slice(&term_dict_offset.to_le_bytes());
    buf.extend_from_slice(&posting_data_offset.to_le_bytes());
}

/// Append the CRC32C footer covering all preceding bytes in `buf`.
pub fn write_footer_crc(buf: &mut Vec<u8>) {
    let crc = crc32c::crc32c(buf);
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Parse the segment header. Returns `Err` on magic/version/length mismatch.
pub fn parse_header(buf: &[u8]) -> Result<SegmentHeader, SegmentError> {
    // Minimum size: header + footer CRC.
    if buf.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(SegmentError::Truncated);
    }
    if &buf[0..4] != MAGIC {
        return Err(SegmentError::BadMagic);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != VERSION {
        return Err(SegmentError::UnsupportedVersion {
            found: version,
            expected: VERSION,
        });
    }
    let num_terms = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]);
    let term_dict_offset = u64::from_le_bytes(
        buf[10..18]
            .try_into()
            .map_err(|_| SegmentError::Truncated)?,
    );
    let posting_data_offset = u64::from_le_bytes(
        buf[18..26]
            .try_into()
            .map_err(|_| SegmentError::Truncated)?,
    );

    Ok(SegmentHeader {
        version,
        num_terms,
        term_dict_offset,
        posting_data_offset,
    })
}

/// Verify the CRC32C footer. The last 4 bytes must equal CRC32C of all preceding bytes.
pub fn verify_footer_crc(buf: &[u8]) -> Result<(), SegmentError> {
    if buf.len() < FOOTER_SIZE {
        return Err(SegmentError::Truncated);
    }
    let (body, crc_bytes) = buf.split_at(buf.len() - FOOTER_SIZE);
    let stored = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    let actual = crc32c::crc32c(body);
    if stored != actual {
        return Err(SegmentError::ChecksumMismatch {
            expected: stored,
            actual,
        });
    }
    Ok(())
}

/// Write a term dictionary entry. Returns `Err(SegmentError::TermTooLong)` if
/// `entry.term` exceeds `MAX_TERM_LEN` bytes.
pub fn write_term_entry(buf: &mut Vec<u8>, entry: &TermDictEntry) -> Result<(), SegmentError> {
    let term_len = entry.term.len();
    if term_len > MAX_TERM_LEN {
        return Err(SegmentError::TermTooLong {
            term_len,
            max: MAX_TERM_LEN,
        });
    }
    buf.extend_from_slice(&(term_len as u16).to_le_bytes());
    buf.extend_from_slice(entry.term.as_bytes());
    buf.extend_from_slice(&entry.posting_offset.to_le_bytes());
    buf.extend_from_slice(&entry.posting_len.to_le_bytes());
    buf.extend_from_slice(&entry.df.to_le_bytes());
    Ok(())
}

/// Parse term dictionary entries from the term dict section.
///
/// Returns `None` if the buffer is malformed (used internally where the caller
/// already checked the CRC and header; `None` here means structural corruption).
pub fn parse_term_dict(buf: &[u8], num_terms: u32) -> Option<Vec<TermDictEntry>> {
    let num_terms = usize::try_from(num_terms).ok()?;
    // A term entry has at least its two-byte length and three fixed numeric
    // fields (16 bytes), so the declared count cannot exceed frame capacity.
    const MIN_TERM_ENTRY_BYTES: usize = 18;
    const MAX_TERM_DICT_ALLOCATION_BYTES: usize = 64 * 1024 * 1024;
    if num_terms > buf.len() / MIN_TERM_ENTRY_BYTES {
        return None;
    }
    let entries_capacity = checked_decode_capacity(
        num_terms,
        size_of::<TermDictEntry>(),
        buf.len(),
        MIN_TERM_ENTRY_BYTES,
        buf.len() / MIN_TERM_ENTRY_BYTES,
        MAX_TERM_DICT_ALLOCATION_BYTES,
    )?;
    let mut entries = Vec::with_capacity(entries_capacity);
    let mut pos = 0;
    for _ in 0..num_terms {
        if pos + 2 > buf.len() {
            return None;
        }
        let term_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if pos + term_len + 16 > buf.len() {
            return None;
        }
        let term = std::str::from_utf8(&buf[pos..pos + term_len])
            .ok()?
            .to_string();
        pos += term_len;
        let posting_offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let posting_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let df = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
        pos += 4;
        entries.push(TermDictEntry {
            term,
            posting_offset,
            posting_len,
            df,
        });
    }
    Some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid segment buffer (no postings, no terms) for format tests.
    fn minimal_segment() -> Vec<u8> {
        let mut buf = Vec::new();
        // Header: 0 terms, term_dict_offset and posting_data_offset both point just after header.
        write_header(&mut buf, 0, HEADER_SIZE as u64, HEADER_SIZE as u64);
        write_footer_crc(&mut buf);
        buf
    }

    // ── golden format test ─────────────────────────────────────────────

    #[test]
    fn golden_magic_version_crc() {
        let buf = minimal_segment();

        // Magic at 0..4.
        assert_eq!(&buf[0..4], b"FTSS", "magic mismatch");

        // Version at 4..6.
        let version = u16::from_le_bytes([buf[4], buf[5]]);
        assert_eq!(version, VERSION, "version mismatch");
        assert_eq!(version, 1, "expected VERSION==1");

        // Footer CRC: last 4 bytes must be internally consistent.
        verify_footer_crc(&buf).expect("footer CRC should be valid");

        // Recompute manually to be sure.
        let (body, crc_bytes) = buf.split_at(buf.len() - FOOTER_SIZE);
        let stored = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let recomputed = crc32c::crc32c(body);
        assert_eq!(stored, recomputed, "manual CRC re-computation mismatch");
    }

    // ── Corruption-rejection tests ───────────────────────────────────────────

    #[test]
    fn corruption_flipped_crc_byte() {
        let mut buf = minimal_segment();
        // Flip the last byte of the CRC field.
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        let err = verify_footer_crc(&buf).unwrap_err();
        assert!(
            matches!(err, SegmentError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err}"
        );
    }

    #[test]
    fn version_reject() {
        let mut buf = minimal_segment();
        // Overwrite version field with 2 (old version).
        let v2: [u8; 2] = 2u16.to_le_bytes();
        buf[4] = v2[0];
        buf[5] = v2[1];
        let err = parse_header(&buf).unwrap_err();
        assert!(
            matches!(
                err,
                SegmentError::UnsupportedVersion {
                    found: 2,
                    expected: 1
                }
            ),
            "expected UnsupportedVersion, got {err}"
        );
    }

    #[test]
    fn bad_magic_reject() {
        let mut buf = minimal_segment();
        buf[0] = 0x00;
        let err = parse_header(&buf).unwrap_err();
        assert!(
            matches!(err, SegmentError::BadMagic),
            "expected BadMagic, got {err}"
        );
    }

    #[test]
    fn term_too_long_reject() {
        let long_term = "x".repeat(u16::MAX as usize + 1);
        let entry = TermDictEntry {
            term: long_term.clone(),
            posting_offset: 0,
            posting_len: 0,
            df: 0,
        };
        let mut buf = Vec::new();
        let err = write_term_entry(&mut buf, &entry).unwrap_err();
        assert!(
            matches!(err, SegmentError::TermTooLong { term_len, max } if term_len == long_term.len() && max == MAX_TERM_LEN),
            "expected TermTooLong, got {err}"
        );
    }

    // ── Existing roundtrip tests (updated for Result API) ───────────────────

    #[test]
    fn header_roundtrip() {
        let buf = minimal_segment();
        let header = parse_header(&buf).unwrap();
        assert_eq!(header.num_terms, 0);
        assert_eq!(header.version, VERSION);
    }

    #[test]
    fn rejects_huge_term_count_with_tiny_dictionary_before_allocation() {
        assert!(parse_term_dict(&[], u32::MAX).is_none());
    }

    #[test]
    fn term_dict_roundtrip() {
        let entries = vec![
            TermDictEntry {
                term: "hello".into(),
                posting_offset: 0,
                posting_len: 50,
                df: 10,
            },
            TermDictEntry {
                term: "world".into(),
                posting_offset: 50,
                posting_len: 30,
                df: 5,
            },
        ];
        let mut buf = Vec::new();
        for e in &entries {
            write_term_entry(&mut buf, e).unwrap();
        }
        let parsed = parse_term_dict(&buf, 2).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].term, "hello");
        assert_eq!(parsed[1].posting_len, 30);
    }

    #[test]
    fn bad_magic_returns_err() {
        let buf = vec![0u8; HEADER_SIZE + FOOTER_SIZE];
        assert!(matches!(parse_header(&buf), Err(SegmentError::BadMagic)));
    }

    #[test]
    fn truncated_buffer_returns_err() {
        // Buffer shorter than minimum.
        let buf = vec![0u8; HEADER_SIZE - 1];
        assert!(matches!(parse_header(&buf), Err(SegmentError::Truncated)));
    }
}
