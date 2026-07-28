// SPDX-License-Identifier: Apache-2.0

//! Segment reader: reads term dictionary and decodes posting blocks on demand.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;

use crate::block::PostingBlock;

use super::error::SegmentError;
use super::format::{self, SegmentHeader, TermDictEntry};

/// Reader for an immutable segment.
///
/// Holds a reference to the raw segment bytes and the parsed term dictionary.
/// Posting blocks are decoded on demand (not all at once).
#[derive(Debug)]
pub struct SegmentReader {
    /// Raw segment bytes (including footer CRC).
    data: Vec<u8>,
    /// Parsed header.
    header: SegmentHeader,
    /// Parsed term dictionary (sorted by term).
    term_dict: Vec<TermDictEntry>,
}

impl SegmentReader {
    /// Open a segment from raw bytes.
    ///
    /// Verifies the CRC32C footer before parsing any other field. Returns a
    /// typed `SegmentError` on any validation failure — no silent `None`.
    pub fn open(data: Vec<u8>) -> Result<Self, SegmentError> {
        // CRC must be verified first so every subsequent parse is over known-good bytes.
        format::verify_footer_crc(&data)?;

        let header = format::parse_header(&data)?;

        let dict_start = header.term_dict_offset as usize;
        // term_dict_offset must point into the body (before the footer CRC).
        let body_end = data.len() - format::FOOTER_SIZE;
        if dict_start > body_end {
            return Err(SegmentError::Truncated);
        }

        let term_dict = format::parse_term_dict(&data[dict_start..body_end], header.num_terms)
            .ok_or(SegmentError::Truncated)?;

        Ok(Self {
            data,
            header,
            term_dict,
        })
    }

    /// Number of terms in this segment.
    pub fn num_terms(&self) -> usize {
        self.term_dict.len()
    }

    /// Get the term dictionary entries (sorted by term).
    pub fn term_dict(&self) -> &[TermDictEntry] {
        &self.term_dict
    }

    /// Look up a term in the dictionary. Returns `None` if not found.
    pub fn find_term(&self, term: &str) -> Option<&TermDictEntry> {
        self.term_dict
            .binary_search_by_key(&term, |e| e.term.as_str())
            .ok()
            .map(|idx| &self.term_dict[idx])
    }

    /// Read and decode posting blocks for a term.
    ///
    /// Returns empty vec if the term is not in this segment.
    pub fn read_postings(&self, term: &str) -> Vec<PostingBlock> {
        let Some(entry) = self.find_term(term) else {
            return Vec::new();
        };

        let Some(start) = usize::try_from(self.header.posting_data_offset)
            .ok()
            .and_then(|base| {
                usize::try_from(entry.posting_offset)
                    .ok()
                    .and_then(|offset| base.checked_add(offset))
            })
        else {
            return Vec::new();
        };
        let Some(end) = usize::try_from(entry.posting_len)
            .ok()
            .and_then(|len| start.checked_add(len))
        else {
            return Vec::new();
        };
        let Some(body_end) = self.data.len().checked_sub(format::FOOTER_SIZE) else {
            return Vec::new();
        };
        if end > body_end {
            return Vec::new();
        }

        let buf = &self.data[start..end];
        decode_term_blocks(buf)
    }

    /// Get all unique terms in this segment.
    pub fn terms(&self) -> Vec<String> {
        self.term_dict.iter().map(|e| e.term.clone()).collect()
    }

    /// Get the document frequency for a term.
    pub fn df(&self, term: &str) -> u32 {
        self.find_term(term).map(|e| e.df).unwrap_or(0)
    }
}

/// Decode posting blocks from the term's posting data bytes.
///
/// Format: [num_blocks: u32 LE][for each: block_len: u32 LE, block_bytes]
fn decode_term_blocks(buf: &[u8]) -> Vec<PostingBlock> {
    if buf.len() < 4 {
        return Vec::new();
    }
    let num_blocks = match usize::try_from(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])) {
        Ok(count) => count,
        Err(_) => return Vec::new(),
    };
    // Each block has at least its four-byte length prefix, so the remaining
    // bytes prove a safe upper bound before reserving the result vector.
    if num_blocks > (buf.len() - 4) / 4 {
        return Vec::new();
    }
    const MAX_POSTING_BLOCK_ALLOCATION_BYTES: usize = 64 * 1024 * 1024;
    let Some(blocks_capacity) = checked_decode_capacity(
        num_blocks,
        size_of::<PostingBlock>(),
        buf.len() - 4,
        4,
        (buf.len() - 4) / 4,
        MAX_POSTING_BLOCK_ALLOCATION_BYTES,
    ) else {
        return Vec::new();
    };
    let mut pos = 4;
    let mut blocks = Vec::with_capacity(blocks_capacity);

    for _ in 0..num_blocks {
        if pos + 4 > buf.len() {
            break;
        }
        let block_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + block_len > buf.len() {
            break;
        }
        if let Some(block) = PostingBlock::from_bytes(&buf[pos..pos + block_len]) {
            blocks.push(block);
        }
        pos += block_len;
    }

    blocks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::CompactPosting;
    use crate::codec::smallfloat;
    use crate::lsm::segment::writer;
    use std::collections::HashMap;

    fn make_segment() -> Vec<u8> {
        let mut postings = HashMap::new();
        postings.insert(
            "alpha".to_string(),
            vec![
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(0),
                    term_freq: 2,
                    fieldnorm: smallfloat::encode(50),
                    positions: vec![0, 3],
                },
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(5),
                    term_freq: 1,
                    fieldnorm: smallfloat::encode(100),
                    positions: vec![7],
                },
            ],
        );
        postings.insert(
            "beta".to_string(),
            vec![CompactPosting {
                doc_id: nodedb_types::Surrogate(0),
                term_freq: 1,
                fieldnorm: smallfloat::encode(50),
                positions: vec![1],
            }],
        );
        writer::flush_to_segment(postings).expect("flush must succeed in test")
    }

    #[test]
    fn rejects_huge_block_count_with_tiny_payload_before_allocation() {
        assert!(decode_term_blocks(&u32::MAX.to_le_bytes()).is_empty());
    }

    #[test]
    fn open_and_read() {
        let seg_data = make_segment();
        let reader = SegmentReader::open(seg_data).unwrap();
        assert_eq!(reader.num_terms(), 2);

        let blocks = reader.read_postings("alpha");
        assert_eq!(blocks.len(), 1); // 2 docs fit in 1 block.
        assert_eq!(
            blocks[0].doc_ids,
            vec![nodedb_types::Surrogate(0), nodedb_types::Surrogate(5)]
        );
        assert_eq!(blocks[0].term_freqs, vec![2, 1]);
    }

    #[test]
    fn overflowing_posting_offset_returns_empty() {
        let seg_data = make_segment();
        let mut reader = SegmentReader::open(seg_data).unwrap();
        reader.term_dict[0].posting_offset = u64::MAX;
        assert!(reader.read_postings("alpha").is_empty());
    }

    #[test]
    fn find_term() {
        let seg_data = make_segment();
        let reader = SegmentReader::open(seg_data).unwrap();

        assert!(reader.find_term("alpha").is_some());
        assert!(reader.find_term("beta").is_some());
        assert!(reader.find_term("gamma").is_none());
        assert_eq!(reader.df("alpha"), 2);
        assert_eq!(reader.df("beta"), 1);
    }

    #[test]
    fn missing_term_returns_empty() {
        let seg_data = make_segment();
        let reader = SegmentReader::open(seg_data).unwrap();
        assert!(reader.read_postings("nonexistent").is_empty());
    }

    #[test]
    fn terms_list() {
        let seg_data = make_segment();
        let reader = SegmentReader::open(seg_data).unwrap();
        let mut terms = reader.terms();
        terms.sort();
        assert_eq!(terms, vec!["alpha", "beta"]);
    }

    #[test]
    fn corrupted_crc_rejected() {
        let mut seg_data = make_segment();
        // Flip the last byte of the CRC footer.
        let last = seg_data.len() - 1;
        seg_data[last] ^= 0xFF;
        let err = SegmentReader::open(seg_data).unwrap_err();
        assert!(
            matches!(err, SegmentError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err}"
        );
    }

    #[test]
    fn old_version_rejected() {
        // Build a valid v3 segment then patch the version field to 2.
        let mut seg_data = make_segment();
        let v2: [u8; 2] = 2u16.to_le_bytes();
        seg_data[4] = v2[0];
        seg_data[5] = v2[1];
        // Also fix up the CRC so the version-check is reached.
        let body_end = seg_data.len() - 4;
        let new_crc = crc32c::crc32c(&seg_data[..body_end]);
        let crc_bytes = new_crc.to_le_bytes();
        seg_data[body_end..].copy_from_slice(&crc_bytes);

        let err = SegmentReader::open(seg_data).unwrap_err();
        assert!(
            matches!(err, SegmentError::UnsupportedVersion { found: 2, .. }),
            "expected UnsupportedVersion, got {err}"
        );
    }
}
