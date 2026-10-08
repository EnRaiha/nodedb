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
    /// Returns an empty vec if the term is not in this segment. Posting data
    /// that lies outside the segment body or does not decode is
    /// [`SegmentError::CorruptPostings`].
    pub fn read_postings(&self, term: &str) -> Result<Vec<PostingBlock>, SegmentError> {
        let Some(entry) = self.find_term(term) else {
            return Ok(Vec::new());
        };
        let corrupt = || SegmentError::CorruptPostings {
            term: term.to_string(),
        };

        let start = usize::try_from(self.header.posting_data_offset)
            .ok()
            .and_then(|base| {
                usize::try_from(entry.posting_offset)
                    .ok()
                    .and_then(|offset| base.checked_add(offset))
            })
            .ok_or_else(corrupt)?;
        let end = usize::try_from(entry.posting_len)
            .ok()
            .and_then(|len| start.checked_add(len))
            .ok_or_else(corrupt)?;
        let body_end = self
            .data
            .len()
            .checked_sub(format::FOOTER_SIZE)
            .ok_or_else(corrupt)?;
        if end > body_end {
            return Err(corrupt());
        }

        decode_term_blocks(&self.data[start..end]).ok_or_else(corrupt)
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
///
/// `None` when the bytes do not hold every block the count names, or a
/// block does not decode.
fn decode_term_blocks(buf: &[u8]) -> Option<Vec<PostingBlock>> {
    if buf.len() < 4 {
        return None;
    }
    let num_blocks = usize::try_from(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])).ok()?;
    // Each block has at least its four-byte length prefix, so the remaining
    // bytes prove a safe upper bound before reserving the result vector.
    if num_blocks > (buf.len() - 4) / 4 {
        return None;
    }
    const MAX_POSTING_BLOCK_ALLOCATION_BYTES: usize = 64 * 1024 * 1024;
    let blocks_capacity = checked_decode_capacity(
        num_blocks,
        size_of::<PostingBlock>(),
        buf.len() - 4,
        4,
        (buf.len() - 4) / 4,
        MAX_POSTING_BLOCK_ALLOCATION_BYTES,
    )?;
    let mut pos: usize = 4;
    let mut blocks = Vec::with_capacity(blocks_capacity);

    for _ in 0..num_blocks {
        let len_end = pos.checked_add(4).filter(|end| *end <= buf.len())?;
        let block_len = usize::try_from(u32::from_le_bytes([
            buf[pos],
            buf[pos + 1],
            buf[pos + 2],
            buf[pos + 3],
        ]))
        .ok()?;
        pos = len_end;
        let block_end = pos.checked_add(block_len).filter(|end| *end <= buf.len())?;
        blocks.push(PostingBlock::from_bytes(&buf[pos..block_end])?);
        pos = block_end;
    }

    Some(blocks)
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
        assert!(decode_term_blocks(&u32::MAX.to_le_bytes()).is_none());
    }

    #[test]
    fn a_truncated_block_list_does_not_decode() {
        // Two blocks named, one empty block present.
        let mut buf = 2u32.to_le_bytes().to_vec();
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_term_blocks(&buf).is_none());
    }

    #[test]
    fn open_and_read() {
        let seg_data = make_segment();
        let reader = SegmentReader::open(seg_data).unwrap();
        assert_eq!(reader.num_terms(), 2);

        let blocks = reader.read_postings("alpha").unwrap();
        assert_eq!(blocks.len(), 1); // 2 docs fit in 1 block.
        assert_eq!(
            blocks[0].doc_ids,
            vec![nodedb_types::Surrogate(0), nodedb_types::Surrogate(5)]
        );
        assert_eq!(blocks[0].term_freqs, vec![2, 1]);
    }

    #[test]
    fn overflowing_posting_offset_is_corrupt() {
        let seg_data = make_segment();
        let mut reader = SegmentReader::open(seg_data).unwrap();
        reader.term_dict[0].posting_offset = u64::MAX;
        assert!(matches!(
            reader.read_postings("alpha"),
            Err(SegmentError::CorruptPostings { ref term }) if term == "alpha"
        ));
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
        assert!(reader.read_postings("nonexistent").unwrap().is_empty());
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
