// SPDX-License-Identifier: Apache-2.0

//! N-way merge of posting lists across multiple segments.
//!
//! Decompresses blocks on demand, merges sorted doc_id streams per-term,
//! and re-encodes into new compressed PostingBlocks.

use std::collections::{BTreeSet, HashMap};

use nodedb_types::Surrogate;

use crate::block::{CompactPosting, PostingBlock, into_blocks};

use super::segment::reader::SegmentReader;

use nodedb_mem::ScopedMemory;

/// Merge multiple segments into a single set of per-term PostingBlocks.
///
/// The result is a sorted list of `(term, blocks)` suitable for
/// `segment::writer::build_from_blocks`.
///
/// When `memory` is `Some`, the `Vec::with_capacity` for the result is
/// budgeted via [`ScopedMemory::reserve`] before the allocation.
/// If the budget is exceeded the allocation still proceeds — the scope
/// serves as an accounting and backpressure signal; callers that need hard
/// rejection should check pressure before dispatching the operation.
pub fn merge_segments(
    segments: &[SegmentReader],
    memory: Option<&ScopedMemory>,
) -> Vec<(String, Vec<PostingBlock>)> {
    // Collect all unique terms across all segments.
    let mut all_terms = BTreeSet::new();
    for seg in segments {
        for entry in seg.term_dict() {
            all_terms.insert(entry.term.clone());
        }
    }

    let _result_guard = memory.and_then(|mem| {
        let bytes = all_terms.len()
            * (std::mem::size_of::<String>() + std::mem::size_of::<Vec<PostingBlock>>());
        mem.reserve(bytes).ok()
    });

    let mut result = Vec::with_capacity(all_terms.len());

    for term in &all_terms {
        // Gather all postings for this term across segments.
        let mut merged_postings: Vec<CompactPosting> = Vec::new();

        for seg in segments {
            let blocks = seg.read_postings(term);
            for block in blocks {
                for i in 0..block.doc_ids.len() {
                    merged_postings.push(CompactPosting {
                        doc_id: block.doc_ids[i],
                        term_freq: block.term_freqs[i],
                        fieldnorm: block.fieldnorms[i],
                        positions: block.positions[i].clone(),
                    });
                }
            }
        }

        if merged_postings.is_empty() {
            continue;
        }

        // Sort by doc_id and deduplicate (later segment wins on conflict).
        merged_postings.sort_by_key(|p| p.doc_id);
        dedup_postings(&mut merged_postings);

        let blocks = into_blocks(merged_postings);
        result.push((term.clone(), blocks));
    }

    result
}

/// Merge posting lists from a memtable and multiple segments for a single term.
///
/// Returns CompactPostings sorted by doc_id. Memtable postings take precedence
/// over segment postings on doc_id conflict (memtable is newer).
pub fn merge_term_postings(
    memtable_postings: &[CompactPosting],
    segment_postings: &[Vec<CompactPosting>],
) -> Vec<CompactPosting> {
    let mut all: Vec<CompactPosting> = Vec::new();

    for seg_posts in segment_postings {
        all.extend(seg_posts.iter().cloned());
    }
    // Memtable postings added last so they win dedup (newer data).
    all.extend(memtable_postings.iter().cloned());

    all.sort_by_key(|p| p.doc_id);
    dedup_postings(&mut all);
    all
}

/// Remove duplicate doc_ids, keeping the LAST occurrence (most recent).
///
/// Public so BMW query can use it when merging LSM + backend postings.
pub fn dedup_postings(postings: &mut Vec<CompactPosting>) {
    if postings.len() <= 1 {
        return;
    }
    // Walk backward: for each doc_id, keep only the last (rightmost) occurrence.
    let mut seen: HashMap<Surrogate, usize> = HashMap::new();
    for (i, p) in postings.iter().enumerate() {
        seen.insert(p.doc_id, i);
    }
    let mut keep: Vec<usize> = seen.into_values().collect();
    keep.sort_unstable();
    let kept: Vec<CompactPosting> = keep.into_iter().map(|i| postings[i].clone()).collect();
    *postings = kept;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::smallfloat;
    use crate::lsm::segment::writer;

    fn cp(doc_id: u32, tf: u32) -> CompactPosting {
        CompactPosting {
            doc_id: Surrogate(doc_id),
            term_freq: tf,
            fieldnorm: smallfloat::encode(100),
            positions: vec![0],
        }
    }

    #[test]
    fn merge_two_segments() {
        let seg1 = {
            let mut m = std::collections::HashMap::new();
            m.insert("hello".to_string(), vec![cp(0, 1), cp(2, 1)]);
            m.insert("world".to_string(), vec![cp(0, 1)]);
            writer::flush_to_segment(m).unwrap()
        };
        let seg2 = {
            let mut m = std::collections::HashMap::new();
            m.insert("hello".to_string(), vec![cp(1, 2), cp(3, 1)]);
            m.insert("foo".to_string(), vec![cp(1, 1)]);
            writer::flush_to_segment(m).unwrap()
        };

        let r1 = SegmentReader::open(seg1).expect("seg1 must be valid");
        let r2 = SegmentReader::open(seg2).expect("seg2 must be valid");

        let merged = merge_segments(&[r1, r2], None);
        let terms: Vec<&str> = merged.iter().map(|(t, _)| t.as_str()).collect();
        assert!(terms.contains(&"hello"));
        assert!(terms.contains(&"world"));
        assert!(terms.contains(&"foo"));

        // "hello" should have 4 docs (0, 1, 2, 3).
        let hello_blocks = &merged.iter().find(|(t, _)| t == "hello").unwrap().1;
        let total_docs: usize = hello_blocks.iter().map(|b| b.len()).sum();
        assert_eq!(total_docs, 4);
    }

    #[test]
    fn merge_term_dedup() {
        let mt_posts = vec![cp(0, 5)]; // Memtable version (newer, tf=5).
        let seg_posts = vec![vec![cp(0, 1), cp(1, 1)]]; // Segment version (older, tf=1).

        let merged = merge_term_postings(&mt_posts, &seg_posts);
        assert_eq!(merged.len(), 2); // doc 0 and doc 1.
        // Doc 0 should have the memtable version (tf=5) since it was added last.
        let doc0 = merged.iter().find(|p| p.doc_id == Surrogate(0)).unwrap();
        assert_eq!(doc0.term_freq, 5);
    }

    #[test]
    fn dedup_keeps_last() {
        let mut posts = vec![cp(0, 1), cp(0, 5), cp(1, 2)];
        dedup_postings(&mut posts);
        assert_eq!(posts.len(), 2);
        assert_eq!(posts[0].doc_id, Surrogate(0));
        assert_eq!(posts[0].term_freq, 5); // Last occurrence wins.
    }
}
