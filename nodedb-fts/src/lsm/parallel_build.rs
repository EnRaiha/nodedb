// SPDX-License-Identifier: Apache-2.0

//! Parallel index build: workers scan disjoint document ranges, build
//! local memtables, flush to temporary segments. Leader performs N-way
//! merge into a single compacted segment.
//!
//! This module provides the building blocks. The caller (Data Plane)
//! is responsible for partitioning documents and spawning workers.

use std::collections::HashMap;

use nodedb_mem::ScopedMemory;
use nodedb_types::Surrogate;

use crate::block::CompactPosting;
use crate::codec::smallfloat;

use super::merge;
use super::segment::{reader::SegmentReader, writer};

/// A worker's accumulated result: per-term postings ready to flush.
pub struct WorkerResult {
    /// term → list of CompactPostings keyed on `Surrogate` row identities.
    pub term_postings: HashMap<String, Vec<CompactPosting>>,
}

impl WorkerResult {
    pub fn new() -> Self {
        Self {
            term_postings: HashMap::new(),
        }
    }

    /// Add a posting for a term (called by each worker per token).
    pub fn insert(&mut self, term: &str, posting: CompactPosting) {
        self.term_postings
            .entry(term.to_string())
            .or_default()
            .push(posting);
    }

    /// Flush this worker's result to a temporary segment.
    ///
    /// Panics if any term exceeds `MAX_TERM_LEN` — callers must validate
    /// term lengths before insertion.
    pub fn flush_to_segment(self) -> Vec<u8> {
        writer::flush_to_segment(self.term_postings).expect(
            "worker result contained a term exceeding u16::MAX bytes — caller invariant violated",
        )
    }
}

impl Default for WorkerResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Merge multiple worker segments into a single compacted segment.
///
/// This is the "leader" step: takes the flushed segments from all workers
/// and performs N-way merge into one final segment.
pub fn merge_worker_segments(worker_segments: Vec<Vec<u8>>, memory: &ScopedMemory) -> Vec<u8> {
    let readers: Vec<SegmentReader> = worker_segments
        .into_iter()
        .filter_map(|data| SegmentReader::open(data).ok())
        .collect();

    if readers.is_empty() {
        return writer::build_from_blocks(&[])
            .expect("build_from_blocks on empty input must not fail");
    }

    let merged_term_blocks = merge::merge_segments(&readers, memory);
    writer::build_from_blocks(&merged_term_blocks)
        .expect("merge produced a term exceeding u16::MAX bytes — data invariant violated")
}

/// Partition a document range into `num_workers` disjoint sub-ranges.
///
/// Returns `(start_doc_id, end_doc_id_exclusive)` for each worker.
/// The caller should iterate docs in `[start, end)` and feed to a `WorkerResult`.
pub fn partition_doc_range(total_docs: u32, num_workers: u32) -> Vec<(u32, u32)> {
    if total_docs == 0 || num_workers == 0 {
        return Vec::new();
    }
    let chunk = total_docs / num_workers;
    let remainder = total_docs % num_workers;
    let mut ranges = Vec::with_capacity(num_workers as usize);
    let mut start = 0;
    for i in 0..num_workers {
        let extra = if i < remainder { 1 } else { 0 };
        let end = start + chunk + extra;
        ranges.push((start, end));
        start = end;
    }
    ranges
}

/// Helper: create a CompactPosting for parallel build.
pub fn make_compact_posting(
    doc_id: Surrogate,
    term_freq: u32,
    doc_len: u32,
    positions: Vec<u32>,
) -> CompactPosting {
    CompactPosting {
        doc_id,
        term_freq,
        fieldnorm: smallfloat::encode(doc_len),
        positions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    #[test]
    fn partition_even() {
        let ranges = partition_doc_range(100, 4);
        assert_eq!(ranges, vec![(0, 25), (25, 50), (50, 75), (75, 100)]);
    }

    #[test]
    fn partition_uneven() {
        let ranges = partition_doc_range(10, 3);
        // 10 / 3 = 3 remainder 1. First worker gets 4, rest get 3.
        assert_eq!(ranges, vec![(0, 4), (4, 7), (7, 10)]);
    }

    #[test]
    fn partition_zero() {
        assert!(partition_doc_range(0, 4).is_empty());
        assert!(partition_doc_range(10, 0).is_empty());
    }

    #[test]
    fn worker_result_flush_and_merge() {
        // Worker 1: docs 0-1.
        let mut w1 = WorkerResult::new();
        w1.insert(
            "hello",
            make_compact_posting(Surrogate(0), 2, 100, vec![0, 3]),
        );
        w1.insert("hello", make_compact_posting(Surrogate(1), 1, 50, vec![5]));
        w1.insert("world", make_compact_posting(Surrogate(0), 1, 100, vec![1]));

        // Worker 2: docs 2-3.
        let mut w2 = WorkerResult::new();
        w2.insert("hello", make_compact_posting(Surrogate(2), 1, 80, vec![0]));
        w2.insert(
            "foo",
            make_compact_posting(Surrogate(3), 3, 120, vec![0, 2, 7]),
        );

        let seg1 = w1.flush_to_segment();
        let seg2 = w2.flush_to_segment();

        // Leader merge.
        let merged = merge_worker_segments(vec![seg1, seg2], &test_memory());

        // Verify merged segment.
        let reader = SegmentReader::open(merged).expect("merged segment must be valid");
        assert_eq!(reader.num_terms(), 3); // hello, world, foo
        assert_eq!(reader.df("hello"), 3); // docs 0, 1, 2
        assert_eq!(reader.df("world"), 1); // doc 0
        assert_eq!(reader.df("foo"), 1); // doc 3
    }

    #[test]
    fn merge_empty_workers() {
        let merged = merge_worker_segments(Vec::new(), &test_memory());
        let reader = SegmentReader::open(merged).expect("merged segment must be valid");
        assert_eq!(reader.num_terms(), 0);
    }
}
