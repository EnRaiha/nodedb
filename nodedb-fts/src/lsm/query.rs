// SPDX-License-Identifier: Apache-2.0

//! Multi-source query: merge postings from memtable + all segments,
//! then feed the merged stream to BMW or exhaustive scorer.

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::index::writer::{memtable_collection_prefix, memtable_key};
use crate::search::bmw::skip_index::TermBlocks;

use super::memtable::Memtable;
use super::merge::merge_term_postings;
use super::segment::reader::SegmentReader;

use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor};

/// Collect posting lists for a set of query tokens by merging across
/// the active memtable and all immutable segments.
///
/// Returns per-term `TermBlocks` ready for BMW scoring.
///
/// When `governor` is `Some`, the `Vec::with_capacity` for `term_blocks_list`
/// is budgeted via [`MemoryGovernor::reserve`] before the allocation.
/// If the budget is exhausted the allocation still proceeds — the governor
/// serves as an accounting and backpressure signal.
pub fn collect_merged_term_blocks<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    collection: &str,
    memtable: &Memtable,
    query_tokens: &[String],
    governor: Option<&Arc<MemoryGovernor>>,
) -> Result<Vec<TermBlocks>, B::Error> {
    let seg_ids = backend.list_segments(database_id, tid, collection)?;
    let mut readers: Vec<SegmentReader> = Vec::new();
    for id in &seg_ids {
        if let Some(data) = backend.read_segment(database_id, tid, collection, id)?
            && let Ok(reader) = SegmentReader::open(data)
        {
            readers.push(reader);
        }
    }

    let _term_blocks_guard = governor.and_then(|gov| {
        let bytes = query_tokens.len() * std::mem::size_of::<TermBlocks>();
        gov.reserve(EngineId::Fts, bytes).ok()
    });

    let mut term_blocks_list = Vec::with_capacity(query_tokens.len());

    for token in query_tokens {
        let scoped_term = memtable_key(database_id, tid, collection, token);
        let mt_postings = memtable.get_postings(&scoped_term);

        let seg_postings: Vec<Vec<CompactPosting>> = readers
            .iter()
            .map(|reader| {
                let blocks = reader.read_postings(token);
                let mut postings = Vec::new();
                for block in blocks {
                    for i in 0..block.doc_ids.len() {
                        postings.push(CompactPosting {
                            doc_id: block.doc_ids[i],
                            term_freq: block.term_freqs[i],
                            fieldnorm: block.fieldnorms[i],
                            positions: block.positions[i].clone(),
                        });
                    }
                }
                postings
            })
            .collect();

        let merged = merge_term_postings(&mt_postings, &seg_postings);
        if merged.is_empty() {
            term_blocks_list.push(TermBlocks::from_blocks(Vec::new()));
            continue;
        }

        let blocks = into_blocks(merged);
        term_blocks_list.push(TermBlocks::from_blocks(blocks));
    }

    Ok(term_blocks_list)
}

/// Collect all unique term names across memtable + segments for a collection.
///
/// Used by fuzzy matching to scan available terms.
pub fn collect_all_terms<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    collection: &str,
    memtable: &Memtable,
) -> Result<Vec<String>, B::Error> {
    let prefix = memtable_collection_prefix(database_id, tid, collection);
    let mut terms: std::collections::HashSet<String> = std::collections::HashSet::new();

    for key in memtable.terms() {
        if let Some(term) = key.strip_prefix(&prefix) {
            terms.insert(term.to_string());
        }
    }

    let seg_ids = backend.list_segments(database_id, tid, collection)?;
    for id in &seg_ids {
        if let Some(data) = backend.read_segment(database_id, tid, collection, id)?
            && let Ok(reader) = SegmentReader::open(data)
        {
            for term in reader.terms() {
                terms.insert(term);
            }
        }
    }

    Ok(terms.into_iter().collect())
}

/// Compute merged corpus stats from memtable + all segments.
pub fn merged_collection_stats<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> Result<(u32, f32), B::Error> {
    let (count, total) = backend.collection_stats(database_id, tid, collection)?;
    let avg = if count > 0 {
        total as f32 / count as f32
    } else {
        1.0
    };
    Ok((count, avg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::codec::smallfloat;
    use crate::lsm::memtable::{Memtable, MemtableConfig};
    use crate::lsm::segment::writer;
    use std::collections::HashMap;

    const DB: u64 = 0;
    const T: u64 = 1;

    fn cp(doc_id: u32, tf: u32) -> CompactPosting {
        CompactPosting {
            doc_id: nodedb_types::Surrogate(doc_id),
            term_freq: tf,
            fieldnorm: smallfloat::encode(100),
            positions: vec![0],
        }
    }

    #[test]
    fn memtable_only() {
        let backend = MemoryBackend::new();
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(&memtable_key(DB, T, "col", "hello"), cp(0, 2));
        mt.insert(&memtable_key(DB, T, "col", "hello"), cp(1, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, "col", &mt, &tokens, None).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 2);
    }

    #[test]
    fn segment_only() {
        let backend = MemoryBackend::new();
        let mut postings = HashMap::new();
        postings.insert("hello".to_string(), vec![cp(0, 1), cp(5, 2)]);
        let seg_bytes = writer::flush_to_segment(postings).unwrap();
        backend
            .write_segment(DB, T, "col", "L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, "col", &mt, &tokens, None).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 2);
    }

    #[test]
    fn memtable_plus_segment_merge() {
        let backend = MemoryBackend::new();

        let mut seg_postings = HashMap::new();
        seg_postings.insert("hello".to_string(), vec![cp(0, 1), cp(5, 2)]);
        let seg_bytes = writer::flush_to_segment(seg_postings).unwrap();
        backend
            .write_segment(DB, T, "col", "L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(&memtable_key(DB, T, "col", "hello"), cp(0, 10));
        mt.insert(&memtable_key(DB, T, "col", "hello"), cp(3, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, "col", &mt, &tokens, None).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 3);
    }

    #[test]
    fn missing_term() {
        let backend = MemoryBackend::new();
        let mt = Memtable::new(MemtableConfig::default());

        let tokens = vec!["nonexistent".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, "col", &mt, &tokens, None).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 0);
    }
}
