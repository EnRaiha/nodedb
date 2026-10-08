// SPDX-License-Identifier: Apache-2.0

//! Multi-source query: merge postings from memtable + all segments,
//! then feed the merged stream to BMW or exhaustive scorer.

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::index::FtsIndexError;
use crate::scope::IndexScope;
use crate::search::bmw::skip_index::TermBlocks;

use super::memtable::Memtable;
use super::merge::merge_term_postings;
use super::segment::reader::SegmentReader;
use super::segment_deletes::SegmentDeletes;

use std::sync::Arc;

use nodedb_mem::MemoryGovernor;
use nodedb_types::SurrogateBitmap;

use crate::mem_scope::fts_scope;

/// One opened segment of an index with the documents removed after it was
/// written.
pub struct LiveSegment {
    pub segment_id: String,
    pub reader: SegmentReader,
    pub deleted: Option<SurrogateBitmap>,
}

/// Open every segment of one index, each with its delete set. A segment
/// that fails validation is a [`FtsIndexError::CorruptSegment`], and a
/// listed segment the backend does not hold is a
/// [`FtsIndexError::MissingSegment`]: a read that skipped either would
/// answer from part of the index.
pub(crate) fn open_segments<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> Result<Vec<LiveSegment>, FtsIndexError<B::Error>> {
    let seg_ids = backend
        .list_segments(database_id, tid, index)
        .map_err(FtsIndexError::Backend)?;
    if seg_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut deletes = SegmentDeletes::load(backend, database_id, tid, index, &seg_ids)?;
    let mut segments: Vec<LiveSegment> = Vec::with_capacity(seg_ids.len());
    for segment_id in seg_ids {
        let Some(data) = backend
            .read_segment(database_id, tid, index, &segment_id)
            .map_err(FtsIndexError::Backend)?
        else {
            return Err(FtsIndexError::MissingSegment { segment_id });
        };
        let reader = SegmentReader::open(data).map_err(|source| FtsIndexError::CorruptSegment {
            segment_id: segment_id.clone(),
            source,
        })?;
        let deleted = deletes.take(&segment_id);
        segments.push(LiveSegment {
            segment_id,
            reader,
            deleted,
        });
    }
    Ok(segments)
}

/// The postings of `token` in one segment, without its deleted documents.
pub(crate) fn live_segment_postings<E: std::fmt::Display>(
    segment: &LiveSegment,
    token: &str,
) -> Result<Vec<CompactPosting>, FtsIndexError<E>> {
    let blocks =
        segment
            .reader
            .read_postings(token)
            .map_err(|source| FtsIndexError::CorruptSegment {
                segment_id: segment.segment_id.clone(),
                source,
            })?;
    let mut postings = Vec::new();
    for block in blocks {
        for i in 0..block.doc_ids.len() {
            let doc_id = block.doc_ids[i];
            if segment.deleted.as_ref().is_some_and(|d| d.contains(doc_id)) {
                continue;
            }
            postings.push(CompactPosting {
                doc_id,
                term_freq: block.term_freqs[i],
                fieldnorm: block.fieldnorms[i],
                positions: block.positions[i].clone(),
            });
        }
    }
    Ok(postings)
}

/// Collect posting lists for a set of query tokens by merging across
/// the active memtable and all immutable segments of one index. A
/// segment's postings of the documents removed after it was written are
/// skipped, so document frequencies count only live documents.
///
/// Returns per-term `TermBlocks` ready for BMW scoring.
///
/// The `Vec::with_capacity` for `term_blocks_list` is budgeted via
/// [`nodedb_mem::ScopedMemory::reserve`] before the allocation. If the
/// budget is exhausted the allocation still proceeds — the scope serves
/// as an accounting and backpressure signal.
pub fn collect_merged_term_blocks<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    memtable: &Memtable,
    query_tokens: &[String],
    governor: &Arc<MemoryGovernor>,
) -> Result<Vec<TermBlocks>, FtsIndexError<B::Error>> {
    let segments = open_segments(backend, database_id, tid, index)?;

    let memory = fts_scope(governor, database_id, tid);
    let bytes = query_tokens.len() * std::mem::size_of::<TermBlocks>();
    let _term_blocks_guard = memory.reserve(bytes).ok();

    let mut term_blocks_list = Vec::with_capacity(query_tokens.len());

    for token in query_tokens {
        let mt_postings = memtable.get_postings(database_id, tid, index, token);

        let seg_postings: Vec<Vec<CompactPosting>> = segments
            .iter()
            .map(|segment| live_segment_postings::<B::Error>(segment, token))
            .collect::<Result<_, _>>()?;

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

/// Collect all unique term names across memtable + segments of one index.
///
/// Used by fuzzy matching to scan available terms.
pub fn collect_all_terms<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    memtable: &Memtable,
) -> Result<Vec<String>, FtsIndexError<B::Error>> {
    let mut terms: std::collections::HashSet<String> = memtable
        .terms(database_id, tid, index)
        .into_iter()
        .collect();

    for segment in open_segments(backend, database_id, tid, index)? {
        for term in segment.reader.terms() {
            terms.insert(term);
        }
    }

    Ok(terms.into_iter().collect())
}

/// Corpus stats of one index: `(doc_count, avg_doc_len)`.
pub fn merged_collection_stats<B: FtsBackend>(
    backend: &B,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> Result<(u32, f32), B::Error> {
    let (count, total) = backend.collection_stats(database_id, tid, index)?;
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
    use crate::test_support::test_governor;
    use std::collections::HashMap;

    const DB: u64 = 0;
    const T: u64 = 1;
    const COL: IndexScope<'static> = IndexScope::document("col");

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
        mt.insert(DB, T, COL, "hello", cp(0, 2));
        mt.insert(DB, T, COL, "hello", cp(1, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();

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
            .write_segment(DB, T, COL, "L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();

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
            .write_segment(DB, T, COL, "L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(DB, T, COL, "hello", cp(0, 10));
        mt.insert(DB, T, COL, "hello", cp(3, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 3);
    }

    #[test]
    fn other_index_postings_are_invisible() {
        let backend = MemoryBackend::new();
        let title = IndexScope::field("col", "title").unwrap();
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert(DB, T, title, "hello", cp(1, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();
        assert_eq!(term_blocks[0].df, 0);
        assert!(
            collect_all_terms(&backend, DB, T, COL, &mt)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            collect_all_terms(&backend, DB, T, title, &mt).unwrap(),
            vec!["hello".to_string()]
        );
    }

    /// A segment that fails validation fails the read instead of being
    /// skipped.
    #[test]
    fn a_corrupt_segment_fails_the_read() {
        let backend = MemoryBackend::new();
        let mut postings = HashMap::new();
        postings.insert("hello".to_string(), vec![cp(1, 1)]);
        let mut seg_bytes = writer::flush_to_segment(postings).unwrap();
        let last = seg_bytes.len() - 1;
        seg_bytes[last] ^= 0xFF;
        backend
            .write_segment(DB, T, COL, "L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        let tokens = vec!["hello".to_string()];
        let err = collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
            .unwrap_err();
        assert!(
            matches!(
                err,
                FtsIndexError::CorruptSegment { ref segment_id, .. }
                    if segment_id == "L0:0000000000000001"
            ),
            "{err}"
        );
        assert!(matches!(
            collect_all_terms(&backend, DB, T, COL, &mt).unwrap_err(),
            FtsIndexError::CorruptSegment { .. }
        ));
    }

    /// A segment's deleted documents are skipped and leave the document
    /// frequency.
    #[test]
    fn deleted_documents_of_a_segment_are_skipped() {
        let backend = MemoryBackend::new();
        let mut postings = HashMap::new();
        postings.insert("hello".to_string(), vec![cp(1, 1), cp(2, 1)]);
        let seg_bytes = writer::flush_to_segment(postings).unwrap();
        let id = "L0:0000000000000001".to_string();
        backend.write_segment(DB, T, COL, &id, &seg_bytes).unwrap();
        let mut deletes = SegmentDeletes::default();
        deletes.mark_removed(std::slice::from_ref(&id), nodedb_types::Surrogate(1));
        deletes.store(&backend, DB, T, COL).unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        let tokens = vec!["hello".to_string()];
        let blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();
        assert_eq!(blocks[0].df, 1);
        assert_eq!(
            blocks[0].doc_ids().collect::<Vec<_>>(),
            vec![nodedb_types::Surrogate(2)]
        );
    }

    #[test]
    fn missing_term() {
        let backend = MemoryBackend::new();
        let mt = Memtable::new(MemtableConfig::default());

        let tokens = vec!["nonexistent".to_string()];
        let term_blocks =
            collect_merged_term_blocks(&backend, DB, T, COL, &mt, &tokens, &test_governor())
                .unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 0);
    }
}
