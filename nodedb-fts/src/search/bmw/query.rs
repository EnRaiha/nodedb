// SPDX-License-Identifier: Apache-2.0

//! BMW query entry point: merges memtable + segments via LSM layer,
//! runs BMW scoring on `Surrogate` row identities.

use nodedb_types::SurrogateBitmap;

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::codec::smallfloat;
use crate::index::FtsIndex;
use crate::lsm::query as lsm_query;
use crate::posting::{Bm25Params, Posting, TextSearchResult};
use crate::search::bmw::skip_index::TermBlocks;

use super::scorer::bmw_score;

/// Corpus-level parameters for BMW search.
pub struct BmwParams<'a> {
    pub query_tokens: &'a [String],
    pub raw_tokens: &'a [String],
    pub fuzzy_enabled: bool,
    pub top_k: usize,
    pub total_docs: u32,
    pub avg_doc_len: f32,
    pub bm25: &'a Bm25Params,
    /// Optional surrogate prefilter. Only surrogates present in the bitmap
    /// will be scored; all others are skipped before BM25 computation.
    pub prefilter: Option<&'a SurrogateBitmap>,
}

/// Run BMW search over the FtsIndex.
pub fn bmw_search<B: FtsBackend>(
    index: &FtsIndex<B>,
    database_id: u64,
    tid: u64,
    collection: &str,
    p: &BmwParams<'_>,
) -> Result<Option<Vec<TextSearchResult>>, B::Error> {
    let mut has_fuzzy = vec![false; p.query_tokens.len()];

    let mut lsm_term_blocks = lsm_query::collect_merged_term_blocks(
        &index.backend,
        database_id,
        tid,
        collection,
        index.memtable(),
        p.query_tokens,
        index.governor.as_ref(),
    )?;

    let all_empty = lsm_term_blocks.iter().all(|tb| tb.df == 0);
    // When the LSM has no entries for any token and fuzzy is disabled, skip the
    // backend exact-lookup pass and return None immediately so the caller falls
    // back to the non-BMW scoring path, which reads from the backend directly.
    // When fuzzy is enabled (or when we may have backend-resident postings that
    // are worth probing), we always enter the per-token resolution loop below.
    if all_empty && !p.fuzzy_enabled {
        // Even with no LSM postings, Origin may have postings stored directly in
        // the redb backend (bypassing LSM). Check at least one token to decide.
        let has_any_backend = p.query_tokens.iter().any(|tok| {
            index
                .backend
                .read_postings(database_id, tid, collection, tok)
                .ok()
                .is_some_and(|v| !v.is_empty())
        });
        if !has_any_backend {
            return Ok(None);
        }
    }

    for (i, token) in p.query_tokens.iter().enumerate() {
        if lsm_term_blocks[i].df == 0 {
            // First, try an exact backend lookup (covers Origin's redb-direct indexing
            // path which bypasses the LSM memtable).
            let backend_posts = index
                .backend
                .read_postings(database_id, tid, collection, token)?;
            if !backend_posts.is_empty() {
                let compact = to_compact(&backend_posts, index, database_id, tid, collection)?;
                let blocks = into_blocks(compact);
                lsm_term_blocks[i] = TermBlocks::from_blocks(blocks);
                continue;
            }
            // Fall back to fuzzy matching when no exact posting exists.
            if p.fuzzy_enabled {
                let raw = p.raw_tokens.get(i).unwrap_or(token);
                let (posts, is_fuzzy) = index.fuzzy_lookup(database_id, tid, collection, raw)?;
                has_fuzzy[i] = is_fuzzy;
                if !posts.is_empty() {
                    let compact = to_compact(&posts, index, database_id, tid, collection)?;
                    let blocks = into_blocks(compact);
                    lsm_term_blocks[i] = TermBlocks::from_blocks(blocks);
                }
            }
        }
    }

    if lsm_term_blocks.iter().all(|tb| tb.df == 0) {
        return Ok(None);
    }

    let all_term_blocks = lsm_term_blocks;

    let heap = bmw_score(
        &all_term_blocks,
        p.total_docs,
        p.avg_doc_len,
        p.bm25,
        p.top_k,
        p.prefilter,
    );
    let scored = heap.into_sorted();

    let is_fuzzy = has_fuzzy.iter().any(|&f| f);
    let results: Vec<TextSearchResult> = scored
        .iter()
        .map(|doc| TextSearchResult {
            doc_id: doc.doc_id,
            score: doc.score,
            fuzzy: is_fuzzy,
        })
        .collect();

    Ok(Some(results))
}

/// Convert `Vec<Posting>` → `Vec<CompactPosting>`, reading fieldnorms from the index.
fn to_compact<B: FtsBackend>(
    postings: &[Posting],
    index: &FtsIndex<B>,
    database_id: u64,
    tid: u64,
    collection: &str,
) -> Result<Vec<CompactPosting>, B::Error> {
    let mut compact = Vec::with_capacity(postings.len());
    for p in postings {
        let fieldnorm = index
            .read_fieldnorm(database_id, tid, collection, p.doc_id)?
            .map(smallfloat::encode)
            .unwrap_or_else(|| smallfloat::encode(p.term_freq));
        compact.push(CompactPosting {
            doc_id: p.doc_id,
            term_freq: p.term_freq,
            fieldnorm,
            positions: p.positions.clone(),
        });
    }
    Ok(compact)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;

    const DB: u64 = 0;
    const T: u64 = 1;
    const D1: Surrogate = Surrogate(1);
    const D2: Surrogate = Surrogate(2);
    const D3: Surrogate = Surrogate(3);

    fn make_params<'a>(
        tokens: &'a [String],
        total: u32,
        avg: f32,
        top_k: usize,
        fuzzy: bool,
        bm25: &'a Bm25Params,
    ) -> BmwParams<'a> {
        BmwParams {
            query_tokens: tokens,
            raw_tokens: tokens,
            fuzzy_enabled: fuzzy,
            top_k,
            total_docs: total,
            avg_doc_len: avg,
            bm25,
            prefilter: None,
        }
    }

    #[test]
    fn bmw_query_basic() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(
            DB,
            T,
            "docs",
            D1,
            "The quick brown fox jumps over the lazy dog",
        )
        .unwrap();
        idx.index_document(DB, T, "docs", D2, "A fast brown dog runs across the field")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "Rust programming language for systems")
            .unwrap();

        let tokens = crate::analyze("brown fox");
        let (total, avg) = idx.index_stats(DB, T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 10, false, &bm25);

        let results = bmw_search(&idx, DB, T, "docs", &p).unwrap().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn bmw_query_empty_collection() {
        let idx = FtsIndex::new(MemoryBackend::new());
        let tokens = crate::analyze("hello");
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, 0, 1.0, 10, false, &bm25);

        let result = bmw_search(&idx, DB, T, "empty", &p).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bmw_query_respects_top_k() {
        let idx = FtsIndex::new(MemoryBackend::new());
        for i in 1..=50u32 {
            idx.index_document(DB, T, "docs", Surrogate(i), &format!("common term word{i}"))
                .unwrap();
        }

        let tokens = crate::analyze("common term");
        let (total, avg) = idx.index_stats(DB, T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 5, false, &bm25);

        let results = bmw_search(&idx, DB, T, "docs", &p).unwrap().unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn bmw_query_with_fuzzy() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "distributed database systems")
            .unwrap();

        let stemmed = crate::analyze("databse");
        let raw = crate::analyzer::pipeline::tokenize_no_stem("databse");
        let (total, avg) = idx.index_stats(DB, T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = BmwParams {
            query_tokens: &stemmed,
            raw_tokens: &raw,
            fuzzy_enabled: true,
            top_k: 10,
            total_docs: total,
            avg_doc_len: avg,
            bm25: &bm25,
            prefilter: None,
        };

        let result = bmw_search(&idx, DB, T, "docs", &p);
        match &result {
            Ok(Some(r)) => assert!(!r.is_empty(), "BMW returned empty results"),
            Ok(None) => panic!("BMW returned None (no term blocks)"),
            Err(e) => panic!("BMW returned Err: {e}"),
        }
    }

    #[test]
    fn bmw_query_uses_memtable() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "hello world greeting")
            .unwrap();

        assert!(!idx.memtable().is_empty());

        let tokens = crate::analyze("hello");
        let (total, avg) = idx.index_stats(DB, T, "docs").unwrap();
        let bm25 = Bm25Params::default();
        let p = make_params(&tokens, total, avg, 10, false, &bm25);

        let results = bmw_search(&idx, DB, T, "docs", &p).unwrap().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }
}
