// SPDX-License-Identifier: Apache-2.0

//! BM25 search over the FtsIndex with AND-first OR-fallback, phrase boost,
//! and NOT-term exclusion.
//!
//! ## AND-first / OR-fallback
//!
//! A multi-term query `rust programming` first attempts AND (all terms must
//! match). If no document satisfies all terms, results fall back to OR with
//! coverage-penalised scores.
//!
//! ## NOT operator
//!
//! `rust NOT python` and `rust -python` are equivalent. The query parser
//! splits the input into positive and negative term lists. BM25 scoring runs
//! on positive terms only; a bitmap of doc IDs that match **any** negative
//! term is built separately and used to filter final results. Negative terms
//! do not affect BM25 scores.
//!
//! Synonym expansion applies to both positive and negative term lists, so
//! `rust NOT db` also excludes documents that contain synonym expansions of
//! `db` (e.g. `database`, `datastore`).

use std::collections::HashMap;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::backend::FtsBackend;
use crate::bm25::bm25_score;
use crate::index::FtsIndex;
use crate::index::error::FtsIndexError;
use crate::posting::{Posting, QueryMode, TextSearchResult};
use crate::search::phrase;
use crate::search::query_parser::parse_query;

/// Query and tuning parameters for a BM25 search.
///
/// The `(database_id, tid, collection)` scope is passed separately so the
/// same struct can be shared by callers that hold the tenant id as either a
/// raw `u64` (this crate) or a strongly-typed `TenantId` (the Origin wrapper).
pub struct FtsSearchParams<'a> {
    /// Raw query string (may contain `NOT <term>` / `-<term>` negation).
    pub query: &'a str,
    /// Maximum number of results to return.
    pub top_k: usize,
    /// When `true`, unmatched terms fall back to fuzzy (Levenshtein) lookup.
    pub fuzzy_enabled: bool,
    /// Boolean combination mode for multi-term queries (AND or OR).
    pub mode: QueryMode,
    /// Optional surrogate bitmap restricting the candidate set before scoring.
    pub prefilter: Option<&'a SurrogateBitmap>,
}

/// Inputs to the AND-mode post-filter that drops BMW candidates which do not
/// match at least `num_terms` of the analyzed query tokens.
struct FilterAndModeParams<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    query_tokens: &'a [String],
    candidates: &'a [TextSearchResult],
    num_terms: usize,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Search the index with explicit boolean mode, fuzzy, and optional prefilter.
    ///
    /// Supports `NOT <term>` and `-<term>` negation in the query string.
    /// Returns `Err(FtsIndexError::InvalidQuery)` for ill-formed queries such
    /// as NOT-only queries or unsupported parenthesised groups.
    pub fn search(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        params: FtsSearchParams<'_>,
    ) -> Result<Vec<TextSearchResult>, FtsIndexError<B::Error>> {
        let FtsSearchParams {
            query,
            top_k,
            fuzzy_enabled,
            mode,
            prefilter,
        } = params;
        // A collection configured with `FUZZY true` falls back to fuzzy
        // matching even when the query did not ask for it — that is what
        // makes it an index property rather than a per-query flag. Resolved
        // here, at the one point every search path funnels through, so no
        // caller can be wired up without it.
        let fuzzy_enabled = fuzzy_enabled
            || self
                .get_collection_fuzzy(database_id, tid, collection)
                .map_err(FtsIndexError::backend)?;

        // Parse the query for NOT / - negation operators before analysis.
        let parsed = parse_query(query)?;

        // Reconstruct the positive-only query string for the existing analyzer path.
        // Each raw positive token is passed to the analyzer individually rather than
        // joining them, because some analyzers are sensitive to token boundaries.
        // Joining with a space is safe for the standard/language analyzers.
        let positive_raw = parsed.positive.join(" ");
        let negative_raw_terms = parsed.negative;

        let base_tokens = self
            .analyze_for_collection(database_id, tid, collection, &positive_raw)
            .map_err(FtsIndexError::backend)?;
        if base_tokens.is_empty() {
            return Ok(Vec::new());
        }

        let base_token_count = base_tokens.len();
        let query_tokens = self
            .expand_query_with_synonyms(database_id, tid, base_tokens)
            .map_err(FtsIndexError::backend)?;
        let num_query_terms = query_tokens.len();
        let and_threshold = base_token_count;

        let raw_tokens = if fuzzy_enabled {
            self.tokenize_raw_for_collection(database_id, tid, collection, &positive_raw)
                .map_err(FtsIndexError::backend)?
        } else {
            Vec::new()
        };

        let (total_docs, avg_doc_len) = self
            .index_stats(database_id, tid, collection)
            .map_err(FtsIndexError::backend)?;
        if total_docs == 0 {
            return Ok(Vec::new());
        }

        // Build the negative-term exclusion set before scoring.
        // Negative terms are analyzed and synonym-expanded just like positive
        // terms. The result is a set of doc IDs that match any negative term.
        let negative_set =
            self.build_negative_set(database_id, tid, collection, &negative_raw_terms)?;

        let bmw_params = super::bmw::query::BmwParams {
            query_tokens: &query_tokens,
            raw_tokens: &raw_tokens,
            fuzzy_enabled,
            top_k: if mode == QueryMode::And && and_threshold > 1 {
                top_k.saturating_mul(3).max(20)
            } else {
                top_k
            },
            total_docs,
            avg_doc_len,
            bm25: &self.bm25_params,
            prefilter,
        };
        if let Ok(Some(bmw_results)) =
            super::bmw::query::bmw_search(self, database_id, tid, collection, &bmw_params)
        {
            if mode == QueryMode::Or || and_threshold == 1 {
                let mut results: Vec<TextSearchResult> = bmw_results
                    .into_iter()
                    .filter(|r| !negative_set.contains(&r.doc_id))
                    .take(top_k)
                    .collect();
                results.truncate(top_k);
                return Ok(results);
            }

            let and_results = self
                .filter_and_mode(FilterAndModeParams {
                    database_id,
                    tid,
                    collection,
                    query_tokens: &query_tokens,
                    candidates: &bmw_results,
                    num_terms: and_threshold,
                })
                .map_err(FtsIndexError::backend)?;

            if !and_results.is_empty() {
                let filtered: Vec<TextSearchResult> = and_results
                    .into_iter()
                    .filter(|r| !negative_set.contains(&r.doc_id))
                    .take(top_k)
                    .collect();
                return Ok(filtered);
            }

            let penalized: Vec<TextSearchResult> = bmw_results
                .into_iter()
                .filter(|r| !negative_set.contains(&r.doc_id))
                .map(|mut r| {
                    let matched = self.count_term_matches(
                        database_id,
                        tid,
                        collection,
                        &query_tokens,
                        r.doc_id,
                    );
                    let coverage = matched as f32 / and_threshold as f32;
                    r.score *= coverage;
                    r
                })
                .collect();
            let mut sorted = penalized;
            sorted.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            sorted.truncate(top_k);
            return Ok(sorted);
        }

        // Fallback: exhaustive BM25 scoring reading directly from the backend.
        let _term_postings_guard = self.governor.as_ref().and_then(|gov| {
            let bytes = num_query_terms
                * (std::mem::size_of::<Vec<Posting>>() + std::mem::size_of::<bool>());
            gov.reserve(nodedb_mem::EngineId::Fts, bytes).ok()
        });
        let mut term_postings: Vec<(Vec<Posting>, bool)> = Vec::with_capacity(num_query_terms);
        for (i, token) in query_tokens.iter().enumerate() {
            let postings = self
                .backend
                .read_postings(database_id, tid, collection, token)
                .map_err(FtsIndexError::backend)?;
            if !postings.is_empty() {
                term_postings.push((postings, false));
            } else if fuzzy_enabled {
                let raw = raw_tokens
                    .get(i)
                    .map(String::as_str)
                    .unwrap_or(token.as_str());
                let (fuzzy_posts, is_fuzzy) = self
                    .fuzzy_lookup(database_id, tid, collection, raw)
                    .map_err(FtsIndexError::backend)?;
                term_postings.push((fuzzy_posts, is_fuzzy));
            } else {
                term_postings.push((Vec::new(), false));
            }
        }

        let mut doc_scores: HashMap<Surrogate, (f32, bool, usize)> = HashMap::new();

        for (token_idx, (postings, is_fuzzy)) in term_postings.iter().enumerate() {
            if postings.is_empty() {
                continue;
            }
            let df = postings.len() as u32;

            for posting in postings {
                // Prefilter: skip surrogates not present in the bitmap.
                if let Some(bm) = prefilter
                    && !bm.contains(posting.doc_id)
                {
                    continue;
                }

                let doc_len = self
                    .backend
                    .read_doc_length(database_id, tid, collection, posting.doc_id)
                    .map_err(FtsIndexError::backend)?
                    .unwrap_or(1);

                let mut score = bm25_score(
                    posting.term_freq,
                    df,
                    doc_len,
                    total_docs,
                    avg_doc_len,
                    &self.bm25_params,
                );

                if *is_fuzzy {
                    score *= crate::fuzzy::fuzzy_discount(1);
                }

                let entry = doc_scores.entry(posting.doc_id).or_insert((0.0, false, 0));
                entry.0 += score;
                if *is_fuzzy {
                    entry.1 = true;
                }
                entry.2 += 1;
            }
            let _ = token_idx;
        }

        if num_query_terms >= 2 {
            let doc_postings_map = phrase::collect_doc_postings(&query_tokens, &term_postings);
            for (doc_id, token_postings) in &doc_postings_map {
                if let Some(entry) = doc_scores.get_mut(doc_id) {
                    let boost = phrase::phrase_boost(&query_tokens, token_postings);
                    entry.0 *= boost;
                }
            }
        }

        if mode == QueryMode::And && and_threshold > 1 {
            let and_results: HashMap<Surrogate, (f32, bool, usize)> = doc_scores
                .iter()
                .filter(|(_, (_, _, match_count))| *match_count >= and_threshold)
                .map(|(k, v)| (*k, *v))
                .collect();

            if !and_results.is_empty() {
                let filtered = and_results
                    .into_iter()
                    .filter(|(doc_id, _)| !negative_set.contains(doc_id))
                    .collect();
                return Ok(Self::to_sorted_results(filtered, top_k));
            }

            for (score, _, match_count) in doc_scores.values_mut() {
                let coverage = *match_count as f32 / and_threshold as f32;
                *score *= coverage;
            }
        }

        // Apply negative filter to final fallback results.
        let filtered: HashMap<Surrogate, (f32, bool, usize)> = doc_scores
            .into_iter()
            .filter(|(doc_id, _)| !negative_set.contains(doc_id))
            .collect();

        Ok(Self::to_sorted_results(filtered, top_k))
    }

    /// Build a set of doc IDs that match any of the given raw negative terms.
    ///
    /// Each raw negative term is analyzed and synonym-expanded before posting
    /// lookup, matching the same pipeline as positive terms.
    fn build_negative_set(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        raw_negative_terms: &[String],
    ) -> Result<std::collections::HashSet<Surrogate>, FtsIndexError<B::Error>> {
        if raw_negative_terms.is_empty() {
            return Ok(std::collections::HashSet::new());
        }

        // Analyze all negative terms together (join is safe for standard analyzer).
        let neg_raw = raw_negative_terms.join(" ");
        let neg_base_tokens = self
            .analyze_for_collection(database_id, tid, collection, &neg_raw)
            .map_err(FtsIndexError::backend)?;

        if neg_base_tokens.is_empty() {
            return Ok(std::collections::HashSet::new());
        }

        // Synonym-expand negative tokens so negating 'db' also excludes 'database'.
        let neg_tokens = self
            .expand_query_with_synonyms(database_id, tid, neg_base_tokens)
            .map_err(FtsIndexError::backend)?;

        let mut excluded: std::collections::HashSet<Surrogate> = std::collections::HashSet::new();

        // Collect postings from memtable + segments for each negative token.
        let term_blocks = crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            database_id,
            tid,
            collection,
            self.memtable(),
            &neg_tokens,
            self.governor.as_ref(),
        )
        .map_err(FtsIndexError::backend)?;

        for tb in &term_blocks {
            for block in &tb.blocks {
                for doc_id in &block.doc_ids {
                    excluded.insert(*doc_id);
                }
            }
        }

        // Also check the backend postings directly (covers the exhaustive path).
        for token in &neg_tokens {
            let postings = self
                .backend
                .read_postings(database_id, tid, collection, token)
                .map_err(FtsIndexError::backend)?;
            for posting in postings {
                excluded.insert(posting.doc_id);
            }
        }

        Ok(excluded)
    }

    fn filter_and_mode(
        &self,
        params: FilterAndModeParams<'_>,
    ) -> Result<Vec<TextSearchResult>, B::Error> {
        let FilterAndModeParams {
            database_id,
            tid,
            collection,
            query_tokens,
            candidates,
            num_terms,
        } = params;
        let term_blocks = crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            database_id,
            tid,
            collection,
            self.memtable(),
            query_tokens,
            self.governor.as_ref(),
        )?;

        let mut results = Vec::new();
        for candidate in candidates {
            let surrogate = candidate.doc_id;
            let matched = term_blocks
                .iter()
                .filter(|tb| tb.blocks.iter().any(|b| b.doc_ids.contains(&surrogate)))
                .count();
            if matched >= num_terms {
                results.push(candidate.clone());
            }
        }
        Ok(results)
    }

    fn count_term_matches(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        query_tokens: &[String],
        doc_id: Surrogate,
    ) -> usize {
        let term_blocks = match crate::lsm::query::collect_merged_term_blocks(
            &self.backend,
            database_id,
            tid,
            collection,
            self.memtable(),
            query_tokens,
            self.governor.as_ref(),
        ) {
            Ok(tb) => tb,
            Err(_) => return 0,
        };
        term_blocks
            .iter()
            .filter(|tb| tb.blocks.iter().any(|b| b.doc_ids.contains(&doc_id)))
            .count()
    }

    fn to_sorted_results(
        doc_scores: HashMap<Surrogate, (f32, bool, usize)>,
        top_k: usize,
    ) -> Vec<TextSearchResult> {
        let mut results: Vec<TextSearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, (score, fuzzy_flag, _))| TextSearchResult {
                doc_id,
                score,
                fuzzy: fuzzy_flag,
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SurrogateBitmap};

    use super::FtsSearchParams;
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use crate::index::error::FtsIndexError;
    use crate::posting::QueryMode;
    use crate::search::query_parser::InvalidQuery;

    const DB: u64 = 0;
    const T: u64 = 1;
    const D1: Surrogate = Surrogate(1);
    const D2: Surrogate = Surrogate(2);
    const D3: Surrogate = Surrogate(3);

    fn make_index() -> FtsIndex<MemoryBackend> {
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
        idx
    }

    #[test]
    fn basic_search() {
        let idx = make_index();
        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "brown fox",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn search_with_stemming() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "running distributed databases")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "the cat sat on a mat")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "database distribution",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn or_mode() {
        let idx = make_index();
        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "brown fox",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.len() >= 2);
    }

    #[test]
    fn and_mode_filters() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "Rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "Python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust programming",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn and_fallback_to_or() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.score > 0.0);
        }
    }

    #[test]
    fn and_no_fallback_when_results_exist() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust programming language")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "python programming language")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust programming",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn empty_query() {
        let idx = make_index();
        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "the a is",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn collections_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "col_a", D1, "alpha bravo charlie")
            .unwrap();
        idx.index_document(DB, T, "col_b", D1, "delta echo foxtrot")
            .unwrap();

        assert_eq!(
            idx.search(
                DB,
                T,
                "col_a",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .len(),
            1
        );
        assert!(
            idx.search(
                DB,
                T,
                "col_b",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn fuzzy_search() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "distributed database systems")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "databse",
                    top_k: 10,
                    fuzzy_enabled: true,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(!results.is_empty());
        assert!(results[0].fuzzy);
    }

    #[test]
    fn phrase_boost_consecutive() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "the quick brown fox jumped")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "a brown dog chased a fox")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "brown fox",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.len() >= 2);
        assert_eq!(results[0].doc_id, D1);
    }

    #[test]
    fn phrase_boost_no_effect_single_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "hello world")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "hello",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn tenants_isolated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, 1, "docs", D1, "alpha bravo")
            .unwrap();
        idx.index_document(DB, 2, "docs", D1, "charlie delta")
            .unwrap();

        let r1 = idx
            .search(
                DB,
                1,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        let r2 = idx
            .search(
                DB,
                2,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(r1.len(), 1);
        assert!(r2.is_empty());
    }

    #[test]
    fn prefilter_excludes_non_member_surrogates() {
        let idx = FtsIndex::new(MemoryBackend::new());

        idx.index_document(DB, T, "docs", D1, "rust language system")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust rust rust rust rust")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust rust rust rust rust rust")
            .unwrap();

        let mut bm = SurrogateBitmap::new();
        bm.insert(D1);

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&bm),
                },
            )
            .unwrap();

        assert_eq!(results.len(), 1, "only D1 should be returned");
        assert_eq!(results[0].doc_id, D1);

        assert!(
            !results.iter().any(|r| r.doc_id == D2),
            "D2 must be excluded"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == D3),
            "D3 must be excluded"
        );

        let all_results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert_eq!(all_results.len(), 3, "all docs returned without prefilter");
        assert!(
            all_results[0].doc_id == D2 || all_results[0].doc_id == D3,
            "D2 or D3 should lead without prefilter (higher tf)"
        );

        let empty_bm = SurrogateBitmap::new();
        let empty_results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&empty_bm),
                },
            )
            .unwrap();
        assert!(empty_results.is_empty(), "empty prefilter → no results");

        let mut bm23 = SurrogateBitmap::new();
        bm23.insert(D2);
        bm23.insert(D3);
        let results23 = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: Some(&bm23),
                },
            )
            .unwrap();
        assert_eq!(results23.len(), 2);
        assert!(!results23.iter().any(|r| r.doc_id == D1));
    }

    // ── NOT operator tests ────────────────────────────────────────────────────

    #[test]
    fn not_keyword_excludes_documents() {
        let idx = FtsIndex::new(MemoryBackend::new());
        // D1: rust + python, D2: rust + ruby, D3: python + ruby
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "python ruby programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        // Must include D2 (rust, no python), must not include D1 (has python).
        assert!(
            results.iter().any(|r| r.doc_id == D2),
            "D2 (rust+ruby) must be in results"
        );
        assert!(
            !results.iter().any(|r| r.doc_id == D1),
            "D1 (rust+python) must be excluded"
        );
    }

    #[test]
    fn dash_prefix_excludes_documents() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "python ruby programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust -python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        assert!(results.iter().any(|r| r.doc_id == D2));
        assert!(!results.iter().any(|r| r.doc_id == D1));
    }

    #[test]
    fn multiple_not_excludes_all_negated() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust python programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust ruby programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D3, "rust systems programming")
            .unwrap();

        let results = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT python NOT ruby",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        // Only D3 has neither python nor ruby.
        assert!(results.iter().any(|r| r.doc_id == D3));
        assert!(!results.iter().any(|r| r.doc_id == D1));
        assert!(!results.iter().any(|r| r.doc_id == D2));
    }

    #[test]
    fn not_nonexistent_term_returns_all_positives() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust programming")
            .unwrap();
        idx.index_document(DB, T, "docs", D2, "rust systems")
            .unwrap();

        let results_plain = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();
        let results_not = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT nonexistentxyz",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap();

        let plain_ids: std::collections::HashSet<Surrogate> =
            results_plain.iter().map(|r| r.doc_id).collect();
        let not_ids: std::collections::HashSet<Surrogate> =
            results_not.iter().map(|r| r.doc_id).collect();
        assert_eq!(
            plain_ids, not_ids,
            "NOT with nonexistent term must not remove any docs"
        );
    }

    #[test]
    fn negative_only_returns_invalid_query_error() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "python programming")
            .unwrap();

        let err = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "NOT python",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::InvalidQuery(InvalidQuery::NegativeOnly)),
            "expected InvalidQuery(NegativeOnly), got {err:?}"
        );
    }

    #[test]
    fn parentheses_after_not_returns_invalid_query_error() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", D1, "rust programming")
            .unwrap();

        let err = idx
            .search(
                DB,
                T,
                "docs",
                FtsSearchParams {
                    query: "rust NOT (python OR ruby)",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                FtsIndexError::InvalidQuery(InvalidQuery::ParenthesesNotSupported)
            ),
            "expected InvalidQuery(ParenthesesNotSupported), got {err:?}"
        );
    }
}
