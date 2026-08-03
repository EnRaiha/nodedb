// SPDX-License-Identifier: BUSL-1.1

//! Search paths for the inverted index: BM25, phrase, fuzzy, and the
//! highlighting/offset helpers used by the SQL projection layer.

use redb::ReadableDatabase;
use tracing::debug;

use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::{MatchOffset, Posting, TextSearchResult};
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::errors::{fts_index_err, inverted_err};
use crate::engine::sparse::fts_redb::tables::POSTINGS;

/// Query and tuning parameters for an inverted-index phrase search.
///
/// The `(database_id, tid, collection)` scope is passed separately so callers
/// can reuse their existing scope variables.
pub struct PhraseSearchParams<'a> {
    /// Ordered terms that must appear as a contiguous sequence.
    pub terms: &'a [String],
    /// Maximum number of results to return.
    pub top_k: usize,
    /// Optional surrogate bitmap restricting candidates before position match.
    pub prefilter: Option<&'a nodedb_types::SurrogateBitmap>,
}

impl InvertedIndex {
    /// Search the inverted index for an exact phrase.
    ///
    /// Returns all documents where `terms` appear as a contiguous sequence in
    /// the original token stream. Positions are stored per-term in every
    /// `Posting`, so phrase matching is a set intersection on position offsets.
    ///
    /// The result is scored by position rank (earlier = higher). An optional
    /// `prefilter` bitmap restricts the candidate set before position matching.
    pub fn phrase_search(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        params: PhraseSearchParams<'_>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        let PhraseSearchParams {
            terms,
            top_k,
            prefilter,
        } = params;
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let t = tid.as_u64();
        let db = self.inner.backend().db();
        let read_txn = db.begin_read().map_err(|e| inverted_err("read txn", e))?;
        let postings_table = read_txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        // Load posting list for each term.
        let mut term_lists: Vec<Vec<Posting>> = Vec::with_capacity(terms.len());
        for term in terms {
            let analyzed = self.analyze_for_collection(database_id, tid, collection, term)?;
            let canonical = analyzed.into_iter().next().unwrap_or_else(|| term.clone());
            let postings: Vec<Posting> = postings_table
                .get((database_id, t, collection, canonical.as_str()))
                .map_err(|e| inverted_err("read posting", e))?
                .and_then(|v| zerompk::from_msgpack(v.value()).ok())
                .unwrap_or_default();
            term_lists.push(postings);
        }

        // The first term's postings are the candidate set.
        // For each candidate doc, verify remaining terms follow consecutively.
        let first = &term_lists[0];
        let mut matches: Vec<(Surrogate, u32)> = Vec::new();

        'outer: for posting in first {
            // Prefilter check.
            if prefilter.is_some_and(|bm| !bm.0.contains(posting.doc_id.as_u32())) {
                continue;
            }

            let surrogate = posting.doc_id;

            // For each start position of the first term, check subsequent terms.
            'pos: for &start_pos in &posting.positions {
                for (offset, list) in term_lists[1..].iter().enumerate() {
                    let expected_pos = start_pos + (offset as u32) + 1;
                    // Find a posting for this doc in this term's list.
                    let Some(other_posting) = list.iter().find(|p| p.doc_id == surrogate) else {
                        // Doc doesn't have this term at all — skip entire doc.
                        continue 'outer;
                    };
                    if !other_posting.positions.contains(&expected_pos) {
                        continue 'pos;
                    }
                }
                // All terms found at consecutive positions — record match.
                matches.push((surrogate, start_pos));
                break; // One match per doc is sufficient.
            }
        }

        // Sort by earliest match position (earlier = more relevant).
        matches.sort_by_key(|(_, pos)| *pos);

        let results: Vec<TextSearchResult> = matches
            .into_iter()
            .take(top_k)
            .enumerate()
            .map(|(rank, (doc_id, pos))| TextSearchResult {
                doc_id,
                score: 1.0 / (1.0 + pos as f32 + rank as f32),
                fuzzy: false,
            })
            .collect();

        debug!(
            tid = t,
            %collection,
            terms = terms.len(),
            hits = results.len(),
            "phrase search"
        );
        Ok(results)
    }

    /// Search the inverted index using BM25 scoring with explicit params.
    ///
    /// Supports `NOT <term>` and `-<term>` negation in the query string.
    /// Returns `Err` for invalid queries (NOT-only, unsupported parentheses).
    pub fn search(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        params: FtsSearchParams<'_>,
    ) -> crate::Result<Vec<TextSearchResult>> {
        self.inner
            .search(database_id, tid.as_u64(), collection, params)
            .map_err(fts_index_err)
    }

    /// Generate highlighted text with matched query terms wrapped in tags.
    pub fn highlight(&self, text: &str, query: &str, prefix: &str, suffix: &str) -> String {
        self.inner.highlight(text, query, prefix, suffix)
    }

    /// Return byte offsets of matched query terms in the original text.
    pub fn offsets(&self, text: &str, query: &str) -> Vec<MatchOffset> {
        self.inner.offsets(text, query)
    }
}
