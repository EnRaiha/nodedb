// SPDX-License-Identifier: BUSL-1.1

//! Staged-document scoring internals shared by the FTS overlay merges in
//! `fts_merge.rs`. Decodes a transaction's staged document body, re-tokenizes
//! it with the collection's configured analyzer — the SAME analyzer
//! resolution (`InvertedIndex::analyze_for_collection`) the forward indexing
//! path uses — and scores it against a query — either bag-of-words BM25
//! (using the base index's corpus stats so scores are comparable) or
//! exact-adjacency phrase matching over the staged doc's own token positions.

use std::collections::HashMap;

use nodedb_fts::bm25::bm25_score;
use nodedb_fts::posting::Bm25Params;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::fts_text::extract_fts_text;
use crate::types::{DatabaseId, TenantId};

/// Immutable corpus context for scoring one staged document: the collection
/// scope needed to read per-term `df`, the collection's storage config key,
/// and the shared corpus stats every staged doc is scored against.
pub(in crate::data::executor) struct StagedFtsScoreCtx<'a> {
    database_id: u64,
    tid: TenantId,
    collection: &'a str,
    config_key: &'a (DatabaseId, TenantId, String),
    total_docs: u32,
    avg_doc_len: f32,
    bm25_params: &'a Bm25Params,
}

impl CoreLoop {
    /// Build the shared corpus context used to score every staged document
    /// in one merge. Reads `(total_docs, avg_doc_len)` once from the same
    /// stats the base search used. An empty base corpus is treated as a
    /// 1-document corpus so `bm25_score`'s IDF term stays finite/positive.
    pub(in crate::data::executor) fn staged_score_ctx<'a>(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &'a str,
        config_key: &'a (DatabaseId, TenantId, String),
        bm25_params: &'a Bm25Params,
    ) -> StagedFtsScoreCtx<'a> {
        let (total_docs, avg_doc_len) = self
            .inverted
            .corpus_stats(database_id.as_u64(), tid, collection)
            .unwrap_or((0, 1.0));
        StagedFtsScoreCtx {
            database_id: database_id.as_u64(),
            tid,
            collection,
            config_key,
            total_docs: total_docs.max(1),
            avg_doc_len: if avg_doc_len > 0.0 { avg_doc_len } else { 1.0 },
            bm25_params,
        }
    }

    /// Decode a staged body, re-tokenize with the forward-indexing
    /// tokenizer, and BM25-score it against `positive_terms`.
    ///
    /// `Ok(None)` is the "this document does not belong in the result" answer:
    /// it extracts to empty text (the forward indexer never indexes such
    /// documents either), matches no positive term, or is excluded by a
    /// negative term. A staged body that will not decode is not that answer —
    /// it is this transaction's own write becoming unreadable — and comes back
    /// as `Err`.
    pub(in crate::data::executor) fn score_staged_fts_doc(
        &self,
        ctx: &StagedFtsScoreCtx<'_>,
        body: &[u8],
        positive_terms: &[String],
        negative_terms: &[String],
    ) -> crate::Result<Option<f32>> {
        let Some(doc_tokens) = self.tokenize_staged_body(ctx.database_id, ctx.config_key, body)?
        else {
            return Ok(None);
        };

        if !negative_terms.is_empty() && negative_terms.iter().any(|t| doc_tokens.contains(t)) {
            return Ok(None);
        }

        let mut term_freq: HashMap<&str, u32> = HashMap::new();
        for token in &doc_tokens {
            *term_freq.entry(token.as_str()).or_insert(0) += 1;
        }
        let doc_len = doc_tokens.len() as u32;

        let mut score = 0.0f32;
        let mut matched_any = false;
        for term in positive_terms {
            let Some(&tf) = term_freq.get(term.as_str()) else {
                continue;
            };
            matched_any = true;
            let df = self
                .inverted
                .term_df(ctx.database_id, ctx.tid, ctx.collection, term)
                .unwrap_or(0)
                .max(1);
            score += bm25_score(
                tf,
                df,
                doc_len,
                ctx.total_docs,
                ctx.avg_doc_len,
                ctx.bm25_params,
            );
        }

        if matched_any && score > 0.0 {
            Ok(Some(score))
        } else {
            Ok(None)
        }
    }

    /// Score a staged body for a PHRASE search: include it only when the
    /// analyzed `phrase_terms` occur as a contiguous, in-order run in the
    /// staged doc's analyzed token stream. Returns `1 / (1 + earliest_start)`
    /// (base phrase formula at rank 0) on a match, `Ok(None)` when the phrase
    /// is not present, and `Err` when the staged body will not decode.
    pub(in crate::data::executor) fn score_staged_phrase_doc(
        &self,
        database_id: u64,
        config_key: &(DatabaseId, TenantId, String),
        body: &[u8],
        phrase_terms: &[String],
    ) -> crate::Result<Option<f32>> {
        if phrase_terms.is_empty() {
            return Ok(None);
        }
        let Some(doc_tokens) = self.tokenize_staged_body(database_id, config_key, body)? else {
            return Ok(None);
        };
        Ok(earliest_contiguous_match(&doc_tokens, phrase_terms)
            .map(|start| 1.0 / (1.0 + start as f32)))
    }

    /// Decode a staged body via the collection's storage mode and analyze it
    /// with the collection's configured analyzer — resolved through
    /// [`InvertedIndex::analyze_for_collection`](crate::engine::sparse::inverted::InvertedIndex::analyze_for_collection),
    /// the exact same lookup the forward indexing path
    /// (`index_document_in_txn`) uses, so a staged doc is tokenized
    /// identically to how it will be tokenized once committed.
    ///
    /// `Ok(None)` means the document has nothing to tokenize: the collection is
    /// unregistered, the extracted text is empty (which the forward indexer
    /// also never indexes), or the analyzer produced no tokens. An undecodable
    /// body is `Err`. Analyzer resolution failure stays a logged skip — it is a
    /// backend metadata read error, not a statement about this document.
    fn tokenize_staged_body(
        &self,
        database_id: u64,
        config_key: &(DatabaseId, TenantId, String),
        body: &[u8],
    ) -> crate::Result<Option<Vec<String>>> {
        let Some(doc) = self.decode_indexed_body(config_key, body)? else {
            return Ok(None);
        };
        let text = extract_fts_text(&doc);
        if text.is_empty() {
            return Ok(None);
        }
        let (_, tid, collection) = config_key;
        let tokens =
            match self
                .inverted
                .analyze_for_collection(database_id, *tid, collection, &text)
            {
                Ok(tokens) => tokens,
                Err(e) => {
                    warn!(
                        error = %e,
                        %collection,
                        "staged FTS scoring: analyzer resolution failed; skipping doc"
                    );
                    return Ok(None);
                }
            };
        if tokens.is_empty() {
            Ok(None)
        } else {
            Ok(Some(tokens))
        }
    }
}

/// Return the earliest start index at which `phrase` occurs as a contiguous,
/// in-order subsequence of `tokens`, or `None` if it never does. Adjacency
/// is exact (zero slop), matching the durable phrase search.
fn earliest_contiguous_match(tokens: &[String], phrase: &[String]) -> Option<u32> {
    if phrase.is_empty() || phrase.len() > tokens.len() {
        return None;
    }
    let last_start = tokens.len() - phrase.len();
    for start in 0..=last_start {
        if tokens[start..start + phrase.len()]
            .iter()
            .zip(phrase)
            .all(|(a, b)| a == b)
        {
            return Some(start as u32);
        }
    }
    None
}
