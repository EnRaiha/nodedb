// SPDX-License-Identifier: BUSL-1.1

//! Document indexing and removal for the inverted index.
//!
//! All writes bypass the LSM memtable and go directly to the persistent
//! POSTINGS / DOC_LENGTHS / STATS tables so they can participate in the
//! caller's redb write transaction (Origin transactional indexing).

use std::collections::HashMap;

use redb::{ReadableTable as _, WriteTransaction};
use tracing::debug;

use nodedb_fts::posting::Posting;
use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::errors::inverted_err;
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, POSTINGS, STATS};

/// `(database_id, tenant, collection, surrogate)` scope shared by the
/// transaction-participating indexing entry points.
pub struct IndexDocScope<'a> {
    /// Owning database id.
    pub database_id: u64,
    /// Owning tenant id.
    pub tid: TenantId,
    /// Collection the document belongs to.
    pub collection: &'a str,
    /// Global surrogate identity of the document.
    pub surrogate: Surrogate,
}

impl InvertedIndex {
    /// Tokenize `text` with the collection's configured analyzer (falls back
    /// to the default analyzer when the collection has none bound).
    ///
    /// This is the single analyzer-resolution entry point for the whole
    /// inverted-index module: forward indexing (`index_document`,
    /// `index_document_in_txn`) and query-term canonicalization
    /// (`phrase_search`) all call through here so a document is always
    /// tokenized the same way it is later matched against, whether the write
    /// is durable or still staged in an open transaction.
    pub fn analyze_for_collection(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        text: &str,
    ) -> crate::Result<Vec<String>> {
        self.inner
            .analyze_for_collection(database_id, tid.as_u64(), collection, text)
    }

    /// Bind a collection's per-collection FTS analyzer, persisted to backend
    /// metadata. `analyze_for_collection` resolves it from this point on for
    /// every write and read of the collection's text.
    pub fn set_collection_analyzer(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        analyzer_name: &str,
    ) -> crate::Result<()> {
        self.inner
            .set_collection_analyzer(database_id, tid.as_u64(), collection, analyzer_name)
    }

    /// Bind whether searches over `collection` fall back to fuzzy matching by
    /// default, persisted to backend metadata. `FtsIndex::search` ORs it into
    /// every query's own fuzzy flag.
    pub fn set_collection_fuzzy(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        fuzzy: bool,
    ) -> crate::Result<()> {
        self.inner
            .set_collection_fuzzy(database_id, tid.as_u64(), collection, fuzzy)
    }

    /// Index a document's text content.
    pub fn index_document(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        text: &str,
    ) -> crate::Result<()> {
        let tokens = self.analyze_for_collection(database_id, tid, collection, text)?;
        if tokens.is_empty() {
            return Ok(());
        }

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        self.write_index_data(
            &write_txn,
            IndexDocScope {
                database_id,
                tid,
                collection,
                surrogate,
            },
            &tokens,
        )?;
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit index", e))?;
        Ok(())
    }

    /// Index a document within an externally-owned write transaction.
    pub fn index_document_in_txn(
        &self,
        txn: &WriteTransaction,
        scope: IndexDocScope<'_>,
        text: &str,
    ) -> crate::Result<()> {
        let tokens =
            self.analyze_for_collection(scope.database_id, scope.tid, scope.collection, text)?;
        if tokens.is_empty() {
            return Ok(());
        }
        self.write_index_data(txn, scope, &tokens)
    }

    /// Core indexing logic: writes postings, doc length, and stats within
    /// a transaction. Bypasses the LSM memtable so Origin transactions can
    /// stay atomic with the document write.
    fn write_index_data(
        &self,
        txn: &WriteTransaction,
        scope: IndexDocScope<'_>,
        tokens: &[String],
    ) -> crate::Result<()> {
        let IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
        } = scope;
        let t = tid.as_u64();

        let mut term_postings: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_postings
                .entry(token.as_str())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;

        let mut postings_table = txn
            .open_table(POSTINGS)
            .map_err(|e| inverted_err("open postings", e))?;

        for (term, (freq, positions)) in &term_postings {
            let posting = Posting {
                doc_id: surrogate,
                term_freq: *freq,
                positions: positions.clone(),
            };

            let mut existing: Vec<Posting> = postings_table
                .get((database_id, t, collection, *term))
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack(v.value()).ok())
                .unwrap_or_default();

            existing.retain(|p| p.doc_id != surrogate);
            existing.push(posting);

            let bytes = zerompk::to_msgpack_vec(&existing)
                .map_err(|e| inverted_err("serialize postings", e))?;
            postings_table
                .insert((database_id, t, collection, *term), bytes.as_slice())
                .map_err(|e| inverted_err("insert posting", e))?;
        }
        drop(postings_table);

        let mut lengths = txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| inverted_err("open doc_lengths", e))?;

        // Read the surrogate's prior length (if any) BEFORE overwriting it, in
        // the same write transaction as the overwrite and the STATS update
        // below, so the check-and-increment is atomic (no TOCTOU). Presence of
        // a DOC_LENGTHS entry is the idempotency key: it means this surrogate
        // was already counted into STATS by a prior index (live write or an
        // earlier WAL replay pass), so a repeat index of the SAME surrogate
        // (e.g. WAL replay re-invoking this exact path) must NOT increment
        // `count` again.
        let prior_len: Option<u32> = lengths
            .get((database_id, t, collection, surrogate.as_u32()))
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<u32>(v.value()).ok());

        let len_bytes =
            zerompk::to_msgpack_vec(&doc_len).map_err(|e| inverted_err("serialize doc_len", e))?;
        lengths
            .insert(
                (database_id, t, collection, surrogate.as_u32()),
                len_bytes.as_slice(),
            )
            .map_err(|e| inverted_err("insert doc_len", e))?;
        drop(lengths);

        let (count_delta, total_delta) = match prior_len {
            // New document: bump the doc count and add its full length.
            None => (1i64, doc_len as i64),
            // Re-index of an already-counted surrogate (replay of an
            // unchanged doc, or a genuine re-index of changed content): the
            // doc was already counted once, so `count` does not change;
            // `total` only moves by the delta between the new and prior
            // length (zero for an identical replay).
            Some(prior) => (0i64, doc_len as i64 - prior as i64),
        };

        Self::update_stats_in_txn(txn, database_id, tid, collection, count_delta, total_delta)?;

        debug!(database_id, tid = t, %collection, surrogate = surrogate.as_u32(), tokens = tokens.len(), terms = term_postings.len(), "indexed document");
        Ok(())
    }

    /// Atomically update `(doc_count, total_token_sum)` in STATS by the given
    /// explicit deltas.
    ///
    /// Callers compute `count_delta` / `total_delta` themselves rather than
    /// this function inferring "new doc vs. removal" from the sign of a
    /// single combined delta: a re-index of an already-counted surrogate
    /// (e.g. WAL replay) needs `count_delta == 0` with a `total_delta` that
    /// may be positive, negative, or zero — a case the old sign-based
    /// inference could not express, which is what caused STATS to be
    /// double-counted on replay.
    pub(super) fn update_stats_in_txn(
        txn: &WriteTransaction,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        count_delta: i64,
        total_delta: i64,
    ) -> crate::Result<()> {
        let t = tid.as_u64();
        let mut stats = txn
            .open_table(STATS)
            .map_err(|e| inverted_err("open stats", e))?;
        let (count, total) = stats
            .get((database_id, t, collection))
            .ok()
            .flatten()
            .and_then(|v| zerompk::from_msgpack::<(u32, u64)>(v.value()).ok())
            .unwrap_or((0, 0));

        let new_count = (i64::from(count) + count_delta).max(0) as u32;
        let new_total = (total as i64 + total_delta).max(0) as u64;

        let bytes = zerompk::to_msgpack_vec(&(new_count, new_total))
            .map_err(|e| inverted_err("serialize stats", e))?;
        stats
            .insert((database_id, t, collection), bytes.as_slice())
            .map_err(|e| inverted_err("insert stats", e))?;
        Ok(())
    }

    /// Remove a document from the inverted index.
    pub fn remove_document(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        let t = tid.as_u64();

        let db = self.inner.backend().db();
        let write_txn = db.begin_write().map_err(|e| inverted_err("write txn", e))?;
        {
            let mut postings_table = write_txn
                .open_table(POSTINGS)
                .map_err(|e| inverted_err("open postings", e))?;

            let terms: Vec<String> = postings_table
                .range(
                    (database_id, t, collection, "")..=(database_id, t, collection, "\u{10ffff}"),
                )
                .map_err(|e| inverted_err("range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().3.to_string()))
                .collect();

            let mut updates: Vec<(String, Option<Vec<u8>>)> = Vec::new();
            for term in &terms {
                if let Ok(Some(val)) =
                    postings_table.get((database_id, t, collection, term.as_str()))
                {
                    let mut list: Vec<Posting> =
                        zerompk::from_msgpack(val.value()).unwrap_or_default();
                    let before = list.len();
                    list.retain(|p| p.doc_id != surrogate);
                    if list.len() != before {
                        if list.is_empty() {
                            updates.push((term.clone(), None));
                        } else {
                            let bytes = zerompk::to_msgpack_vec(&list).unwrap_or_default();
                            updates.push((term.clone(), Some(bytes)));
                        }
                    }
                }
            }

            for (term, new_val) in &updates {
                match new_val {
                    None => {
                        postings_table
                            .remove((database_id, t, collection, term.as_str()))
                            .map_err(|e| inverted_err("remove posting", e))?;
                    }
                    Some(bytes) => {
                        postings_table
                            .insert(
                                (database_id, t, collection, term.as_str()),
                                bytes.as_slice(),
                            )
                            .map_err(|e| inverted_err("update posting", e))?;
                    }
                }
            }

            let mut lengths = write_txn
                .open_table(DOC_LENGTHS)
                .map_err(|e| inverted_err("open doc_lengths", e))?;

            let old_len = lengths
                .get((database_id, t, collection, surrogate.as_u32()))
                .ok()
                .flatten()
                .and_then(|v| zerompk::from_msgpack::<u32>(v.value()).ok())
                .unwrap_or(0);

            lengths
                .remove((database_id, t, collection, surrogate.as_u32()))
                .map_err(|e| inverted_err("remove doc length", e))?;
            drop(lengths);

            if old_len > 0 {
                Self::update_stats_in_txn(
                    &write_txn,
                    database_id,
                    tid,
                    collection,
                    -1,
                    -(old_len as i64),
                )?;
            }

            // Note: the docmap sub-key in INDEX_META (previously maintained by the
            // old DocIdMap abstraction) is no longer updated. Searches filter via
            // Surrogate prefilter bitmaps instead.
        }
        write_txn
            .commit()
            .map_err(|e| inverted_err("commit remove", e))?;

        Ok(())
    }
}
