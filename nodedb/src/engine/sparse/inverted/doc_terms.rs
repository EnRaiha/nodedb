// SPDX-License-Identifier: BUSL-1.1

//! Per-document term sets: the record of which posting lists a document is
//! currently a member of.
//!
//! ## Why this exists
//!
//! `write_index_data` only ever touches the terms present in the text it was
//! handed. That makes an insert correct and a same-text re-index idempotent,
//! but it makes an UPDATE wrong: the terms the PREVIOUS version of the
//! document contributed are invisible to it, so a word removed by the update
//! keeps its posting, the document keeps matching a word it no longer
//! contains, and that term's document frequency stays inflated — which skews
//! the IDF of every other query hitting the same term.
//!
//! Fixing that needs the old term set. Two ways to obtain it:
//!
//! * Range-scan every posting list in the collection and strip the surrogate
//!   from any list that still names it. Needs no extra storage, but costs
//!   O(terms in the collection) on every single re-index — an unbounded,
//!   corpus-sized cost on the write path.
//! * Persist the document's own term set beside its `DOC_LENGTHS` row, so a
//!   re-index removes exactly the terms that dropped out: O(terms in the old
//!   document).
//!
//! The second wins: the cost scales with the document being written rather
//! than with the corpus it lands in. Storage cost is one row per indexed
//! document holding its DISTINCT analyzed terms — bounded by the document's
//! own text (post-stemming, deduplicated, so typically well under the raw
//! field bytes) and independent of corpus size.
//!
//! The scan survives only as the bounded fallback for documents indexed
//! before term sets were recorded: it runs at most once per such document
//! (the re-index that triggers it writes the term set, so the next one takes
//! the targeted path) and never for a document that is not already in the
//! index — `stored` is consulted only when `DOC_LENGTHS` proves a prior
//! index exists.

use std::collections::BTreeSet;

use redb::{ReadableTable as _, WriteTransaction};

use nodedb_fts::posting::Posting;

use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::fts_redb::tables::{DOC_TERMS, POSTINGS};

/// Upper-bound sentinel for the `term` component of a
/// `(database_id, tid, collection, term)` range scan: the highest scalar
/// value representable in UTF-8, so every real term sorts below it.
const MAX_TERM: &str = "\u{10ffff}";

/// The term set a previous index of this surrogate recorded, or `None` when
/// none was ever stored (a document first indexed by a build that predates
/// the term set).
pub(super) fn stored(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
) -> crate::Result<Option<Vec<String>>> {
    let table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    let key = (
        scope.database_id,
        scope.tid.as_u64(),
        scope.collection,
        scope.surrogate.as_u32(),
    );
    let stored = table
        .get(key)
        .map_err(|e| inverted_err("read doc_terms", e))?
        .and_then(|v| zerompk::from_msgpack::<Vec<String>>(v.value()).ok());
    Ok(stored)
}

/// Record `terms` as the document's current term set, replacing any prior
/// one. Must be called in the same transaction as the posting writes it
/// describes, so a crash can never leave the set disagreeing with POSTINGS.
pub(super) fn put(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
    terms: &BTreeSet<&str>,
) -> crate::Result<()> {
    let encoded: Vec<&str> = terms.iter().copied().collect();
    let bytes =
        zerompk::to_msgpack_vec(&encoded).map_err(|e| inverted_err("serialize doc_terms", e))?;
    let mut table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    table
        .insert(
            (
                scope.database_id,
                scope.tid.as_u64(),
                scope.collection,
                scope.surrogate.as_u32(),
            ),
            bytes.as_slice(),
        )
        .map_err(|e| inverted_err("insert doc_terms", e))?;
    Ok(())
}

/// Drop the document's term-set row. Paired with the removal of its
/// `DOC_LENGTHS` row so the two never disagree about whether the document is
/// in the index.
pub(super) fn clear(txn: &WriteTransaction, scope: IndexDocScope<'_>) -> crate::Result<()> {
    let mut table = txn
        .open_table(DOC_TERMS)
        .map_err(|e| inverted_err("open doc_terms", e))?;
    table
        .remove((
            scope.database_id,
            scope.tid.as_u64(),
            scope.collection,
            scope.surrogate.as_u32(),
        ))
        .map_err(|e| inverted_err("remove doc_terms", e))?;
    Ok(())
}

/// Every term the collection currently has a posting list for.
///
/// The fallback source of "which lists might name this surrogate" for a
/// document that has no stored term set. Callers must gate it on the document
/// actually being indexed — see the module docs for why this is not the
/// general path.
pub(super) fn collection_terms(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
) -> crate::Result<Vec<String>> {
    let table = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;
    let t = scope.tid.as_u64();
    let terms = table
        .range(
            (scope.database_id, t, scope.collection, "")
                ..=(scope.database_id, t, scope.collection, MAX_TERM),
        )
        .map_err(|e| inverted_err("postings range", e))?
        .filter_map(|r| r.ok().map(|(k, _)| k.value().3.to_string()))
        .collect();
    Ok(terms)
}

/// Remove the surrogate's posting from each of `terms`, deleting a list that
/// this empties so the term's `df` returns to its true value instead of
/// counting a document that no longer contains it.
pub(super) fn strip_postings(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
    terms: &[String],
) -> crate::Result<()> {
    if terms.is_empty() {
        return Ok(());
    }
    let t = scope.tid.as_u64();
    let mut table = txn
        .open_table(POSTINGS)
        .map_err(|e| inverted_err("open postings", e))?;

    // Collect the edits before applying them: `get` borrows the table
    // immutably for as long as its result lives, so the writes cannot be
    // interleaved with the reads.
    let mut updates: Vec<(&str, Option<Vec<u8>>)> = Vec::new();
    for term in terms {
        let key = (scope.database_id, t, scope.collection, term.as_str());
        let Some(existing) = table
            .get(key)
            .map_err(|e| inverted_err("read postings", e))?
            .and_then(|v| zerompk::from_msgpack::<Vec<Posting>>(v.value()).ok())
        else {
            continue;
        };
        let mut remaining = existing;
        let before = remaining.len();
        remaining.retain(|p| p.doc_id != scope.surrogate);
        if remaining.len() == before {
            continue;
        }
        if remaining.is_empty() {
            updates.push((term.as_str(), None));
        } else {
            let bytes = zerompk::to_msgpack_vec(&remaining)
                .map_err(|e| inverted_err("serialize postings", e))?;
            updates.push((term.as_str(), Some(bytes)));
        }
    }

    for (term, new_value) in updates {
        let key = (scope.database_id, t, scope.collection, term);
        match new_value {
            None => {
                table
                    .remove(key)
                    .map_err(|e| inverted_err("remove posting", e))?;
            }
            Some(bytes) => {
                table
                    .insert(key, bytes.as_slice())
                    .map_err(|e| inverted_err("update posting", e))?;
            }
        }
    }
    Ok(())
}

/// The terms a document currently occupies, for a caller that is about to
/// remove it from some or all of them.
///
/// Returns the stored term set when there is one, and otherwise falls back to
/// the collection's full term list — bounded to documents that a prior index
/// actually recorded, which the caller proves by passing `previously_indexed`.
pub(super) fn occupied_terms(
    txn: &WriteTransaction,
    scope: IndexDocScope<'_>,
    previously_indexed: bool,
) -> crate::Result<Vec<String>> {
    if !previously_indexed {
        return Ok(Vec::new());
    }
    match stored(txn, scope)? {
        Some(terms) => Ok(terms),
        None => collection_terms(txn, scope),
    }
}
