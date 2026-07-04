// SPDX-License-Identifier: BUSL-1.1

//! Shared full-text extraction: concatenate a document's string field values
//! into the single text blob the inverted index analyzes.

/// Concatenate all top-level string-valued fields of a document object into
/// the text the full-text inverted index indexes.
///
/// Used by the forward PUT indexing path AND by DELETE-rollback re-indexing so
/// both produce byte-identical text (and therefore identical postings, since
/// `nodedb_fts::analyze` is deterministic). Non-object values and non-string
/// fields contribute nothing.
pub(in crate::data::executor) fn extract_fts_text(doc: &serde_json::Value) -> String {
    match doc.as_object() {
        Some(obj) => obj
            .values()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        None => String::new(),
    }
}
