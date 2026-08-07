// SPDX-License-Identifier: BUSL-1.1

//! Row-level helpers for the MERGE APPLY pass: undo capture for the in-memory
//! indexes, and post/pre-image projection for a `RETURNING` clause.

use crate::data::executor::doc_format;
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::data::executor::handlers::transaction::undo::UndoEntry;

/// One committed Phase-A put captured for post-commit event emission:
/// `(row_key, new stored body borrowed from the plan, prior stored value)`.
/// The body borrows from the merge plan (owned for the whole apply) rather than
/// being cloned.
pub(super) type MergePutEvent<'a> = (String, &'a [u8], Option<Vec<u8>>);

/// Record the in-memory index mutations a successful
/// [`crate::data::executor::core_loop::CoreLoop::apply_point_put`] performed as
/// undo entries. The HNSW vector index and the spatial R-tree live OUTSIDE the
/// shared redb transaction, so dropping that transaction on abort does not
/// reverse them — they must be undone explicitly. Drains the outcome's insert
/// deltas (leaving `prior_value` for the caller's event emission).
pub(super) fn record_put_index_undo(undo_log: &mut Vec<UndoEntry>, outcome: &mut PointPutOutcome) {
    for d in std::mem::take(&mut outcome.vector_inserts) {
        undo_log.push(UndoEntry::InsertVector {
            index_key: d.index_key,
            vector_id: d.vector_id,
            collection: d.collection,
            field: d.field,
            doc_id: d.doc_id,
        });
    }
    for (key, entry_id) in std::mem::take(&mut outcome.spatial_inserts) {
        undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
    }
}

/// Decode one merge row body into the JSON document a RETURNING projection
/// reads, with the row's storage key injected as `id`. Same shape the point and
/// bulk DML RETURNING paths emit, so a MERGE row projects identically.
///
/// Injection is a no-op when the body already carries an `id` field, so a
/// collection with a declared primary key keeps its own key rather than the
/// surrogate storage key.
pub(super) fn returning_doc(body: &[u8], doc_id: &str) -> Option<serde_json::Value> {
    let with_id = nodedb_query::msgpack_scan::inject_str_field(body, "id", doc_id);
    doc_format::decode_document(&with_id)
}
