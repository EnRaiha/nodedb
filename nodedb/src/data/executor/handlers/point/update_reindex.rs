// SPDX-License-Identifier: BUSL-1.1

//! Atomic bitemporal UPDATE: write the new document version and reconcile the
//! versioned secondary index in a single write transaction.
//!
//! Bitemporal collections never populate the plain `INDEXES` table — every
//! secondary-index entry lives in the versioned index. The `PointUpdate`
//! fast-path therefore has to teach the versioned index about the change:
//! values the update removed must be tombstoned (so a later
//! `versioned_index_lookup_as_of` skips this doc for the old value) and the
//! current values must be asserted at the new system time (so a lookup on the
//! new value finds it). This mirrors, on the versioned index, the
//! insert/delete-time maintenance in `apply_point_put` / `apply_point_delete`.

use std::collections::BTreeSet;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::{IndexPath, extract_index_values};
use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};

/// Inputs for [`CoreLoop::bitemporal_update_reindex`].
pub(in crate::data::executor) struct BitemporalUpdateReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub new_body: &'a [u8],
    pub index_paths: &'a [IndexPath],
    /// Decoded pre-update document, if the prior row could be decoded. `None`
    /// means no old index values to reconcile (nothing to tombstone).
    pub old_doc: Option<&'a serde_json::Value>,
    /// Decoded post-update document, used to compute the current index values.
    pub new_doc: &'a serde_json::Value,
}

impl CoreLoop {
    /// Extract the indexed values a document contributes for one path, honoring
    /// the path's partial predicate and case-folding — matching the put-time
    /// semantics in `apply_point_put`.
    fn indexed_values_for_path(doc: &serde_json::Value, path: &IndexPath) -> BTreeSet<String> {
        if let Some(ref pred) = path.predicate
            && !pred.evaluate_json(doc)
        {
            return BTreeSet::new();
        }
        extract_index_values(doc, &path.path, path.is_array)
            .into_iter()
            .map(|v| {
                if path.case_insensitive {
                    v.to_lowercase()
                } else {
                    v
                }
            })
            .collect()
    }

    /// Write the new bitemporal body and reconcile the versioned secondary
    /// index atomically. Removed values are tombstoned; current values are
    /// asserted live at `sys_from_ms`.
    pub(in crate::data::executor) fn bitemporal_update_reindex(
        &self,
        p: BitemporalUpdateReindex<'_>,
    ) -> crate::Result<()> {
        let txn = self.sparse.begin_write()?;

        self.sparse.versioned_put_in_txn(
            &txn,
            VersionedPut {
                database_id: p.database_id,
                tenant: p.tid,
                coll: p.collection,
                doc_id: p.doc_id,
                sys_from_ms: p.sys_from_ms,
                valid_from_ms: p.valid_from_ms,
                valid_until_ms: p.valid_until_ms,
                body: p.new_body,
            },
        )?;

        for path in p.index_paths {
            let new_values = Self::indexed_values_for_path(p.new_doc, path);
            let old_values = p
                .old_doc
                .map(|d| Self::indexed_values_for_path(d, path))
                .unwrap_or_default();

            // Tombstone values the update dropped so lookups on them skip this
            // doc from `sys_from_ms` onward.
            for value in old_values.difference(&new_values) {
                self.sparse.versioned_index_tombstone_in_txn(
                    &txn,
                    VersionedIndexEntry {
                        database_id: p.database_id,
                        tenant: p.tid,
                        coll: p.collection,
                        field: &path.path,
                        value,
                        doc_id: p.doc_id,
                        sys_from_ms: p.sys_from_ms,
                    },
                )?;
            }

            // Assert every current value live at the new system time.
            for value in &new_values {
                self.sparse.versioned_index_put_in_txn(
                    &txn,
                    VersionedIndexEntry {
                        database_id: p.database_id,
                        tenant: p.tid,
                        coll: p.collection,
                        field: &path.path,
                        value,
                        doc_id: p.doc_id,
                        sys_from_ms: p.sys_from_ms,
                    },
                )?;
            }
        }

        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("bitemporal update reindex commit: {e}"),
        })?;
        Ok(())
    }
}
