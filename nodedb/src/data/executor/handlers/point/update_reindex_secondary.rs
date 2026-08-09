// SPDX-License-Identifier: BUSL-1.1

//! Re-index a just-updated row into the secondary vector and sparse indexes.
//!
//! The body rewrite on the update path (`sparse.put` /
//! `bitemporal_update_reindex`) reconciles storage plus the btree / FTS /
//! graph overlays, but touches neither the HNSW vector index nor the sparse
//! inverted index. Both are maintained here, together, so a caller cannot
//! remember one and forget the other.

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;

/// Inputs to [`CoreLoop::update_reindex_vector_and_sparse`].
pub(in crate::data::executor) struct UpdateSecondaryReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub row_key: &'a str,
    pub surrogate: Surrogate,
    pub new_body: &'a [u8],
    pub is_strict: bool,
    /// Precomputed by the caller so a loop over N rows pays the
    /// `vector_params`-scanning check once, not once per row.
    pub has_vectors: bool,
}

impl CoreLoop {
    /// Re-index `row_key`'s vectors and sparse literal from its new body.
    ///
    /// Each half is a no-op when the collection declares nothing of that kind.
    /// A vector whose width disagrees with the index is an error, so an
    /// embedding change that would land at the wrong size fails the update
    /// instead of silently leaving the old embedding searchable.
    pub(in crate::data::executor) fn update_reindex_vector_and_sparse(
        &mut self,
        p: UpdateSecondaryReindex<'_>,
    ) -> crate::Result<()> {
        self.update_reindex_vector_indexes(super::update_reindex_vector::UpdateVectorReindex {
            database_id: p.database_id,
            tid: p.tid,
            collection: p.collection,
            row_key: p.row_key,
            surrogate: p.surrogate,
            new_body: p.new_body,
            is_strict: p.is_strict,
            has_vectors: p.has_vectors,
        })?;

        let has_sparse = self.collection_has_sparse(p.database_id, p.tid, p.collection);
        self.update_reindex_sparse_indexes(super::update_reindex_sparse::UpdateSparseReindex {
            database_id: p.database_id,
            tid: p.tid,
            collection: p.collection,
            row_key: p.row_key,
            new_body: p.new_body,
            is_strict: p.is_strict,
            has_sparse,
        })?;
        Ok(())
    }
}
