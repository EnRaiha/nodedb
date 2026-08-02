// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp::DropIndex` handler: tear down one vector index.
//!
//! The inverse of [`super::vector_params`]'s `SetParams`. Every piece of
//! per-index state that CREATE established is removed here — the
//! materialized graph, the IVF sidecar, the build parameters, the declared
//! dimension, the document→vector-id reverse map, and the on-disk checkpoint
//! — while every sibling index of the same collection is left untouched.
//!
//! Leaving any one of them behind is a silent failure: a live
//! `vector_collections` entry keeps answering searches for an index the user
//! dropped, a surviving `index_configs` entry makes a replacement CREATE look
//! like a duplicate, and a surviving checkpoint file restores the index on
//! the next boot.

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Remove the index covering `(collection, field_name)` from this core.
    ///
    /// `field_name` is the empty string for a collection's default (unnamed)
    /// vector field, matching `CoreLoop::vector_index_key`.
    pub(in crate::data::executor) fn execute_drop_vector_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> Response {
        let database_id = task.request.database_id;
        let index_key =
            CoreLoop::vector_index_key(database_id.as_u64(), tid, collection, field_name);
        let (db, tenant, collection_key) = index_key.clone();

        let had_index = self.vector_collections.remove(&index_key).is_some();
        self.ivf_indexes.remove(&index_key);
        self.vector_params.remove(&index_key);
        self.index_configs.remove(&index_key);
        self.declared_dims.remove(&index_key);

        // The sparse inverted index is keyed by (collection, field) rather
        // than by the joined collection key, and a sparse index declares no
        // field of its own (`_sparse`). Drop the entry that matches this
        // index's field so a sparse index dropped by name stops answering.
        self.sparse_vector_indexes.retain(|(d, t, c, f), _| {
            !(*d == db && *t == tenant && c == collection && f == field_name)
        });

        // Reverse doc→vector-id map, keyed by (db, tenant, collection, field, doc).
        self.vector_doc_map.retain(|(d, t, c, f, _), _| {
            !(*d == db && *t == tenant && c == collection && f == field_name)
        });

        // Unlink this index's checkpoint so a restart does not restore it.
        // A failure here is fatal to the drop: the index would come back on
        // the next boot with the user having been told it was gone.
        if let Err(e) = super::reclaim::vector::reclaim_vector_index_checkpoint(
            &self.data_dir,
            self.core_id,
            db.as_u64(),
            tenant.as_u64(),
            collection,
            field_name,
        ) {
            warn!(
                core = self.core_id,
                %collection,
                field = field_name,
                error = %e,
                "DropIndex: vector checkpoint unlink failed"
            );
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("vector index checkpoint reclaim failed: {e}"),
                },
            );
        }

        info!(
            core = self.core_id,
            %collection,
            field = field_name,
            key = %collection_key,
            had_index,
            "dropped vector index"
        );
        self.response_ok(task)
    }
}
