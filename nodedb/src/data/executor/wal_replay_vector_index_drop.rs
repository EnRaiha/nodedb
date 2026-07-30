// SPDX-License-Identifier: BUSL-1.1

//! Replay one WAL `VectorIndexDrop` record.
//!
//! Records replay in LSN order, so applying the drop where it appears gives
//! the same outcome the live path produced: every `VectorParams` / `VectorPut`
//! record that preceded it is discarded, and anything that follows it (a
//! re-`CREATE VECTOR INDEX` on the same column) rebuilds the index from
//! scratch. Skipping the record instead would resurrect an index the user
//! dropped, because the `VectorParams` record that created it is still in the
//! log.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Evict every piece of replayed state belonging to the index this record
    /// dropped. A payload that matches no known shape is counted in `skipped`.
    pub(in crate::data::executor) fn restore_vector_index_drop_record(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        payload: &[u8],
        skipped: &mut usize,
    ) {
        let Ok((collection, field_name)) = zerompk::from_msgpack::<(String, String)>(payload)
        else {
            *skipped += 1;
            return;
        };

        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
        let (db, tenant, _) = index_key.clone();
        self.vector_collections.remove(&index_key);
        self.ivf_indexes.remove(&index_key);
        self.vector_params.remove(&index_key);
        self.index_configs.remove(&index_key);
        self.declared_dims.remove(&index_key);
        self.sparse_vector_indexes.retain(|(d, t, c, f), _| {
            !(*d == db && *t == tenant && *c == collection && *f == field_name)
        });
        self.vector_doc_map.retain(|(d, t, c, f, _), _| {
            !(*d == db && *t == tenant && *c == collection && *f == field_name)
        });

        tracing::debug!(
            core = self.core_id,
            %collection,
            field = %field_name,
            "WAL vector replay: index drop applied"
        );
    }
}
