// SPDX-License-Identifier: BUSL-1.1

//! Document delete path.
//!
//! On bitemporal collections this appends a tombstone version so AS-OF
//! queries still see prior history; on non-bitemporal collections the
//! row is removed in place.

use super::batch::{DocumentEngine, wall_now_ms};
use crate::engine::document::store::extract::extract_index_values_rmpv;

impl<'a> DocumentEngine<'a> {
    pub fn delete(&self, collection: &str, doc_id: &str) -> crate::Result<bool> {
        if self.is_bitemporal(collection) {
            let prior_body = self.sparse.versioned_get_current(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
            )?;
            let Some(body) = prior_body else {
                return Ok(false);
            };
            let sys_from = wall_now_ms();
            self.sparse.versioned_tombstone(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
                sys_from,
            )?;
            if let Some(config) = self.configs.get(collection)
                && let Ok(rmpv_val) = crate::util::bounded_msgpack::read_value(&body)
            {
                for index_path in &config.index_paths {
                    for v in
                        extract_index_values_rmpv(&rmpv_val, &index_path.path, index_path.is_array)
                    {
                        self.sparse.versioned_index_tombstone(
                            crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                database_id: self.database_id,
                                tenant: self.tenant_id,
                                coll: collection,
                                field: &index_path.path,
                                value: &v,
                                doc_id,
                                sys_from_ms: sys_from,
                            },
                        )?;
                    }
                }
            }
            return Ok(true);
        }
        self.sparse.delete_indexes_for_document(
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
        )?;
        Ok(self
            .sparse
            .delete(self.database_id, self.tenant_id, collection, doc_id)?
            .is_some())
    }
}
