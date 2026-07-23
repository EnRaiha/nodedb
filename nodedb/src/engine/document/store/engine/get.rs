// SPDX-License-Identifier: BUSL-1.1

//! Document read paths.

use super::batch::DocumentEngine;
use crate::engine::document::store::extract::rmpv_to_json;

impl<'a> DocumentEngine<'a> {
    /// Get a document and deserialize from MessagePack to JSON.
    pub fn get(&self, collection: &str, doc_id: &str) -> crate::Result<Option<serde_json::Value>> {
        let bytes_opt = if self.is_bitemporal(collection) {
            self.sparse.versioned_get_current(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
            )?
        } else {
            self.sparse
                .get(self.database_id, self.tenant_id, collection, doc_id)?
        };
        match bytes_opt {
            Some(bytes) => {
                let rmpv_val = crate::util::bounded_msgpack::read_value(&bytes).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("decode: {e}"),
                    }
                })?;
                Ok(Some(rmpv_to_json(&rmpv_val)))
            }
            None => Ok(None),
        }
    }

    /// Get raw MessagePack bytes (zero-copy path for DataFusion UDFs).
    pub fn get_raw(&self, collection: &str, doc_id: &str) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(collection) {
            self.sparse
                .versioned_get_current(self.database_id, self.tenant_id, collection, doc_id)
        } else {
            self.sparse
                .get(self.database_id, self.tenant_id, collection, doc_id)
        }
    }
}
