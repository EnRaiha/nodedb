// SPDX-License-Identifier: BUSL-1.1

//! Document write paths: JSON and raw-MessagePack entry points.

use super::batch::{DocumentEngine, wall_now_ms};
use crate::engine::document::store::extract::{
    extract_index_values_rmpv, json_to_msgpack, rmpv_to_json,
};

impl<'a> DocumentEngine<'a> {
    /// Put a document (JSON value) into a collection.
    pub fn put(
        &self,
        collection: &str,
        doc_id: &str,
        document: &serde_json::Value,
    ) -> crate::Result<()> {
        let msgpack = json_to_msgpack(document);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode: {e}"),
        })?;

        // Delegate to put_raw so both JSON and raw-MessagePack entry points
        // share the same bitemporal-aware write path.
        self.put_raw(collection, doc_id, &buf)?;

        let _ = document;
        Ok(())
    }

    /// Put a document from raw MessagePack bytes.
    pub fn put_raw(
        &self,
        collection: &str,
        doc_id: &str,
        msgpack_bytes: &[u8],
    ) -> crate::Result<()> {
        let bitemporal = self.is_bitemporal(collection);

        if bitemporal {
            let sys_from = wall_now_ms();
            self.sparse
                .versioned_put(crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id: self.database_id,
                    tenant: self.tenant_id,
                    coll: collection,
                    doc_id,
                    sys_from_ms: sys_from,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: msgpack_bytes,
                })?;
        } else {
            self.sparse.put(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
                msgpack_bytes,
            )?;
        }

        if let Some(config) = self.configs.get(collection)
            && let Ok(value) = crate::util::bounded_msgpack::read_value(msgpack_bytes)
        {
            for index_path in &config.index_paths {
                let values =
                    extract_index_values_rmpv(&value, &index_path.path, index_path.is_array);
                for v in values {
                    if bitemporal {
                        let sys_from = wall_now_ms();
                        self.sparse.versioned_index_put(
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
                    } else {
                        self.sparse.index_put(
                            self.database_id,
                            self.tenant_id,
                            collection,
                            &index_path.path,
                            &v,
                            doc_id,
                        )?;
                    }
                }
            }
        }

        let _ = rmpv_to_json;
        Ok(())
    }
}
