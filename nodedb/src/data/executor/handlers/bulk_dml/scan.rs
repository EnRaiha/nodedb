// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

impl CoreLoop {
    /// Scan documents in a collection matching the given filters.
    ///
    /// Returns document IDs of all matching documents.
    pub(in crate::data::executor) fn scan_matching_documents(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &[ScanFilter],
    ) -> crate::Result<Vec<String>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        // Check if this is a strict (Binary Tuple) collection.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        let mut ids = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                let matches = if let Some(ref schema) = strict_schema {
                    // Strict: Binary Tuple → Value → MessagePack → matches_binary.
                    match super::super::super::strict_format::binary_tuple_to_json(
                        value_bytes,
                        schema,
                    ) {
                        Some(doc) => {
                            let msgpack = doc_format::encode_to_msgpack(&doc);
                            filters.iter().all(|f| f.matches_binary(&msgpack))
                        }
                        None => false,
                    }
                } else {
                    filters.iter().all(|f| f.matches_binary(value_bytes))
                };
                if matches && let Some(doc_id) = key.strip_prefix(&prefix) {
                    ids.push(doc_id.to_string());
                }
            }
        }
        Ok(ids)
    }
}

/// Compute the sorted list of surrogates from scanned document IDs.
///
/// Document storage keys are 8-character hex-encoded u32 surrogates
/// (see `engine::document::store::key`). Ids that cannot be parsed are
/// silently skipped — they represent legacy non-surrogate documents that
/// do not participate in OLLP verification.
///
/// The output is sorted ascending, matching the contract expected by the
/// OLLP verification comparison on both sides (Data Plane and Control
/// Plane pre-exec).
pub(super) fn ollp_actual_surrogates(doc_ids: &[String]) -> Vec<u32> {
    let mut surrogates: Vec<u32> = doc_ids
        .iter()
        .filter_map(|id| {
            if id.len() == 8 {
                u32::from_str_radix(id, 16).ok()
            } else {
                None
            }
        })
        .collect();
    surrogates.sort_unstable();
    surrogates
}
