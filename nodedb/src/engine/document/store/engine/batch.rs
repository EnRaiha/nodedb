// SPDX-License-Identifier: BUSL-1.1

//! `DocumentEngine` struct, constructor, registration, and index lookups.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::document::store::config::CollectionConfig;
use crate::engine::sparse::btree::SparseEngine;

/// Wall-clock millisecond timestamp for versioned writes.
///
/// Used only by [`DocumentEngine`] (the lower-level struct API). The
/// [`CoreLoop`] Calvin write path uses `bitemporal_now_ms()` instead, which
/// threads the deterministic epoch timestamp through `CoreLoop::epoch_system_ms`.
/// This function is therefore NOT reachable from any Calvin write path.
pub(super) fn wall_now_ms() -> i64 {
    // no-determinism: off the Calvin path; CoreLoop uses bitemporal_now_ms() which reads epoch_system_ms
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub struct DocumentEngine<'a> {
    pub(super) sparse: &'a SparseEngine,
    pub(super) database_id: u64,
    pub(super) tenant_id: u64,
    pub(super) configs: HashMap<String, CollectionConfig>,
}

impl<'a> DocumentEngine<'a> {
    pub fn new(sparse: &'a SparseEngine, database_id: u64, tenant_id: u64) -> Self {
        Self {
            sparse,
            database_id,
            tenant_id,
            configs: HashMap::new(),
        }
    }

    /// Register a collection configuration with index paths.
    pub fn register_collection(&mut self, config: CollectionConfig) {
        self.configs.insert(config.name.clone(), config);
    }

    pub(super) fn is_bitemporal(&self, collection: &str) -> bool {
        self.configs.get(collection).is_some_and(|c| c.bitemporal)
    }

    /// Drop all secondary index entries for a field across the entire collection.
    pub fn drop_field_index(&self, collection: &str, field: &str) -> crate::Result<usize> {
        self.sparse.delete_index_entries_for_field(
            self.database_id,
            self.tenant_id,
            collection,
            field,
        )
    }

    /// Lookup documents by a secondary index value.
    ///
    /// When `bitemporal` is true the collection never populates the plain
    /// `INDEXES` table — every secondary-index write lands in the versioned
    /// index. Resolve current-version doc IDs through
    /// `versioned_index_lookup_as_of(.., None)`, which groups by doc_id, keeps
    /// the newest entry, and filters tombstoned entries (so deleted or
    /// superseded values are hidden). Non-bitemporal collections keep the exact
    /// plain `range_scan` path below.
    pub fn index_lookup(
        &self,
        collection: &str,
        path: &str,
        value: &str,
        bitemporal: bool,
    ) -> crate::Result<Vec<String>> {
        if bitemporal {
            return self.sparse.versioned_index_lookup_as_of(
                self.database_id,
                self.tenant_id,
                collection,
                path,
                value,
                None,
            );
        }
        let prefix_with_value = format!("{value}:");
        let results =
            self.sparse
                .range_scan(crate::engine::sparse::btree_index::RangeScanParams {
                    database_id: self.database_id,
                    tenant_id: self.tenant_id,
                    collection,
                    field: path,
                    lower: Some(prefix_with_value.as_bytes()),
                    upper: None,
                    limit: 1000,
                })?;

        let mut doc_ids = Vec::new();
        for (key, _) in results {
            if let Some(doc_id) = key.rsplit(':').next() {
                let expected_prefix = format!(
                    "{}:{}:{collection}:{path}:{value}:",
                    self.database_id, self.tenant_id
                );
                if key.starts_with(&expected_prefix) {
                    doc_ids.push(doc_id.to_string());
                }
            }
        }
        Ok(doc_ids)
    }
}
