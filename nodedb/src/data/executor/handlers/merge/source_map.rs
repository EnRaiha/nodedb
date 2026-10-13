// SPDX-License-Identifier: BUSL-1.1

//! The strict schema lookup MERGE decodes target rows with. The source side
//! is built from shipped rows by `handlers::join_source_map`.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Resolve a collection's strict Binary-Tuple schema, if it is a strict
    /// document collection. `None` for schemaless collections.
    pub(in crate::data::executor) fn merge_strict_schema(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }
}
