// SPDX-License-Identifier: BUSL-1.1

//! Collection configuration for the Document Engine.

use super::index_path::IndexPath;

/// Collection configuration for the Document Engine.
#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    /// Declared secondary index paths.
    pub index_paths: Vec<IndexPath>,
    /// Whether this collection uses CRDT-backed storage (Loro).
    pub crdt_enabled: bool,
    /// Storage encoding mode (schemaless MessagePack or strict Binary Tuple).
    pub storage_mode: nodedb_physical::physical_plan::StorageMode,
    /// Collection enforcement options (append-only, period lock, retention, etc.).
    pub enforcement: nodedb_physical::physical_plan::EnforcementOptions,
    /// Bitemporal storage: every write goes to the versioned document
    /// table, keyed by `system_from_ms`. Enables `FOR SYSTEM_TIME AS OF`
    /// queries.
    pub bitemporal: bool,
    /// Durable CRDT conflict-resolution policy (JSON-serialized
    /// `CollectionPolicy`), carried through from the catalog so
    /// `execute_register_document_collection` can rehydrate it into this
    /// core's `PolicyRegistry`. `None` = no explicit policy persisted.
    pub conflict_policy: Option<String>,
}

impl CollectionConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            index_paths: Vec::new(),
            crdt_enabled: false,
            storage_mode: nodedb_physical::physical_plan::StorageMode::Schemaless,
            enforcement: nodedb_physical::physical_plan::EnforcementOptions::default(),
            bitemporal: false,
            conflict_policy: None,
        }
    }

    pub fn with_bitemporal(mut self, on: bool) -> Self {
        self.bitemporal = on;
        self
    }

    pub fn with_index(mut self, path: &str) -> Self {
        self.index_paths.push(IndexPath::new(path));
        self
    }

    pub fn with_crdt(mut self) -> Self {
        self.crdt_enabled = true;
        self
    }

    pub fn with_storage_mode(mut self, mode: nodedb_physical::physical_plan::StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}
