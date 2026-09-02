// SPDX-License-Identifier: Apache-2.0

//! Per-column vector model metadata for embedding tracking and migration.
//!
//! Stored in the system catalog keyed by
//! `(database_id, tenant_id, collection, column)`. Informational by default;
//! optional strict dimension enforcement.

use serde::{Deserialize, Serialize};

/// Metadata about the embedding model that produced vectors in a column.
///
/// Stored in the system catalog via `ALTER COLLECTION x SET VECTOR METADATA ON column (...)`.
/// Retrieved via `SELECT VECTOR_METADATA('collection', 'column')` or `SHOW VECTOR MODELS`.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorModelMetadata {
    /// Embedding model identifier (e.g. "text-embedding-3-large", "all-MiniLM-L6-v2").
    pub model: String,
    /// Expected vector dimensionality for this model.
    pub dimensions: usize,
    /// ISO-8601 date when the metadata was set (informational).
    pub created_at: String,
    /// If true, INSERT rejects vectors whose dimension doesn't match `dimensions`.
    /// Default: false (no enforcement — metadata only).
    #[serde(default)]
    pub strict_dimensions: bool,
}

/// Catalog entry for vector model metadata.
///
/// Wraps `VectorModelMetadata` with the key fields needed for catalog storage
/// and the `_system.vector_models` view.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct VectorModelEntry {
    /// Database that owns the collection.
    pub database_id: u64,
    /// Tenant that owns this collection.
    pub tenant_id: u64,
    /// Collection name.
    pub collection: String,
    /// Vector column name.
    pub column: String,
    /// The model metadata.
    pub metadata: VectorModelMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        let entry = VectorModelEntry {
            database_id: 5,
            tenant_id: 1,
            collection: "chunks".into(),
            column: "embedding".into(),
            metadata: VectorModelMetadata {
                model: "text-embedding-3-large".into(),
                dimensions: 1536,
                created_at: "2026-03-01".into(),
                strict_dimensions: false,
            },
        };
        let bytes = zerompk::to_msgpack_vec(&entry).unwrap();
        let restored: VectorModelEntry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(restored.database_id, 5);
        assert_eq!(restored.metadata.model, "text-embedding-3-large");
        assert_eq!(restored.metadata.dimensions, 1536);
        assert!(!restored.metadata.strict_dimensions);
    }

    #[test]
    fn strict_dimensions_default_false() {
        // Deserialize without strict_dimensions field → defaults to false.
        let meta = VectorModelMetadata {
            model: "test".into(),
            dimensions: 128,
            created_at: "2026-01-01".into(),
            strict_dimensions: false,
        };
        let bytes = zerompk::to_msgpack_vec(&meta).unwrap();
        let restored: VectorModelMetadata = zerompk::from_msgpack(&bytes).unwrap();
        assert!(!restored.strict_dimensions);
    }
}
