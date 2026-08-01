// SPDX-License-Identifier: BUSL-1.1

//! Shape definition schema — re-exports from `nodedb-types`.
//!
//! All shape types are defined in `nodedb-types::sync::shape` so that both
//! Origin and NodeDB-Lite share identical definitions. This module
//! re-exports them for use within the Origin codebase.

pub use nodedb_types::id::ShapeId;
pub use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};

use nodedb_types::DatabaseId;

/// Authenticated tenant and database boundary for shape subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShapeScope {
    pub tenant_id: u64,
    pub database_id: DatabaseId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_shape_matches_collection() {
        let shape = ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "all orders".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("orders", "o1"));
        assert!(!shape.could_match("users", "u1"));
        assert_eq!(shape.collection(), Some("orders"));
    }

    #[test]
    fn graph_shape() {
        let shape = ShapeDefinition {
            shape_id: "g1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Graph {
                root_nodes: vec!["alice".into()],
                max_depth: 2,
                edge_label: Some("KNOWS".into()),
            },
            description: "alice's network".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("any_collection", "any_doc"));
        assert_eq!(shape.collection(), None);
    }

    #[test]
    fn vector_shape() {
        let shape = ShapeDefinition {
            shape_id: "v1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Vector {
                collection: "embeddings".into(),
                field_name: Some("title".into()),
            },
            description: "title embeddings".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("embeddings", "e1"));
        assert!(!shape.could_match("other", "e1"));
    }
}
