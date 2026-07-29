// SPDX-License-Identifier: Apache-2.0

//! Typed parsing for the graph DSL (`GRAPH ...`, `MATCH ...`).
//!
//! The handler layer historically parsed graph statements with
//! `upper.find("KEYWORD")` substring matching, which collapsed when
//! a node id, label, or property value shadowed a DSL keyword. This
//! module is the structural replacement: a quote- and brace-aware
//! tokeniser feeds a variant-building parser that produces a typed
//! [`NodedbStatement`]. Every graph DSL command flows through here
//! before reaching a pgwire handler, so the handlers never touch
//! raw SQL again.
//!
//! Values collected here are intentionally unvalidated — numeric
//! bounds, absent-but-required fields, and engine-level caps are
//! enforced at the pgwire boundary where the error response is
//! formed. That keeps this module free of `pgwire` dependencies
//! and out of the `nodedb` → `nodedb-sql` dependency edge.

pub mod fusion_params;
mod helpers;
mod tokenizer;
mod variants;

pub use fusion_params::{
    FusionKeywords, FusionParams, RAG_FUSION_KEYWORDS, SEARCH_FUSION_KEYWORDS,
    parse_search_using_fusion,
};

use super::statement::{GraphStmt, NodedbStatement};

/// Entry point: returns `Some` when `sql` starts with a graph DSL
/// keyword (`GRAPH ...`, `MATCH ...`, `OPTIONAL MATCH ...`), else
/// `None` so the main DDL parser can continue trying other cases.
pub fn try_parse(sql: &str) -> Option<NodedbStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("MATCH ") || upper.starts_with("OPTIONAL MATCH ") {
        return Some(NodedbStatement::Graph(GraphStmt::MatchQuery {
            body: trimmed.to_string(),
        }));
    }

    if !upper.starts_with("GRAPH ") {
        return None;
    }

    let toks = tokenizer::tokenize(trimmed);

    if upper.starts_with("GRAPH INSERT EDGE ") {
        return variants::parse_insert_edge(&toks);
    }
    if upper.starts_with("GRAPH DELETE EDGE ") {
        return variants::parse_delete_edge(&toks);
    }
    if upper.starts_with("GRAPH LABEL ") {
        return variants::parse_set_labels(&toks, false);
    }
    if upper.starts_with("GRAPH UNLABEL ") {
        return variants::parse_set_labels(&toks, true);
    }
    if upper.starts_with("GRAPH TRAVERSE ") {
        return variants::parse_traverse(&toks);
    }
    if upper.starts_with("GRAPH NEIGHBORS ") {
        return variants::parse_neighbors(&toks);
    }
    if upper.starts_with("GRAPH PATH ") {
        return variants::parse_path(&toks);
    }
    if upper.starts_with("GRAPH ALGO ") {
        return variants::parse_algo(&toks);
    }
    if upper.starts_with("GRAPH RAG FUSION ") {
        return variants::parse_rag_fusion(&toks, trimmed);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::{GraphDirection, GraphProperties};

    #[test]
    fn parse_graph_insert_edge_keyword_shaped_ids() {
        let stmt =
            try_parse("GRAPH INSERT EDGE IN 'myedges' FROM 'TO' TO 'FROM' TYPE 'LABEL'").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
                collection,
                src,
                dst,
                label,
                properties,
            }) => {
                assert_eq!(collection, "myedges");
                assert_eq!(src, "TO");
                assert_eq!(dst, "FROM");
                assert_eq!(label, "LABEL");
                assert_eq!(properties, GraphProperties::None);
            }
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_delete_edge_with_collection() {
        let stmt = try_parse("GRAPH DELETE EDGE IN 'myedges' FROM 'a' TO 'b' TYPE 'l'").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
                collection,
                src,
                dst,
                label,
            }) => {
                assert_eq!(collection, "myedges");
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(label, "l");
            }
            other => panic!("expected GraphDeleteEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_insert_edge_missing_collection_returns_none() {
        let result = try_parse("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'");
        assert!(
            result.is_none(),
            "missing IN <collection> must not produce a statement"
        );
    }

    #[test]
    fn parse_graph_insert_edge_with_object_properties() {
        let stmt = try_parse(
            "GRAPH INSERT EDGE IN 'edges' FROM 'a' TO 'b' TYPE 'l' PROPERTIES { note: '} DEPTH 999' }",
        )
        .unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
                collection,
                properties,
                ..
            }) => {
                assert_eq!(collection, "edges");
                match properties {
                    GraphProperties::Object(s) => assert!(s.contains("} DEPTH 999")),
                    other => panic!("expected Object properties, got {other:?}"),
                }
            }
            other => panic!("expected GraphInsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_traverse_keyword_substring_id() {
        let stmt =
            try_parse("GRAPH TRAVERSE IN 'kw' FROM 'node_with_DEPTH_in_name' DEPTH 2 LABEL 'l'")
                .unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphTraverse {
                collection,
                start,
                depth,
                ..
            }) => {
                assert_eq!(collection, "kw");
                assert_eq!(start, "node_with_DEPTH_in_name");
                assert_eq!(depth, 2);
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_path() {
        let stmt = try_parse("GRAPH PATH IN 'docs' FROM 'a' TO 'b' MAX_DEPTH 5 LABEL 'l'").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphPath {
                collection,
                src,
                dst,
                max_depth,
                edge_label,
            }) => {
                assert_eq!(collection, "docs");
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(max_depth, 5);
                assert_eq!(edge_label.as_deref(), Some("l"));
            }
            other => panic!("expected GraphPath, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_labels_list() {
        let stmt = try_parse("GRAPH LABEL 'alice' AS 'Person', 'User'").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphSetLabels {
                node_id,
                labels,
                remove,
            }) => {
                assert_eq!(node_id, "alice");
                assert_eq!(labels, vec!["Person".to_string(), "User".to_string()]);
                assert!(!remove);
            }
            other => panic!("expected GraphSetLabels, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_algo_pagerank() {
        let stmt = try_parse("GRAPH ALGO PAGERANK ON users ITERATIONS 5 DAMPING 0.85").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphAlgo {
                algorithm,
                collection,
                damping,
                max_iterations,
                ..
            }) => {
                assert_eq!(algorithm, "PAGERANK");
                assert_eq!(collection, "users");
                assert_eq!(damping, Some(0.85));
                assert_eq!(max_iterations, Some(5));
            }
            other => panic!("expected GraphAlgo, got {other:?}"),
        }
    }

    #[test]
    fn parse_graph_algo_personalization() {
        let stmt = try_parse(
            r#"GRAPH ALGO PAGERANK ON 'users' DAMPING 0.9 PERSONALIZATION {"alice": 1.0, "bob": 0.5}"#,
        )
        .unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphAlgo {
                algorithm,
                collection,
                damping,
                personalization,
                ..
            }) => {
                assert_eq!(algorithm, "PAGERANK");
                assert_eq!(collection, "users");
                assert_eq!(damping, Some(0.9));
                let raw = personalization.expect("personalization object present");
                assert!(raw.contains("alice"));
                assert!(raw.contains("bob"));
                // Round-trips as a JSON node→weight map.
                let map: std::collections::HashMap<String, f64> = sonic_rs::from_str(&raw).unwrap();
                assert_eq!(map.get("alice"), Some(&1.0));
                assert_eq!(map.get("bob"), Some(&0.5));
            }
            other => panic!("expected GraphAlgo, got {other:?}"),
        }
    }

    #[test]
    fn parse_match_query_captures_raw() {
        let stmt = try_parse("MATCH (x)-[:l]->(y) RETURN x, y").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::MatchQuery { body }) => {
                assert!(body.starts_with("MATCH"));
            }
            other => panic!("expected MatchQuery, got {other:?}"),
        }
    }

    #[test]
    fn non_graph_returns_none() {
        assert!(try_parse("SELECT * FROM users").is_none());
        assert!(try_parse("CREATE COLLECTION users").is_none());
    }

    // ── GraphRagFusion parser tests ──────────────────────────────────────

    #[test]
    fn parse_rag_fusion_full_syntax() {
        let stmt = try_parse(
            "GRAPH RAG FUSION ON entities \
             QUERY ARRAY[0.1, 0.2, 0.3] \
             VECTOR_TOP_K 50 \
             EXPANSION_DEPTH 2 \
             EDGE_LABEL 'related_to' \
             FINAL_TOP_K 10 \
             RRF_K (60.0, 35.0)",
        )
        .unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => {
                assert_eq!(collection, "entities");
                let v = params.query_vector.expect("QUERY ARRAY parsed");
                assert_eq!(v.len(), 3);
                assert!((v[0] - 0.1f32).abs() < 1e-5);
                assert_eq!(params.vector_top_k, Some(50));
                assert_eq!(params.expansion_depth, Some(2));
                assert_eq!(params.edge_label.as_deref(), Some("related_to"));
                assert_eq!(params.final_top_k, Some(10));
                let (k1, k2) = params.rrf_k.unwrap();
                assert!((k1 - 60.0).abs() < 1e-10);
                assert!((k2 - 35.0).abs() < 1e-10);
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_minimal_defaults_to_none() {
        let stmt = try_parse("GRAPH RAG FUSION ON mycol QUERY ARRAY[1.0, 0.0]").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => {
                assert_eq!(collection, "mycol");
                assert!(params.query_vector.is_some());
                assert_eq!(params.vector_top_k, None);
                assert_eq!(params.expansion_depth, None);
                assert_eq!(params.edge_label, None);
                assert_eq!(params.final_top_k, None);
                assert_eq!(params.rrf_k, None);
                assert_eq!(params.vector_field, None);
                assert_eq!(params.direction, None);
                assert_eq!(params.max_visited, None);
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_direction_and_max_visited() {
        let stmt =
            try_parse("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] DIRECTION both MAX_VISITED 500")
                .unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                assert_eq!(params.direction, Some(GraphDirection::Both));
                assert_eq!(params.max_visited, Some(500));
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_vector_field_is_captured() {
        let stmt =
            try_parse("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] VECTOR_FIELD 'embedding'").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                assert_eq!(params.vector_field.as_deref(), Some("embedding"));
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_rrf_k_both_values_captured() {
        let stmt = try_parse("GRAPH RAG FUSION ON col QUERY ARRAY[0.5] RRF_K (1.0, 99.5)").unwrap();
        match stmt {
            NodedbStatement::Graph(GraphStmt::GraphRagFusion { params, .. }) => {
                let (k1, k2) = params.rrf_k.expect("RRF_K must be parsed");
                assert!((k1 - 1.0).abs() < 1e-10, "vector_k must be 1.0, got {k1}");
                assert!((k2 - 99.5).abs() < 1e-10, "graph_k must be 99.5, got {k2}");
            }
            other => panic!("expected GraphRagFusion, got {other:?}"),
        }
    }

    #[test]
    fn parse_rag_fusion_missing_collection_returns_none() {
        // ON keyword is absent — must not produce a statement.
        let result = try_parse("GRAPH RAG FUSION QUERY ARRAY[0.1] VECTOR_TOP_K 5");
        assert!(result.is_none(), "missing ON <collection> must return None");
    }
}
