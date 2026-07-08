// SPDX-License-Identifier: Apache-2.0

//! Default-provided method bodies for [`crate::traits::core::NodeDb`].
//!
//! Factored out of the trait declaration in `trait_def.rs` so that file
//! stays signatures + docs; each function here backs exactly one
//! default-provided `NodeDb` method, and the trait method delegates to it
//! in one line. Pure relocation — no behavior change.

use std::collections::{HashMap, HashSet};

use nodedb_types::document::Document;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::NodeId;
use nodedb_types::result::SearchResult;
use nodedb_types::text_search::TextSearchParams;

use super::quote::quote_ident;
use super::trait_def::NodeDb;

pub(super) fn graph_pagerank_default(
    collection: &str,
    personalization: Option<HashMap<String, f64>>,
    damping: Option<f64>,
    max_iterations: Option<u32>,
) -> NodeDbResult<Vec<(String, f64)>> {
    let _ = (collection, personalization, damping, max_iterations);
    Err(NodeDbError::storage(
        "graph_pagerank is not implemented for this NodeDb backend",
    ))
}

pub(super) async fn document_put_with_vector_default<T: NodeDb + ?Sized>(
    this: &T,
    doc_collection: &str,
    doc: Document,
    vector_collection: &str,
    id: &str,
    embedding: &[f32],
) -> NodeDbResult<()> {
    this.document_put(doc_collection, doc).await?;
    if !embedding.is_empty() {
        this.vector_insert(vector_collection, id, embedding, None)
            .await?;
    }
    Ok(())
}

pub(super) fn document_get_as_of_default(
    collection: &str,
    id: &str,
    as_of_ms: Option<i64>,
    valid_time_ms: Option<i64>,
) -> NodeDbResult<Option<Document>> {
    let _ = (collection, id, as_of_ms, valid_time_ms);
    Err(NodeDbError::storage(
        "document_get_as_of is not implemented on this client",
    ))
}

pub(super) fn document_put_with_valid_time_default(
    collection: &str,
    doc: Document,
    valid_from_ms: Option<i64>,
    valid_until_ms: Option<i64>,
) -> NodeDbResult<()> {
    let _ = (collection, doc, valid_from_ms, valid_until_ms);
    Err(NodeDbError::storage(
        "document_put_with_valid_time is not implemented on this client",
    ))
}

pub(super) fn vector_insert_field_default(
    collection: &str,
    field_name: &str,
    id: &str,
    embedding: &[f32],
    metadata: Option<Document>,
) -> NodeDbResult<()> {
    let _ = (collection, id, embedding, metadata);
    Err(NodeDbError::storage(format!(
        "vector_insert_field is not implemented on this client; \
         field_name={field_name} would have been silently dropped"
    )))
}

pub(super) fn vector_search_field_default(
    collection: &str,
    field_name: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
) -> NodeDbResult<Vec<SearchResult>> {
    let _ = (collection, query, k, filter);
    Err(NodeDbError::storage(format!(
        "vector_search_field is not implemented on this client; \
         field_name={field_name} would have been silently dropped"
    )))
}

/// Default forward-BFS shortest-path search built on `graph_traverse`.
///
/// See [`crate::traits::core::NodeDb::graph_shortest_path`] for the
/// full contract.
pub(super) async fn graph_shortest_path_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    from: &NodeId,
    to: &NodeId,
    max_depth: u8,
    edge_filter: Option<&EdgeFilter>,
) -> NodeDbResult<Option<Vec<NodeId>>> {
    if from == to {
        return Ok(Some(vec![from.clone()]));
    }
    if max_depth == 0 {
        return Ok(None);
    }

    // Map of `node -> parent` used to reconstruct the path once the
    // target is reached. The source has no parent entry.
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
    let mut frontier: Vec<NodeId> = vec![from.clone()];

    for _ in 0..max_depth {
        let mut next_frontier: Vec<NodeId> = Vec::new();
        for node in &frontier {
            let sg = this
                .graph_traverse(collection, node, 1, edge_filter)
                .await?;
            for edge in &sg.edges {
                // Only follow edges originating from the current
                // node — `graph_traverse` may include adjacent
                // edges that don't extend the BFS frontier.
                if &edge.from != node {
                    continue;
                }
                let dst = &edge.to;
                if dst == from || parent.contains_key(dst) {
                    continue;
                }
                parent.insert(dst.clone(), node.clone());
                if dst == to {
                    let mut path = vec![to.clone()];
                    let mut cur = to.clone();
                    while &cur != from {
                        let p = parent
                            .get(&cur)
                            .expect("BFS reached `to` so all ancestors are tracked")
                            .clone();
                        path.push(p.clone());
                        cur = p;
                    }
                    path.reverse();
                    return Ok(Some(path));
                }
                next_frontier.push(dst.clone());
            }
        }
        if next_frontier.is_empty() {
            return Ok(None);
        }
        frontier = next_frontier;
    }
    Ok(None)
}

pub(super) fn text_search_default(
    collection: &str,
    field: &str,
    query: &str,
    top_k: usize,
    params: TextSearchParams,
    allowed_ids: Option<&HashSet<String>>,
) -> NodeDbResult<Vec<SearchResult>> {
    let _ = (collection, field, query, top_k, params, allowed_ids);
    Err(NodeDbError::storage(
        "text_search is not implemented on this client",
    ))
}

pub(super) async fn batch_vector_insert_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    vectors: &[(&str, &[f32])],
) -> NodeDbResult<()> {
    for &(id, embedding) in vectors {
        this.vector_insert(collection, id, embedding, None).await?;
    }
    Ok(())
}

pub(super) async fn batch_graph_insert_edges_default<T: NodeDb + ?Sized>(
    this: &T,
    collection: &str,
    edges: &[(&str, &str, &str)],
) -> NodeDbResult<()> {
    for &(from, to, label) in edges {
        let src = NodeId::try_new(from)
            .map_err(|e| NodeDbError::storage(format!("invalid node id: {e}")))?;
        let dst = NodeId::try_new(to)
            .map_err(|e| NodeDbError::storage(format!("invalid node id: {e}")))?;
        this.graph_insert_edge(collection, &src, &dst, label, None)
            .await?;
    }
    Ok(())
}

pub(super) async fn undrop_collection_default<T: NodeDb + ?Sized>(
    this: &T,
    name: &str,
) -> NodeDbResult<()> {
    let sql = format!("UNDROP COLLECTION {}", quote_ident(name));
    this.execute_sql(&sql, &[]).await?;
    Ok(())
}

pub(super) async fn drop_collection_purge_default<T: NodeDb + ?Sized>(
    this: &T,
    name: &str,
) -> NodeDbResult<()> {
    let sql = format!("DROP COLLECTION {} PURGE", quote_ident(name));
    this.execute_sql(&sql, &[]).await?;
    Ok(())
}

pub(super) async fn list_dropped_collections_default<T: NodeDb + ?Sized>(
    this: &T,
) -> NodeDbResult<Vec<DroppedCollection>> {
    let sql = "SELECT tenant_id, name, owner, engine_type, \
               deactivated_at_ns, retention_expires_at_ns \
               FROM _system.dropped_collections";
    let result = this.execute_sql(sql, &[]).await?;
    crate::row_decode::parse_dropped_collection_rows(&result.rows)
}

pub(super) fn on_collection_purged_default() -> NodeDbResult<()> {
    Err(NodeDbError::storage(
        "on_collection_purged is not supported on this client — \
         requires a push-capable sync connection (NodeDbLite or a \
         sync-enabled remote client)",
    ))
}
