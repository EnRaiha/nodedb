// SPDX-License-Identifier: BUSL-1.1

//! Graph DSL commands: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL/UNLABEL, GRAPH TRAVERSE, GRAPH NEIGHBORS, GRAPH PATH,
//! GRAPH ALGO.
//!
//! Every handler consumes already-parsed typed fields from
//! `nodedb_sql::ddl_ast::NodedbStatement`. Raw-SQL tokenising lives
//! in `nodedb-sql::ddl_ast::graph_parse` — no handler in this
//! directory does string-prefix parsing.

mod algo;
mod edge;
pub(super) mod rag_fusion;
mod response;
mod stats;
mod traverse;

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::statement::{GraphStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Dispatch a parsed graph-DSL variant to its handler.
///
/// Returns `None` when the statement is not a graph DSL variant,
/// so the caller can fall through to other dispatchers.
pub async fn dispatch_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stmt: NodedbStatement,
) -> Option<PgWireResult<Vec<Response>>> {
    match stmt {
        NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
            collection,
            src,
            dst,
            label,
            properties,
        }) => Some(
            edge::insert_edge(
                state,
                identity,
                database_id,
                collection,
                src,
                dst,
                label,
                properties,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
            collection,
            src,
            dst,
            label,
        }) => Some(edge::delete_edge(state, identity, collection, src, dst, label).await),
        NodedbStatement::Graph(GraphStmt::GraphSetLabels {
            node_id,
            labels,
            remove,
        }) => Some(edge::set_node_labels(state, identity, node_id, labels, remove).await),
        NodedbStatement::Graph(GraphStmt::GraphTraverse {
            start,
            depth,
            edge_label,
            direction,
        }) => Some(traverse::traverse(state, identity, start, depth, edge_label, direction).await),
        NodedbStatement::Graph(GraphStmt::GraphNeighbors {
            node,
            edge_label,
            direction,
        }) => Some(traverse::neighbors(state, identity, node, edge_label, direction).await),
        NodedbStatement::Graph(GraphStmt::GraphPath {
            src,
            dst,
            max_depth,
            edge_label,
        }) => Some(traverse::shortest_path(state, identity, src, dst, max_depth, edge_label).await),
        NodedbStatement::Graph(GraphStmt::GraphAlgo {
            algorithm,
            collection,
            edge_label,
            damping,
            tolerance,
            resolution,
            max_iterations,
            sample_size,
            source_node,
            direction,
            mode,
            personalization,
        }) => Some(
            algo::algo(
                state,
                identity,
                &algorithm,
                collection,
                edge_label,
                damping,
                tolerance,
                resolution,
                max_iterations,
                sample_size,
                source_node,
                direction,
                mode,
                personalization,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphRagFusion { collection, params }) => {
            Some(rag_fusion::rag_fusion(state, identity, collection, params).await)
        }
        NodedbStatement::Graph(GraphStmt::ShowGraphStats {
            collection,
            verbose,
            as_of,
        }) => Some(stats::show_graph_stats(state, identity, collection, verbose, as_of).await),
        // Non-graph NodedbStatement variants (CreateCollection, DropCollection,
        // etc.) return None so the caller can route them to the correct handler.
        _ => None,
    }
}
