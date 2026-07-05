// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a parsed graph-overlay statement to its protocol-neutral handler.

use nodedb_sql::ddl_ast::statement::{GraphStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TxnId};

use super::super::super::result::{DdlError, DdlResult};
use super::{algo, edge, rag_fusion, stats, traverse};

/// Dispatch a parsed graph-overlay variant to its handler.
///
/// Returns `None` when the statement is not a graph-overlay variant this family
/// owns (e.g. `GraphStmt::MatchQuery`, which the router dispatches to the neutral
/// `match_ops` handler from its own typed arm before calling this), so the caller
/// falls through to the transitional pgwire delegation.
pub async fn dispatch_graph(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stmt: NodedbStatement,
    txn_id: Option<TxnId>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
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
        }) => {
            Some(edge::delete_edge(state, identity, database_id, collection, src, dst, label).await)
        }
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
        }) => Some(
            traverse::traverse(
                state,
                identity,
                database_id,
                start,
                depth,
                edge_label,
                direction,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphNeighbors {
            node,
            edge_label,
            direction,
        }) => Some(
            traverse::neighbors(
                state,
                identity,
                database_id,
                node,
                edge_label,
                direction,
                txn_id,
            )
            .await,
        ),
        NodedbStatement::Graph(GraphStmt::GraphPath {
            src,
            dst,
            max_depth,
            edge_label,
        }) => Some(
            traverse::shortest_path(
                state,
                identity,
                database_id,
                src,
                dst,
                max_depth,
                edge_label,
            )
            .await,
        ),
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
                database_id,
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
            Some(rag_fusion::rag_fusion(state, identity, database_id, collection, params).await)
        }
        NodedbStatement::Graph(GraphStmt::ShowGraphStats {
            collection,
            verbose,
            as_of,
        }) => Some(
            stats::show_graph_stats(state, identity, database_id, collection, verbose, as_of).await,
        ),
        // `MatchQuery` (handled by the router's typed arm → neutral `match_ops`)
        // and every non-graph-overlay variant return None so the caller can route
        // them elsewhere.
        _ => None,
    }
}
