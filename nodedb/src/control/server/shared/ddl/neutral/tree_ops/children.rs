// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TREE_CHILDREN(graph_index, root_id)`
//!
//! BFS traversal from `root_id`, returns all descendant IDs.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::parse::{extract_function_args, extract_number_after};
use super::support::ddl_err;

pub async fn tree_children(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let upper = sql.to_uppercase();

    let args = extract_function_args(&upper, sql, "TREE_CHILDREN")?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "TREE_CHILDREN requires (graph_index, root_id)",
        ));
    }
    let graph_index = args[0].trim().to_lowercase();
    let root_id = args[1]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    let max_depth = extract_number_after(&upper, "MAX_DEPTH")?.unwrap_or(100);

    let dir = crate::engine::graph::edge_store::Direction::Out;
    let bfs_result = crate::control::server::graph_dispatch::cross_core_bfs_with_options(
        state,
        crate::control::server::graph_dispatch::CrossCoreBfsParams {
            tenant_id,
            // Tree-index BFS walks edges by index label; no catalog record maps
            // an index name back to the collection it was built on.
            collection: None,
            database_id,
            start_nodes: vec![root_id],
            edge_label: Some(graph_index),
            direction: dir,
            max_depth,
            options: &GraphTraversalOptions::default(),
        },
    )
    .await
    .map_err(|e| ddl_err("XX000", format!("BFS failed: {e}")))?;

    let bfs_json =
        crate::data::executor::response_codec::decode_payload_to_json(&bfs_result.payload);
    let node_ids: Vec<String> = sonic_rs::from_str::<Vec<serde_json::Value>>(&bfs_json)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let mut rows: Vec<Map<String, JsonValue>> = Vec::with_capacity(node_ids.len());
    for id in &node_ids {
        if id.is_empty() {
            continue;
        }
        let mut row = Map::new();
        row.insert("child_id".to_string(), JsonValue::String(id.to_string()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["child_id".to_string()],
        column_types: ShapedRows::text_types(1),
        rows,
        notice: None,
    })])
}
