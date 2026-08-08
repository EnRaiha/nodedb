// SPDX-License-Identifier: BUSL-1.1

//! Graph operation plan builders.

use nodedb_types::protocol::TextFields;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use crate::engine::graph::traversal_options::MAX_GRAPH_TRAVERSAL_DEPTH;
use nodedb_physical::physical_plan::GraphOp;

use super::parse_direction;

/// Clamp a depth parameter coming in over the native protocol,
/// rejecting out-of-range values rather than forwarding them to the
/// engine. Mirrors the pgwire ingress so no entry point can saturate
/// traversal with an unbounded fan-out.
fn clamped_depth(value: Option<u32>, default: usize, field: &str) -> crate::Result<usize> {
    let v = value.map(|v| v as usize).unwrap_or(default);
    if v > MAX_GRAPH_TRAVERSAL_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "{field} {v} exceeds maximum allowed value {MAX_GRAPH_TRAVERSAL_DEPTH}"
            ),
        });
    }
    Ok(v)
}

pub(crate) fn build_rag_fusion(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::RagFusion {
        collection: collection.to_string(),
        query_vector: query_vector.clone(),
        vector_top_k: fields.vector_top_k.unwrap_or(20) as usize,
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        expansion_depth: clamped_depth(fields.expansion_depth, 2, "expansion_depth")?,
        final_top_k: fields.final_top_k.unwrap_or(10) as usize,
        rrf_k: (
            fields.vector_k.unwrap_or(60.0),
            fields.graph_k.unwrap_or(10.0),
        ),
        rrf_k_triple: None,
        vector_field: fields.vector_field.clone().unwrap_or_default(),
        options: Default::default(),
        bm25_query: None,
        bm25_field: None,
    }))
}

pub(crate) fn build_hop(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Hop {
        collection: fields.collection.as_deref().map(str::to_lowercase),
        start_nodes: vec![start.clone()],
        depth: clamped_depth(fields.depth, 2, "depth")?,
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
    }))
}

pub(crate) fn build_neighbors(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Neighbors {
        collection: fields.collection.as_deref().map(str::to_lowercase),
        node_id: start.clone(),
        edge_label: fields.edge_label.clone(),
        direction: parse_direction(fields.direction.as_deref()),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_path(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let from = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    let to = fields
        .end_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'end_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Path {
        collection: fields.collection.as_deref().map(str::to_lowercase),
        src: from.clone(),
        dst: to.clone(),
        max_depth: clamped_depth(fields.depth, 10, "depth")?,
        edge_label: fields.edge_label.clone(),
        options: Default::default(),
        rls_filters: Vec::new(),
        frontier_bitmap: None,
    }))
}

pub(crate) fn build_subgraph(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let start = fields
        .start_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'start_node'".to_string(),
        })?;
    Ok(PhysicalPlan::Graph(GraphOp::Subgraph {
        collection: fields.collection.as_deref().map(str::to_lowercase),
        start_nodes: vec![start.clone()],
        depth: clamped_depth(fields.depth, 2, "depth")?,
        edge_label: fields.edge_label.clone(),
        options: Default::default(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_edge_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    if collection.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "edge PUT requires a non-empty collection".to_string(),
        });
    }
    let src = fields
        .from_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'from_node'".to_string(),
        })?;
    let dst = fields
        .to_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'to_node'".to_string(),
        })?;
    let label = fields
        .edge_type
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'edge_type'".to_string(),
        })?;
    let props = match fields.properties.as_ref() {
        Some(v) => sonic_rs::to_string(v).map_err(|e| crate::Error::BadRequest {
            detail: format!("edge properties are not serializable to JSON: {e}"),
        })?,
        None => String::new(),
    };
    let src_surrogate = ctx.state.surrogate_assigner.assign(
        ctx.database_id(),
        ctx.tenant_id(),
        collection,
        src.as_bytes(),
    )?;
    let dst_surrogate = ctx.state.surrogate_assigner.assign(
        ctx.database_id(),
        ctx.tenant_id(),
        collection,
        dst.as_bytes(),
    )?;
    Ok(PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: collection.to_string(),
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
        properties: props.into_bytes(),
        src_surrogate,
        dst_surrogate,
    }))
}

pub(crate) fn build_edge_delete(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    if collection.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "edge DELETE requires a non-empty collection".to_string(),
        });
    }
    let src = fields
        .from_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'from_node'".to_string(),
        })?;
    let dst = fields
        .to_node
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'to_node'".to_string(),
        })?;
    let label = fields
        .edge_type
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'edge_type'".to_string(),
        })?;
    // Resolve endpoint surrogates exactly as `build_edge_put` does (get-or-assign
    // returns the existing node identities) so a cross-shard delete dual-homes
    // and locks against a concurrent insert of the same edge.
    let src_surrogate = ctx.state.surrogate_assigner.assign(
        ctx.database_id(),
        ctx.tenant_id(),
        collection,
        src.as_bytes(),
    )?;
    let dst_surrogate = ctx.state.surrogate_assigner.assign(
        ctx.database_id(),
        ctx.tenant_id(),
        collection,
        dst.as_bytes(),
    )?;
    Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
        collection: collection.to_string(),
        src_id: src.clone(),
        label: label.clone(),
        dst_id: dst.clone(),
        src_surrogate,
        dst_surrogate,
        // Filled by the RLS injection pass that runs over this plan before
        // dispatch; empty here means "not yet resolved", never "no policy".
        rls_write_check: Vec::new(),
    }))
}

pub(crate) fn build_algo(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let algo_name = fields
        .algorithm
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'algorithm'".to_string(),
        })?;

    let algorithm = match algo_name.to_lowercase().as_str() {
        "pagerank" => crate::engine::graph::algo::params::GraphAlgorithm::PageRank,
        "wcc" => crate::engine::graph::algo::params::GraphAlgorithm::Wcc,
        "label_propagation" => crate::engine::graph::algo::params::GraphAlgorithm::LabelPropagation,
        "lcc" => crate::engine::graph::algo::params::GraphAlgorithm::Lcc,
        "sssp" => crate::engine::graph::algo::params::GraphAlgorithm::Sssp,
        "betweenness" => crate::engine::graph::algo::params::GraphAlgorithm::Betweenness,
        "closeness" => crate::engine::graph::algo::params::GraphAlgorithm::Closeness,
        "harmonic" => crate::engine::graph::algo::params::GraphAlgorithm::Harmonic,
        "degree" => crate::engine::graph::algo::params::GraphAlgorithm::Degree,
        "louvain" => crate::engine::graph::algo::params::GraphAlgorithm::Louvain,
        "triangles" => crate::engine::graph::algo::params::GraphAlgorithm::Triangles,
        "diameter" => crate::engine::graph::algo::params::GraphAlgorithm::Diameter,
        "kcore" => crate::engine::graph::algo::params::GraphAlgorithm::KCore,
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("unknown graph algorithm: {other}"),
            });
        }
    };

    let personalization_vector = parse_algo_personalization(fields.algo_params.as_ref())?;

    let params = crate::engine::graph::algo::params::AlgoParams {
        collection: collection.to_string(),
        edge_label: None,
        source_node: fields.start_node.clone(),
        max_iterations: fields.depth.map(|d| d as usize),
        tolerance: None,
        damping: None,
        sample_size: None,
        direction: fields.direction.clone(),
        resolution: None,
        mode: None,
        personalization_vector,
    };

    Ok(PhysicalPlan::Graph(GraphOp::Algo { algorithm, params }))
}

/// Extract the Personalized PageRank seed map from the raw-protocol
/// `algo_params` object (`{"personalization_vector": {"alice": 1.0, …}}`).
///
/// Returns `Ok(None)` when absent or empty. A present-but-malformed value
/// (not an object, or a non-numeric weight) surfaces a structured
/// `BadRequest` rather than being silently dropped. Parses the JSON object
/// directly (no runtime JSON de/serialization functions).
fn parse_algo_personalization(
    algo_params: Option<&serde_json::Value>,
) -> crate::Result<Option<std::collections::HashMap<String, f64>>> {
    let Some(pv) = algo_params.and_then(|p| p.get("personalization_vector")) else {
        return Ok(None);
    };
    if pv.is_null() {
        return Ok(None);
    }
    let obj = pv.as_object().ok_or_else(|| crate::Error::BadRequest {
        detail: "personalization_vector must be a JSON object of node_id → weight".to_string(),
    })?;
    let mut map = std::collections::HashMap::with_capacity(obj.len());
    for (node, weight) in obj {
        let w = weight.as_f64().ok_or_else(|| crate::Error::BadRequest {
            detail: format!("personalization_vector weight for '{node}' must be a number"),
        })?;
        map.insert(node.clone(), w);
    }
    if map.is_empty() {
        return Ok(None);
    }
    Ok(Some(map))
}

pub(crate) fn build_match(fields: &TextFields, _collection: &str) -> crate::Result<PhysicalPlan> {
    let query_str = fields
        .match_query
        .as_ref()
        .or(fields.sql.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'match_query'".to_string(),
        })?;

    // Serialize the MATCH query string as MessagePack for the Data Plane.
    let query = zerompk::to_msgpack_vec(query_str).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("match query serialization: {e}"),
    })?;

    Ok(PhysicalPlan::Graph(GraphOp::Match {
        query,
        frontier_bitmap: None,
        // B1: native MATCH stays single-node; B2 wires cluster orchestration.
        cluster_mode: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn algo_fields(algo_params: Option<serde_json::Value>) -> TextFields {
        TextFields {
            algorithm: Some("pagerank".to_string()),
            algo_params,
            ..Default::default()
        }
    }

    fn params_of(plan: PhysicalPlan) -> crate::engine::graph::algo::params::AlgoParams {
        let PhysicalPlan::Graph(GraphOp::Algo { params, .. }) = plan else {
            panic!("expected GraphOp::Algo");
        };
        params
    }

    #[test]
    fn build_algo_parses_personalization_from_algo_params() {
        let fields = algo_fields(Some(json!({
            "personalization_vector": { "alice": 1.0, "bob": 0.5 }
        })));
        let pv = params_of(build_algo(&fields, "social").unwrap())
            .personalization_vector
            .expect("personalization present");
        assert_eq!(pv.get("alice"), Some(&1.0));
        assert_eq!(pv.get("bob"), Some(&0.5));
    }

    #[test]
    fn build_algo_without_personalization_is_none() {
        assert!(
            params_of(build_algo(&algo_fields(None), "social").unwrap())
                .personalization_vector
                .is_none()
        );
        // An algo_params object that omits the key is also None.
        let fields = algo_fields(Some(json!({ "other": 1 })));
        assert!(
            params_of(build_algo(&fields, "social").unwrap())
                .personalization_vector
                .is_none()
        );
    }

    #[test]
    fn build_algo_rejects_non_numeric_weight() {
        let fields = algo_fields(Some(
            json!({ "personalization_vector": { "alice": "high" } }),
        ));
        assert!(build_algo(&fields, "social").is_err());
    }

    #[test]
    fn build_algo_rejects_non_object_personalization() {
        let fields = algo_fields(Some(json!({ "personalization_vector": [1, 2, 3] })));
        assert!(build_algo(&fields, "social").is_err());
    }
}
