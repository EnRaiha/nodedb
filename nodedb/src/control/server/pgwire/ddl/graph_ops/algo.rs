// SPDX-License-Identifier: BUSL-1.1

//! GRAPH ALGO handler and result-schema rendering.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::broadcast;
use crate::control::server::pgwire::types::{sqlstate_error, text_field};
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::DatabaseId;

const MAX_ITERATIONS_CAP: usize = 1_000;
const MAX_SAMPLE_CAP: usize = 1_000_000;

#[allow(clippy::too_many_arguments)]
pub async fn algo(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    algorithm_name: &str,
    collection: String,
    edge_label: Option<String>,
    damping: Option<f64>,
    tolerance: Option<f64>,
    resolution: Option<f64>,
    max_iterations: Option<usize>,
    sample_size: Option<usize>,
    source_node: Option<String>,
    direction: Option<String>,
    mode: Option<String>,
    personalization: Option<String>,
) -> PgWireResult<Vec<Response>> {
    let algorithm = resolve_algorithm(algorithm_name)?;

    let max_iterations = clamp_opt(max_iterations, "ITERATIONS", MAX_ITERATIONS_CAP)?;
    let sample_size = clamp_opt(sample_size, "SAMPLE", MAX_SAMPLE_CAP)?;
    let personalization_vector = parse_personalization(personalization.as_deref())?;

    let params = crate::engine::graph::algo::AlgoParams {
        collection: collection.clone(),
        edge_label,
        damping,
        max_iterations,
        tolerance,
        source_node,
        sample_size,
        direction,
        resolution,
        mode,
        personalization_vector,
    };

    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Graph(GraphOp::Algo { algorithm, params });

    match broadcast::broadcast_to_all_cores(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(resp) => algo_payload_to_query_response(&resp.payload, algorithm),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Resolve a `GRAPH ALGO <name>` keyword to its [`GraphAlgorithm`] variant.
///
/// `COMMUNITY` and `LABEL_PROPAGATION` are accepted aliases that both map to
/// label propagation. Unknown names surface a structured `42601` error rather
/// than a catch-all default.
fn resolve_algorithm(
    algorithm_name: &str,
) -> PgWireResult<crate::engine::graph::algo::GraphAlgorithm> {
    use crate::engine::graph::algo::GraphAlgorithm;
    Ok(match algorithm_name {
        "PAGERANK" => GraphAlgorithm::PageRank,
        "WCC" => GraphAlgorithm::Wcc,
        "COMMUNITY" | "LABEL_PROPAGATION" => GraphAlgorithm::LabelPropagation,
        "LCC" => GraphAlgorithm::Lcc,
        "SSSP" => GraphAlgorithm::Sssp,
        "BETWEENNESS" => GraphAlgorithm::Betweenness,
        "CLOSENESS" => GraphAlgorithm::Closeness,
        "HARMONIC" => GraphAlgorithm::Harmonic,
        "DEGREE" => GraphAlgorithm::Degree,
        "LOUVAIN" => GraphAlgorithm::Louvain,
        "TRIANGLES" => GraphAlgorithm::Triangles,
        "DIAMETER" => GraphAlgorithm::Diameter,
        "KCORE" => GraphAlgorithm::KCore,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown graph algorithm '{other}'"),
            ));
        }
    })
}

/// Parse the `PERSONALIZATION {…}` JSON object literal into a `node_id → weight`
/// seed map for Personalized PageRank. Returns `Ok(None)` when absent; a
/// malformed object surfaces a structured `22023` error rather than being
/// silently dropped.
fn parse_personalization(
    raw: Option<&str>,
) -> PgWireResult<Option<std::collections::HashMap<String, f64>>> {
    let Some(text) = raw else {
        return Ok(None);
    };
    let map: std::collections::HashMap<String, f64> = sonic_rs::from_str(text).map_err(|e| {
        sqlstate_error(
            "22023",
            &format!("invalid PERSONALIZATION object (expected JSON node→weight map): {e}"),
        )
    })?;
    if map.is_empty() {
        return Ok(None);
    }
    Ok(Some(map))
}

fn clamp_opt(value: Option<usize>, field: &'static str, cap: usize) -> PgWireResult<Option<usize>> {
    match value {
        Some(v) if v > cap => Err(sqlstate_error(
            "22023",
            &format!("{field} {v} exceeds maximum allowed value {cap}"),
        )),
        other => Ok(other),
    }
}

fn algo_payload_to_query_response(
    payload: &crate::bridge::envelope::Payload,
    algorithm: crate::engine::graph::algo::GraphAlgorithm,
) -> PgWireResult<Vec<Response>> {
    use crate::engine::graph::algo::params::AlgoColumnType;

    let result_schema = algorithm.result_schema();
    let schema = Arc::new(
        result_schema
            .iter()
            .map(|&(name, _)| text_field(name))
            .collect::<Vec<_>>(),
    );

    if payload.is_empty() {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let json_text = response_codec::decode_payload_to_json(payload);
    let rows: Vec<serde_json::Value> = sonic_rs::from_str(&json_text)
        .map_err(|e| sqlstate_error("XX000", &format!("invalid algorithm result JSON: {e}")))?;

    let mut pgwire_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for &(col_name, col_type) in result_schema {
            let field = row.get(col_name).unwrap_or(&serde_json::Value::Null);
            let val_str = match col_type {
                AlgoColumnType::Text => field.as_str().unwrap_or("").to_string(),
                AlgoColumnType::Float64 => match field.as_f64() {
                    Some(v) => format!("{v}"),
                    None => "Infinity".to_string(),
                },
                AlgoColumnType::Int64 => field.as_i64().map_or("0".into(), |v| v.to_string()),
            };
            encoder
                .encode_field(&val_str)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        pgwire_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(pgwire_rows),
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::algo::GraphAlgorithm;

    #[test]
    fn community_resolves_to_label_propagation() {
        assert!(matches!(
            resolve_algorithm("COMMUNITY").unwrap(),
            GraphAlgorithm::LabelPropagation
        ));
    }

    #[test]
    fn label_propagation_alias_resolves_to_label_propagation() {
        assert!(matches!(
            resolve_algorithm("LABEL_PROPAGATION").unwrap(),
            GraphAlgorithm::LabelPropagation
        ));
    }

    #[test]
    fn unknown_algorithm_is_rejected() {
        assert!(resolve_algorithm("NOPE").is_err());
    }
}
