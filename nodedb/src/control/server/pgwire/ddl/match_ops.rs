// SPDX-License-Identifier: BUSL-1.1

//! MATCH pattern query handler — parses Cypher-style MATCH syntax,
//! compiles to PhysicalPlan::GraphMatch, and dispatches to Data Plane.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;
use sonic_rs;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::graph_dispatch;
use crate::control::state::SharedState;
use crate::data::executor::response_codec;
use crate::types::TraceId;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::DatabaseId;

use super::super::types::{sqlstate_error, text_field};

/// Returned when a MATCH could not be fully resolved within its expansion
/// budget — either the cross-shard hop rounds or the variable-length paging
/// rounds were exhausted with work still pending, or a single-node
/// variable-length expansion hit its hard cap with no coordinator to drain it.
/// The result set would be INCOMPLETE, so it is surfaced as a fail-closed error
/// (SQLSTATE 54001, `program_limit_exceeded`) rather than silently returning a
/// truncated result the client cannot distinguish from a complete one.
const MATCH_INCOMPLETE_MESSAGE: &str = "MATCH result incomplete: the pattern exceeded the expansion budget; \
     narrow the pattern or its variable-length `*min..max` bound";

/// Handle a MATCH query.
///
/// Parses the Cypher-style MATCH syntax, serializes the MatchQuery AST,
/// constructs PhysicalPlan::GraphMatch, and broadcasts to all Data Plane cores.
pub async fn match_query(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    // Parse the MATCH query.
    let query = crate::engine::graph::pattern::compiler::parse(sql)
        .map_err(|e| sqlstate_error("42601", &format!("MATCH parse error: {e}")))?;

    // Enforce the tenant's max_graph_depth quota.  Reject if any edge in the
    // pattern exceeds the cap.  Unbounded [*] (max_hops == usize::MAX) is
    // rejected when the tenant has any finite cap.
    {
        let tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        let limit = tenants.quota(identity.tenant_id).max_graph_depth;
        if limit > 0 {
            for clause in &query.clauses {
                for chain in &clause.patterns {
                    for triple in &chain.triples {
                        let hops = triple.edge.max_hops;
                        if hops > limit as usize {
                            return Err(sqlstate_error(
                                "42P17",
                                &format!(
                                    "MATCH traversal depth {hops} exceeds tenant quota \
                                     max_graph_depth={limit}"
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    // Collect column names for response schema.
    let column_names: Vec<String> = if query.return_columns.is_empty() {
        // Return all bound node variables.
        query.bound_node_names()
    } else {
        query
            .return_columns
            .iter()
            .map(|c| c.alias.clone().unwrap_or_else(|| c.expr.clone()))
            .collect()
    };

    // Serialize the MatchQuery for SPSC transport.
    let query_bytes = zerompk::to_msgpack_vec(&query)
        .map_err(|e| sqlstate_error("XX000", &format!("serialize match query: {e}")))?;

    let tenant_id = identity.tenant_id;

    // Single-node mode: keep the B1 path byte-identical — broadcast the `Match`
    // plan with `cluster_mode = false` to all local cores. The Data Plane emits
    // no frontier, so the unwrapped rows payload is exactly the prior bare-array
    // gather. No cross-shard orchestration is needed (and there is no routing
    // table to consult).
    if state.cluster_routing.is_none() {
        let plan = crate::bridge::envelope::PhysicalPlan::Graph(GraphOp::Match {
            query: query_bytes,
            frontier_bitmap: None,
            cluster_mode: false,
        });
        return match graph_dispatch::broadcast_match_to_all_cores(
            state,
            tenant_id,
            database_id,
            plan,
            TraceId::ZERO,
        )
        .await
        {
            Ok(outcome) => {
                // Single-node frontier is always empty (cluster_mode=false). A
                // variable-length expansion can still hit its hard cap on a
                // single node (no coordinator to drive resume), so a partial
                // result is surfaced fail-closed rather than silently truncated.
                let _frontier = outcome.frontier;
                if outcome.partial {
                    Err(sqlstate_error("54001", MATCH_INCOMPLETE_MESSAGE))
                } else {
                    match_payload_to_response(&outcome.rows_payload, &column_names)
                }
            }
            Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
        };
    }

    // Cluster mode: scatter-all to local + every remote owner, then drive the
    // continuation round loop across shard boundaries. `scatter_match` returns
    // the deduped rows in the same bare-array shape and a `partial` flag set on
    // truncation / round exhaustion.
    let deadline_ms = crate::control::gateway::dispatcher::default_deadline_ms(state);
    match graph_dispatch::scatter_match(state, tenant_id, database_id, query_bytes, deadline_ms)
        .await
    {
        Ok(outcome) => {
            // A `partial` result means the cross-shard hop rounds or the
            // variable-length resume paging budget were exhausted with work
            // still pending: the result set is INCOMPLETE. Surface it
            // fail-closed so a client never mistakes it for a complete result.
            if outcome.partial {
                Err(sqlstate_error("54001", MATCH_INCOMPLETE_MESSAGE))
            } else {
                match_payload_to_response(&outcome.rows_payload, &column_names)
            }
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Convert MATCH result payload to pgwire multi-row response.
fn match_payload_to_response(
    payload: &crate::bridge::envelope::Payload,
    column_names: &[String],
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(
        column_names
            .iter()
            .map(|name| text_field(name))
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
        .map_err(|e| sqlstate_error("XX000", &format!("invalid match result JSON: {e}")))?;

    let mut pgwire_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for col_name in column_names {
            let val = row.get(col_name).and_then(|v| v.as_str()).unwrap_or("NULL");
            encoder
                .encode_field(&val.to_string())
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        pgwire_rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(pgwire_rows),
    ))])
}

// Tenant-prefix stripping lives in the Data Plane, in
// `engine::graph::pattern::executor::rows_to_msgpack`, so every
// `GraphOp::Match` consumer (pgwire, native, HTTP) receives
// already-unscoped node ids on the wire.
