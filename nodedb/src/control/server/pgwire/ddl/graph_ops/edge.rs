// SPDX-License-Identifier: BUSL-1.1

//! Edge mutation handlers: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL / GRAPH UNLABEL.
//!
//! Each function receives already-parsed typed fields from
//! `nodedb_sql::ddl_ast::NodedbStatement`. Raw-SQL tokenising lives
//! in the AST parser — handlers never touch `&str` parse paths.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::GraphProperties;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::server::{dispatch_utils, wal_dispatch};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;

/// Maximum byte length for an edge label string. Keeps a single `TYPE`
/// clause from bloating the CSR label table and the msgpack wire payload.
const MAX_EDGE_LABEL_BYTES: usize = 256;

/// Validate a user-supplied edge label. Rejects empty, overlong, and
/// labels containing ASCII control characters (0x00..=0x1F, 0x7F).
///
/// Runs at every DSL ingress so the CSR interner never sees degenerate
/// input — a complement to the `u32` widening of the label id space.
fn validate_edge_label(label: &str) -> PgWireResult<()> {
    if label.is_empty() {
        return Err(sqlstate_error("42601", "edge TYPE label must not be empty"));
    }
    if label.len() > MAX_EDGE_LABEL_BYTES {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "edge TYPE label is {} bytes; maximum is {MAX_EDGE_LABEL_BYTES}",
                label.len()
            ),
        ));
    }
    if label.chars().any(|c| c.is_control() || c == '\u{007F}') {
        return Err(sqlstate_error(
            "42601",
            "edge TYPE label must not contain control characters",
        ));
    }
    Ok(())
}

/// `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>' [PROPERTIES '<json>' | { ... }]`
#[allow(clippy::too_many_arguments)]
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    src: String,
    dst: String,
    label: String,
    properties: GraphProperties,
) -> PgWireResult<Vec<Response>> {
    if collection.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH INSERT EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH INSERT EDGE requires FROM and TO",
        ));
    }
    validate_edge_label(&label)?;
    let properties_json = properties_to_json(properties)?;
    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let src_surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, &collection, src.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let dst_surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, &collection, dst.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
        collection,
        src_id: src,
        label,
        dst_id: dst,
        properties: properties_json.into_bytes(),
        src_surrogate,
        dst_surrogate,
    });

    crate::control::server::sync::raft_dispatch::dispatch_sync_response(
        state,
        tenant_id,
        vshard_id,
        plan,
        TraceId::ZERO,
        crate::event::EventSource::User,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Execution(Tag::new("INSERT EDGE"))])
}

/// `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: String,
    src: String,
    dst: String,
    label: String,
) -> PgWireResult<Vec<Response>> {
    if collection.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH DELETE EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH DELETE EDGE requires FROM and TO",
        ));
    }
    validate_edge_label(&label)?;
    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::Graph(GraphOp::EdgeDelete {
        collection,
        src_id: src,
        label,
        dst_id: dst,
    });

    crate::control::server::sync::raft_dispatch::dispatch_sync_response(
        state,
        tenant_id,
        vshard_id,
        plan,
        TraceId::ZERO,
        crate::event::EventSource::User,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Execution(Tag::new("DELETE EDGE"))])
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    node_id: String,
    labels: Vec<String>,
    remove: bool,
) -> PgWireResult<Vec<Response>> {
    if node_id.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH LABEL/UNLABEL requires a quoted node id",
        ));
    }
    if labels.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "missing AS '<label>' [, '<label2>']",
        ));
    }

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(node_id.as_bytes());

    let plan = if remove {
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels })
    } else {
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels })
    };

    wal_dispatch::wal_append_if_write(&state.wal, tenant_id, vshard_id, database_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(_) => {
            let tag = if remove { "UNLABEL" } else { "LABEL" };
            Ok(vec![Response::Execution(Tag::new(tag))])
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Convert a parsed `PROPERTIES` clause to the JSON string stored
/// in `GraphOp::EdgePut`. Object-literal forms go through the
/// existing `nodedb_sql::parser::object_literal::parse_object_literal`
/// so the type coercions (numbers, bools, nested objects) match
/// every other object-literal ingress (INSERT { ... }, UPSERT).
fn properties_to_json(properties: GraphProperties) -> PgWireResult<String> {
    match properties {
        GraphProperties::None => Ok(String::new()),
        GraphProperties::Quoted(s) => Ok(s),
        GraphProperties::Object(obj_str) => {
            match nodedb_sql::parser::object_literal::parse_object_literal(&obj_str) {
                Some(Ok(fields)) => sonic_rs::to_string(&nodedb_types::Value::Object(fields))
                    .map_err(|e| {
                        sqlstate_error("XX000", &format!("PROPERTIES serialize error: {e}"))
                    }),
                Some(Err(msg)) => Err(sqlstate_error(
                    "42601",
                    &format!("PROPERTIES object literal error: {msg}"),
                )),
                None => Ok(String::new()),
            }
        }
    }
}
