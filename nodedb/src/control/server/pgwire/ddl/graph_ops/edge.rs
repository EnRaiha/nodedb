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
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

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

    // Dual-home routing (F1b-dualhome): an edge is reachable from BOTH endpoints
    // (forward from src, reverse from dst), so a cross-shard edge must be written
    // on the home vShard of src AND dst — otherwise reverse/IN traversal that
    // scatters to `from_key(dst)` never finds it. `vsrc`/`vdst` are those two home
    // vShards. Each endpoint's surrogate is resolved by its OWNING leader
    // (F1b-rpc routed assign) so both homes agree on the same global identity.
    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        &collection,
        src.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        &collection,
        dst.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let edge_put = GraphOp::EdgePut {
        collection,
        src_id: src,
        label,
        dst_id: dst,
        properties: properties_json.into_bytes(),
        src_surrogate,
        dst_surrogate,
    };

    // Calvin cross-shard atomicity is only operational in cluster mode with a
    // wired sequencer. In single-node (no cluster transport) every vShard is
    // local, so the F1a single-home write already lands BOTH the EDGES and
    // REVERSE_EDGES rows on this node — there is nothing to dual-home and Calvin
    // is not available. Gating here keeps single-node edge inserts on the F1a
    // fast path (no regression) and routes only genuine cross-shard cluster edges
    // through Calvin.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();

    if vsrc == vdst || !calvin_available {
        // F1a fast path (unchanged): both endpoints share one home vShard (or we
        // are single-node), so a single-home Raft write to `vsrc` covers both
        // forward and reverse traversal — EDGES + REVERSE_EDGES land together.
        let plan = PhysicalPlan::Graph(edge_put);
        crate::control::server::sync::raft_dispatch::dispatch_sync_response(
            state,
            tenant_id,
            vsrc,
            plan,
            TraceId::ZERO,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    } else {
        // Cross-shard edge: dual-home it ATOMICALLY via Calvin. `build_static_tx_class`
        // enumerates {vsrc, vdst} as the participating vShards (the dh-1 substrate),
        // each running the SAME EdgePut with identical pre-resolved surrogates →
        // EDGES on `vsrc`, REVERSE_EDGES on `vdst`, committed atomically. The
        // submit-and-await is routed to the SEQUENCER-GROUP leader (Cv1) so the
        // transaction is actually sequenced and acked from any coordinator.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(edge_put),
            post_set_op: PostSetOp::None,
        };
        let tx_class = build_static_tx_class(&[task], tenant_id)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    Ok(vec![Response::Execution(Tag::new("INSERT EDGE"))])
}

/// `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
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

    // Dual-home routing (F1b-dualhome): a cross-shard edge is stored forward on
    // `from_key(src)` (EDGES + CSR) and reverse on `from_key(dst)` (REVERSE_EDGES),
    // so a delete must tombstone BOTH homes — otherwise reverse/IN traversal that
    // scatters to `from_key(dst)` keeps finding the deleted edge. `vsrc`/`vdst`
    // are those two home vShards. Surrogates are resolved via the same routed
    // get-or-assign as insert (existing node surrogates are returned), giving
    // Calvin its participant shards and the lock identity that serializes against
    // a concurrent EdgePut of the same edge.
    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        &collection,
        src.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        &collection,
        dst.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let edge_delete = GraphOp::EdgeDelete {
        collection,
        src_id: src,
        label,
        dst_id: dst,
        src_surrogate,
        dst_surrogate,
    };

    // Calvin cross-shard atomicity is only operational in cluster mode with a
    // wired sequencer. In single-node every vShard is local, so the F1a
    // single-home delete already tombstones BOTH the EDGES and REVERSE_EDGES
    // rows on this node — nothing to dual-home and Calvin is not available.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();

    if vsrc == vdst || !calvin_available {
        // F1a fast path (unchanged): both endpoints share one home vShard (or we
        // are single-node), so a single-home write to `vsrc` tombstones both the
        // forward and reverse rows together.
        let plan = PhysicalPlan::Graph(edge_delete);
        crate::control::server::sync::raft_dispatch::dispatch_sync_response(
            state,
            tenant_id,
            vsrc,
            plan,
            TraceId::ZERO,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    } else {
        // Cross-shard edge: dual-home the delete ATOMICALLY via Calvin, mirroring
        // the insert path. `build_static_tx_class` enumerates {vsrc, vdst} as the
        // participating vShards, each running the SAME EdgeDelete with identical
        // surrogates → forward tombstone on `vsrc`, REVERSE_EDGES tombstone on
        // `vdst`, committed atomically and conflict-serialized against a
        // concurrent EdgePut of the same edge.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(edge_delete),
            post_set_op: PostSetOp::None,
        };
        let tx_class = build_static_tx_class(&[task], tenant_id)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    Ok(vec![Response::Execution(Tag::new("DELETE EDGE"))])
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
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

    // A node label is single-keyed on `node_id`, so it is SINGLE-HOME: route the
    // write to the node's home vShard `from_key(node_id)` and replicate via Raft,
    // exactly like the edge F1a single-home fast path. `dispatch_sync_response`
    // provides WAL durability + Raft replication internally, so no separate
    // `wal_append_if_write` is needed (and adding one would double-append the WAL
    // record). Calvin is not involved — there is only one home vShard.
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

    let tag = if remove { "UNLABEL" } else { "LABEL" };
    Ok(vec![Response::Execution(Tag::new(tag))])
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
