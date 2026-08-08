// SPDX-License-Identifier: BUSL-1.1

//! Edge mutation handlers: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL / GRAPH UNLABEL.
//!
//! Each function receives already-parsed typed fields from
//! `nodedb_sql::ddl_ast::NodedbStatement`. Raw-SQL tokenising lives
//! in the AST parser — handlers never touch `&str` parse paths.

use nodedb_sql::ddl_ast::GraphProperties;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::{DdlError, DdlResult};
use super::edge_parse::{properties_to_json, validate_edge_label};
use super::support::{data_plane_verdict, ddl_err};

/// `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>' [PROPERTIES '<json>' | { ... }]`
///
/// The edge identity (`collection`/`src`/`dst`/`label`) is bundled in [`EdgeRef`]
/// so this stays within the argument budget without an `#[allow]`, matching
/// [`delete_edge`].
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    edge: EdgeRef,
    properties: GraphProperties,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let EdgeRef {
        collection,
        src,
        dst,
        label,
    } = edge;
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH INSERT EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(ddl_err("42601", "GRAPH INSERT EDGE requires FROM and TO"));
    }
    validate_edge_label(&label)?;
    let properties_json = properties_to_json(properties)?;
    let tenant_id = identity.tenant_id;

    // Flag the collection edge-bearing so a later predicate DELETE on it routes
    // through OLLP (which derives the matching `EdgeDelete`) instead of the
    // single-shard fast path. Idempotent; skips the Raft write once already set.
    crate::control::planner::implicit_edges::mark_collection_edge_bearing(
        state,
        database_id,
        tenant_id,
        &collection,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;
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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // The write policy decides the edge's `PROPERTIES` image before anything is
    // staged or dispatched: this handler builds its plan by hand and dispatches
    // it as trusted internal work, so nothing downstream will resolve a policy
    // for it.
    let edge_put = super::edge_rls::resolve_edge_write_rls(
        state,
        identity,
        database_id,
        GraphOp::EdgePut {
            collection,
            src_id: src,
            label,
            dst_id: dst,
            properties: properties_json.into_bytes(),
            src_surrogate,
            dst_surrogate,
        },
    )?;

    // Calvin cross-shard atomicity is only operational in cluster mode with a
    // wired sequencer. In single-node (no cluster transport) every vShard is
    // local, so the F1a single-home write already lands BOTH the EDGES and
    // REVERSE_EDGES rows on this node — there is nothing to dual-home and Calvin
    // is not available. Gating here keeps single-node edge inserts on the F1a
    // fast path (no regression) and routes only genuine cross-shard cluster edges
    // through Calvin.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();
    let single_home = vsrc == vdst || !calvin_available;

    // Inside an explicit transaction block an edge insert stages into the
    // per-transaction `GraphTxnOverlay` through the neutral gate instead of
    // applying durably now: an in-transaction `MATCH` / `GRAPH NEIGHBORS` then
    // observes the edge as present (read-your-own-writes), COMMIT replays the
    // buffered `EdgePut`, and ROLLBACK discards the overlay. A cross-shard
    // (dual-home) edge stages into BOTH endpoint overlays via
    // `stage_edge_dual_home` so RYOW works from either endpoint; a single-home
    // edge stages once. This is the write-side complement to the `delete_edge`
    // staging below. Autocommit is untouched.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        super::edge_stage::stage_edge_dual_home(
            state,
            tenant_id,
            database_id,
            EdgeHomes {
                vsrc,
                vdst,
                single_home,
            },
            edge_put,
            txn_ctx,
        )
        .await?;
        return Ok(vec![DdlResult::Status {
            command: "INSERT EDGE".to_string(),
            rows_affected: None,
        }]);
    }

    if single_home {
        // F1a fast path (unchanged): both endpoints share one home vShard (or we
        // are single-node), so a single-home Raft write to `vsrc` covers both
        // forward and reverse traversal — EDGES + REVERSE_EDGES land together.
        let plan = PhysicalPlan::Graph(edge_put);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                TraceId::ZERO,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        data_plane_verdict(&response)?;
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
            txn_id: None,
        };
        let tx_class = build_static_tx_class(&[task], tenant_id, &[])
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let response = submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        if let Some(response) = response {
            data_plane_verdict(&response)?;
        }
    }

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGE".to_string(),
        rows_affected: None,
    }])
}

/// A parsed edge identity: the collection, endpoints, and label a
/// `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` statement addresses. Bundled so
/// [`insert_edge`] and [`delete_edge`] each stay within the argument budget
/// without an `#[allow]`.
pub struct EdgeRef {
    pub collection: String,
    pub src: String,
    pub dst: String,
    pub label: String,
}

/// The home vShard(s) an edge resolves to. An edge is reachable from BOTH
/// endpoints (forward from `src`, reverse from `dst`), so a cross-shard edge
/// (`!single_home`) has two distinct homes: `vsrc` holds the forward row and
/// `vdst` holds the reverse row. `single_home` is true when both endpoints share
/// one vShard, or when Calvin is unavailable (single-node) so one write covers
/// both. Bundled so [`stage_edge_dual_home`](super::edge_stage::stage_edge_dual_home)
/// stays within the argument budget.
pub struct EdgeHomes {
    pub vsrc: VShardId,
    pub vdst: VShardId,
    pub single_home: bool,
}

/// `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    edge: EdgeRef,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let EdgeRef {
        collection,
        src,
        dst,
        label,
    } = edge;
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH DELETE EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(ddl_err("42601", "GRAPH DELETE EDGE requires FROM and TO"));
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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;
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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // A delete carries no image, so the policy is compiled into the plan's
    // write-gate slot here and decided in the Data Plane against the edge's
    // stored properties — this handler dispatches trusted internal work that
    // nothing downstream resolves a policy for.
    let edge_delete = super::edge_rls::resolve_edge_write_rls(
        state,
        identity,
        database_id,
        GraphOp::EdgeDelete {
            collection,
            src_id: src,
            label,
            dst_id: dst,
            src_surrogate,
            dst_surrogate,
            rls_write_check: Vec::new(),
        },
    )?;

    // Calvin cross-shard atomicity is only operational in cluster mode with a
    // wired sequencer. In single-node every vShard is local, so the F1a
    // single-home delete already tombstones BOTH the EDGES and REVERSE_EDGES
    // rows on this node — nothing to dual-home and Calvin is not available.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();
    let single_home = vsrc == vdst || !calvin_available;

    // Inside an explicit transaction block an edge delete stages into the
    // per-transaction `GraphTxnOverlay` through the neutral gate instead of
    // applying durably now: an in-transaction `MATCH` / `GRAPH NEIGHBORS` then
    // observes the edge as removed (read-your-own-writes), COMMIT replays the
    // buffered `EdgeDelete`, and ROLLBACK discards the overlay. Autocommit is
    // untouched.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        super::edge_stage::stage_edge_dual_home(
            state,
            tenant_id,
            database_id,
            EdgeHomes {
                vsrc,
                vdst,
                single_home,
            },
            edge_delete,
            txn_ctx,
        )
        .await?;
        return Ok(vec![DdlResult::Status {
            command: "DELETE EDGE".to_string(),
            rows_affected: None,
        }]);
    }

    if single_home {
        // F1a fast path (unchanged): both endpoints share one home vShard (or we
        // are single-node), so a single-home write to `vsrc` tombstones both the
        // forward and reverse rows together.
        let plan = PhysicalPlan::Graph(edge_delete);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                TraceId::ZERO,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        data_plane_verdict(&response)?;
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
            txn_id: None,
        };
        let tx_class = build_static_tx_class(&[task], tenant_id, &[])
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let response = submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        if let Some(response) = response {
            data_plane_verdict(&response)?;
        }
    }

    Ok(vec![DdlResult::Status {
        command: "DELETE EDGE".to_string(),
        rows_affected: None,
    }])
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    node_id: String,
    labels: Vec<String>,
    remove: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    if node_id.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH LABEL/UNLABEL requires a quoted node id",
        ));
    }
    if labels.is_empty() {
        return Err(ddl_err("42601", "missing AS '<label>' [, '<label2>']"));
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
    // exactly like the edge F1a single-home fast path. Calvin is not involved —
    // there is only one home vShard.
    //
    // The node-label bitset has no redb-backed durability of its own (unlike
    // edges, which survive via redb's synchronous commit at apply time and are
    // rebuilt from there at startup) — a WAL record is its only durable
    // backing. `dispatch_sync_response`'s single-node fallback dispatches
    // straight to the Data Plane with no WAL append of its own, so this write
    // is appended to the local WAL here, unconditionally and before dispatch —
    // mirroring the spatial/FTS sync handlers' pattern of always appending
    // locally even when a Raft proposer is wired: Raft replication and this
    // node's own crash-recovery replay are independent concerns, and the local
    // WAL is what the graph node-label replay pass reads on restart.
    crate::control::server::wal_dispatch::wal_append_if_write(
        &state.wal,
        tenant_id,
        vshard_id,
        DatabaseId::DEFAULT,
        &plan,
    )
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    let response =
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
            state,
            tenant_id,
            DatabaseId::DEFAULT,
            vshard_id,
            plan,
            TraceId::ZERO,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    data_plane_verdict(&response)?;

    let tag = if remove { "UNLABEL" } else { "LABEL" };
    Ok(vec![DdlResult::Status {
        command: tag.to_string(),
        rows_affected: None,
    }])
}
