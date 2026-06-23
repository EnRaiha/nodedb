// SPDX-License-Identifier: BUSL-1.1

//! Top-level scatter orchestration, result feeding, frontier resolution,
//! dedup/encode, and shared utilities used across the scatter sub-modules.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::bridge::envelope::Payload;
use crate::control::gateway::RouteDecision;
use crate::control::server::graph_dispatch::cluster_resolve::resolve_for_vshard;
use crate::control::state::SharedState;
use crate::engine::graph::pattern::executor::{UnresolvedExpansion, rows_to_msgpack};
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_cluster::distributed_graph::{
    DistributedMatchCoordinator, PatternContinuation, ShardMatchResult,
};

use super::round_loop::dispatch_continuations;
use super::round_zero::scatter_round_zero;

/// Result of a cross-shard MATCH scatter: the deduped binding rows as the bare
/// msgpack array shape `match_payload_to_response` expects, plus a `partial`
/// flag set when any shard truncated OR the coordinator exhausted `max_rounds`
/// with continuations still pending (a real partial result — never silently
/// dropped).
pub struct MatchScatterOutcome {
    pub rows_payload: Payload,
    pub partial: bool,
}

/// One shard's round-0 / round-N result tagged with the node that produced it.
///
/// The emitting node id is required for the self-leaf drop: a frontier entry
/// whose owning node equals the node that emitted it is a true local leaf, not
/// a cross-shard ghost.
pub(super) struct TaggedShardResult {
    pub(super) emitting_node: u64,
    pub(super) rows: Vec<HashMap<String, String>>,
    pub(super) frontier: Vec<UnresolvedExpansion>,
    /// `true` if the shard truncated its result (a hard cap fired). Surfaced
    /// up to the scatter outcome's `partial` so a truncated cross-shard MATCH
    /// is never silently presented as complete. Remote dispatch collapses the
    /// per-frame `partial` flag into the payload bytes, so remote truncation is
    /// not recoverable here (a known gap — see module docs); the LOCAL
    /// broadcast partial IS threaded.
    pub(super) truncated: bool,
}

/// Orchestrate a cross-shard MATCH. Caller guarantees cluster mode
/// (`cluster_routing.is_some()`); single-node never enters here.
pub async fn scatter_match(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: Vec<u8>,
    deadline_ms: u64,
) -> crate::Result<MatchScatterOutcome> {
    // The pattern's total triple count bounds the rounds: each round advances
    // the frontier by at least one hop, so a triple-count of rounds suffices to
    // resolve any acyclic pattern. This is a correctness-derived bound, not an
    // arbitrary truncation cap.
    let max_rounds = pattern_triple_count(&query_bytes).max(1) as u32;
    let mut coordinator = DistributedMatchCoordinator::new(max_rounds);

    let mut partial = false;

    // ---- Round 0: scatter the Match plan to local + every remote owner. ----
    let round0 =
        scatter_round_zero(state, tenant_id, database_id, &query_bytes, deadline_ms).await?;
    for tagged in round0 {
        if feed_result(state, &mut coordinator, tagged)? {
            partial = true;
        }
    }

    // ---- Round loop: dispatch pending continuations until none remain. ----
    while coordinator.has_pending() {
        if !coordinator.advance() {
            // Exhausted max_rounds with work still pending: surface as partial
            // rather than silently dropping the remaining continuations.
            if coordinator.has_pending() {
                partial = true;
            }
            break;
        }
        let pending = coordinator.take_all_pending();
        let tagged = dispatch_continuations(
            state,
            tenant_id,
            database_id,
            &query_bytes,
            deadline_ms,
            pending,
        )
        .await?;
        for t in tagged {
            if feed_result(state, &mut coordinator, t)? {
                partial = true;
            }
        }
    }

    // ---- Dedup + encode. ----
    let rows_payload = dedup_and_encode(&coordinator.completed)?;
    Ok(MatchScatterOutcome {
        rows_payload,
        partial,
    })
}

/// Convert a tagged shard result into a `ShardMatchResult` (filtering its
/// frontier to genuine cross-shard continuations) and feed it to the
/// coordinator. Returns `true` if this shard truncated its result, so the
/// caller can set the scatter outcome's `partial` flag.
pub(super) fn feed_result(
    state: &SharedState,
    coordinator: &mut DistributedMatchCoordinator,
    tagged: TaggedShardResult,
) -> crate::Result<bool> {
    let TaggedShardResult {
        emitting_node,
        rows,
        frontier,
        truncated,
    } = tagged;

    let continuations = frontier_to_continuations(state, emitting_node, frontier)?;
    coordinator.add_shard_result(ShardMatchResult {
        // shard_id is informational on the coordinator; tag with the emitting
        // node id (the routing decision already lives in each continuation).
        shard_id: emitting_node as u32,
        completed_rows: rows,
        continuations,
    });
    Ok(truncated)
}

/// Convert a shard's `UnresolvedExpansion` frontier into cross-shard
/// `PatternContinuation`s, applying the self-leaf drop.
///
/// A frontier entry's `node_name` is resolved to its owning node via
/// [`resolve_decision`]. If the owner is the SAME node that emitted the entry,
/// it is a true local leaf (the shard already held its edges and found none) —
/// DROP it. Otherwise emit a continuation targeting the owning vShard.
fn frontier_to_continuations(
    state: &SharedState,
    emitting_node: u64,
    frontier: Vec<UnresolvedExpansion>,
) -> crate::Result<Vec<PatternContinuation>> {
    let mut out = Vec::new();
    for entry in frontier {
        let target_vshard = VShardId::from_key(entry.node_name.as_bytes()).as_u32();
        let decision = resolve_for_vshard(state, target_vshard);
        let owner_node = match decision {
            RouteDecision::Local => state.node_id,
            RouteDecision::Remote { node_id, .. } => node_id,
            RouteDecision::LeaderUnknown { vshard_id } => {
                return Err(crate::Error::NotLeader {
                    vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                    leader_node: 0,
                    leader_addr: String::new(),
                });
            }
            RouteDecision::Broadcast { .. } => {
                return Err(crate::Error::Internal {
                    detail: "match scatter: resolve_decision returned Broadcast for a \
                             single vShard"
                        .into(),
                });
            }
        };
        // Self-leaf drop: the frontier node is owned by the very shard that
        // emitted it — its own pass already had the edges and found none.
        if owner_node == emitting_node {
            continue;
        }
        out.push(PatternContinuation::from_resolved(
            target_vshard,
            emitting_node as u32,
            entry.partial_row,
            entry.triple_idx,
            entry.node_name,
            entry.binding_var,
        ));
    }
    Ok(out)
}

/// Decode a bare msgpack rows array (the shape `rows_to_msgpack` produces and
/// `unwrap_match_envelope` returns) into binding rows. An empty payload is an
/// empty row set.
pub(super) fn decode_rows(payload: &Payload) -> crate::Result<Vec<HashMap<String, String>>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack::<Vec<HashMap<String, String>>>(payload.as_ref()).map_err(|e| {
        crate::Error::Codec {
            detail: format!("match scatter: invalid rows array: {e}"),
        }
    })
}

/// Dedup completed rows by a canonical sorted-(k,v) fingerprint and encode them
/// into the bare msgpack array shape `match_payload_to_response` expects.
///
/// Cross-shard union can legitimately overlap (undirected / edge cases), so we
/// ALWAYS dedup — not only when `RETURN DISTINCT` was requested.
fn dedup_and_encode(rows: &[HashMap<String, String>]) -> crate::Result<Payload> {
    let mut seen: HashSet<Vec<(String, String)>> = HashSet::new();
    let mut deduped: Vec<HashMap<String, String>> = Vec::with_capacity(rows.len());
    for row in rows {
        // BTreeMap gives a deterministic key order for the fingerprint.
        let fingerprint: Vec<(String, String)> = row
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect();
        if seen.insert(fingerprint) {
            deduped.push(row.clone());
        }
    }
    let bytes = rows_to_msgpack(&deduped)?;
    Ok(Payload::from_vec(bytes))
}

/// Count the total pattern triples across every clause/chain in the serialized
/// `MatchQuery`. Used to bound the continuation rounds. A malformed query (or a
/// query with no triples) yields 0 — the caller floors `max_rounds` at 1.
fn pattern_triple_count(query_bytes: &[u8]) -> usize {
    use crate::engine::graph::pattern::ast::MatchQuery;
    let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
        Ok(q) => q,
        Err(_) => return 0,
    };
    query
        .clauses
        .iter()
        .flat_map(|c| c.patterns.iter())
        .map(|chain| chain.triples.len())
        .sum()
}
