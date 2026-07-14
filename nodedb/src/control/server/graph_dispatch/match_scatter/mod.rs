// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard MATCH scatter-all orchestration (Control Plane).
//!
//! Graph edges are Raft-homed on `VShardId::from_key(src)` and each Data-Plane
//! core's CSR is PARTITIONED — it holds only its owned nodes' out-edges. A
//! MATCH pattern coordinated from any node therefore cannot complete a chain
//! that crosses a shard boundary on its local cores alone: the intermediate
//! node's edges live on the owning shard. This module drives the end-to-end
//! cross-shard MATCH:
//!
//! 1. **Round-0 scatter.** Run the `Match` plan (with `cluster_mode = true`)
//!    LOCALLY via [`broadcast_match_to_all_cores`] AND on every distinct REMOTE
//!    owner node via [`dispatch_route`]. Each shard returns a `{rows, frontier}`
//!    envelope: completed local rows plus an `UnresolvedExpansion` frontier of
//!    bound zero-degree sources whose edges are homed elsewhere.
//! 2. **Frontier → continuation.** For each frontier entry, resolve the owning
//!    vShard of its `node_name` via the SAME routing primitives the BFS hop
//!    uses ([`resolve_decision`] over live Raft leadership). A frontier entry
//!    whose resolved owner is the SAME node that emitted it is a true local
//!    leaf (its own shard already had the edges and found none) and is DROPPED;
//!    everything else becomes a [`PatternContinuation`] targeting the owner.
//! 3. **Round loop.** While the coordinator has pending continuations and has
//!    not exhausted `max_rounds` (= the pattern's triple count, a
//!    correctness-derived bound — each round advances >= 1 hop), dispatch each
//!    pending continuation as a `MatchContinuation` plan to its target shard
//!    (local broadcast or remote dispatch), unwrap the envelope, and feed the
//!    result (completed rows + its OWN deeper frontier) back into the
//!    coordinator.
//! 4. **Dedup + encode.** Union across shards can overlap (undirected / edge
//!    cases), so completed rows are ALWAYS deduped by a canonical sorted-(k,v)
//!    fingerprint before re-encoding into the bare msgpack array shape
//!    `match_payload_to_response` expects.
//!
//! Single-node deployments NEVER reach this module — `match_ops` keeps the
//! direct `broadcast_match_to_all_cores` path byte-identical when
//! `cluster_routing.is_none()`.

mod coord;
mod resume_queue;
mod round_loop;
mod round_zero;

pub use coord::{MatchScatterOutcome, scatter_match};
