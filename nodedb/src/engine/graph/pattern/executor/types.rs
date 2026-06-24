// SPDX-License-Identifier: BUSL-1.1

//! Data types shared across the MATCH pattern executor.

use std::collections::HashMap;

/// A single result row: variable bindings.
pub type BindingRow = HashMap<String, String>;

/// An expansion source that has zero local adjacency in the queried
/// direction — its out-edges (or in-edges) are homed on another shard.
///
/// Emitted by the executor so the Control Plane can dispatch a
/// continuation query to the owning shard. The frontier is produced
/// on every MATCH execution; on a fully-local CSR it is always empty.
///
/// # Cross-shard contract
///
/// The executor emits an `UnresolvedExpansion` for a source node when
/// **all four** conditions hold:
/// 1. The source variable was **bound** — it was resolved from an existing
///    binding in `input_row` (the multi-hop intermediate case), not
///    free-ranged over all local nodes.  A free-ranging anchor must NOT
///    emit because every shard covers all its own local nodes during its
///    own pass; emitting would duplicate work and flood the frontier with
///    every zero-degree local sink.
/// 2. The node has **zero raw adjacency** in the triple's direction
///    (regardless of edge-label filter).
/// 3. The caller supplied a locality predicate (`is_remote_node`).
/// 4. The predicate returns `true` for the node's name.
///
/// A node that has edges in the direction but none that pass the label
/// filter produces an empty local result and is NOT included in the
/// frontier (that is a legitimate "no match locally," not a
/// missing-shard situation).
///
/// Passing `None` for the predicate (the default for fully-local,
/// single-node deployments) guarantees the frontier is always empty.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct UnresolvedExpansion {
    /// The source binding variable name from the triple (e.g. `"b"`).
    pub binding_var: String,
    /// The resolved source node name with no local edges (e.g. `"bob"`).
    pub node_name: String,
    /// 0-based index of the triple in its chain that could not expand.
    pub triple_idx: usize,
    /// Bindings accumulated up to (but not including) this triple.
    pub partial_row: BindingRow,
}

/// Result of running a MATCH query.
///
/// `truncated` is `true` iff a hard cap inside variable-length expansion
/// fired — the binding rows are incomplete. Data Plane handlers MUST set
/// the `partial` flag on the response envelope when this is set so
/// clients can observe the incomplete result.
///
/// `unresolved_frontier` lists expansion sources whose edges are not
/// present in the local CSR partition. On a fully-local CSR this vec
/// is always empty and existing behaviour is byte-identical to before.
pub struct MatchOutcome {
    pub rows: Vec<BindingRow>,
    pub truncated: bool,
    pub unresolved_frontier: Vec<UnresolvedExpansion>,
}

/// Shared mutable state collected during triple execution: the list of
/// binding rows being built + the across-query truncation flag +
/// the cross-shard unresolved frontier.
///
/// `is_remote_node` is an optional caller-supplied predicate: when
/// `Some(pred)`, `pred(node_name)` returns `true` for nodes that are
/// homed on a remote shard. When `None` every node is treated as local
/// and no frontier entries are ever emitted. The predicate is borrowed
/// for the lifetime `'a` of the execution call to avoid allocation.
pub(super) struct ExecutionState<'a> {
    pub truncated: bool,
    pub frontier: Vec<UnresolvedExpansion>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
}

impl<'a> ExecutionState<'a> {
    pub(super) fn new(is_remote_node: Option<&'a dyn Fn(&str) -> bool>) -> Self {
        Self {
            truncated: false,
            frontier: Vec::new(),
            is_remote_node,
        }
    }
}
