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

/// A resume cursor for a variable-length expansion that hit a hard cap.
///
/// When `MATCH (a)-[*min..max]->(b)` truncates inside the executor, this
/// captures the LIVE state needed to continue the BFS on a later round
/// instead of silently dropping the un-expanded frontier:
///
/// - `triple_idx` — the within-chain triple whose expansion was capped.
/// - `source_row` — the bindings present at that triple's source (so the
///   resumed rows re-bind `a`/`b`/the edge var identically to a single pass).
/// - `frontier` — the surviving un-expanded node ids (reached at
///   `depth - 1`, awaiting expansion AT `depth`).
/// - `depth` — the hop depth at which `frontier` is resumed.
///
/// There is deliberately **no `visited` set**: termination relies on the
/// varlen `min..max` depth bound plus downstream coordinator row-dedup.
/// Carrying `visited` across a future wire boundary is explicitly rejected,
/// so the executor never depends on it for correctness — a node re-reached
/// on resume produces a duplicate row that the coordinator collapses, never
/// a skipped or mis-depthed row.
//
// The fields are produced by the executor in this sub-unit (2a) and consumed
// by the cross-plane resume dispatch in the following sub-units (2b–2d), so
// they are write-only within 2a.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct VarLenResume {
    /// 0-based index of the capped triple within its pattern chain.
    pub triple_idx: usize,
    /// Bindings present at the capped expansion's source.
    pub source_row: BindingRow,
    /// Surviving un-expanded frontier node ids.
    pub frontier: Vec<u32>,
    /// Hop depth reached at the cap (the depth `frontier` resumes at).
    pub depth: usize,
}

/// Result of running a MATCH query.
///
/// `truncation` is `Some` iff a hard cap inside variable-length expansion
/// fired — the binding rows are incomplete and the [`VarLenResume`] cursor
/// records where to continue. Data Plane handlers MUST set the `partial`
/// flag on the response envelope when this is set so clients can observe
/// the incomplete result. Use [`MatchOutcome::truncated`] for the bare bool.
///
/// `unresolved_frontier` lists expansion sources whose edges are not
/// present in the local CSR partition. On a fully-local CSR this vec
/// is always empty and existing behaviour is byte-identical to before.
pub struct MatchOutcome {
    pub rows: Vec<BindingRow>,
    pub truncation: Option<VarLenResume>,
    pub unresolved_frontier: Vec<UnresolvedExpansion>,
}

impl MatchOutcome {
    /// `true` iff a variable-length expansion hit a hard cap and the result
    /// set is incomplete. Convenience for callers that only need the flag.
    pub fn truncated(&self) -> bool {
        self.truncation.is_some()
    }
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
    /// Structured resume cursor for the first variable-length expansion that
    /// hit a cap during this execution; `None` until one truncates. Only the
    /// first capped expansion is retained — resume is single-cursor by design
    /// for 2a (multi-cursor fan-out is a later unit).
    pub varlen_resume: Option<VarLenResume>,
    pub frontier: Vec<UnresolvedExpansion>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
}

impl<'a> ExecutionState<'a> {
    pub(super) fn new(is_remote_node: Option<&'a dyn Fn(&str) -> bool>) -> Self {
        Self {
            varlen_resume: None,
            frontier: Vec::new(),
            is_remote_node,
        }
    }

    /// `true` iff a variable-length expansion hit a cap during this execution.
    pub(super) fn truncated(&self) -> bool {
        self.varlen_resume.is_some()
    }

    /// Record the resume cursor from the first capped expansion. Subsequent
    /// truncations are ignored (single-cursor design for 2a).
    pub(super) fn record_truncation(&mut self, resume: VarLenResume) {
        if self.varlen_resume.is_none() {
            self.varlen_resume = Some(resume);
        }
    }
}
