//! Borrowed execution context shared by every MATCH entry point.

use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::graph::pattern::executor::expansion::VarLenCaps;
use crate::engine::graph::pattern::executor::predicates::PropertyLookup;

/// Borrowed execution context shared by every MATCH entry point: the CSR
/// index, edge store, cross-shard frontier/remote-node hooks, variable-length
/// caps, property lookup, and the in-transaction staged-edge overlay.
///
/// Bundles the parameters that travel together on every `execute*` call so
/// each entry point stays within clippy's argument budget.
#[derive(Clone, Copy)]
pub struct MatchExecCtx<'a> {
    pub csr: &'a CsrIndex,
    pub edge_store: &'a EdgeStore,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
    pub varlen_caps: VarLenCaps,
    pub props: &'a PropertyLookup<'a>,
    pub overlay: Option<&'a GraphOverlayDelta>,
}
