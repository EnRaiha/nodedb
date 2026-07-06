// SPDX-License-Identifier: Apache-2.0

//! In-transaction graph overlay, expressed purely as node/label strings.
//!
//! A traversal running inside a transaction must observe that transaction's
//! own not-yet-durable edge writes and deletes (read-your-own-writes),
//! including through nodes that are reachable only via a staged edge and thus
//! have no durable CSR surrogate. This type carries exactly that staged state
//! for one traversal scope, built from plain strings so the shared crate
//! stays free of any host-specific overlay type.
//!
//! It is consumed by [`crate::csr::CsrIndex::traverse_bfs`] and
//! [`crate::csr::CsrIndex::subgraph`]: staged out/in edges are unioned into
//! the discovered set, and staged tombstones subtract durable edges.

use std::collections::{HashMap, HashSet};

/// Staged, not-yet-durable graph edges and deletes for one traversal scope.
///
/// `out_edges`/`in_edges` mirror each other: [`Self::stage_edge`] inserts into
/// both so out- and in-neighbour lookups are each O(1) on the node string.
#[derive(Debug, Default, Clone)]
pub struct GraphOverlayDelta {
    /// Staged out-edges keyed by source node: `src -> [(label, dst)]`.
    out_edges: HashMap<String, Vec<(String, String)>>,
    /// Staged in-edges keyed by destination node: `dst -> [(label, src)]`.
    in_edges: HashMap<String, Vec<(String, String)>>,
    /// Staged deleted edges as `(src, label, dst)`.
    tombstones: HashSet<(String, String, String)>,
}

impl GraphOverlayDelta {
    /// An empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a staged edge `src -[label]-> dst`, indexing it for both
    /// out-neighbour (`src`) and in-neighbour (`dst`) lookups.
    pub fn stage_edge(&mut self, src: &str, label: &str, dst: &str) {
        self.out_edges
            .entry(src.to_string())
            .or_default()
            .push((label.to_string(), dst.to_string()));
        self.in_edges
            .entry(dst.to_string())
            .or_default()
            .push((label.to_string(), src.to_string()));
    }

    /// Record a staged deletion of `src -[label]-> dst`.
    pub fn stage_tombstone(&mut self, src: &str, label: &str, dst: &str) {
        self.tombstones
            .insert((src.to_string(), label.to_string(), dst.to_string()));
    }

    /// True when there is no staged state at all — the caller then takes the
    /// durable-only fast path.
    pub fn is_empty(&self) -> bool {
        self.out_edges.is_empty() && self.in_edges.is_empty() && self.tombstones.is_empty()
    }

    /// Staged out-neighbours of `src` as `(label, dst)`, honouring
    /// `label_filter` (when `Some`, only edges with that exact label).
    pub fn out_neighbors<'a>(
        &'a self,
        src: &str,
        label_filter: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.out_edges.get(src).into_iter().flat_map(move |edges| {
            edges
                .iter()
                .filter_map(move |(label, dst)| match label_filter {
                    Some(f) if f != label => None,
                    _ => Some((label.as_str(), dst.as_str())),
                })
        })
    }

    /// Staged in-neighbours of `dst` as `(label, src)`, honouring
    /// `label_filter` (when `Some`, only edges with that exact label).
    pub fn in_neighbors<'a>(
        &'a self,
        dst: &str,
        label_filter: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.in_edges.get(dst).into_iter().flat_map(move |edges| {
            edges
                .iter()
                .filter_map(move |(label, src)| match label_filter {
                    Some(f) if f != label => None,
                    _ => Some((label.as_str(), src.as_str())),
                })
        })
    }

    /// Every node name that appears as an endpoint of a staged edge (as a
    /// source in `out_edges` or a destination in `in_edges`).
    ///
    /// A node created purely by an in-transaction edge write has no durable CSR
    /// id, so a pattern engine that enumerates free-ranging MATCH anchors from
    /// the CSR alone would never admit it. This lets such staged-only nodes be
    /// offered as candidate anchors. Names may repeat (a node that is both a
    /// staged source and a staged destination); callers dedup as needed.
    pub fn staged_endpoint_names(&self) -> impl Iterator<Item = &str> {
        self.out_edges
            .keys()
            .chain(self.in_edges.keys())
            .map(String::as_str)
    }

    /// True when `src -[label]-> dst` has been staged-deleted.
    pub fn is_tombstoned(&self, src: &str, label: &str, dst: &str) -> bool {
        self.tombstones
            .contains(&(src.to_string(), label.to_string(), dst.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_by_default() {
        assert!(GraphOverlayDelta::new().is_empty());
    }

    #[test]
    fn staged_edge_visible_both_directions() {
        let mut d = GraphOverlayDelta::new();
        d.stage_edge("a", "knows", "b");
        assert!(!d.is_empty());
        let out: Vec<_> = d.out_neighbors("a", None).collect();
        assert_eq!(out, vec![("knows", "b")]);
        let inn: Vec<_> = d.in_neighbors("b", None).collect();
        assert_eq!(inn, vec![("knows", "a")]);
    }

    #[test]
    fn label_filter_applied() {
        let mut d = GraphOverlayDelta::new();
        d.stage_edge("a", "knows", "b");
        d.stage_edge("a", "likes", "c");
        let out: Vec<_> = d.out_neighbors("a", Some("likes")).collect();
        assert_eq!(out, vec![("likes", "c")]);
    }

    #[test]
    fn tombstone_only_is_not_empty() {
        let mut d = GraphOverlayDelta::new();
        d.stage_tombstone("a", "knows", "b");
        assert!(!d.is_empty());
        assert!(d.is_tombstoned("a", "knows", "b"));
        assert!(!d.is_tombstoned("a", "knows", "z"));
    }
}
