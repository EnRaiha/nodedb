// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay for GRAPH writes.
//!
//! Graph is the first engine whose unit-of-mutation is NOT a single
//! surrogate-addressed row: an edge's identity is the string tuple
//! `(src_id, label, dst_id)` (the same identity the durable `EdgeStore` /
//! `ShardedCsrIndex` key by), and a node-label mutation touches a bitset
//! keyed by a raw node id, not a surrogate. Neither fits [`super::TxnOverlay`]
//! (which is keyed by `u32` surrogate), so this is a parallel, independent
//! overlay type held alongside it on `CoreLoop` (`graph_txn_overlays`).
//!
//! Scope: this overlay only serves read-your-own-writes for Neighbors / Hop
//! (single-hop reads). COMMIT durability is unchanged -- the buffered
//! `GraphOp` plan is still replayed through the real `execute_edge_put` /
//! `execute_edge_delete` / ... handlers inside the COMMIT `TransactionBatch`.
//! This overlay is in-memory only and is dropped at commit or rollback, same
//! lifecycle as [`super::TxnOverlay`].

use std::collections::{HashMap, HashSet};

use crate::types::{DatabaseId, TenantId};

/// Collection overlay key: `(database, tenant, collection)`. Same shape as
/// the surrogate overlay's `CollKey`, re-declared here (not exported from
/// `stage_write::context`, which is module-private) so this type can be
/// shared between the staging handlers and the Neighbors/Hop read-merge.
pub type GraphCollKey = (DatabaseId, TenantId, String);

/// One staged edge identity: `(src_id, label, dst_id)`, exactly the tuple
/// `EdgeStore` / `ShardedCsrIndex` key an edge by.
type EdgeKey = (String, String, String);

/// Staged node-label delta: labels added and labels removed in this
/// transaction. A label that appears in both (added then removed, or vice
/// versa within the same statement sequence) is resolved by insertion order:
/// [`GraphTxnOverlay::stage_node_labels_set`] / `stage_node_labels_remove`
/// each clear the opposite set's membership for the labels they touch, so
/// the two sets stay disjoint and the last write wins.
#[derive(Debug, Default, Clone)]
pub struct NodeLabelDelta {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

/// Staged graph mutations for a single collection within one transaction.
#[derive(Debug, Default)]
struct GraphCollectionOverlay {
    /// Staged edge add-set: identity -> encoded properties.
    pending_edges: HashMap<EdgeKey, Vec<u8>>,
    /// Staged edge delete-set (tombstones).
    pending_edge_tombstones: HashSet<EdgeKey>,
    /// Staged node-label deltas, keyed by raw node id.
    pending_node_labels: HashMap<String, NodeLabelDelta>,
}

impl GraphCollectionOverlay {
    fn memory_size_estimate(&self) -> usize {
        let edges: usize = self
            .pending_edges
            .iter()
            .map(|((s, l, d), props)| s.len() + l.len() + d.len() + props.len())
            .sum();
        let tombstones: usize = self
            .pending_edge_tombstones
            .iter()
            .map(|(s, l, d)| s.len() + l.len() + d.len())
            .sum();
        let labels: usize = self
            .pending_node_labels
            .iter()
            .map(|(node, delta)| {
                node.len()
                    + delta.added.iter().map(String::len).sum::<usize>()
                    + delta.removed.iter().map(String::len).sum::<usize>()
            })
            .sum();
        edges + tombstones + labels
    }
}

/// Per-transaction GRAPH staging overlay: holds not-yet-durable edge/label
/// writes for every collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug, Default)]
pub struct GraphTxnOverlay {
    collections: HashMap<GraphCollKey, GraphCollectionOverlay>,
}

impl GraphTxnOverlay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage an edge put: adds to the pending add-set and clears any
    /// pending tombstone for the same identity (last-writer-wins within the
    /// transaction).
    pub fn stage_edge_put(
        &mut self,
        coll_key: GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
        properties: Vec<u8>,
    ) {
        let overlay = self.collections.entry(coll_key).or_default();
        let key = (src.to_string(), label.to_string(), dst.to_string());
        overlay.pending_edge_tombstones.remove(&key);
        overlay.pending_edges.insert(key, properties);
    }

    /// Stage an edge delete: adds a tombstone and clears any pending put for
    /// the same identity.
    pub fn stage_edge_delete(&mut self, coll_key: GraphCollKey, src: &str, label: &str, dst: &str) {
        let overlay = self.collections.entry(coll_key).or_default();
        let key = (src.to_string(), label.to_string(), dst.to_string());
        overlay.pending_edges.remove(&key);
        overlay.pending_edge_tombstones.insert(key);
    }

    /// Stage a node-label SET: records the labels as added, clearing them
    /// from any pending "removed" set for the same node.
    pub fn stage_node_labels_set(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        labels: &[String],
    ) {
        let overlay = self.collections.entry(coll_key).or_default();
        let delta = overlay
            .pending_node_labels
            .entry(node_id.to_string())
            .or_default();
        for label in labels {
            delta.removed.remove(label);
            delta.added.insert(label.clone());
        }
    }

    /// Stage a node-label REMOVE: records the labels as removed, clearing
    /// them from any pending "added" set for the same node.
    pub fn stage_node_labels_remove(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        labels: &[String],
    ) {
        let overlay = self.collections.entry(coll_key).or_default();
        let delta = overlay
            .pending_node_labels
            .entry(node_id.to_string())
            .or_default();
        for label in labels {
            delta.added.remove(label);
            delta.removed.insert(label.clone());
        }
    }

    /// True if `(src, label, dst)` has been staged-deleted in this
    /// transaction.
    pub fn is_edge_tombstoned(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> bool {
        self.collections.get(coll_key).is_some_and(|overlay| {
            overlay.pending_edge_tombstones.contains(&(
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            ))
        })
    }

    /// Staged out-edges from `src_id`: `(label, dst, properties)`, excluding
    /// anything tombstoned (staging never leaves a key in both sets, so no
    /// extra filter is needed here).
    pub fn edges_for_src<'a>(
        &'a self,
        coll_key: &GraphCollKey,
        src_id: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a [u8])> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(move |overlay| {
                overlay
                    .pending_edges
                    .iter()
                    .filter_map(move |((s, l, d), props)| {
                        (s == src_id).then_some((l.as_str(), d.as_str(), props.as_slice()))
                    })
            })
    }

    /// Staged in-edges into `dst_id`: `(label, src, properties)`.
    pub fn edges_for_dst<'a>(
        &'a self,
        coll_key: &GraphCollKey,
        dst_id: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a [u8])> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(move |overlay| {
                overlay
                    .pending_edges
                    .iter()
                    .filter_map(move |((s, l, d), props)| {
                        (d == dst_id).then_some((l.as_str(), s.as_str(), props.as_slice()))
                    })
            })
    }

    /// The staged node-label delta for `node_id`, if any.
    pub fn labels_delta(&self, coll_key: &GraphCollKey, node_id: &str) -> Option<&NodeLabelDelta> {
        self.collections
            .get(coll_key)?
            .pending_node_labels
            .get(node_id)
    }

    /// Staged out-edges from `src_id` across every collection this
    /// transaction has touched for `(database_id, tenant)`: `(label, dst,
    /// properties)`. Neighbors/Hop read the CSR partition tenant-wide (it
    /// carries no `collection` field on the plan), so the read-merge cannot
    /// scope to one collection the way the write side does.
    pub fn edges_for_src_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src_id: &str,
    ) -> Vec<(String, String, Vec<u8>)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .iter()
                    .filter(move |((s, _, _), _)| s == src_id)
                    .map(|((_, l, d), props)| (l.clone(), d.clone(), props.clone()))
            })
            .collect()
    }

    /// Staged in-edges into `dst_id` across every collection for
    /// `(database_id, tenant)`: `(label, src, properties)`.
    pub fn edges_for_dst_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        dst_id: &str,
    ) -> Vec<(String, String, Vec<u8>)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .iter()
                    .filter(move |((_, _, d), _)| d == dst_id)
                    .map(|((s, l, _), props)| (l.clone(), s.clone(), props.clone()))
            })
            .collect()
    }

    /// True if `(src, label, dst)` was tombstoned in ANY collection this
    /// transaction has touched for `(database_id, tenant)` -- the tenant-wide
    /// counterpart of `is_edge_tombstoned`.
    pub fn is_edge_tombstoned_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src: &str,
        label: &str,
        dst: &str,
    ) -> bool {
        let key = (src.to_string(), label.to_string(), dst.to_string());
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .any(|(_, overlay)| overlay.pending_edge_tombstones.contains(&key))
    }

    /// The staged node-label delta for `node_id`, searching every collection
    /// for `(database_id, tenant)` -- `SetNodeLabels` / `RemoveNodeLabels`
    /// stage under a fixed sentinel collection key (see
    /// `GRAPH_LABEL_COLL_KEY` in `stage_write::stage_graph`), so callers that
    /// don't know that constant can still find the delta.
    pub fn labels_delta_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        node_id: &str,
    ) -> Option<NodeLabelDelta> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .find_map(|(_, overlay)| overlay.pending_node_labels.get(node_id).cloned())
    }

    /// Every staged edge put `(src, label, dst)` across every collection this
    /// transaction has touched for `(database_id, tenant)`. Feeds the
    /// multi-hop / subgraph read-your-own-writes overlay translation.
    pub fn all_staged_edges(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> Vec<(String, String, String)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .keys()
                    .map(|(s, l, d)| (s.clone(), l.clone(), d.clone()))
            })
            .collect()
    }

    /// Every staged edge tombstone `(src, label, dst)` across every collection
    /// this transaction has touched for `(database_id, tenant)`.
    pub fn all_tombstones(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> Vec<(String, String, String)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edge_tombstones
                    .iter()
                    .map(|(s, l, d)| (s.clone(), l.clone(), d.clone()))
            })
            .collect()
    }

    /// Sum of staged edge/tombstone/label-delta byte footprint across every
    /// collection this transaction has touched.
    pub fn memory_size_estimate(&self) -> usize {
        self.collections
            .values()
            .map(GraphCollectionOverlay::memory_size_estimate)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn stage_edge_put_then_visible_for_src() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1, 2]);
        let out: Vec<_> = overlay.edges_for_src(&key("g"), "a").collect();
        assert_eq!(out, vec![("knows", "b", &[1u8, 2u8][..])]);
        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
    }

    #[test]
    fn stage_edge_delete_tombstones_and_clears_put() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1]);
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");
        assert!(overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 0);
    }

    #[test]
    fn stage_put_after_delete_clears_tombstone() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![9]);
        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 1);
    }

    #[test]
    fn edges_for_dst_returns_in_edges() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![]);
        let out: Vec<_> = overlay.edges_for_dst(&key("g"), "b").collect();
        assert_eq!(out, vec![("knows", "a", &[][..])]);
    }

    #[test]
    fn node_label_set_then_remove_resolves_last_writer() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(key("g"), "n1", &["Person".to_string()]);
        overlay.stage_node_labels_remove(key("g"), "n1", &["Person".to_string()]);
        let delta = overlay.labels_delta(&key("g"), "n1").unwrap();
        assert!(delta.added.is_empty());
        assert!(delta.removed.contains("Person"));
    }

    #[test]
    fn memory_size_estimate_counts_bytes() {
        let mut overlay = GraphTxnOverlay::new();
        assert_eq!(overlay.memory_size_estimate(), 0);
        overlay.stage_edge_put(key("g"), "a", "l", "b", vec![1, 2, 3]);
        assert!(overlay.memory_size_estimate() > 0);
    }
}
