// SPDX-License-Identifier: BUSL-1.1

//! Shared value types for the GRAPH transaction staging overlay: the
//! collection key, the edge identity, the per-collection staged-write state,
//! and the node-label delta shape `SetNodeLabels` / `RemoveNodeLabels` stage.

use std::collections::{HashMap, HashSet};

use crate::types::{DatabaseId, TenantId};

/// Collection overlay key: `(database, tenant, collection)`. Same shape as
/// the surrogate overlay's `CollKey`, re-declared here (not exported from
/// `stage_write::context`, which is module-private) so this type can be
/// shared between the staging handlers and the Neighbors/Hop read-merge.
pub type GraphCollKey = (DatabaseId, TenantId, String);

/// One staged edge identity: `(src_id, label, dst_id)`, exactly the tuple
/// `EdgeStore` / `ShardedCsrIndex` key an edge by.
pub(super) type EdgeKey = (String, String, String);

/// Staged node-label delta: labels added and labels removed in this
/// transaction. A label that appears in both (added then removed, or vice
/// versa within the same statement sequence) is resolved by insertion order:
/// `GraphTxnOverlay::stage_node_labels_set` / `stage_node_labels_remove`
/// each clear the opposite set's membership for the labels they touch, so
/// the two sets stay disjoint and the last write wins.
#[derive(Debug, Default, Clone)]
pub struct NodeLabelDelta {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

/// Staged graph mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub(super) struct GraphCollectionOverlay {
    /// Staged edge add-set: identity -> encoded properties.
    pub(super) pending_edges: HashMap<EdgeKey, Vec<u8>>,
    /// Staged edge delete-set (tombstones).
    pub(super) pending_edge_tombstones: HashSet<EdgeKey>,
    /// Staged node-label deltas, keyed by raw node id.
    pub(super) pending_node_labels: HashMap<String, NodeLabelDelta>,
}

impl GraphCollectionOverlay {
    pub(super) fn memory_size_estimate(&self) -> usize {
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
