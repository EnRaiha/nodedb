// SPDX-License-Identifier: BUSL-1.1

//! Node-label staging and read-your-own-writes accessors for
//! [`GraphTxnOverlay`].

use crate::types::{DatabaseId, TenantId};

use super::txn_overlay::GraphTxnOverlay;
use super::types::{GraphCollKey, NodeLabelDelta};

impl GraphTxnOverlay {
    /// Stage a node-label SET: records the labels as added, clearing them
    /// from any pending "removed" set for the same node.
    pub fn stage_node_labels_set(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        labels: &[String],
    ) {
        self.record_labels_undo(&coll_key, node_id);
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
        self.record_labels_undo(&coll_key, node_id);
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

    /// The staged node-label delta for `node_id`, if any.
    pub fn labels_delta(&self, coll_key: &GraphCollKey, node_id: &str) -> Option<&NodeLabelDelta> {
        self.collections
            .get(coll_key)?
            .pending_node_labels
            .get(node_id)
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

    /// Every staged node-label delta in `coll_key`: `(node_id, delta)`. The
    /// per-collection counterpart of `labels_delta_any_collection`, feeding
    /// transaction-resolve serialization (`resolve/graph.rs`), which needs
    /// every touched node's delta for exactly the label sentinel collection
    /// rather than a single node's lookup.
    pub fn staged_node_label_deltas_for_collection<'a>(
        &'a self,
        coll_key: &GraphCollKey,
    ) -> impl Iterator<Item = (&'a str, &'a NodeLabelDelta)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay
                    .pending_node_labels
                    .iter()
                    .map(|(node_id, delta)| (node_id.as_str(), delta))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
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
}
