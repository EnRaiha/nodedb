// SPDX-License-Identifier: BUSL-1.1

//! The scatter side of a hop: what is being sent where.
//!
//! Owns the accumulation of cross-shard destinations into one vectorized
//! envelope per hop level, and the local/remote partition that decides which
//! node ids belong in it at all. Kept apart from the fan-out policy that reads
//! the envelope and from the dispatch that consumes the batches, because this
//! is pure grouping: it holds no routing decision, performs no I/O, and is the
//! only place that knows the envelope's internal shape.

use std::collections::HashMap;

use crate::types::VShardId;

/// A batch of node IDs targeted at a specific shard.
///
/// Produced by the scatter phase when graph traversal discovers nodes
/// that live on a different shard than the current core.
#[derive(Debug, Clone)]
pub struct ScatterBatch {
    /// Target shard for this batch of node IDs.
    pub target_shard: VShardId,
    /// Node IDs that need to be explored on the target shard.
    pub node_ids: Vec<String>,
}

/// Vectorized scatter envelope for one hop level.
///
/// Groups all cross-shard destinations by target shard, preventing
/// scatter amplification.
#[derive(Debug, Clone, Default)]
pub struct ScatterEnvelope {
    /// Batches grouped by target shard.
    batches: HashMap<VShardId, Vec<String>>,
}

impl ScatterEnvelope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node ID destined for a specific shard.
    pub fn add(&mut self, shard: VShardId, node_id: String) {
        self.batches.entry(shard).or_default().push(node_id);
    }

    /// Number of distinct shards in this envelope.
    pub fn shard_count(&self) -> usize {
        self.batches.len()
    }

    /// Consume into scatter batches.
    pub fn into_batches(self) -> Vec<ScatterBatch> {
        self.batches
            .into_iter()
            .map(|(shard, node_ids)| ScatterBatch {
                target_shard: shard,
                node_ids,
            })
            .collect()
    }

    /// Total number of node IDs across all shards.
    pub fn total_nodes(&self) -> usize {
        self.batches.values().map(|v| v.len()).sum()
    }

    /// Check if the envelope is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Partition a set of node IDs into local nodes (served by this node) and
/// a `ScatterEnvelope` grouping remote nodes by their target shard.
///
/// "Local" means the shard's leader is `local_node_id`. Any node whose
/// `VShardId::from_key` maps to a shard led by a different node is remote.
///
/// When `cluster_routing` is `None` (single-node mode), all nodes are
/// considered local and the envelope is empty.
pub fn partition_local_remote(
    node_ids: &[String],
    local_node_id: u64,
    routing: &nodedb_cluster::RoutingTable,
) -> (Vec<String>, ScatterEnvelope) {
    let mut local = Vec::new();
    let mut envelope = ScatterEnvelope::new();

    for node_id in node_ids {
        let shard = VShardId::from_key(node_id.as_bytes());
        let leader = routing
            .leader_for_vshard(shard.as_u32())
            .unwrap_or(local_node_id);

        if leader == local_node_id {
            local.push(node_id.clone());
        } else {
            envelope.add(shard, node_id.clone());
        }
    }

    (local, envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_envelope_grouping() {
        let mut env = ScatterEnvelope::new();
        env.add(VShardId::new(0), "a".into());
        env.add(VShardId::new(0), "b".into());
        env.add(VShardId::new(1), "c".into());

        assert_eq!(env.shard_count(), 2);
        assert_eq!(env.total_nodes(), 3);

        let batches = env.into_batches();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn empty_envelope() {
        let env = ScatterEnvelope::new();
        assert!(env.is_empty());
        assert_eq!(env.shard_count(), 0);
        assert_eq!(env.total_nodes(), 0);
    }
}
