// SPDX-License-Identifier: BUSL-1.1

//! Partition assignment for consumer groups.
//!
//! Within a consumer group, partitions (vShards) are distributed across
//! connected consumers using range-based assignment. Each consumer gets
//! a contiguous range of partition IDs.
//!
//! On consumer join/leave, partitions are reassigned automatically.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;

use crate::types::DatabaseId;

/// Tracks active consumers and their partition assignments per group.
pub struct ConsumerAssignments {
    /// (database_id, tenant_id, stream_name, group_name) → assignment state.
    groups: RwLock<HashMap<(DatabaseId, u64, String, String), GroupAssignment>>,
}

/// Per-group assignment state.
struct GroupAssignment {
    /// Active consumer IDs (sorted for deterministic assignment).
    consumers: BTreeSet<String>,
    /// Known partition IDs in this stream (discovered as events arrive).
    partitions: BTreeSet<u32>,
    /// Current assignment: consumer_id → set of partition IDs.
    assignments: BTreeMap<String, Vec<u32>>,
}

impl GroupAssignment {
    fn new() -> Self {
        Self {
            consumers: BTreeSet::new(),
            partitions: BTreeSet::new(),
            assignments: BTreeMap::new(),
        }
    }

    /// Rebalance partitions across active consumers using range assignment.
    fn rebalance(&mut self) {
        self.assignments.clear();

        if self.consumers.is_empty() || self.partitions.is_empty() {
            return;
        }

        let consumers: Vec<&String> = self.consumers.iter().collect();
        let partitions: Vec<u32> = self.partitions.iter().copied().collect();
        let n_consumers = consumers.len();
        let n_partitions = partitions.len();

        // Range-based: each consumer gets floor(n/c) partitions, first (n%c) get one extra.
        let base = n_partitions / n_consumers;
        let remainder = n_partitions % n_consumers;

        let mut offset = 0;
        for (i, consumer) in consumers.iter().enumerate() {
            let count = base + if i < remainder { 1 } else { 0 };
            let assigned: Vec<u32> = partitions[offset..offset + count].to_vec();
            self.assignments.insert(consumer.to_string(), assigned);
            offset += count;
        }
    }
}

impl ConsumerAssignments {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Register a consumer joining a group. Triggers rebalance.
    pub fn join(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        let state = groups.entry(key).or_insert_with(GroupAssignment::new);
        state.consumers.insert(consumer_id.to_string());
        state.rebalance();

        tracing::debug!(
            stream,
            group,
            consumer_id,
            total_consumers = state.consumers.len(),
            "consumer joined, rebalanced"
        );
    }

    /// Deregister a consumer leaving a group. Triggers rebalance.
    pub fn leave(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = groups.get_mut(&key) {
            state.consumers.remove(consumer_id);
            state.rebalance();

            tracing::debug!(
                stream,
                group,
                consumer_id,
                remaining = state.consumers.len(),
                "consumer left, rebalanced"
            );
        }
    }

    /// Register a partition as known (called when events with new partition IDs are seen).
    pub fn register_partition(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        partition_id: u32,
    ) {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut groups = self.groups.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = groups.get_mut(&key)
            && state.partitions.insert(partition_id)
        {
            state.rebalance();
        }
    }

    /// Get the partitions assigned to a specific consumer.
    /// Returns None if the consumer is not registered (meaning: all partitions).
    pub fn assigned_partitions(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
        consumer_id: &str,
    ) -> Option<Vec<u32>> {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups
            .get(&key)
            .and_then(|state| state.assignments.get(consumer_id).cloned())
    }

    /// Number of active consumers in a group.
    pub fn consumer_count(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> usize {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let groups = self.groups.read().unwrap_or_else(|p| p.into_inner());
        groups.get(&key).map(|s| s.consumers.len()).unwrap_or(0)
    }
}

impl Default for ConsumerAssignments {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::new(7);

    fn add_partitions(assignments: &ConsumerAssignments, count: u32) {
        let mut groups = assignments.groups.write().unwrap();
        let state = groups.get_mut(&(DB, 1, "s".into(), "g".into())).unwrap();
        for partition in 0..count {
            state.partitions.insert(partition);
        }
        state.rebalance();
    }

    #[test]
    fn single_consumer_gets_all_partitions() {
        let assignments = ConsumerAssignments::new();
        assignments.join(DB, 1, "s", "g", "c1");
        add_partitions(&assignments, 4);
        assert_eq!(
            assignments.assigned_partitions(DB, 1, "s", "g", "c1"),
            Some(vec![0, 1, 2, 3])
        );
    }

    #[test]
    fn database_assignments_are_isolated() {
        let assignments = ConsumerAssignments::new();
        assignments.join(DB, 1, "s", "g", "c1");
        assignments.join(DatabaseId::new(8), 1, "s", "g", "c2");
        assert_eq!(assignments.consumer_count(DB, 1, "s", "g"), 1);
        assert_eq!(
            assignments.consumer_count(DatabaseId::new(8), 1, "s", "g"),
            1
        );
    }

    #[test]
    fn leave_triggers_rebalance() {
        let assignments = ConsumerAssignments::new();
        assignments.join(DB, 1, "s", "g", "c1");
        assignments.join(DB, 1, "s", "g", "c2");
        add_partitions(&assignments, 4);
        assignments.leave(DB, 1, "s", "g", "c2");
        assert_eq!(
            assignments
                .assigned_partitions(DB, 1, "s", "g", "c1")
                .unwrap()
                .len(),
            4
        );
        assert!(
            assignments
                .assigned_partitions(DB, 1, "s", "g", "c2")
                .is_none()
        );
    }
}
