// SPDX-License-Identifier: Apache-2.0

//! Event-Plane operations executed by the receiving Control Plane.
//!
//! These plans are cluster-RPC envelopes only. They must never cross the
//! Control Plane → Data Plane bridge.

use nodedb_types::DatabaseId;

/// Hard cap for committed CDC cursors supplied in one cluster consume request.
///
/// This bounds a caller-controlled wire vector while still allowing one cursor
/// for every partition in a reasonably sized routed stream.
pub const MAX_REMOTE_CDC_COMMITTED_OFFSETS: usize = 4_096;

/// Cluster-routed Event-Plane operation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ClusterEventOp {
    /// Consume CDC events from a replica's local event buffer.
    ///
    /// `committed_offsets` belongs to the caller node: each tuple is
    /// `(partition_id, epoch, index, sequence)`. The receiver must use these
    /// cursors rather than its own consumer-group offset store. Missing
    /// partitions start at the initial `(0, 0, 0)` position.
    ConsumeStream {
        database_id: DatabaseId,
        stream_name: String,
        group_name: String,
        partition: Option<u32>,
        limit: u64,
        committed_offsets: Vec<(u32, u64, u64, u64)>,
    },
    /// Read the receiving node's tenant write marks of `group_ids`, once it
    /// applied every entry the groups committed before the request. RESTORE's
    /// staleness guard asks a replica of each group this way when the
    /// restoring node does not replicate the group.
    TenantWriteMarks { tenant_id: u64, group_ids: Vec<u64> },
    /// Read the receiving node's PK→surrogate binds of `tenant_id`'s
    /// `collections` in `database_id` with a home among `vshards`. A backup
    /// or MOVE TENANT capture asks the source node of each vShard this way,
    /// so every bind comes from a node that holds it.
    SurrogateBinds {
        tenant_id: u64,
        database_id: DatabaseId,
        vshards: Vec<u32>,
        collections: Vec<String>,
    },
    /// Answer once the receiving node applied the metadata log through
    /// `index`. A restore asks every node this way after it raises the
    /// surrogate high-water mark, so no node issues a surrogate the restore
    /// binds.
    MetadataApplied { index: u64 },
    /// Read which primary key the receiving node binds each
    /// `(collection, surrogate)` of `entries` to, in `tenant_id`'s
    /// `database_id`. A re-issue asks each carried surrogate's home leader
    /// this way before it binds, so a surrogate already bound to another key
    /// fails the re-issue.
    SurrogateHolders {
        tenant_id: u64,
        database_id: DatabaseId,
        entries: Vec<(String, u32)>,
    },
}

#[cfg(test)]
mod tests {
    use super::{ClusterEventOp, DatabaseId};
    use crate::physical_plan::{PhysicalPlan, wire};

    #[test]
    fn cluster_event_plan_roundtrips_over_cluster_wire() {
        let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            database_id: DatabaseId::new(7),
            stream_name: "orders; no SQL".into(),
            group_name: "Analytics".into(),
            partition: Some(7),
            limit: 128,
            committed_offsets: vec![(7, 1, 42, 3)],
        });
        let encoded = wire::encode(&plan).expect("encode typed cluster event");
        assert_eq!(
            wire::decode(&encoded).expect("decode typed cluster event"),
            plan
        );
    }
}
