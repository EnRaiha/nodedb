// SPDX-License-Identifier: Apache-2.0

//! Event-Plane operations executed by the receiving Control Plane.
//!
//! These plans are cluster-RPC envelopes only. They must never cross the
//! Control Plane → Data Plane bridge.

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
    /// Consume one CDC partition from the leader node's local event buffer.
    ConsumeStream {
        stream_name: String,
        group_name: String,
        partition: u32,
        limit: u64,
    },
    /// Publish one durable-topic message on the topic's home node.
    PublishTopic { topic_name: String, payload: String },
}

#[cfg(test)]
mod tests {
    use super::ClusterEventOp;
    use crate::physical_plan::{PhysicalPlan, wire};

    #[test]
    fn cluster_event_plan_roundtrips_over_cluster_wire() {
        let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            stream_name: "orders; no SQL".into(),
            group_name: "Analytics".into(),
            partition: 7,
            limit: 128,
        });
        let encoded = wire::encode(&plan).expect("encode typed cluster event");
        assert_eq!(
            wire::decode(&encoded).expect("decode typed cluster event"),
            plan
        );
    }
}
