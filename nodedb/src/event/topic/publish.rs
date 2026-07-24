// SPDX-License-Identifier: BUSL-1.1

//! Publish a message to a durable topic.
//!
//! Creates a CdcEvent from the user payload and pushes it into the
//! topic's StreamBuffer (same buffer type used by change streams).
//!
//! **Cluster-wide:** Each topic has a "home node" determined by hashing
//! the topic name to a vShard. PUBLISH on a non-home node forwards the
//! request to the home node as a typed operation over authenticated cluster
//! RPC. This ensures
//! all messages for a topic live on one node's buffer, maintaining ordering.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan, wire as plan_wire};
use sonic_rs;
use tracing::debug;

use crate::control::state::SharedState;
use crate::event::cdc::buffer::StreamBuffer;
use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::stream_def::RetentionConfig;

/// Publish a message to a durable topic.
///
/// Returns the sequence number assigned to the message.
///
/// **Cluster-aware:** If the topic's home vShard leader is on another node,
/// returns `PublishError::RemoteHome` so the caller can forward via QUIC.
pub fn publish_to_topic(
    state: &SharedState,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
) -> Result<u64, PublishError> {
    // Verify topic exists.
    let topic = state
        .ep_topic_registry
        .get(tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?;

    // Cluster-aware: check if this topic's home node is remote.
    if let Some(leader) = topic_home_node(state, topic_name)
        && leader != state.node_id
    {
        debug!(
            topic = topic_name,
            home_node = leader,
            "topic home is remote — forwarding publish"
        );
        return Err(PublishError::RemoteHome {
            topic_name: topic_name.to_string(),
            leader_node: leader,
        });
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Parse payload as JSON (or wrap raw string in a JSON object).
    let value: serde_json::Value =
        sonic_rs::from_str(payload).unwrap_or_else(|_| serde_json::json!({"message": payload}));

    // Get or create the topic's buffer via the CdcRouter buffer pool.
    let buffer = get_or_create_topic_buffer(state, tenant_id, topic_name, &topic.retention);

    // Use buffer's total_pushed as monotonic sequence.
    let sequence = buffer.total_pushed() + 1;

    let event = CdcEvent {
        sequence,
        partition: 0, // Topics use a single partition (no vShard routing).
        collection: format!("topic:{topic_name}"),
        op: "PUBLISH".into(),
        row_id: format!("msg-{sequence}"),
        event_time: now_ms,
        lsn: now_ms, // Topics don't have WAL LSNs; use timestamp as monotonic ordering.
        tenant_id,
        new_value: Some(value),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
    };

    buffer.push(event);
    Ok(sequence)
}

/// Get or create a StreamBuffer for a topic.
fn get_or_create_topic_buffer(
    state: &SharedState,
    tenant_id: u64,
    topic_name: &str,
    retention: &RetentionConfig,
) -> Arc<StreamBuffer> {
    // Topics use the CdcRouter's buffer pool with a "topic:" prefix
    // to avoid name collisions with change streams.
    let buffer_key = format!("topic:{topic_name}");

    if let Some(buf) = state.cdc_router.get_buffer(tenant_id, &buffer_key) {
        return buf;
    }

    // Create a new buffer. Use the router's internal mechanism.
    // Since CdcRouter.get_or_create_buffer is private, we route through
    // a dummy event to force buffer creation, then return it.
    // Instead, let's add a public create method to CdcRouter.
    // For now, use the public get_buffer after forcing creation.
    //
    // Actually, we can just create the buffer directly and register it.
    state
        .cdc_router
        .ensure_buffer(tenant_id, &buffer_key, retention)
}

/// Determine the home node for a topic.
///
/// Topics are hashed to a vShard for deterministic routing. The vShard's
/// leader is the topic's "home node" where all messages are stored.
/// Returns `None` in single-node mode.
fn topic_home_node(state: &SharedState, topic_name: &str) -> Option<u64> {
    let routing_lock = state.cluster_routing.as_ref()?;
    // Topics route under `DatabaseId::DEFAULT` today; when topics gain
    // database scope, plumb it through here.
    let vshard_id = nodedb_cluster::routing::vshard_for_collection(
        nodedb_types::id::DatabaseId::DEFAULT,
        topic_name,
    );
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    routing.leader_for_vshard(vshard_id).ok()
}

fn build_publish_plan(topic_name: &str, payload: &str) -> PhysicalPlan {
    PhysicalPlan::ClusterEvent(ClusterEventOp::PublishTopic {
        topic_name: topic_name.to_owned(),
        payload: payload.to_owned(),
    })
}

/// Forward a PUBLISH directly to the topic's home node.
pub async fn publish_remote(
    state: &SharedState,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
    leader_node: u64,
) -> Result<u64, PublishError> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| PublishError::RemoteError("cluster transport not available".into()))?;
    let plan = build_publish_plan(topic_name, payload);
    let plan_bytes =
        plan_wire::encode(&plan).map_err(|error| PublishError::RemoteError(error.to_string()))?;
    let request = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: nodedb_types::id::DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: 30_000,
        trace_id: nodedb_types::TraceId::generate().0,
        descriptor_versions: Vec::new(),
        txn_id: None,
    });
    let response = transport
        .send_rpc(leader_node, request)
        .await
        .map_err(|error| PublishError::RemoteError(error.to_string()))?;
    let payload = match response {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            payloads,
            ..
        }) => payloads.into_iter().next().ok_or_else(|| {
            PublishError::RemoteError("remote PUBLISH returned no payload".into())
        })?,
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(error), ..
        }) => return Err(PublishError::RemoteError(format!("{error:?}"))),
        RaftRpc::ExecuteResponse(_) => {
            return Err(PublishError::RemoteError(
                "remote PUBLISH returned an empty error".into(),
            ));
        }
        _ => {
            return Err(PublishError::RemoteError(
                "remote PUBLISH returned an unexpected response".into(),
            ));
        }
    };
    crate::util::bounded_msgpack::read_value(&payload)
        .map_err(|error| PublishError::RemoteError(error.to_string()))?;
    zerompk::from_msgpack::<u64>(&payload)
        .map_err(|error| PublishError::RemoteError(error.to_string()))
}

#[derive(Debug)]
pub enum PublishError {
    TopicNotFound(String),
    /// Topic's home node is remote — caller should use `publish_remote()`.
    RemoteHome {
        topic_name: String,
        leader_node: u64,
    },
    /// Remote publish failed.
    RemoteError(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopicNotFound(t) => write!(f, "topic '{t}' does not exist"),
            Self::RemoteHome {
                topic_name,
                leader_node,
            } => {
                write!(f, "topic '{topic_name}' home is on node {leader_node}")
            }
            Self::RemoteError(e) => write!(f, "remote publish error: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::build_publish_plan;
    use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan};

    #[test]
    fn remote_publish_preserves_payload_as_typed_data() {
        let topic = "topic; DROP TOPIC audit";
        let payload = "' OR true; --";
        assert_eq!(
            build_publish_plan(topic, payload),
            PhysicalPlan::ClusterEvent(ClusterEventOp::PublishTopic {
                topic_name: topic.into(),
                payload: payload.into(),
            })
        );
    }
}
