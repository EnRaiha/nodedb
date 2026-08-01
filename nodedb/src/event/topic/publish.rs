// SPDX-License-Identifier: BUSL-1.1

//! Publish committed messages to durable topics.
//!
//! A local publication is serialized per topic, committed to the catalog, then
//! made visible to the canonical CDC buffer and live subscribers in that order.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan, wire as plan_wire};
use tracing::debug;

use crate::control::state::SharedState;
use crate::event::cdc::stream_def::RetentionConfig;
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;

/// Publish a message to a durable topic.
///
/// Returns the persistent sequence number assigned by the catalog. If the
/// topic's home vShard leader is remote, returns [`PublishError::RemoteHome`]
/// so the caller can forward the request through cluster RPC.
pub async fn publish_to_topic(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
) -> Result<u64, PublishError> {
    // Resolve before routing so nonexistent local topics cannot be forwarded.
    state
        .ep_topic_registry
        .get(database_id, tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?;

    if let Some(leader) = topic_home_node(state, database_id, topic_name)
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

    let lifecycle_lock = state
        .ep_topic_registry
        .lifecycle_lock(database_id, tenant_id, topic_name);
    let _guard = lifecycle_lock.lock().await;

    // Revalidate both runtime and durable definitions after acquiring the
    // scope lock. A catalog error is fail-closed: no buffer or live delivery
    // can occur without the durable record being readable.
    let _runtime_definition = state
        .ep_topic_registry
        .get(database_id, tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?;
    let catalog = state.credentials.catalog();
    let definition = load_catalog_definition(catalog, database_id, tenant_id, topic_name)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let committed = Arc::new(
        catalog
            .append_ep_topic_message(
                database_id,
                tenant_id,
                topic_name,
                payload,
                now_ms,
                definition.last_lsn.max(now_ms),
            )
            .map_err(|error| PublishError::Persistence(error.to_string()))?,
    );

    // The catalog transaction has committed. The returned message is the sole
    // source for both buffer and live delivery, so all consumers observe its
    // exact persistent sequence, LSN, timestamp, and unnormalized payload.
    let buffer = get_or_create_topic_buffer(
        state,
        database_id,
        tenant_id,
        topic_name,
        &definition.retention,
    );
    buffer.push(Arc::new(committed.to_cdc_event()));
    let _ = state
        .ep_topic_registry
        .sender(database_id, tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?
        // No receivers is a normal durable-only publication.
        .send(Arc::clone(&committed));
    state
        .ep_topic_registry
        .broadcast_committed(Arc::clone(&committed));

    Ok(committed.sequence)
}

fn load_catalog_definition(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
) -> Result<TopicDef, PublishError> {
    catalog
        .load_all_ep_topics()
        .map_err(|error| PublishError::Persistence(error.to_string()))?
        .into_iter()
        .find(|definition| {
            definition.database_id == database_id
                && definition.tenant_id == tenant_id
                && definition.name == topic_name
        })
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))
}

/// Get or create the canonical CDC buffer for a topic.
fn get_or_create_topic_buffer(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    retention: &RetentionConfig,
) -> Arc<crate::event::cdc::buffer::StreamBuffer> {
    state.cdc_router.ensure_buffer(
        database_id,
        tenant_id,
        &format!("topic:{topic_name}"),
        retention,
    )
}

/// Determine the home node for a topic.
fn topic_home_node(state: &SharedState, database_id: DatabaseId, topic_name: &str) -> Option<u64> {
    let routing_lock = state.cluster_routing.as_ref()?;
    let vshard_id = nodedb_cluster::routing::vshard_for_collection(database_id, topic_name);
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    routing.leader_for_vshard(vshard_id).ok()
}

fn build_publish_plan(database_id: DatabaseId, topic_name: &str, payload: &str) -> PhysicalPlan {
    PhysicalPlan::ClusterEvent(ClusterEventOp::PublishTopic {
        database_id,
        topic_name: topic_name.to_owned(),
        payload: payload.to_owned(),
    })
}

/// Forward a PUBLISH directly to the topic's home node.
pub async fn publish_remote(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
    leader_node: u64,
) -> Result<u64, PublishError> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| PublishError::RemoteError("cluster transport not available".into()))?;
    let plan = build_publish_plan(database_id, topic_name, payload);
    let plan_bytes =
        plan_wire::encode(&plan).map_err(|error| PublishError::RemoteError(error.to_string()))?;
    let request = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: database_id.as_u64(),
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
    /// The durable catalog could not be read or committed.
    Persistence(String),
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
            Self::TopicNotFound(topic) => write!(f, "topic '{topic}' does not exist"),
            Self::Persistence(error) => write!(f, "topic persistence error: {error}"),
            Self::RemoteHome {
                topic_name,
                leader_node,
            } => write!(f, "topic '{topic_name}' home is on node {leader_node}"),
            Self::RemoteError(error) => write!(f, "remote publish error: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_publish_plan, publish_to_topic};
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::TopicDef;
    use crate::types::DatabaseId;
    use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan};

    #[test]
    fn remote_publish_preserves_payload_as_typed_data() {
        let topic = "topic; DROP TOPIC audit";
        let payload = "' OR true; --";
        assert_eq!(
            build_publish_plan(DatabaseId::new(7), topic, payload),
            PhysicalPlan::ClusterEvent(ClusterEventOp::PublishTopic {
                database_id: DatabaseId::new(7),
                topic_name: topic.into(),
                payload: payload.into(),
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_catalog_definition_does_not_create_buffer_or_broadcast() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let database_id = DatabaseId::new(7);
        let tenant_id = 11;
        let topic_name = "missing";
        state.ep_topic_registry.register(TopicDef {
            database_id,
            tenant_id,
            name: topic_name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        });
        let mut live = state
            .ep_topic_registry
            .subscribe(database_id, tenant_id, topic_name)
            .expect("live receiver");

        let error = publish_to_topic(&state, database_id, tenant_id, topic_name, "{}")
            .await
            .expect_err("catalog definition is required");
        assert!(matches!(error, super::PublishError::TopicNotFound(_)));
        assert!(
            state
                .cdc_router
                .get_buffer(database_id, tenant_id, "topic:missing")
                .is_none()
        );
        assert!(matches!(
            live.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_commits_align_catalog_buffer_and_live_bus() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let database_id = DatabaseId::new(7);
        let tenant_id = 11;
        let topic_name = "events";
        let definition = TopicDef {
            database_id,
            tenant_id,
            name: topic_name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        };
        state
            .credentials
            .catalog()
            .put_ep_topic(&definition)
            .expect("persist topic");
        state.ep_topic_registry.register(definition);
        let mut live = state
            .ep_topic_registry
            .subscribe(database_id, tenant_id, topic_name)
            .expect("live receiver");

        let payloads: Vec<String> = (0..8)
            .map(|number| format!("{{\"number\":{number}}}"))
            .collect();
        let publishes = payloads
            .iter()
            .map(|payload| publish_to_topic(&state, database_id, tenant_id, topic_name, payload));
        let sequences = futures::future::join_all(publishes)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("publish");
        assert_eq!(sequences.len(), 8);

        let catalog_messages = state
            .credentials
            .catalog()
            .load_ep_topic_messages(database_id, tenant_id, topic_name)
            .expect("catalog messages");
        let buffer_messages = state
            .cdc_router
            .get_buffer(database_id, tenant_id, "topic:events")
            .expect("topic buffer")
            .read_from(crate::event::cdc::CdcOffset::ZERO, 16);
        let mut live_messages = Vec::new();
        for _ in 0..8 {
            live_messages.push(live.recv().await.expect("live message"));
        }

        let expected: Vec<u64> = (1..=8).collect();
        assert_eq!(
            catalog_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            buffer_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            live_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            live_messages
                .iter()
                .map(|message| &message.payload)
                .collect::<Vec<_>>(),
            catalog_messages
                .iter()
                .map(|message| &message.payload)
                .collect::<Vec<_>>()
        );
    }
}
