// SPDX-License-Identifier: BUSL-1.1

//! Shared stream consumption logic.
//!
//! Used by both HTTP endpoints and pgwire SELECT to read events from a
//! change stream's buffer using consumer group offsets.
//!
//! **Cluster-wide:** When a specific partition is requested and the vShard
//! leader for that partition is on another node, the request is forwarded
//! as a typed operation over the authenticated cluster RPC transport. The
//! remote Control Plane reads its local Event-Plane buffer and returns the
//! serialized events.

use tracing::debug;

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::event::CdcEvent;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc};
use nodedb_physical::physical_plan::{ClusterEventOp, PhysicalPlan, wire as plan_wire};

/// Parameters for consuming events from a stream.
pub struct ConsumeParams<'a> {
    pub tenant_id: u64,
    pub stream_name: &'a str,
    pub group_name: &'a str,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u32>,
    /// Maximum events to return.
    pub limit: usize,
}

/// Result of consuming events from a stream.
pub struct ConsumeResult {
    /// The events read from the buffer. Events are shared `Arc<CdcEvent>`
    /// so consumer fan-out (webhook, Kafka, SHOW, commit) doesn't deep-clone.
    pub events: Vec<Arc<CdcEvent>>,
    /// Per-partition latest LSN seen in this batch (for offset tracking).
    pub partition_offsets: Vec<(u32, u64)>,
    /// Number of events dropped from this stream's buffer since the consumer
    /// group's previous poll. Zero on the first ever poll for this group, or
    /// when no evictions have occurred.
    pub evicted_since_last_poll: u64,
    /// Oldest LSN still available in the stream buffer. Zero when the buffer
    /// is empty. A consumer whose `from_lsn` < this value has experienced a
    /// gap and should resync or alert.
    pub oldest_available_lsn: u64,
}

/// Consume events from a change stream using consumer group offsets.
///
/// Reads events with LSN > the group's committed offset for each partition.
/// Does NOT auto-commit offsets — the caller must explicitly COMMIT OFFSET.
///
/// **Cluster-aware:** If a specific partition is requested and the vShard
/// leader is remote, returns `ConsumeError::RemotePartition` so the caller
/// can use `consume_remote` over the authenticated cluster transport.
pub fn consume_stream(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    // Verify stream (or topic) exists.
    // Topics use buffer keys with the "topic:" prefix.  When the stream_name
    // already carries that prefix we accept it if the corresponding topic is
    // registered in ep_topic_registry.
    let stream_exists = state
        .stream_registry
        .get(params.tenant_id, params.stream_name)
        .is_some();
    let topic_exists = params
        .stream_name
        .strip_prefix("topic:")
        .is_some_and(|bare| {
            state
                .ep_topic_registry
                .get(params.tenant_id, bare)
                .is_some()
        });
    if !stream_exists && !topic_exists {
        return Err(ConsumeError::StreamNotFound(params.stream_name.to_string()));
    }

    // Verify consumer group exists.
    // For topics: the group may have been registered under the bare name
    // ("order_events") even though we query with the prefixed name
    // ("topic:order_events").  Accept either.
    let bare_stream = params
        .stream_name
        .strip_prefix("topic:")
        .unwrap_or(params.stream_name);
    let group_exists = state
        .group_registry
        .get(params.tenant_id, params.stream_name, params.group_name)
        .is_some()
        || state
            .group_registry
            .get(params.tenant_id, bare_stream, params.group_name)
            .is_some();
    if !group_exists {
        return Err(ConsumeError::GroupNotFound(
            params.group_name.to_string(),
            params.stream_name.to_string(),
        ));
    }

    // Cluster-aware: check if the requested partition is remote.
    if let Some(partition_id) = params.partition
        && let Some(remote_node) = remote_partition_leader(state, partition_id)
    {
        debug!(
            partition = partition_id,
            remote_node,
            stream = params.stream_name,
            "partition is remote — forwarding consume request"
        );
        return Err(ConsumeError::RemotePartition {
            partition_id,
            leader_node: remote_node,
        });
    }

    // Local consumption path.
    consume_local(state, params)
}

/// Consume events from a local stream buffer.
///
/// This is the core logic, always reads from the local `CdcRouter` buffers.
/// Used directly for local partitions and by `consume_remote` on the remote
/// node after the gateway routes and executes the stream SELECT.
pub fn consume_local(
    state: &SharedState,
    params: &ConsumeParams<'_>,
) -> Result<ConsumeResult, ConsumeError> {
    // Get the stream buffer.
    let buffer = state
        .cdc_router
        .get_buffer(params.tenant_id, params.stream_name)
        .ok_or_else(|| ConsumeError::BufferEmpty(params.stream_name.to_string()))?;

    // Read events based on committed offsets.
    let events = if let Some(partition_id) = params.partition {
        // Single partition read.
        let from_lsn = state.offset_store.get_offset(
            params.tenant_id,
            params.stream_name,
            params.group_name,
            partition_id,
        );
        buffer.read_partition_from_lsn(partition_id, from_lsn, params.limit)
    } else {
        // All partitions: read from the minimum committed offset.
        // Each event's partition field lets consumers track per-partition progress.
        let all_offsets = state.offset_store.get_all_offsets(
            params.tenant_id,
            params.stream_name,
            params.group_name,
        );
        // Use the minimum offset across all committed partitions, or 0 if none committed.
        let min_lsn = all_offsets
            .iter()
            .map(|o| o.committed_lsn)
            .min()
            .unwrap_or(0);
        buffer.read_from_lsn(min_lsn, params.limit)
    };

    // Compute per-partition max LSN for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u32, u64> =
        std::collections::BTreeMap::new();
    for e in &events {
        let entry = partition_offsets.entry(e.partition).or_insert(0);
        if e.lsn > *entry {
            *entry = e.lsn;
        }
    }

    // Compute eviction delta and oldest LSN for the poll response.
    let total_evicted_now = buffer.total_evicted();
    let evicted_since_last_poll = state.offset_store.swap_eviction_baseline(
        params.tenant_id,
        params.stream_name,
        params.group_name,
        total_evicted_now,
    );
    let oldest_available_lsn = buffer.earliest_lsn().unwrap_or(0);

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
        evicted_since_last_poll,
        oldest_available_lsn,
    })
}

/// Check if a partition's vShard leader is on a remote node.
///
/// Returns `Some(remote_node_id)` if the leader is remote, `None` if local
/// or if we're in single-node mode.
fn remote_partition_leader(state: &SharedState, partition_id: u32) -> Option<u64> {
    let routing_lock = state.cluster_routing.as_ref()?;
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
    let leader = routing.leader_for_vshard(partition_id).ok()?;
    if leader == state.node_id || leader == 0 {
        None // Local or no leader known.
    } else {
        Some(leader)
    }
}

fn build_consume_plan(params: &ConsumeParams<'_>) -> Result<PhysicalPlan, ConsumeError> {
    let partition = params.partition.ok_or_else(|| {
        ConsumeError::RemoteError("remote CDC consume requires one partition".into())
    })?;
    let limit =
        u64::try_from(params.limit).map_err(|_| ConsumeError::InvalidLimit(params.limit))?;
    Ok(PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
        stream_name: params.stream_name.to_owned(),
        group_name: params.group_name.to_owned(),
        partition,
        limit,
    }))
}

/// Forward a consume request directly to the remote partition leader.
///
/// The authenticated cluster RPC carries a typed Control-Plane operation;
/// reconstructed SQL is deliberately not used for Event-Plane routing.
pub async fn consume_remote(
    state: &SharedState,
    params: &ConsumeParams<'_>,
    leader_node: u64,
) -> Result<ConsumeResult, ConsumeError> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or(ConsumeError::NoClusterTransport)?;
    let plan = build_consume_plan(params)?;
    let plan_bytes =
        plan_wire::encode(&plan).map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let request = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id: params.tenant_id,
        database_id: nodedb_types::id::DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: 30_000,
        trace_id: nodedb_types::TraceId::generate().0,
        descriptor_versions: Vec::new(),
        txn_id: None,
    });
    let response = transport
        .send_rpc(leader_node, request)
        .await
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let payload = match response {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            payloads,
            ..
        }) => payloads.into_iter().next().ok_or_else(|| {
            ConsumeError::RemoteError("remote CDC consume returned no payload".into())
        })?,
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(error), ..
        }) => return Err(ConsumeError::RemoteError(format!("{error:?}"))),
        RaftRpc::ExecuteResponse(_) => {
            return Err(ConsumeError::RemoteError(
                "remote CDC consume returned an empty error".into(),
            ));
        }
        _ => {
            return Err(ConsumeError::RemoteError(
                "remote CDC consume returned an unexpected response".into(),
            ));
        }
    };
    crate::util::bounded_msgpack::read_value(&payload)
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?;
    let events = zerompk::from_msgpack::<Vec<CdcEvent>>(&payload)
        .map_err(|error| ConsumeError::RemoteError(error.to_string()))?
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Compute per-partition max LSN for the returned batch.
    let mut partition_offsets: std::collections::BTreeMap<u32, u64> =
        std::collections::BTreeMap::new();
    for e in &events {
        let entry = partition_offsets.entry(e.partition).or_insert(0);
        if e.lsn > *entry {
            *entry = e.lsn;
        }
    }

    Ok(ConsumeResult {
        events,
        partition_offsets: partition_offsets.into_iter().collect(),
        // For remote consumes the eviction metadata comes from the remote node.
        // The remote `consume_local` path already computed the delta on that
        // node; we cannot reconstruct it here. Surface 0 so callers always get
        // a valid (conservative) value rather than stale or fabricated data.
        evicted_since_last_poll: 0,
        oldest_available_lsn: 0,
    })
}

/// Errors from stream consumption.
#[derive(Debug)]
pub enum ConsumeError {
    StreamNotFound(String),
    GroupNotFound(String, String),
    /// Stream exists but buffer is empty (no events yet).
    BufferEmpty(String),
    /// Partition is on a remote node — caller should use `consume_remote()`.
    RemotePartition {
        partition_id: u32,
        leader_node: u64,
    },
    /// Remote consume failed.
    RemoteError(String),
    /// Gateway not available (cluster transport not ready).
    NoClusterTransport,
    /// Requested LIMIT cannot be represented by the SQL integer type.
    InvalidLimit(usize),
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamNotFound(s) => write!(f, "change stream '{s}' does not exist"),
            Self::GroupNotFound(g, s) => {
                write!(f, "consumer group '{g}' does not exist on stream '{s}'")
            }
            Self::BufferEmpty(s) => write!(f, "stream '{s}' has no buffered events"),
            Self::RemotePartition {
                partition_id,
                leader_node,
            } => {
                write!(
                    f,
                    "partition {partition_id} is on remote node {leader_node}"
                )
            }
            Self::RemoteError(e) => write!(f, "remote consume error: {e}"),
            Self::NoClusterTransport => {
                write!(f, "cluster transport not available for remote stream read")
            }
            Self::InvalidLimit(limit) => {
                write!(f, "stream LIMIT {limit} exceeds cluster wire range")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_error_display() {
        let e = ConsumeError::StreamNotFound("orders".into());
        assert!(e.to_string().contains("orders"));
    }

    #[test]
    fn remote_partition_error_display() {
        let e = ConsumeError::RemotePartition {
            partition_id: 5,
            leader_node: 3,
        };
        assert!(e.to_string().contains("partition 5"));
        assert!(e.to_string().contains("node 3"));
    }

    #[test]
    fn build_consume_plan_preserves_typed_inputs() {
        let params = ConsumeParams {
            tenant_id: 1,
            stream_name: "orders; DROP STREAM audit",
            group_name: "group\"; --",
            partition: Some(5),
            limit: 100,
        };
        assert_eq!(
            build_consume_plan(&params).expect("typed consume plan"),
            PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
                stream_name: params.stream_name.to_owned(),
                group_name: params.group_name.to_owned(),
                partition: 5,
                limit: 100,
            })
        );
    }

    #[test]
    fn remote_consume_plan_requires_a_partition() {
        let params = ConsumeParams {
            tenant_id: 1,
            stream_name: "orders_stream",
            group_name: "analytics",
            partition: None,
            limit: 50,
        };
        assert!(build_consume_plan(&params).is_err());
    }

    #[tokio::test]
    async fn single_node_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        // No cluster_routing → always local.
        assert!(remote_partition_leader(&state, 5).is_none());
    }
}
