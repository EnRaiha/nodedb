// SPDX-License-Identifier: BUSL-1.1

//! Which node runs a change stream's external sink.
//!
//! A webhook or Kafka sink runs on exactly one node at a time: the node that
//! holds the leader lease of the stream's owning data group, the group of the
//! vShard the stream's name hashes to. Every other node idles. The lease
//! lapses before another node can win the group's election, so two nodes
//! never deliver at once. The next lease holder resumes from the replicated
//! consumer-group offsets.
//!
//! A sink checks the lease right before each delivery and right before each
//! offset commit ([`SinkFence`]). It hands the destination the lease term as
//! a fencing token ([`SinkLease`]), so a destination can reject a stale
//! owner whose delivery was already in flight.

use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::{ConsumerGroupDef, GroupRegistry};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::types::DatabaseId;

/// The data group that owns `stream_name`'s sink, `None` before the node's
/// routing table is wired or when the routing has no group for it.
pub fn owning_group(
    state: &SharedState,
    database_id: DatabaseId,
    stream_name: &str,
) -> Option<u64> {
    let routing = state.cluster_routing.as_ref()?;
    let vshard = nodedb_cluster::routing::vshard_for_collection(
        nodedb_types::CollectionKey::from_bare(database_id, stream_name),
    );
    routing
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(vshard)
        .ok()
}

/// The lease a node runs a sink under.
///
/// `term` is the fencing token a sink hands its destination: the Raft term of
/// the owning group's leader lease. Each later owner of the sink leads the
/// group at a higher term, so a destination that keeps the highest token it
/// saw rejects a stale owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkLease {
    /// The owning data group.
    pub group_id: u64,
    /// The owning group's lease term.
    pub term: u64,
}

/// The lease this node runs `stream_name`'s sink under now, `None` when
/// another node runs it or before `start_raft` wires the node's Raft.
pub fn sink_lease(
    state: &SharedState,
    database_id: DatabaseId,
    stream_name: &str,
) -> Option<SinkLease> {
    let gate = state.raft_read_gate.get()?;
    let group_id = owning_group(state, database_id, stream_name)?;
    gate.leader_lease_term(group_id)
        .map(|term| SinkLease { group_id, term })
}

/// A sink's check that it still runs under the lease it started a batch
/// under. The sink checks it right before each delivery and right before
/// each offset commit, so a node whose lease lapsed or moved to a later term
/// stops before it acts.
pub struct SinkFence<'a> {
    pub state: &'a SharedState,
    pub database_id: DatabaseId,
    pub stream_name: &'a str,
    pub lease: SinkLease,
}

impl SinkFence<'_> {
    /// Whether this node still holds the batch's lease, at the same term.
    pub fn holds(&self) -> bool {
        sink_lease(self.state, self.database_id, self.stream_name) == Some(self.lease)
    }
}

/// The internal consumer group a webhook sink commits its offsets under.
pub fn webhook_group(stream_name: &str) -> String {
    format!("_webhook:{stream_name}")
}

/// The internal consumer group a Kafka sink commits its offsets under.
pub fn kafka_group(stream_name: &str) -> String {
    format!("_kafka_{stream_name}")
}

/// Register on this node the internal consumer group of every sink `def`
/// configures.
///
/// A sink's offset commit is a replicated catalog entry, and a node applies
/// it only to a group it has registered. Every node registers the sink groups
/// when it installs the stream, in metadata log order, and again from the
/// catalog at boot. So every commit of a sink finds its group on every node,
/// and the next lease holder resumes after the last one. A group already
/// registered keeps its definition.
pub fn register_sink_groups(groups: &GroupRegistry, def: &ChangeStreamDef) {
    if def.webhook.is_configured() {
        register_sink_group(groups, def, webhook_group(&def.name), "_system_webhook");
    }
    if def.kafka.enabled {
        register_sink_group(groups, def, kafka_group(&def.name), "_system_kafka");
    }
}

fn register_sink_group(groups: &GroupRegistry, def: &ChangeStreamDef, name: String, owner: &str) {
    if groups
        .get(def.database_id, def.tenant_id, &def.name, &name)
        .is_some()
    {
        return;
    }
    groups.register(ConsumerGroupDef {
        database_id: def.database_id,
        tenant_id: def.tenant_id,
        name,
        stream_name: def.name.clone(),
        owner: owner.into(),
        created_at: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Installing a stream registers the group of each sink it configures,
    /// and only those, so its replicated offset commits apply on this node.
    #[test]
    fn a_stream_registers_the_group_of_each_sink_it_configures() {
        use crate::event::cdc::stream_def::{
            CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
        };
        let def = ChangeStreamDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: "orders_feed".into(),
            collection: "orders".into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig {
                enabled: true,
                ..Default::default()
            },
            owner: "admin".into(),
            created_at: 0,
            subscriber_roles: Vec::new(),
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        let groups = GroupRegistry::new();
        register_sink_groups(&groups, &def);
        let kafka = groups
            .get(
                DatabaseId::DEFAULT,
                1,
                "orders_feed",
                &kafka_group("orders_feed"),
            )
            .expect("the Kafka sink group");
        assert_eq!(kafka.modification_hlc, nodedb_types::Hlc::ZERO);
        assert!(
            groups
                .get(
                    DatabaseId::DEFAULT,
                    1,
                    "orders_feed",
                    &webhook_group("orders_feed")
                )
                .is_none(),
            "a stream with no webhook registers no webhook group"
        );
    }

    /// A node whose Raft is not wired yet runs no sink.
    #[test]
    fn a_node_before_raft_wiring_runs_no_sink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert_eq!(sink_lease(&state, DatabaseId::DEFAULT, "orders_feed"), None);
    }

    /// A one-node cluster leads every group, so it runs the sink under the
    /// owning group's lease. A fence at that lease holds, and one at another
    /// term does not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_owning_groups_leader_runs_the_sink_under_its_lease() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let group_id =
            owning_group(state, DatabaseId::DEFAULT, "orders_feed").expect("an owning group");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let lease = loop {
            if let Some(lease) = sink_lease(state, DatabaseId::DEFAULT, "orders_feed") {
                break lease;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the leader never took the owning group's lease"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        assert_eq!(lease.group_id, group_id);

        let fence = SinkFence {
            state,
            database_id: DatabaseId::DEFAULT,
            stream_name: "orders_feed",
            lease,
        };
        assert!(fence.holds());
        let stale = SinkFence {
            lease: SinkLease {
                group_id,
                term: lease.term + 1,
            },
            ..fence
        };
        assert!(!stale.holds(), "a fence at another term does not hold");
        cluster.shutdown().await;
    }
}
