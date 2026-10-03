// SPDX-License-Identifier: BUSL-1.1

//! Steps that several cluster test cases share: backup and restore over
//! pgwire `COPY`, database switching, group and leader lookups, and name
//! search.

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry};
use nodedb_types::id::VShardId;
use nodedb_types::{DatabaseId, TenantId};

use super::{TestCluster, TestClusterNode, wait_for};

/// Wait for the single-node sequencer and metadata Raft groups to elect
/// `node` as leader. Every spawn, fresh boot and restart alike, needs both
/// before DDL against group 0 can proceed.
pub async fn wait_for_single_node_ready(node: &TestClusterNode) {
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;
    wait_for(
        "single-node metadata leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.shared.is_metadata_leader(),
    )
    .await;
}

/// Render a pgwire error as `SQLSTATE: message`, or its display text when
/// the server sent no database error.
pub fn db_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// Take a backup of `tenant` over `client` and return the envelope bytes.
/// Panics with the server's error when the backup fails.
pub async fn drain_backup(client: &tokio_postgres::Client, tenant: u64) -> Vec<u8> {
    let stream = client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .unwrap_or_else(|e| panic!("BACKUP TENANT {tenant}: {}", db_detail(&e)));
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(
            &chunk.unwrap_or_else(|e| panic!("backup chunk: {}", db_detail(&e))),
        );
    }
    bytes
}

/// Restore `envelope` into `tenant` over `client`. The error carries the
/// server's error text.
pub async fn try_push_restore(
    client: &tokio_postgres::Client,
    tenant: u64,
    envelope: Vec<u8>,
) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .map_err(|e| db_detail(&e))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| db_detail(&e))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| db_detail(&e))
}

/// Restore `envelope` into `tenant` over `client`. Panics with the server's
/// error when the restore fails.
pub async fn push_restore(client: &tokio_postgres::Client, tenant: u64, envelope: Vec<u8>) {
    try_push_restore(client, tenant, envelope)
        .await
        .unwrap_or_else(|e| panic!("RESTORE tenant {tenant}: {e}"));
}

/// Switch every node's harness session to `database`.
pub async fn use_database(cluster: &TestCluster, database: &str) {
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE {database} on node {}: {e}", node.node_id));
    }
}

/// The id `node`'s catalog holds for the database `name`.
pub fn database_id(node: &TestClusterNode, name: &str) -> DatabaseId {
    node.shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("look up database id")
        .unwrap_or_else(|| panic!("node {} lacks database '{name}'", node.node_id))
}

/// Whether `node` leads vShard 0's group under a valid leader lease.
pub fn holds_vshard0_lease(node: &TestClusterNode) -> bool {
    let group = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(0)
        .expect("vShard 0 maps to a group");
    node.shared
        .raft_read_gate
        .get()
        .is_some_and(|gate| gate.holds_leader_lease(group))
}

/// Whether any Data Plane core of `node` has fail-stopped.
pub fn fail_stopped(node: &TestClusterNode) -> bool {
    node.shared
        .system_metrics
        .as_ref()
        .is_some_and(|metrics| metrics.core_fail_stops.is_stopped())
}

/// The number of transactions the sequencer on `node` has admitted, or 0
/// when `node` runs no sequencer.
pub fn sequencer_admitted(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Propose `entry` to group 0 and wait for it to apply on `node`.
pub async fn propose_and_apply(node: &TestClusterNode, entry: &MetadataEntry) {
    let handle = node
        .shared
        .metadata_raft
        .get()
        .expect("metadata raft handle installed");
    let index = handle
        .propose_async(encode_entry(entry).expect("encode metadata entry"))
        .await
        .expect("propose metadata entry");
    let watcher = node.shared.applied_index_watcher(METADATA_GROUP_ID);
    wait_for(
        "metadata entry applied",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || watcher.current() >= index,
    )
    .await;
}

/// The rows of the timeseries `collection` in the default database of
/// `tenant` that `node`'s own replica stores, each rendered as JSON text and
/// sorted.
pub async fn local_timeseries_rows(
    node: &TestClusterNode,
    tenant: u64,
    collection: &str,
) -> Vec<String> {
    let mut rows: Vec<String> = node
        .timeseries_rows_local(TenantId::new(tenant), collection)
        .await
        .iter()
        .map(|row| sonic_rs::to_string(row).expect("render a row"))
        .collect();
    rows.sort();
    rows
}

/// The data group `collection` maps to on `node`.
pub fn group_of(node: &TestClusterNode, collection: &str) -> u64 {
    node.group_id_for_collection(collection)
        .unwrap_or_else(|| panic!("no group mapping for {collection}"))
}

/// The data group `key` hashes to in `node`'s routing table.
pub fn group_of_key(node: &TestClusterNode, key: &str) -> u64 {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing");
    let table = routing.read().unwrap_or_else(|p| p.into_inner());
    table
        .group_for_vshard(VShardId::from_key(key.as_bytes()).as_u32())
        .expect("vshard maps to a group")
}

/// The member node ids of `group_id` in `node`'s routing table.
pub fn group_members(node: &TestClusterNode, group_id: u64) -> Vec<u64> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing");
    let table = routing.read().unwrap_or_else(|p| p.into_inner());
    table
        .group_info(group_id)
        .map(|info| info.members.clone())
        .unwrap_or_default()
}

/// `node`'s Raft status of `group_id`, when it hosts the group.
pub fn group_status(node: &TestClusterNode, group_id: u64) -> Option<nodedb_cluster::GroupStatus> {
    node.shared
        .cluster_observer
        .get()?
        .group_status
        .upgrade()?
        .group_statuses()
        .into_iter()
        .find(|g| g.group_id == group_id)
}

/// The collection name inside a document storage key, `None` when the key
/// has no collection part.
pub fn key_collection(key: &str) -> Option<&str> {
    let rest = key.splitn(3, ':').nth(2)?;
    rest.split([':', '\u{0}']).next()
}

/// The index in `cluster.nodes` of the node that leads `collection`'s data
/// group. Panics when no live node leads it.
pub fn leader_index_of(cluster: &TestCluster, collection: &str) -> usize {
    let probe = &cluster.nodes[0];
    let group = group_of(probe, collection);
    let leader = leader_of(probe, group);
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap_or_else(|| panic!("no live node leads {collection}'s group {group}"))
}

/// Shut down `cluster.nodes[idx]`, remove it from the cluster, and wait for
/// the survivors to elect a live leader in every group.
pub async fn kill_node(cluster: &mut TestCluster, idx: usize) {
    let dead = cluster.nodes.remove(idx);
    let dead_id = dead.node_id;
    dead.shutdown().await;
    wait_for(
        "the survivors elect new leaders",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || {
            cluster.nodes.iter().all(|node| {
                node.all_group_leaders()
                    .into_iter()
                    .all(|(_, leader)| leader != 0 && leader != dead_id)
            })
        },
    )
    .await;
}

/// The leader `node` sees for `group_id`, or 0 when it sees none.
pub fn leader_of(node: &TestClusterNode, group_id: u64) -> u64 {
    node.all_group_leaders()
        .into_iter()
        .find(|(group, _)| *group == group_id)
        .map(|(_, leader)| leader)
        .unwrap_or(0)
}

/// The number of rows `SELECT id` returns for `collection` on `node`.
pub async fn row_count(node: &TestClusterNode, collection: &str) -> usize {
    let rows = node
        .client
        .simple_query(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT from {collection}: {e}"));
    rows.iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// The first collection name `<prefix>_<i>` for which `pick` holds.
pub fn name_where(prefix: &str, pick: impl Fn(&str) -> bool) -> String {
    (0..4096u32)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| pick(name))
        .unwrap_or_else(|| panic!("no collection name for prefix {prefix}"))
}
