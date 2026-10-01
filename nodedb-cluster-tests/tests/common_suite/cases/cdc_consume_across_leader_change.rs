// SPDX-License-Identifier: BUSL-1.1

//! A change-stream consumer keeps its place across a data-group leader
//! change.
//!
//! Every replica routes a committed write's change events into its own
//! buffer at the entry's Raft log position, and `COMMIT OFFSET` is a
//! replicated catalog entry. So:
//!
//! - every replica serves the same events at the same positions;
//! - a consumer that reads part of the stream from the leader, commits, and
//!   resumes on another node after the leader dies receives every event
//!   exactly once, in order.
//!
//! A position taken from a node-local WAL LSN, or an offset kept only on the
//! committing node, fails the resume: the survivor compares the cursor
//! against its own positions and skips or re-reads events.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::consume::{ConsumeError, ConsumeParams, consume_local};
use nodedb_types::DatabaseId;

const COLLECTION: &str = "cdc_failover";
const STREAM: &str = "cdc_failover_feed";
const GROUP: &str = "cdc_failover_readers";
const TENANT: u64 = 1;

/// Rows written before the leader dies.
const BEFORE: usize = 6;
/// Rows written after the leader dies.
const AFTER: usize = 3;
/// Events the consumer reads, from one partition, and commits before the
/// leader dies.
const FIRST_READ: usize = 3;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// One consumed event: its partition, position, and row id.
type Seen = (u32, CdcOffset, String);

/// The events `node` serves after the group's committed offsets, from one
/// partition or from all of them.
fn read_from(node: &TestClusterNode, partition: Option<u32>, limit: usize) -> Vec<Seen> {
    let params = ConsumeParams {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TENANT,
        stream_name: STREAM,
        group_name: GROUP,
        partition,
        limit,
    };
    match consume_local(&node.shared, &params) {
        Ok(result) => result
            .events
            .iter()
            .map(|event| (event.partition, event.position(), event.row_id.clone()))
            .collect(),
        Err(ConsumeError::BufferEmpty(_)) => Vec::new(),
        Err(error) => panic!("node {}: consume failed: {error}", node.node_id),
    }
}

/// Every partition's events `node` serves after the committed offsets.
fn read(node: &TestClusterNode) -> Vec<Seen> {
    read_from(node, None, 1_000)
}

/// `events` by partition, each partition in the order it was served.
/// Replicas interleave partitions in their own arrival order, so only the
/// per-partition sequences are comparable across nodes.
fn by_partition(events: &[Seen]) -> BTreeMap<u32, Vec<(CdcOffset, String)>> {
    let mut out: BTreeMap<u32, Vec<(CdcOffset, String)>> = BTreeMap::new();
    for (partition, position, row_id) in events {
        out.entry(*partition)
            .or_default()
            .push((*position, row_id.clone()));
    }
    out
}

/// Insert `row` through `node`, retrying while the cluster elects a leader.
async fn insert(node: &TestClusterNode, row: usize) {
    let sql = format!("INSERT INTO {COLLECTION} {{ id: 'row-{row}', n: {row} }}");
    let deadline = Instant::now() + CONVERGE;
    loop {
        match node.client.simple_query(&sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(row, %error, "insert not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("insert row-{row}: {error}"),
        }
    }
}

/// The leader of the data group that owns `partition`, as `node` sees it.
fn partition_leader(node: &TestClusterNode, partition: u32) -> (u64, u64) {
    let group = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(partition)
        .expect("partition maps to a data group");
    let leader = node
        .all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0);
    (group, leader)
}

/// The highest position per partition in `events`.
fn tails(events: &[Seen]) -> BTreeMap<u32, CdcOffset> {
    let mut tails = BTreeMap::new();
    for (partition, position, _) in events {
        let tail = tails.entry(*partition).or_insert(CdcOffset::ZERO);
        if *position > *tail {
            *tail = *position;
        }
    }
    tails
}

fn row_number(row_id: &str) -> usize {
    row_id
        .strip_prefix("row-")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unexpected row id {row_id}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_resumes_on_another_node_after_the_leader_dies() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create collection");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CHANGE STREAM {STREAM} ON {COLLECTION}"))
        .await
        .expect("create change stream");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CONSUMER GROUP {GROUP} ON {STREAM}"))
        .await
        .expect("create consumer group");
    wait_for(
        "every node registers the stream and the group",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM)
                    && node
                        .shared
                        .group_registry
                        .get(DatabaseId::DEFAULT, TENANT, STREAM, GROUP)
                        .is_some()
            })
        },
    )
    .await;

    for row in 0..BEFORE {
        insert(&cluster.nodes[0], row).await;
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    wait_for("every replica buffers every event", CONVERGE, STEP, || {
        cluster.nodes.iter().all(|node| read(node).len() == BEFORE)
    })
    .await;

    // A replica-served consume returns the same events, at the same
    // positions, as every other replica, the leader included.
    let reference = by_partition(&read(&cluster.nodes[0]));
    for node in &cluster.nodes {
        assert_eq!(
            by_partition(&read(node)),
            reference,
            "node {} serves a different change sequence",
            node.node_id
        );
    }

    // Consume part of one partition from the leader of its data group.
    let (&partition, expected) = reference.iter().next().expect("one partition");
    let (group, leader) = partition_leader(&cluster.nodes[0], partition);
    let leader_idx = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .unwrap_or_else(|| panic!("no live node leads data group {group}"));
    let first = read_from(&cluster.nodes[leader_idx], Some(partition), FIRST_READ);
    let expected_first: Vec<(CdcOffset, String)> =
        expected.iter().take(FIRST_READ).cloned().collect();
    assert_eq!(
        by_partition(&first).remove(&partition).unwrap_or_default(),
        expected_first
    );

    for (partition, offset) in tails(&first) {
        cluster.nodes[leader_idx]
            .client
            .simple_query(&format!(
                "COMMIT OFFSET PARTITION {partition} AT {offset} ON {STREAM} CONSUMER GROUP {GROUP}"
            ))
            .await
            .unwrap_or_else(|e| panic!("commit offset {offset} on partition {partition}: {e}"));
    }
    let committed = tails(&first);
    wait_for(
        "every node holds the committed offsets",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                committed.iter().all(|(partition, offset)| {
                    node.shared.offset_store.get_offset(
                        DatabaseId::DEFAULT,
                        TENANT,
                        STREAM,
                        GROUP,
                        *partition,
                    ) == *offset
                })
            })
        },
    )
    .await;

    // Kill the leader the consumer read from.
    let dead = cluster.nodes.remove(leader_idx);
    let dead_id = dead.node_id;
    dead.shutdown().await;
    wait_for(
        "the survivors elect a new leader for the data group",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.all_group_leaders()
                    .into_iter()
                    .any(|(id, leader)| id == group && leader != 0 && leader != dead_id)
            })
        },
    )
    .await;

    for row in BEFORE..BEFORE + AFTER {
        insert(&cluster.nodes[0], row).await;
    }
    let remaining = BEFORE - first.len() + AFTER;
    wait_for(
        "every survivor buffers the new events",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| read(node).len() == remaining)
        },
    )
    .await;

    // Resume on a survivor. Both survivors serve the same continuation.
    let resumed = read(&cluster.nodes[0]);
    for node in &cluster.nodes {
        assert_eq!(
            by_partition(&read(node)),
            by_partition(&resumed),
            "survivor {} resumes with a different sequence",
            node.node_id
        );
    }

    // Every row arrives exactly once across the two reads.
    let delivered: Vec<&Seen> = first.iter().chain(resumed.iter()).collect();
    let rows: Vec<usize> = delivered.iter().map(|(_, _, id)| row_number(id)).collect();
    let distinct: BTreeSet<usize> = rows.iter().copied().collect();
    assert_eq!(
        rows.len(),
        BEFORE + AFTER,
        "duplicate or missing events: {rows:?}"
    );
    assert_eq!(
        distinct,
        (0..BEFORE + AFTER).collect::<BTreeSet<_>>(),
        "missing rows: {rows:?}"
    );

    // In order: within each partition, positions rise with the insert order.
    let mut last: BTreeMap<u32, (CdcOffset, usize)> = BTreeMap::new();
    for (partition, position, id) in delivered {
        let row = row_number(id);
        if let Some((previous, previous_row)) = last.get(partition) {
            assert!(
                position > previous && row > *previous_row,
                "partition {partition}: row-{row} at {position} follows row-{previous_row} at {previous}"
            );
        }
        last.insert(*partition, (*position, row));
    }

    cluster.shutdown().await;
}
