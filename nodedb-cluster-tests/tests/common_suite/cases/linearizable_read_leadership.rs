// SPDX-License-Identifier: BUSL-1.1
//! A linearizable read is served only by a node that can prove it leads.
//!
//! Believing you are the leader is not the same as being one. A partition
//! does not notify the node it cut off, so a deposed leader keeps answering
//! from a log the rest of the cluster has already moved past. Every read
//! below asks the same question: does this node answer from a belief, or
//! from a quorum?
//!
//! The consistency level is what decides it. `strong` — the default — must
//! reach a confirmed leader. `bounded_staleness` accepts a replica, but only
//! one that can show how far behind the leader it is. `eventual` accepts any
//! replica at all, so it keeps being served when nothing else is. All three
//! are asserted: one alone cannot tell a working guarantee apart from a read
//! path that is broken.
//!
//! A leader can answer from its lease without asking the quorum. The lease
//! ends `election_timeout_min` minus a drift margin after the quorum last
//! answered, so an isolated leader must refuse once that has passed.

use crate::common;
use common::cluster_harness::{TestCluster, wait::wait_for};

use std::time::Duration;

const COLLECTION: &str = "linread";

/// `election_timeout_min` of the harness cluster tuning. A leader lease never
/// outlives it.
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(500);

/// How long an isolated leader waits before the strong read that must be
/// refused. A lease ends within one `ELECTION_TIMEOUT_MIN`. Three give a loaded
/// test host room for scheduling delay.
const LEASE_EXPIRY_MARGIN: u32 = 3;

/// Bring up three nodes holding one row.
async fn seeded_cluster() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (id TEXT PRIMARY KEY, val TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create collection");

    let insert = format!("INSERT INTO {COLLECTION} (id, val) VALUES ('a', 'v')");
    wait_for(
        "seed row accepted",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[0].client.simple_query(&insert))
            })
            .is_ok()
        },
    )
    .await;

    cluster
}

/// The baseline: with every node up, the default read is served.
///
/// Without this, a test that only asserts refusal cannot tell leadership
/// confirmation from a read path that is simply broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_linearizable_read_is_served_while_the_quorum_is_healthy() {
    let cluster = seeded_cluster().await;

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "linearizable read served",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[0].client.simple_query(&select))
            })
            .is_ok()
        },
    )
    .await;

    cluster.shutdown().await;
}

/// The case the confirmation exists for: one node left, no quorum to ask.
///
/// The survivor's routing table still names a leader for a while, so the
/// read has everything it needs to be answered from local state — and must
/// not be.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_without_a_quorum_is_refused_rather_than_answered_locally() {
    let mut cluster = seeded_cluster().await;

    let survivor = cluster.nodes.remove(0);
    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "read refused once the quorum is gone",
        Duration::from_secs(20),
        Duration::from_millis(250),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(survivor.client.simple_query(&select))
            })
            .is_err()
        },
    )
    .await;

    survivor.shutdown().await;
}

/// `eventual` is the caller saying a local replica is acceptable, so the
/// same query on the same quorum-less node keeps being served.
///
/// This is what pins the refusal above to the consistency level rather than
/// to the node having become unable to answer anything at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_eventual_read_is_still_served_without_a_quorum() {
    let mut cluster = seeded_cluster().await;

    let survivor = cluster.nodes.remove(0);
    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }

    survivor
        .client
        .simple_query("SET default_read_consistency = 'eventual'")
        .await
        .expect("set session consistency");

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    survivor
        .client
        .simple_query(&select)
        .await
        .expect("an eventual read accepts the local replica");

    survivor.shutdown().await;
}

/// Index of a node leading no Raft group, if the cluster has one.
fn follower_index(cluster: &TestCluster) -> Option<usize> {
    cluster.nodes.iter().position(|node| {
        let leaders = node.all_group_leaders();
        !leaders.is_empty() && leaders.iter().all(|&(_, leader)| leader != node.node_id)
    })
}

/// A replica in normal contact with its leader serves a bounded-staleness
/// read locally.
///
/// The freshness check must admit a healthy replica, not just reject a
/// lagging one — a bound that refuses everything would look identical to a
/// bound that works, and would send every replica read to the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_in_contact_serves_a_bounded_staleness_read() {
    let cluster = seeded_cluster().await;

    let Some(idx) = follower_index(&cluster) else {
        cluster.shutdown().await;
        return;
    };

    cluster.nodes[idx]
        .client
        .simple_query("SET default_read_consistency = 'bounded_staleness:5s'")
        .await
        .expect("set session consistency");

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "bounded-staleness read served from a replica in contact",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[idx].client.simple_query(&select))
            })
            .is_ok()
        },
    )
    .await;

    cluster.shutdown().await;
}

/// Index of the node that leads the most Raft groups.
fn busiest_leader_index(cluster: &TestCluster) -> usize {
    (0..cluster.nodes.len())
        .max_by_key(|&i| {
            let node = &cluster.nodes[i];
            node.all_group_leaders()
                .iter()
                .filter(|&&(_, leader)| leader == node.node_id)
                .count()
        })
        .unwrap_or(0)
}

/// `val` of the first row in `messages`.
fn first_val(messages: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get("val").map(str::to_string),
        _ => None,
    })
}

/// A leader cut off from its quorum serves from its lease only until the
/// lease ends. Past it the followers can already have elected a successor, so
/// the next strong read must be refused, with no retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_refuses_strong_reads_once_its_lease_expires() {
    let mut cluster = seeded_cluster().await;
    let old = busiest_leader_index(&cluster);

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    wait_for(
        "strong read served by the leader while the quorum is healthy",
        Duration::from_secs(15),
        Duration::from_millis(200),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[old].client.simple_query(&select))
            })
            .is_ok()
        },
    )
    .await;

    let isolated = cluster.nodes.remove(old);
    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }
    // No quorum has answered since the shutdowns finished, so every lease
    // anchor predates this point.
    tokio::time::sleep(ELECTION_TIMEOUT_MIN * LEASE_EXPIRY_MARGIN).await;

    let read = isolated.client.simple_query(&select).await;
    assert!(
        read.is_err(),
        "an isolated leader served a strong read past its lease: {read:?}"
    );

    isolated.shutdown().await;
}

/// A write the new leader acknowledged is visible to every strong read that
/// is served after it. A read can be refused while routing settles on the new
/// leader. It must never return the value from before the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_acknowledged_by_the_new_leader_is_visible_to_the_next_strong_read() {
    let mut cluster = seeded_cluster().await;
    let old = busiest_leader_index(&cluster);
    let old_leader = cluster.nodes.remove(old);
    old_leader.shutdown().await;

    let update = format!("UPDATE {COLLECTION} SET val = 'w' WHERE id = 'a'");
    wait_for(
        "write acknowledged by the new leader",
        Duration::from_secs(20),
        Duration::from_millis(250),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(cluster.nodes[0].client.simple_query(&update))
            })
            .is_ok()
        },
    )
    .await;

    let select = format!("SELECT val FROM {COLLECTION} WHERE id = 'a'");
    for node in &cluster.nodes {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match node.client.simple_query(&select).await {
                Ok(messages) => {
                    assert_eq!(
                        first_val(&messages).as_deref(),
                        Some("w"),
                        "node {} served a strong read that misses an acknowledged write",
                        node.node_id
                    );
                    break;
                }
                Err(error) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "node {} never served the strong read: {error}",
                        node.node_id
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    for node in cluster.nodes.drain(..) {
        node.shutdown().await;
    }
}
