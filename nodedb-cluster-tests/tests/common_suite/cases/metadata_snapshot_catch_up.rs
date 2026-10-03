// SPDX-License-Identifier: BUSL-1.1

//! Catch-up of metadata Raft group 0 by snapshot after its log compacted.
//!
//! A cluster with a low compaction threshold stops one member, runs enough
//! DDL to compact group 0 past that member's log, and brings it back. A
//! fresh node then joins. Neither can be caught up by `AppendEntries`: each
//! installs a group 0 image. Afterwards their replicated catalog rows equal
//! the leader's, a collection purged while the member was down is gone from
//! its storage, and a collection created meanwhile takes writes on it.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb_types::{DatabaseId, TenantId};

const COMPACTION_THRESHOLD: u64 = 4;
const TENANT: u64 = 1;
const PURGED: &str = "mss_purged";
const KEPT_COUNT: usize = 12;

fn kept(i: usize) -> String {
    format!("mss_kept_{i}")
}

/// Replicated tables that legitimately differ across nodes. Each also takes
/// writes on one node outside group 0 apply, and an install keeps this
/// node's higher counter or its own pending rows:
/// - `surrogate_hwm`: each node flushes its own surrogate assigner.
/// - `sync_producer_hwm`: each node flushes its own producer allocator.
/// - `sync_producers`: a registration or fence lands locally before it
///   commits.
/// - `sync_peer_bindings`: a peer-id claim lands locally before it commits.
/// - `tenant_id_hwm`: tenant creation allocates the next id locally.
/// - `topics_ep`: a publish advances the topic's sequence on its node.
/// - `pending_leave_cleanup`: each node removes a row once its own view of
///   the leaver's leases and drains is clear.
const LOCAL_WRITE_TABLES: &[&str] = &[
    "surrogate_hwm",
    "sync_producer_hwm",
    "sync_producers",
    "sync_peer_bindings",
    "tenant_id_hwm",
    "topics_ep",
    "pending_leave_cleanup",
];

/// Whether a row of `label` is node-local and left out of the comparison:
/// the `metadata` counter the credential store writes, and the GAP_FREE log
/// rows each node writes into `sequence_state`.
fn node_local_row(label: &str, key: &[u8]) -> bool {
    match label {
        "metadata" => key == b"next_user_id",
        "sequence_state" => key
            .splitn(3, |byte| *byte == b':')
            .nth(2)
            .is_some_and(|name| name.starts_with(b"log:")),
        _ => false,
    }
}

/// A replicated table's label paired with its `(key, value)` rows.
type LabeledRows = (String, Vec<(Vec<u8>, Vec<u8>)>);

/// Every replicated table except [`LOCAL_WRITE_TABLES`], without node-local
/// rows. Every node holds the same rows once it applied the same entries.
fn compared_rows(node: &TestClusterNode) -> Vec<LabeledRows> {
    node.shared
        .credentials
        .catalog()
        .begin_replicated_read()
        .expect("begin replicated read")
        .dump()
        .expect("dump replicated tables")
        .into_iter()
        .filter(|(label, _)| !LOCAL_WRITE_TABLES.contains(&label.as_str()))
        .map(|(label, rows)| {
            let rows = rows
                .into_iter()
                .filter(|(key, _)| !node_local_row(&label, key))
                .collect();
            (label, rows)
        })
        .collect()
}

async fn keys_of(node: &TestClusterNode, collection: &str) -> Vec<String> {
    let core = node.home_core_of(collection);
    node.document_keys_on_core(core, TenantId::new(TENANT))
        .await
        .into_iter()
        .filter(|key| key.contains(collection))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn lagging_member_and_joiner_catch_up_by_metadata_snapshot() {
    let mut cluster =
        TestCluster::spawn_three_with_compaction_threshold_and_rf(COMPACTION_THRESHOLD, 4)
            .await
            .expect("3-node cluster with low compaction threshold and rf=4");

    // A collection every member stores, purged later while one member is down.
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {PURGED} (id TEXT PRIMARY KEY, payload TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the collection to purge");
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {PURGED} (id, payload) VALUES ('p1', 'v1')"
        ))
        .await
        .expect("insert into the collection to purge");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let lagging_id = cluster.nodes[2].node_id;
    assert!(
        !keys_of(&cluster.nodes[2], PURGED).await.is_empty(),
        "the member to stop stores the collection before it stops"
    );

    let stopped = cluster
        .stop_member(2)
        .await
        .expect("stop the lagging member");

    cluster
        .exec_ddl_on_any_leader(&format!("DROP COLLECTION {PURGED} PURGE"))
        .await
        .expect("purge while the member is down");
    for i in 0..KEPT_COUNT {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {} (id TEXT PRIMARY KEY, payload TEXT) \
                 WITH (engine='document_strict')",
                kept(i)
            ))
            .await
            .expect("create a collection while the member is down");
    }
    wait_for(
        "group 0 compacted on a running member",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .any(|n| n.local_snapshot_index_for_group(0) > 0)
        },
    )
    .await;

    cluster
        .restart_member(stopped)
        .await
        .expect("restart the lagging member");
    let joiner_id = cluster
        .add_learner_node()
        .await
        .expect("add a joining node")
        .node_id;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    for caught_up in [lagging_id, joiner_id] {
        let node = cluster
            .nodes
            .iter()
            .find(|n| n.node_id == caught_up)
            .expect("caught-up node present");
        wait_for(
            "the caught-up node installed a group 0 snapshot",
            Duration::from_secs(30),
            Duration::from_millis(50),
            || node.local_snapshot_index_for_group(0) > 0,
        )
        .await;
        let leader = &cluster.nodes[0];
        wait_for(
            "the caught-up node's replicated rows equal the leader's",
            Duration::from_secs(30),
            Duration::from_millis(100),
            || compared_rows(node) == compared_rows(leader),
        )
        .await;
        let catalog = node.shared.credentials.catalog();
        assert!(
            catalog
                .get_collection(DatabaseId::DEFAULT, TENANT, PURGED)
                .expect("read the purged collection")
                .is_none(),
            "node {caught_up}: the purged collection is gone from the catalog"
        );
        for i in 0..KEPT_COUNT {
            assert!(
                catalog
                    .get_collection(DatabaseId::DEFAULT, TENANT, &kept(i))
                    .expect("read a kept collection")
                    .is_some(),
                "node {caught_up}: collection {} arrived with the snapshot",
                kept(i)
            );
        }
    }

    let lagging = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == lagging_id)
        .expect("lagging member present");
    assert!(
        keys_of(lagging, PURGED).await.is_empty(),
        "the purged collection's storage is reclaimed on the lagging member"
    );

    // A collection created while the member was down takes writes there.
    let written = kept(0);
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {written} (id, payload) VALUES ('w1', 'v1')"
        ))
        .await
        .expect("insert into a collection created while the member was down");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let lagging = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == lagging_id)
        .expect("lagging member present");
    assert!(
        !keys_of(lagging, &written).await.is_empty(),
        "the lagging member stores writes to a collection it learned by snapshot"
    );

    cluster.shutdown().await;
}
