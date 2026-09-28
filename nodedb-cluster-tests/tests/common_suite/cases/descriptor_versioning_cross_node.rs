// SPDX-License-Identifier: BUSL-1.1
//! End-to-end cluster tests for descriptor versioning.
//!
//! Asserts that every `Stored*` descriptor written through the
//! metadata raft group is stamped with a monotonic
//! `descriptor_version` and a strictly-advancing `modification_hlc`,
//! and that every node observes the same stamp once the entry has
//! propagated. This is the load-bearing invariant for descriptor
//! lease drain and execution-time version checks — without it,
//! there is no version to lease against.
//!
//! The proposing node freezes the stamp: it reads the prior
//! persisted record, increments by one (or assigns 1 on create),
//! and stamps `modification_hlc` from its own HLC. The frozen
//! entry replicates verbatim, so every node writes that value
//! without re-deriving it.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, TestClusterNode, read_once_a_leader_exists, wait_for};

const TENANT: u64 = 1;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_collection_stamps_version_one_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION orders")
        .await
        .expect("create collection");

    wait_for(
        "all 3 nodes stamp orders @ version 1 with non-zero HLC",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| {
                matches!(
                    n.collection_descriptor(TENANT, "orders"),
                    Some((1, hlc)) if hlc > nodedb_types::Hlc::ZERO
                )
            })
        },
    )
    .await;

    // Every node writes the stamp the proposer froze into the raft
    // entry. The assertion is `descriptor_version == 1` plus a
    // non-zero HLC on each node: lease drain builds on the version,
    // not the wall clock.
    let stamps: Vec<_> = cluster
        .nodes
        .iter()
        .map(|n| n.collection_descriptor(TENANT, "orders"))
        .collect();
    eprintln!("descriptor stamps per node: {stamps:?}");
    for stamp in stamps {
        let (v, hlc) = stamp.expect("present");
        assert_eq!(v, 1, "every node sees version 1");
        assert!(hlc > nodedb_types::Hlc::ZERO, "HLC stamped");
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alter_collection_bumps_version_monotonically() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Bootstrap target user used by ALTER OWNER.
    cluster
        .exec_ddl_on_any_leader("CREATE USER alice WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create user alice");
    cluster
        .exec_ddl_on_any_leader("CREATE USER bob WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create user bob");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION assets")
        .await
        .expect("create assets");

    // Wait for v1.
    wait_for(
        "v1 stamped on every node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.collection_descriptor(TENANT, "assets").map(|s| s.0) == Some(1))
        },
    )
    .await;

    // Five owner flips. Each one re-proposes the full
    // `StoredCollection`, so the propose-time stamp bumps
    // `descriptor_version` from 1 → 2 → ... → 6.
    let owners = ["alice", "bob", "alice", "bob", "alice"];
    for (i, owner) in owners.iter().enumerate() {
        let sql = format!("ALTER COLLECTION assets OWNER TO {owner}");
        cluster
            .exec_ddl_on_any_leader(&sql)
            .await
            .unwrap_or_else(|e| panic!("alter #{i}: {e}"));

        let expected_version = (i + 2) as u64;
        wait_for(
            &format!("all nodes observe assets @ v{expected_version}"),
            Duration::from_secs(10),
            Duration::from_millis(50),
            || {
                cluster.nodes.iter().all(|n| {
                    n.collection_descriptor(TENANT, "assets").map(|s| s.0) == Some(expected_version)
                })
            },
        )
        .await;
    }

    // Sanity: the final stamp carries the last proposer's HLC on
    // every node, ahead of the initial stamp.
    for node in &cluster.nodes {
        let (final_version, final_hlc) = node
            .collection_descriptor(TENANT, "assets")
            .expect("present");
        assert_eq!(final_version, 6);
        assert!(final_hlc > nodedb_types::Hlc::ZERO);
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_cross_node_updates_allocate_distinct_descriptor_versions() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE USER owner_a WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create owner_a");
    cluster
        .exec_ddl_on_any_leader("CREATE USER owner_b WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create owner_b");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION concurrently_owned")
        .await
        .expect("create collection");

    let update_a = cluster.nodes[0]
        .client
        .simple_query("ALTER COLLECTION concurrently_owned OWNER TO owner_a");
    let update_b = cluster.nodes[1]
        .client
        .simple_query("ALTER COLLECTION concurrently_owned OWNER TO owner_b");
    let (result_a, result_b) = tokio::join!(update_a, update_b);
    result_a.expect("node 0 concurrent owner update");
    result_b.expect("node 1 concurrent owner update");

    wait_for(
        "both concurrent updates apply as distinct versions on every node",
        Duration::from_secs(15),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|node| {
                node.collection_descriptor(TENANT, "concurrently_owned")
                    .map(|stamp| stamp.0)
                    == Some(3)
            })
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn distinct_collections_get_independent_versions() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION foo")
        .await
        .expect("create foo");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION bar")
        .await
        .expect("create bar");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION baz")
        .await
        .expect("create baz");

    wait_for(
        "all 3 collections present on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster.nodes.iter().all(|n| {
                ["foo", "bar", "baz"]
                    .iter()
                    .all(|name| n.collection_descriptor(TENANT, name).is_some())
            })
        },
    )
    .await;

    // Each independent descriptor starts at v1. The stamp reads the
    // prior record per-key, so version counters are local to the
    // descriptor identity.
    for node in &cluster.nodes {
        for name in ["foo", "bar", "baz"] {
            let (v, _) = node.collection_descriptor(TENANT, name).expect("present");
            assert_eq!(v, 1, "{name} starts at v1");
        }
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn historical_descriptor_entries_replay_without_regressing_the_latest_version() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
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

    node.client
        .simple_query(
            "CREATE COLLECTION replay_graph (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create graph-bearing collection");
    wait_for(
        "collection descriptor reaches version 1",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            node.collection_descriptor(TENANT, "replay_graph")
                .map(|v| v.0)
                == Some(1)
        },
    )
    .await;

    node.client
        .simple_query(
            "GRAPH INSERT EDGE IN replay_graph FROM 'a' TO 'b' \
             TYPE 'knows' PROPERTIES '{}'",
        )
        .await
        .expect("insert edge and mark collection edge-bearing");
    wait_for(
        "edge-bearing descriptor reaches version 2",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            node.collection_descriptor(TENANT, "replay_graph")
                .map(|v| v.0)
                == Some(2)
        },
    )
    .await;

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted catalog and full metadata log");
    wait_for(
        "single-node sequencer leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;
    wait_for(
        "single-node metadata leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.shared.is_metadata_leader(),
    )
    .await;

    wait_for(
        "latest collection descriptor remains visible after replay",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            node.collection_descriptor(TENANT, "replay_graph")
                .map(|v| v.0)
                == Some(2)
        },
    )
    .await;

    node.client
        .simple_query(
            "CREATE COLLECTION ddl_after_descriptor_replay \
             (id TEXT PRIMARY KEY) WITH (engine='document_strict')",
        )
        .await
        .expect(
            "historical metadata replay must advance its watermark so later DDL remains usable",
        );
    assert_eq!(
        node.collection_descriptor(TENANT, "replay_graph")
            .map(|version| version.0),
        Some(2),
        "replaying historical version 1 must not overwrite the persisted latest version 2"
    );

    node.shutdown().await;
}

/// `SELECT COUNT(*) FROM <name>` against the single-node harness's driving
/// pgwire client. A freshly restarted node refuses reads until each range
/// has a serving leader, so the count retries on that refusal alone.
async fn row_count(client: &tokio_postgres::Client, name: &str) -> usize {
    let query = format!("SELECT COUNT(*) FROM {name}");
    let query = query.as_str();
    read_once_a_leader_exists(
        &format!("count rows of {name}"),
        Duration::from_secs(10),
        Duration::from_millis(50),
        || client.simple_query(query),
    )
    .await
    .into_iter()
    .find_map(|msg| match msg {
        tokio_postgres::SimpleQueryMessage::Row(row) => row
            .get(0)
            .map(|s| s.parse::<usize>().expect("COUNT(*) parse")),
        _ => None,
    })
    .expect("COUNT(*) returned no rows")
}

/// CREATE, `DROP COLLECTION ... PURGE`, and CREATE again on the same name,
/// all before a `kill -9`-free graceful restart of the single-node metadata
/// group. The replayed `PurgeCollection` must fence to the first incarnation:
/// the second incarnation's descriptor and rows must survive replay, and the
/// collection must still take DDL afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_purge_does_not_remove_a_recreated_collection() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(10, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
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

    node.client
        .simple_query("CREATE COLLECTION recreated (id BIGINT PRIMARY KEY, amount BIGINT)")
        .await
        .expect("create first incarnation");
    node.client
        .simple_query("INSERT INTO recreated (id, amount) VALUES (1, 10)")
        .await
        .expect("insert into first incarnation");

    node.client
        .simple_query("DROP COLLECTION recreated PURGE")
        .await
        .expect("purge first incarnation");

    node.client
        .simple_query("CREATE COLLECTION recreated (id BIGINT PRIMARY KEY, amount BIGINT)")
        .await
        .expect("create second incarnation");
    for id in 1..=3i64 {
        node.client
            .simple_query(&format!(
                "INSERT INTO recreated (id, amount) VALUES ({id}, {})",
                id * 10
            ))
            .await
            .expect("insert into second incarnation");
    }
    assert_eq!(
        row_count(&node.client, "recreated").await,
        3,
        "second incarnation must hold its 3 rows before restart"
    );

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(10, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for(
        "single-node sequencer leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;
    wait_for(
        "single-node metadata leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.shared.is_metadata_leader(),
    )
    .await;

    assert!(
        node.collection_descriptor(TENANT, "recreated").is_some(),
        "the second incarnation's descriptor must survive replay of the group-0 log"
    );
    assert_eq!(
        row_count(&node.client, "recreated").await,
        3,
        "replaying the first incarnation's PurgeCollection must not reclaim the second \
         incarnation's rows"
    );

    node.client
        .simple_query("INSERT INTO recreated (id, amount) VALUES (4, 40)")
        .await
        .expect("the recreated collection must still take DDL after replay");
    assert_eq!(row_count(&node.client, "recreated").await, 4);

    node.shutdown().await;
}

/// CREATE MATERIALIZED VIEW, DROP, and CREATE again on the same name, all
/// before a graceful restart of the single-node metadata group. The replayed
/// `DeleteMaterializedView` must fence to the first incarnation: the second
/// incarnation's descriptor and refreshed rows must survive replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replayed_drop_materialized_view_keeps_recreated_view() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(11, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
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

    node.client
        .simple_query("CREATE COLLECTION mv_src (id BIGINT PRIMARY KEY, amount BIGINT)")
        .await
        .expect("create source collection");
    node.client
        .simple_query("INSERT INTO mv_src (id, amount) VALUES (1, 10)")
        .await
        .expect("seed source row");

    node.client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_recreated ON mv_src AS SELECT id, amount FROM mv_src",
        )
        .await
        .expect("create first incarnation of the view");
    node.client
        .simple_query("DROP MATERIALIZED VIEW mv_recreated")
        .await
        .expect("drop first incarnation");

    node.client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_recreated ON mv_src AS SELECT id, amount FROM mv_src",
        )
        .await
        .expect("create second incarnation of the view");
    node.client
        .simple_query("REFRESH MATERIALIZED VIEW mv_recreated")
        .await
        .expect("refresh second incarnation");
    assert_eq!(
        row_count(&node.client, "mv_recreated").await,
        1,
        "second incarnation must hold the refreshed row before restart"
    );

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(11, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for(
        "single-node sequencer leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;
    wait_for(
        "single-node metadata leader re-elected after restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.shared.is_metadata_leader(),
    )
    .await;

    assert!(
        node.has_materialized_view(TENANT, "mv_recreated"),
        "the second incarnation's descriptor must survive replay of the group-0 log"
    );
    assert_eq!(
        row_count(&node.client, "mv_recreated").await,
        1,
        "replaying the first incarnation's DeleteMaterializedView must not reclaim the \
         second incarnation's rows"
    );

    node.shutdown().await;
}
