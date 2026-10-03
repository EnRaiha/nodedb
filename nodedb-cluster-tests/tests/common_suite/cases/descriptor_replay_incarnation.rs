// SPDX-License-Identifier: BUSL-1.1

//! Group-0 metadata replay after a graceful restart of a single-node
//! metadata group keeps the latest descriptor of every name.
//!
//! - A historical descriptor entry never lowers the persisted latest
//!   version.
//! - A replayed `PurgeCollection` or `DeleteMaterializedView` fences to the
//!   incarnation it dropped and leaves a recreated one intact.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::wait_for_single_node_ready;
use common::cluster_harness::{TestClusterNode, read_once_a_leader_exists, wait_for};

const TENANT: u64 = 1;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn historical_descriptor_entries_replay_without_regressing_the_latest_version() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

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
    wait_for_single_node_ready(&node).await;

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
/// all before a graceful restart of the single-node metadata
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
    wait_for_single_node_ready(&node).await;

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
    wait_for_single_node_ready(&node).await;

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
    wait_for_single_node_ready(&node).await;

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
    wait_for_single_node_ready(&node).await;

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
