// SPDX-License-Identifier: BUSL-1.1
//! Cross-node replication of version-history checkpoint DDL.
//!
//! `CREATE CHECKPOINT`, `DROP CHECKPOINT`, and the `COMPACT HISTORY` range
//! delete propose `CatalogEntry::PutCheckpoint`, `DeleteCheckpoint`, and
//! `DeleteCheckpointsBefore` through the metadata raft group. Every node writes
//! or removes the `_system.checkpoints` row. Checkpoints have no in-memory
//! mirror, so the durable row is the whole observation: `SHOW VERSIONS` and
//! `COMPACT HISTORY` read it straight from the catalog. A checkpoint that
//! reached only the executing node resolves nowhere else.

use crate::common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use nodedb::control::security::catalog::types::CheckpointRecord;

/// Document collection every checkpoint targets.
const COLLECTION: &str = "checkpoint_cross_docs";
/// Document every checkpoint targets.
const DOC: &str = "doc-1";
/// Tenant of the harness superuser.
const TENANT: u64 = 1;
/// Deadline for follower convergence.
const CONVERGE: Duration = Duration::from_secs(10);
/// Poll step for follower convergence.
const STEP: Duration = Duration::from_millis(50);

/// Index of the metadata-group leader.
fn pick_leader_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id == leader_id)
        .expect("the metadata leader must be one of the spawned nodes")
}

/// Index of a node that is not the metadata-group leader.
fn pick_follower_index(cluster: &TestCluster) -> usize {
    let leader_id = leader_id(cluster);
    cluster
        .nodes
        .iter()
        .position(|n| n.node_id != leader_id)
        .expect("a 3-node cluster must have a follower")
}

/// Node id every node agrees is the metadata-group leader.
fn leader_id(cluster: &TestCluster) -> u64 {
    cluster
        .nodes
        .iter()
        .map(|n| n.metadata_group_leader())
        .find(|&id| id != 0)
        .expect("at least one node must report a non-zero leader id")
}

/// The durable `_system.checkpoints` row this node holds for `name`.
fn stored_checkpoint(node: &TestClusterNode, name: &str) -> Option<CheckpointRecord> {
    node.shared
        .credentials
        .catalog()
        .get_checkpoint(TENANT, COLLECTION, DOC, name)
        .expect("read the checkpoint row")
}

/// Names of every durable checkpoint row this node holds for [`DOC`], sorted.
fn stored_names(node: &TestClusterNode) -> Vec<String> {
    let mut names: Vec<String> = node
        .shared
        .credentials
        .catalog()
        .list_checkpoints(TENANT, COLLECTION, DOC, 0)
        .expect("list checkpoints")
        .into_iter()
        .map(|r| r.checkpoint_name)
        .collect();
    names.sort();
    names
}

/// Checkpoint names `SHOW VERSIONS` reports on this node, sorted.
/// The handler reads the catalog, so this is the user-visible view of the row.
async fn shown_versions(node: &TestClusterNode) -> Vec<String> {
    let messages = node
        .client
        .simple_query(&format!("SHOW VERSIONS OF {COLLECTION} WHERE id = '{DOC}'"))
        .await
        .expect("SHOW VERSIONS");
    let mut names: Vec<String> = messages
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get("checkpoint_name").map(str::to_string)
            }
            _ => None,
        })
        .collect();
    names.sort();
    names
}

/// Current wall clock in seconds, the resolution `created_at` carries.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Block until the wall clock leaves `second`.
///
/// `created_at` has one-second resolution, so two checkpoints need two seconds
/// to land on distinct sides of a range-delete boundary.
async fn wait_past_second(second: u64) {
    wait_for(
        "the wall clock to leave the second the last checkpoint recorded",
        Duration::from_secs(5),
        STEP,
        || now_secs() > second,
    )
    .await;
}

/// A Loro snapshot inserting `name=alice` on `COLLECTION`/`DOC`, hex encoded.
/// `crdt_apply` hex-decodes it before merging into the tenant document.
fn build_delta_hex() -> String {
    let doc = loro::LoroDoc::new();
    let collection = doc.get_map(COLLECTION);
    let row = collection
        .insert_container(DOC, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", "alice").expect("field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");
    hex::encode(delta)
}

/// Run `CREATE CHECKPOINT` on this node and return the row it wrote.
async fn create_checkpoint_on(node: &TestClusterNode, name: &str) -> CheckpointRecord {
    node.client
        .simple_query(&format!(
            "CREATE CHECKPOINT '{name}' ON {COLLECTION} WHERE id = '{DOC}'"
        ))
        .await
        .expect("CREATE CHECKPOINT on the metadata leader");
    stored_checkpoint(node, name).expect("the executing node writes its own checkpoint row")
}

/// Bring up a cluster holding a replicated document with CRDT history.
/// The version vector a checkpoint captures is that document's.
async fn cluster_with_document() -> TestCluster {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION for the checkpoint target");

    let leader_index = pick_leader_index(&cluster);
    let delta_hex = build_delta_hex();
    cluster.nodes[leader_index]
        .client
        .simple_query(&format!(
            "SELECT crdt_apply('{COLLECTION}', '{DOC}', '{delta_hex}')"
        ))
        .await
        .expect("seed one CRDT delta so the checkpoint captures a real version");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;
    cluster
}

/// Wait until this node holds a durable row for `name`.
async fn wait_for_row(cluster: &TestCluster, index: usize, name: &str) {
    wait_for(
        "the follower to write the replicated checkpoint row",
        CONVERGE,
        STEP,
        || stored_checkpoint(&cluster.nodes[index], name).is_some(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn checkpoint_created_on_leader_reaches_follower() {
    let cluster = cluster_with_document().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];
    let name = "launch-ready";

    // Baseline: the follower carries no checkpoint row and shows no version.
    assert!(stored_checkpoint(follower, name).is_none());
    assert!(shown_versions(follower).await.is_empty());

    let on_leader = create_checkpoint_on(&cluster.nodes[leader_index], name).await;
    wait_for_row(&cluster, follower_index, name).await;

    let on_follower = stored_checkpoint(follower, name)
        .expect("PutCheckpoint must write the row on the follower, not only the leader");
    assert_eq!(
        on_follower.version_vector_json, on_leader.version_vector_json,
        "the follower must resolve the same version the leader captured"
    );
    assert_eq!(on_follower.created_at, on_leader.created_at);
    assert_eq!(on_follower.created_by, on_leader.created_by);
    assert_eq!(on_follower.collection, COLLECTION);
    assert_eq!(on_follower.doc_id, DOC);

    assert_eq!(
        shown_versions(follower).await,
        vec![name.to_string()],
        "SHOW VERSIONS on follower node {} must report the leader's checkpoint",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn checkpoint_dropped_on_leader_reaches_follower() {
    let cluster = cluster_with_document().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let follower = &cluster.nodes[follower_index];
    let dropped = "drop-me";
    let kept = "keep-me";

    create_checkpoint_on(&cluster.nodes[leader_index], dropped).await;
    create_checkpoint_on(&cluster.nodes[leader_index], kept).await;
    wait_for_row(&cluster, follower_index, dropped).await;
    wait_for_row(&cluster, follower_index, kept).await;

    // Baseline: the follower holds both replicated rows.
    assert_eq!(
        stored_names(follower),
        vec![dropped.to_string(), kept.to_string()]
    );

    cluster.nodes[leader_index]
        .client
        .simple_query(&format!(
            "DROP CHECKPOINT '{dropped}' ON {COLLECTION} WHERE id = '{DOC}'"
        ))
        .await
        .expect("DROP CHECKPOINT on the metadata leader");

    wait_for(
        "the follower to remove the dropped checkpoint row",
        CONVERGE,
        STEP,
        || stored_checkpoint(&cluster.nodes[follower_index], dropped).is_none(),
    )
    .await;

    assert_eq!(
        stored_names(follower),
        vec![kept.to_string()],
        "DeleteCheckpoint must remove one row and keep its sibling on follower node {}",
        follower.node_id,
    );
    assert_eq!(
        shown_versions(follower).await,
        vec![kept.to_string()],
        "SHOW VERSIONS on follower node {} must drop the removed checkpoint",
        follower.node_id,
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn compact_history_on_leader_keeps_the_boundary_on_follower() {
    let cluster = cluster_with_document().await;
    let leader_index = pick_leader_index(&cluster);
    let follower_index = pick_follower_index(&cluster);
    let leader = &cluster.nodes[leader_index];
    let follower = &cluster.nodes[follower_index];

    // Three rows on three distinct seconds: one below the boundary, the
    // boundary itself, one above.
    let older = create_checkpoint_on(leader, "older").await;
    wait_past_second(older.created_at).await;
    let boundary = create_checkpoint_on(leader, "boundary").await;
    wait_past_second(boundary.created_at).await;
    let newer = create_checkpoint_on(leader, "newer").await;
    assert!(
        older.created_at < boundary.created_at && boundary.created_at < newer.created_at,
        "the three checkpoints must carry strictly increasing timestamps"
    );

    for name in ["older", "boundary", "newer"] {
        wait_for_row(&cluster, follower_index, name).await;
    }

    // Baseline: the follower holds all three replicated rows.
    assert_eq!(
        stored_names(follower),
        vec![
            "boundary".to_string(),
            "newer".to_string(),
            "older".to_string()
        ]
    );

    leader
        .client
        .simple_query(&format!(
            "COMPACT HISTORY ON {COLLECTION} WHERE id = '{DOC}' BEFORE 'boundary'"
        ))
        .await
        .expect("COMPACT HISTORY on the metadata leader");

    wait_for(
        "the follower to apply the replicated range delete",
        CONVERGE,
        STEP,
        || stored_checkpoint(&cluster.nodes[follower_index], "older").is_none(),
    )
    .await;

    // The boundary is exclusive: `created_at < before_timestamp` goes, the row
    // at the boundary stays.
    let kept = stored_checkpoint(follower, "boundary").expect(
        "DeleteCheckpointsBefore is exclusive: the boundary row must survive on the follower",
    );
    assert_eq!(kept.created_at, boundary.created_at);
    assert_eq!(
        stored_names(follower),
        vec!["boundary".to_string(), "newer".to_string()],
        "the range delete must remove exactly the rows below the boundary on follower node {}",
        follower.node_id,
    );
    assert_eq!(
        shown_versions(follower).await,
        vec!["boundary".to_string(), "newer".to_string()],
        "SHOW VERSIONS on follower node {} must reflect the replicated compaction",
        follower.node_id,
    );

    cluster.shutdown().await;
}
