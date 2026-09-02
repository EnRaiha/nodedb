// SPDX-License-Identifier: BUSL-1.1
//! Cross-node oplog compaction for `COMPACT HISTORY`.
//!
//! `COMPACT HISTORY` proposes `CatalogEntry::CompactHistory`. Apply removes
//! the checkpoint rows below the boundary, and post-apply dispatches
//! `CrdtOp::CompactAtVersion` to every core of every node that applied the
//! entry. The divergence this rules out: the checkpoint rows replicate while
//! the oplog compaction runs on the executing node alone, so one node reclaims
//! its history and its peers keep serving reads at versions the cluster agreed
//! to discard. `checkpoint_cross_node` covers the row half; the row half alone
//! cannot see the dispatch, because a node that never compacts still deletes
//! the rows.
//!
//! The observation is the follower's own Data Plane. Compaction replaces the
//! node's Loro document with a shallow snapshot, and Loro refuses `fork_at` on
//! a shallow document, so a historical read below the boundary answers with a
//! value before the compaction and names the shallow document after it. The
//! oplog version vector survives compaction, so the same node's counters pin
//! that the live document kept every operation.

use crate::common;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

use nodedb::control::security::catalog::types::CheckpointRecord;

/// CRDT collection carrying the revised document.
const COLLECTION: &str = "crdt_compact_docs";
/// Document every revision and every checkpoint targets.
const DOC: &str = "doc-1";
/// Tenant of the harness superuser.
const TENANT: u64 = 1;
/// Deadline for a replicated effect to reach a follower.
const CONVERGE: Duration = Duration::from_secs(15);
/// Poll step while waiting for that effect.
const STEP: Duration = Duration::from_millis(100);

/// Title written before the checkpoint the compaction cuts at.
const REV_BELOW: &str = "rev-below";
/// Title written at the checkpoint the compaction cuts at.
const REV_AT: &str = "rev-at";
/// Title written after that checkpoint.
const REV_ABOVE: &str = "rev-above";

/// One Loro peer authoring every revision of [`DOC`].
///
/// A single peer keeps the revisions on one operation chain, so each
/// [`Author::revise`] extends the collection's oplog instead of opening a
/// branch. Each call exports only the operations added since the last one, so
/// the node merges a real incremental history rather than one flat snapshot.
struct Author {
    doc: loro::LoroDoc,
    row: loro::LoroMap,
    sent: loro::VersionVector,
}

impl Author {
    fn new() -> Self {
        let doc = loro::LoroDoc::new();
        let row = doc
            .get_map(COLLECTION)
            .insert_container(DOC, loro::LoroMap::new())
            .expect("row container");
        doc.commit();
        Self {
            doc,
            row,
            sent: loro::VersionVector::default(),
        }
    }

    /// Set `title` and return the hex delta carrying only the new operations.
    fn revise(&mut self, title: &str) -> String {
        self.row
            .insert("title", title)
            .expect("set the title field");
        self.doc.commit();
        let delta = self
            .doc
            .export(loro::ExportMode::updates(&self.sent))
            .expect("export the incremental Loro updates");
        self.sent = self.doc.oplog_vv();
        hex::encode(delta)
    }
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

/// The message a pgwire error carries, SQLSTATE included.
fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// The durable `_system.checkpoints` row this node holds for `name`.
fn stored_checkpoint(node: &TestClusterNode, name: &str) -> Option<CheckpointRecord> {
    node.shared
        .credentials
        .catalog()
        .get_checkpoint(TENANT, COLLECTION, DOC, name)
        .expect("read the checkpoint row")
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
/// to land on distinct sides of the compaction boundary.
async fn wait_past_second(second: u64) {
    wait_for(
        "the wall clock to leave the second the last checkpoint recorded",
        Duration::from_secs(5),
        STEP,
        || now_secs() > second,
    )
    .await;
}

/// Run `CREATE CHECKPOINT` on this node and return the row it wrote.
///
/// The handler resolves the version vector over this node's own SPSC bridge,
/// so the row carries the executing node's oplog counters. A follower forwards
/// the proposal and applies the committed row from its applier, so the row is
/// awaited rather than read straight back.
async fn create_checkpoint_on(node: &TestClusterNode, name: &str) -> CheckpointRecord {
    node.client
        .simple_query(&format!(
            "CREATE CHECKPOINT '{name}' ON {COLLECTION} WHERE id = '{DOC}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE CHECKPOINT '{name}': {}", pg_detail(&e)));
    wait_for(
        &format!("node {} to apply the '{name}' checkpoint row", node.node_id),
        CONVERGE,
        STEP,
        || stored_checkpoint(node, name).is_some(),
    )
    .await;
    stored_checkpoint(node, name).expect("the executing node holds its own checkpoint row")
}

/// Per-peer operation counters a version-vector envelope carries.
///
/// The envelope's `vv` field serializes from a `HashMap`, so its key order is
/// not stable across calls. A `BTreeMap` compares the counters themselves.
fn version_counters(version_vector_json: &str) -> BTreeMap<String, i64> {
    let envelope: serde_json::Value =
        serde_json::from_str(version_vector_json).expect("version vector JSON");
    envelope
        .get("vv")
        .and_then(|vv| vv.as_object())
        .expect("the envelope must carry a vv object")
        .iter()
        .map(|(peer, counter)| {
            (
                peer.clone(),
                counter.as_i64().expect("counters are integers"),
            )
        })
        .collect()
}

/// Apply one revision through `node` and wait for every replica to apply it.
async fn apply_revision(cluster: &TestCluster, node: &TestClusterNode, delta_hex: &str) {
    node.client
        .simple_query(&format!(
            "SELECT crdt_apply('{COLLECTION}', '{DOC}', '{delta_hex}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("crdt_apply on node {}: {}", node.node_id, pg_detail(&e)));
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
}

/// This node's own historical read of [`DOC`] at `version_vector_json`.
///
/// `SELECT … AT VERSION` is claimed by the string DDL router before the
/// planner runs, and its handler dispatches `CrdtOp::ReadAtVersion` over this
/// node's own SPSC bridge. The plan carries no replicated encoding, so it is
/// never proposed and never routed to a vShard leader: the answer is this
/// node's oplog and no peer's.
///
/// The version travels as a raw version-vector literal, which the handler
/// resolves without a catalog row. The probe therefore outlives the checkpoint
/// rows the compaction deletes, and cannot mistake a deleted row for a
/// compacted oplog.
async fn read_at_version(
    node: &TestClusterNode,
    version_vector_json: &str,
) -> Result<String, String> {
    let sql =
        format!("SELECT * FROM {COLLECTION} AT VERSION '{version_vector_json}' WHERE id = '{DOC}'");
    match node.client.simple_query(&sql).await {
        Ok(messages) => Ok(messages
            .into_iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    row.get("document").map(str::to_string)
                }
                _ => None,
            })
            .unwrap_or_default()),
        Err(e) => Err(pg_detail(&e)),
    }
}

/// A committed `COMPACT HISTORY` must discard the follower's own oplog, not
/// only the executing node's.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn compact_history_on_leader_compacts_the_follower_oplog() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION for the compaction target");

    let leader = &cluster.nodes[pick_leader_index(&cluster)];
    let follower = &cluster.nodes[pick_follower_index(&cluster)];
    let mut author = Author::new();

    // Three revisions on one operation chain: one below the boundary, one at
    // it, one above.
    apply_revision(&cluster, leader, &author.revise(REV_BELOW)).await;
    let below = create_checkpoint_on(leader, "below").await;
    wait_for(
        "the follower to write the replicated boundary-below checkpoint row",
        CONVERGE,
        STEP,
        || stored_checkpoint(follower, "below").is_some(),
    )
    .await;
    // The follower's own row: the version the probe reads at is the one this
    // node resolved, not one the leader asserted for it.
    let below_version = stored_checkpoint(follower, "below")
        .expect("the follower must hold the replicated checkpoint row")
        .version_vector_json;

    wait_past_second(below.created_at).await;
    apply_revision(&cluster, leader, &author.revise(REV_AT)).await;
    let boundary = create_checkpoint_on(leader, "boundary").await;
    assert!(
        below.created_at < boundary.created_at,
        "the two checkpoints must carry distinct timestamps, so the compaction \
         boundary is exclusive of the earlier one",
    );
    apply_revision(&cluster, leader, &author.revise(REV_ABOVE)).await;

    // Baseline: the follower serves the pre-boundary version from its own
    // oplog, and the value it returns is the one written below the boundary —
    // not the current one.
    let historical = read_at_version(follower, &below_version)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "follower node {} must serve the pre-boundary version before COMPACT HISTORY: {e}",
                follower.node_id
            )
        });
    assert!(
        historical.contains(REV_BELOW),
        "follower node {} must read '{REV_BELOW}' at the pre-boundary version; got {historical}",
        follower.node_id,
    );
    assert!(
        !historical.contains(REV_ABOVE),
        "the historical read must project the version asked for, not the current \
         document; got {historical}",
    );

    // Baseline: the follower's own oplog counters, captured through a
    // checkpoint this node resolves locally.
    let counters_before = version_counters(
        &create_checkpoint_on(follower, "follower-before")
            .await
            .version_vector_json,
    );
    assert!(
        counters_before.values().any(|&counter| counter > 0),
        "follower node {} must hold authored operations before the compaction; \
         got {counters_before:?}",
        follower.node_id,
    );

    leader
        .client
        .simple_query(&format!(
            "COMPACT HISTORY ON {COLLECTION} WHERE id = '{DOC}' BEFORE 'boundary'"
        ))
        .await
        .expect("COMPACT HISTORY on the metadata leader");

    wait_for_async(
        "the follower to discard its pre-boundary oplog",
        CONVERGE,
        STEP,
        || async { read_at_version(follower, &below_version).await.is_err() },
    )
    .await;

    // The refusal names the compaction boundary. Only a compacted oplog
    // produces it: a node that never ran the dispatch still holds the
    // discarded operations and answers with the value.
    let refusal = read_at_version(follower, &below_version)
        .await
        .expect_err("the compacted follower must not serve the pre-boundary version");
    assert!(
        refusal.contains("predates the compaction boundary"),
        "follower node {} must refuse the pre-boundary read by naming the boundary; \
         got {refusal}",
        follower.node_id,
    );

    // Compaction takes only the history below the cutoff. The boundary
    // version names the shallow root, which the document keeps, so the
    // follower still serves it.
    let boundary_version = stored_checkpoint(follower, "boundary")
        .expect("the boundary checkpoint survives its own exclusive cutoff")
        .version_vector_json;
    let at_boundary = read_at_version(follower, &boundary_version)
        .await
        .expect("the compacted follower must serve the boundary version");
    assert!(
        at_boundary.contains(REV_AT),
        "follower node {} must read '{REV_AT}' at the boundary version after \
         compaction; got {at_boundary}",
        follower.node_id,
    );

    // The compaction keeps the live document: the same node reports the same
    // per-peer counters it held before, so every operation at and above the
    // boundary survived the swap to the shallow snapshot.
    let counters_after = version_counters(
        &create_checkpoint_on(follower, "follower-after")
            .await
            .version_vector_json,
    );
    assert_eq!(
        counters_after, counters_before,
        "follower node {} must keep every operation counter through the compaction",
        follower.node_id,
    );

    cluster.shutdown().await;
}
