// SPDX-License-Identifier: BUSL-1.1

//! A Raft group snapshot carries array cells.
//!
//! Array cells are written while the data groups compact their logs past the
//! writes. A fresh learner then joins. It cannot catch up by `AppendEntries`,
//! so it installs group snapshots, and afterwards its own core holds exactly
//! the cell versions the original nodes hold: every put, overwrite and
//! tombstone, with the same system times and surrogates.
//!
//! The comparison reads each node's local core, never a pgwire query: the
//! gateway forwards array reads to the shard owners, so a query on the
//! learner proves nothing about its own state.

use std::time::{Duration, Instant};

use nodedb::engine::array::export::ArrayCellVersion;
use nodedb::types::ArrayCellsBlob;
use nodedb_types::TenantId;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// Low enough that the writes below compact the data-group logs.
const COMPACTION_THRESHOLD: u64 = 4;
const CELLS: i64 = 48;
const TENANT: u64 = 1;

/// Every cell version of `blobs`, canonical and sorted.
fn versions(blobs: &[ArrayCellsBlob]) -> Vec<(String, u32, Vec<u8>)> {
    let mut out: Vec<(String, u32, Vec<u8>)> = blobs
        .iter()
        .flat_map(|blob| {
            let decoded: Vec<ArrayCellVersion> =
                zerompk::from_msgpack(&blob.cells).expect("decode array cells");
            decoded.into_iter().map(move |version| {
                let bytes = zerompk::to_msgpack_vec(&version).expect("encode version");
                (blob.array.clone(), blob.vshard, bytes)
            })
        })
        .collect();
    out.sort();
    out
}

/// cluster/array_install_snapshot
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_learner_installs_array_cells_from_group_snapshots() {
    // `replication_factor = 4`, the post-join node count, places every node,
    // the learner included, on every data group, so every node's core holds
    // every cell.
    let mut cluster =
        TestCluster::spawn_three_with_compaction_threshold_and_rf(COMPACTION_THRESHOLD, 4)
            .await
            .expect("3-node cluster with low compaction threshold and rf=4");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE ARRAY snap_grid DIMS (x INT64 [0..63], y INT64 [0..63]) \
             ATTRS (v INT64) TILE_EXTENTS (8, 8)",
        )
        .await
        .expect("CREATE ARRAY");

    // One Raft entry per statement, spread over the grid's vShards.
    for i in 0..CELLS {
        cluster.nodes[0]
            .exec(&format!(
                "INSERT INTO ARRAY snap_grid COORDS ({}, {}) VALUES ({i})",
                i % 64,
                (i * 7) % 64
            ))
            .await
            .unwrap_or_else(|e| panic!("insert cell {i}: {e}"));
    }
    // An overwrite and a tombstone ride the snapshot too.
    tokio::time::sleep(Duration::from_millis(10)).await;
    cluster.nodes[0]
        .exec("INSERT INTO ARRAY snap_grid COORDS (1, 7) VALUES (1000)")
        .await
        .expect("overwrite");
    cluster.nodes[0]
        .exec("DELETE FROM ARRAY snap_grid WHERE COORDS IN ((2, 14))")
        .await
        .expect("delete");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let expected = versions(&cluster.nodes[0].array_cells(TenantId::new(TENANT)).await);
    assert!(
        expected.len() > CELLS as usize,
        "the original node holds every put, the overwrite and the tombstone: {}",
        expected.len()
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            versions(&node.array_cells(TenantId::new(TENANT)).await),
            expected,
            "node {} holds the same cells before the learner joins",
            node.node_id
        );
    }
    let compacted = cluster
        .nodes
        .iter()
        .map(|n| n.max_data_group_snapshot_index())
        .max()
        .unwrap_or(0);
    assert!(
        compacted > 0,
        "a data group must compact before the learner joins, so it installs a snapshot"
    );

    let learner_id = cluster
        .add_learner_node()
        .await
        .expect("add learner node")
        .node_id;
    let learner = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == learner_id)
        .expect("learner present");

    wait_for(
        "the learner installs a data-group snapshot",
        Duration::from_secs(30),
        Duration::from_millis(200),
        || learner.max_data_group_snapshot_index() > 0,
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let found = versions(&learner.array_cells(TenantId::new(TENANT)).await);
        if found == expected {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "learner {learner_id} holds {} cell versions, the original nodes {}",
                found.len(),
                expected.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    cluster.shutdown().await;
}
