// SPDX-License-Identifier: BUSL-1.1
//! `MOVE TENANT` of a database that holds an array, on a three-node cluster
//! with two Data-Plane cores per node.
//!
//! A cell routes to its vShard by Hilbert prefix alone, so the move copies no
//! cell: the cutover rekeys the catalog row, the surrogate bindings, and each
//! core's store. The test writes cells, flushes some and leaves the rest in
//! memtables, moves the database, and reads every cell through the target
//! from every node. No node keeps the array under the source.

use std::time::Duration;

use nodedb_types::DatabaseId;

use crate::common;
use common::cluster_harness::TestCluster;
use common::cluster_harness::shared_steps::{database_id, db_detail, use_database};

const SOURCE: &str = "mt_arr_src";
const TARGET: &str = "mt_arr_tgt";
const MOVED_TENANT: &str = "mt_arr_owner";
const ARRAY: &str = "mt_grid";
/// `qual` of every cell written: 1 + 2 + 10 flushed, 100 + 200 in memtables.
const TOTAL: f64 = 313.0;
const CELLS: usize = 5;

/// Every row of `sql` on `node_idx`, as `column -> text`.
async fn rows(
    cluster: &TestCluster,
    node_idx: usize,
    sql: &str,
) -> Vec<std::collections::HashMap<String, String>> {
    let messages = cluster.nodes[node_idx]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {node_idx}: {}", db_detail(&e)));
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name().to_string(), row.get(i).unwrap_or("").to_string()))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Whether node `node_idx` holds the array under `database`, in the durable
/// catalog and in the in-memory mirror.
fn holds_array(cluster: &TestCluster, node_idx: usize, database: DatabaseId) -> (bool, bool) {
    let shared = &cluster.nodes[node_idx].shared;
    let durable = shared
        .credentials
        .catalog()
        .load_all_arrays()
        .expect("catalog read")
        .iter()
        .any(|a| a.array_id.database_id == database && a.name == ARRAY);
    let mirror = shared
        .array_catalog
        .read()
        .expect("array mirror")
        .all_entries()
        .iter()
        .any(|a| a.array_id.database_id == database && a.name == ARRAY);
    (durable, mirror)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_tenant_array_cells_are_readable_through_the_target_from_every_node() {
    let cluster = TestCluster::spawn_three_with_cores(2)
        .await
        .expect("cluster");

    for database in [SOURCE, TARGET] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
    }
    use_database(&cluster, SOURCE).await;
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY {ARRAY} \
             DIMS (chr INT64 [0..9], pos INT64 [0..99]) \
             ATTRS (qual FLOAT64) \
             TILE_EXTENTS (1, 100) \
             CELL_ORDER HILBERT"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {ARRAY}: {e}"));

    // Flushed cells through one node, then memtable-only cells through
    // another: the rekey must carry both.
    let writer = (ddl_idx + 1) % cluster.nodes.len();
    for sql in [
        format!(
            "INSERT INTO ARRAY {ARRAY} COORDS (0, 10) VALUES (1.0), \
             COORDS (0, 20) VALUES (2.0), COORDS (1, 10) VALUES (10.0)"
        ),
        format!("SELECT ARRAY_FLUSH('{ARRAY}')"),
    ] {
        cluster.nodes[writer]
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} on node {writer}: {e}"));
    }
    let late_writer = (writer + 1) % cluster.nodes.len();
    let sql = format!(
        "INSERT INTO ARRAY {ARRAY} COORDS (2, 10) VALUES (100.0), COORDS (2, 20) VALUES (200.0)"
    );
    cluster.nodes[late_writer]
        .exec(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {late_writer}: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    use_database(&cluster, "default").await;
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE TENANT {MOVED_TENANT} ID 78"))
        .await
        .unwrap_or_else(|e| panic!("CREATE TENANT {MOVED_TENANT}: {e}"));
    cluster
        .exec_ddl_on_any_leader(&format!(
            "MOVE TENANT {MOVED_TENANT} FROM {SOURCE} TO {TARGET}"
        ))
        .await
        .unwrap_or_else(|e| panic!("MOVE TENANT {MOVED_TENANT}: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    let source_id = database_id(&cluster.nodes[0], SOURCE);
    let target_id = database_id(&cluster.nodes[0], TARGET);
    use_database(&cluster, TARGET).await;
    for node_idx in 0..cluster.nodes.len() {
        assert_eq!(
            holds_array(&cluster, node_idx, source_id),
            (false, false),
            "node {node_idx} must hold no array under the source"
        );
        assert_eq!(
            holds_array(&cluster, node_idx, target_id),
            (true, true),
            "node {node_idx} must hold the array under the target"
        );

        let sum = rows(
            &cluster,
            node_idx,
            &format!("SELECT * FROM ARRAY_AGG('{ARRAY}', 'qual', 'sum')"),
        )
        .await;
        let total: f64 = sum
            .first()
            .and_then(|row| row.get("result"))
            .and_then(|text| text.parse().ok())
            .unwrap_or_else(|| panic!("node {node_idx}: no sum in {sum:?}"));
        assert!(
            (total - TOTAL).abs() < 1e-4,
            "node {node_idx}: sum over the moved array must be {TOTAL}, got {total}"
        );

        let cells = rows(
            &cluster,
            node_idx,
            &format!(
                "SELECT * FROM ARRAY_SLICE('{ARRAY}', '{{chr: [0, 9], pos: [0, 99]}}', ['qual'], 100)"
            ),
        )
        .await;
        assert_eq!(cells.len(), CELLS, "node {node_idx}: cells {cells:?}");
    }

    // The source database names the array on no node.
    use_database(&cluster, SOURCE).await;
    for (node_idx, node) in cluster.nodes.iter().enumerate() {
        let sql = format!("SELECT * FROM ARRAY_AGG('{ARRAY}', 'qual', 'sum')");
        assert!(
            node.client.simple_query(&sql).await.is_err(),
            "node {node_idx}: {SOURCE}.{ARRAY} must not resolve after the move"
        );
    }

    cluster.shutdown().await;
}
