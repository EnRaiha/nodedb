// SPDX-License-Identifier: BUSL-1.1
//! Cluster BACKUP / RESTORE of a tenant whose collections span databases.
//!
//! Each source node snapshots every database of the tenant after one
//! consistent cut, and each snapshot becomes one data section that names its
//! database. A restore into a FRESH cluster creates the named databases
//! through the metadata group and brings every row back into the database it
//! came from, readable from a node that did not coordinate the restore.
//!
//! The tenant writes the same collection names in every database, with a
//! different row count in each: a row restored into the wrong database
//! changes that database's count.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use nodedb_types::backup_envelope::{
    CollectionVerification, DEFAULT_MAX_TOTAL_BYTES, DatabaseBlob, DatabaseDataSection,
    SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_DATABASES, SECTION_ORIGIN_VERIFICATION,
    VerifiedPart, parse_encrypted as parse_envelope,
};

use crate::common;
use common::cluster_harness::TestCluster;

/// Fixed test KEK injected into cluster nodes via `cluster_harness`.
const TEST_KEK: [u8; 32] = [0x42u8; 32];

const TENANT: u64 = 1;

/// Every database the tenant writes, with the rows each collection holds
/// there.
const DATABASES: [(&str, usize); 3] = [("default", 2), ("cl_sales", 3), ("cl_ops", 4)];

/// Every collection the tenant creates in each database.
const COLLECTIONS: [&str; 3] = ["cl_docs", "cl_kv", "cl_cols"];

fn db_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

async fn drain_backup(cluster: &TestCluster) -> Vec<u8> {
    let stream = cluster.nodes[0]
        .client
        .copy_out(&format!("COPY (BACKUP TENANT {TENANT}) TO STDOUT"))
        .await
        .unwrap_or_else(|e| panic!("BACKUP TENANT: {}", db_detail(&e)));
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap_or_else(|e| panic!("copy chunk: {}", db_detail(&e))));
    }
    bytes
}

async fn push_restore(cluster: &TestCluster, envelope: Vec<u8>) -> Result<(), String> {
    let sink = cluster.nodes[0]
        .client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({TENANT}) FROM STDIN"))
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

/// Switch every node's harness session to `database`.
async fn use_database(cluster: &TestCluster, database: &str) {
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE {database} on node {}: {e}", node.node_id));
    }
}

/// Create the named databases, then every collection in every database,
/// and fill each through node 0. Each row's text names its database.
async fn seed(cluster: &TestCluster) {
    for (database, _) in DATABASES.iter().skip(1) {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
    }
    for (database, rows) in DATABASES {
        use_database(cluster, database).await;
        for ddl in [
            "CREATE COLLECTION cl_docs (id TEXT PRIMARY KEY, content TEXT) \
             WITH (engine='document_strict')",
            "CREATE COLLECTION cl_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
            "CREATE COLLECTION cl_cols COLUMNS (id TEXT, region TEXT, ts BIGINT) \
             WITH (engine='columnar')",
        ] {
            cluster
                .exec_ddl_on_any_leader(ddl)
                .await
                .unwrap_or_else(|e| panic!("{ddl} in {database}: {e}"));
        }
        for i in 0..rows {
            for insert in [
                format!("INSERT INTO cl_docs (id, content) VALUES ('k{i}', '{database}-{i}')"),
                format!("INSERT INTO cl_kv (key, value) VALUES ('k{i}', '{database}-{i}')"),
                format!(
                    "INSERT INTO cl_cols (id, region, ts) VALUES ('k{i}', '{database}-{i}', {i})"
                ),
            ] {
                cluster.nodes[0]
                    .client
                    .simple_query(&insert)
                    .await
                    .unwrap_or_else(|e| panic!("{insert} in {database}: {}", db_detail(&e)));
            }
        }
    }
    use_database(cluster, "default").await;
}

/// The first column of every row `sql` returns on node `node_idx`.
async fn first_column(cluster: &TestCluster, node_idx: usize, sql: &str) -> Vec<String> {
    let messages = cluster.nodes[node_idx]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {node_idx}: {}", db_detail(&e)));
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_backup_gathers_one_section_per_node_and_database() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    seed(&cluster).await;

    let bytes = drain_backup(&cluster).await;
    let env = parse_envelope(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_KEK).expect("parse envelope");
    assert_eq!(env.meta.tenant_id, TENANT);

    // The database section lists every database the tenant writes, by name.
    let listed: Vec<DatabaseBlob> = env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_DATABASES)
        .flat_map(|s| {
            zerompk::from_msgpack::<Vec<DatabaseBlob>>(&s.body).expect("decode databases")
        })
        .collect();
    let names: BTreeSet<&str> = listed.iter().map(|b| b.name.as_str()).collect();
    let expected: BTreeSet<&str> = DATABASES.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, expected, "the backup must list every database");

    // Each source node contributes one data section per database — three
    // nodes, so 1..=3 sections per database. Metadata sections carry
    // sentinel origins and are not counted.
    let mut per_database: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for section in env
        .sections
        .iter()
        .filter(|s| s.origin_node_id < SECTION_ORIGIN_CATALOG_ROWS)
    {
        assert_ne!(section.origin_node_id, 0, "origin 0 must not appear");
        let data: DatabaseDataSection =
            zerompk::from_msgpack(&section.body).expect("a data section names its database");
        assert!(
            per_database
                .entry(data.database_id)
                .or_default()
                .insert(section.origin_node_id),
            "node {} snapshots database {} once",
            section.origin_node_id,
            data.database_id
        );
    }
    let listed_ids: BTreeSet<u64> = listed.iter().map(|b| b.database_id).collect();
    let sectioned_ids: BTreeSet<u64> = per_database.keys().copied().collect();
    assert_eq!(
        sectioned_ids, listed_ids,
        "every listed database must have data sections, and no other"
    );
    for (database_id, nodes) in &per_database {
        assert!(
            (1..=3).contains(&nodes.len()),
            "database {database_id}: expected 1..=3 source nodes, got {nodes:?}"
        );
    }

    // The verification section counts each collection's rows once, summed
    // over the source nodes, never once per replica.
    let verification: Vec<CollectionVerification> = env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_VERIFICATION)
        .flat_map(|s| {
            zerompk::from_msgpack::<Vec<CollectionVerification>>(&s.body)
                .expect("decode verification")
        })
        .collect();
    for (database, rows) in DATABASES {
        let database_id = listed
            .iter()
            .find(|b| b.name == database)
            .map(|b| b.database_id)
            .unwrap_or_else(|| panic!("database {database} is listed"));
        for (collection, part) in [
            ("cl_docs", VerifiedPart::Documents),
            ("cl_kv", VerifiedPart::KeyValue),
            ("cl_cols", VerifiedPart::Columnar),
        ] {
            let count = verification
                .iter()
                .find(|r| {
                    r.database_id == database_id && r.collection == collection && r.part == part
                })
                .map(|r| r.tally.count);
            assert_eq!(
                count,
                Some(rows as u64),
                "{database}.{collection} ({part}) must record {rows} rows"
            );
        }
    }

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_restore_brings_back_every_database() {
    // ── SOURCE cluster A ─────────────────────────────────────────────────────
    let cluster_a = TestCluster::spawn_three().await.expect("cluster A");
    seed(&cluster_a).await;
    let envelope = drain_backup(&cluster_a).await;
    cluster_a.shutdown().await;

    // ── TARGET cluster B: fresh, none of the named databases exist ──────────
    let cluster_b = TestCluster::spawn_three().await.expect("cluster B");
    push_restore(&cluster_b, envelope)
        .await
        .expect("RESTORE into a fresh cluster");
    cluster_b
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // Read from node 1: the databases, the catalog rows and the rows reached
    // it through replication, not through the coordinator's own apply.
    for (database, rows) in DATABASES {
        use_database(&cluster_b, database).await;
        for collection in COLLECTIONS {
            let count =
                first_column(&cluster_b, 1, &format!("SELECT COUNT(*) FROM {collection}")).await;
            assert_eq!(
                count,
                vec![rows.to_string()],
                "{database}.{collection} must hold exactly its own {rows} rows after the restore"
            );
        }
        let last = rows - 1;
        let expected = vec![format!("{database}-{last}")];
        for sql in [
            format!("SELECT content FROM cl_docs WHERE id = 'k{last}'"),
            format!("SELECT value FROM cl_kv WHERE key = 'k{last}'"),
            format!("SELECT region FROM cl_cols WHERE id = 'k{last}'"),
        ] {
            assert_eq!(
                first_column(&cluster_b, 1, &sql).await,
                expected,
                "{sql} in {database} must return the row of {database}"
            );
        }
    }

    cluster_b.shutdown().await;
}
