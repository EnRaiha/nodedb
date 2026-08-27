// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time flush of buffered DDL must populate the in-memory registries.
//!
//! Branch covered: the REPLICATED flush. `server.single_node_calvin` defaults
//! on (`config/server/section.rs`), so the harness synthesises a one-node
//! cluster, `start_raft` installs `metadata_raft`, and `DISTRIBUTED_CATALOG_VERSION`
//! (1) is always met — COMMIT proposes a `MetadataEntry::Batch` and the raft
//! applier runs the post-apply hooks.
//!
//! The unreplicated twin (`ddl_flush::flush_local`, reached only when
//! `[cluster]` is absent AND `single_node_calvin = false`) is unreachable from
//! this harness and is pinned by the unit tests in `session::ddl_flush`.
//!
//! Each case observes the registry only through SQL — the `SHOW` handlers and
//! the `DROP` existence pre-checks read the registry, never the catalog.

use crate::harness::TestServer;

/// First column of every row returned by `sql`.
async fn names(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} must succeed: {error}"))
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

/// `CREATE SEQUENCE` in a transaction must reach `sequence_registry` once the
/// replicated batch applies.
///
/// `SHOW SEQUENCES` lists the registry, and `DROP SEQUENCE` errors 42P01 when
/// the registry has no entry — both fail if COMMIT wrote redb alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_flush_populates_the_sequence_registry() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SEQUENCE txn_reg_seq START 1 INCREMENT 1")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let listed = names(&server, "SHOW SEQUENCES").await;
    assert!(
        listed.iter().any(|n| n == "txn_reg_seq"),
        "SHOW SEQUENCES reads sequence_registry; after COMMIT the sequence must \
         be there, got {listed:?}"
    );

    // The DROP handler's existence check is a registry lookup, so a
    // catalog-only commit fails here with 42P01 even though redb holds the row.
    server.exec("DROP SEQUENCE txn_reg_seq").await.unwrap();
}

/// `CREATE TRIGGER` in a transaction must reach `trigger_registry` once the
/// replicated batch applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_flush_populates_the_trigger_registry() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_reg_trg_coll (\
                id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "CREATE TRIGGER txn_reg_trg AFTER INSERT ON txn_reg_trg_coll \
             FOR EACH ROW \
             BEGIN INSERT INTO txn_reg_trg_log (id) VALUES (NEW.id); END",
        )
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let listed = names(&server, "SHOW TRIGGERS").await;
    assert!(
        listed.iter().any(|n| n == "txn_reg_trg"),
        "SHOW TRIGGERS reads trigger_registry; after COMMIT the trigger must \
         be there, got {listed:?}"
    );
}

/// `CREATE CHANGE STREAM` in a transaction must reach `stream_registry` once
/// the replicated batch applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_flush_populates_the_stream_registry() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION txn_reg_cs_coll (\
                id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE CHANGE STREAM txn_reg_cs ON txn_reg_cs_coll")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let listed = names(&server, "SHOW CHANGE STREAMS").await;
    assert!(
        listed.iter().any(|n| n == "txn_reg_cs"),
        "SHOW CHANGE STREAMS reads stream_registry; after COMMIT the stream \
         must be there, got {listed:?}"
    );
}

/// Every sub-entry of one replicated batch gets its own post-apply hook: two
/// sequences created in one transaction both land and are usable after COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_flush_hooks_every_batch_sub_entry() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SEQUENCE txn_reg_multi_a START 1 INCREMENT 1")
        .await
        .unwrap();
    server
        .exec("CREATE SEQUENCE txn_reg_multi_b START 5 INCREMENT 1")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let listed = names(&server, "SHOW SEQUENCES").await;
    for expected in ["txn_reg_multi_a", "txn_reg_multi_b"] {
        assert!(
            listed.iter().any(|n| n == expected),
            "every batch sub-entry must run its post-apply hook, {expected} is \
             missing from {listed:?}"
        );
    }

    server.exec("DROP SEQUENCE txn_reg_multi_a").await.unwrap();
    server.exec("DROP SEQUENCE txn_reg_multi_b").await.unwrap();
}
