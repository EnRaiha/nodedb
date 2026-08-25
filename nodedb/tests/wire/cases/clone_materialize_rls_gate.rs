// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE clone MATERIALIZE` refuses when an RLS policy applies.
//!
//! The clone materializer writes without an `AuthenticatedIdentity` (see
//! `dispatch_local` in `clone_materializer/dispatch.rs`), so a `$auth.*`
//! predicate has nothing to evaluate against. `materialize_one` gates every
//! engine on this before a single row is streamed: a policy on either the
//! source (read) or the target (write) side must refuse the whole
//! materialization, and no rows may land.
//!
//! Covers a columnar case (one of the two sites a pre-existing dispatch
//! guard already caught) and a KV case (one of the two "silent" sites —
//! `KvOp::Put` carries no `rls_write_check` field at all, so nothing but
//! this gate protects it).

use crate::harness::TestServer;

/// Seed a columnar source database with `n` rows, returning nothing — the
/// caller switches back to `default` before cloning.
async fn seed_columnar_source(server: &TestServer, db: &str, n: u32) {
    server
        .exec(&format!("CREATE DATABASE {db}"))
        .await
        .unwrap_or_else(|e| panic!("create database {db}: {e}"));
    server
        .exec(&format!("USE DATABASE {db}"))
        .await
        .unwrap_or_else(|e| panic!("use database {db}: {e}"));
    server
        .exec("CREATE COLLECTION rows (id TEXT, payload TEXT) WITH (engine='columnar')")
        .await
        .expect("create columnar rows");
    for i in 0..n {
        server
            .exec(&format!(
                "INSERT INTO rows (id, payload) VALUES ('r{i}', 'data{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert r{i}: {e}"));
    }
}

/// Seed a KV source database with `n` rows.
async fn seed_kv_source(server: &TestServer, db: &str, n: u32) {
    server
        .exec(&format!("CREATE DATABASE {db}"))
        .await
        .unwrap_or_else(|e| panic!("create database {db}: {e}"));
    server
        .exec(&format!("USE DATABASE {db}"))
        .await
        .unwrap_or_else(|e| panic!("use database {db}: {e}"));
    server
        .exec("CREATE COLLECTION rows (key TEXT PRIMARY KEY, val TEXT) WITH (engine='kv')")
        .await
        .expect("create kv rows");
    for i in 0..n {
        server
            .exec(&format!(
                "INSERT INTO rows (key, val) VALUES ('k{i}', 'data{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert k{i}: {e}"));
    }
}

/// A write policy on the CLONE TARGET refuses the whole materialization, and
/// no rows are copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_write_policy_blocks_columnar_materialize() {
    let server = TestServer::start().await;
    seed_columnar_source(&server, "rlsgate_col_src", 5).await;

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .exec("CLONE DATABASE rlsgate_col_tgt FROM rlsgate_col_src")
        .await
        .expect("clone database");

    server
        .exec("USE DATABASE rlsgate_col_tgt")
        .await
        .expect("use clone target");
    server
        .exec("CREATE RLS POLICY rlsgate_col_tgt_w ON rows FOR WRITE USING (id != '')")
        .await
        .expect("create target write policy");

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .expect_error(
            "ALTER DATABASE rlsgate_col_tgt MATERIALIZE",
            "target write policy",
        )
        .await;

    server
        .exec("USE DATABASE rlsgate_col_tgt")
        .await
        .expect("use clone target");
    let rows = server
        .query_rows("SELECT id FROM rows")
        .await
        .expect("select from clone target");
    assert_eq!(
        rows.len(),
        0,
        "a refused materialization must copy no rows: {rows:?}"
    );
}

/// A read policy on the CLONE SOURCE refuses the whole materialization, and
/// no rows are copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_read_policy_blocks_columnar_materialize() {
    let server = TestServer::start().await;
    seed_columnar_source(&server, "rlsgate_col_src2", 5).await;
    server
        .exec("CREATE RLS POLICY rlsgate_col_src2_r ON rows FOR READ USING (id != '')")
        .await
        .expect("create source read policy");

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .exec("CLONE DATABASE rlsgate_col_tgt2 FROM rlsgate_col_src2")
        .await
        .expect("clone database");

    server
        .expect_error(
            "ALTER DATABASE rlsgate_col_tgt2 MATERIALIZE",
            "source read policy",
        )
        .await;

    server
        .exec("USE DATABASE rlsgate_col_tgt2")
        .await
        .expect("use clone target");
    let rows = server
        .query_rows("SELECT id FROM rows")
        .await
        .expect("select from clone target");
    assert_eq!(
        rows.len(),
        0,
        "a refused materialization must copy no rows: {rows:?}"
    );
}

/// The KV engine is one of the two "silent" write sites: `KvOp::Put` carries
/// no `rls_write_check` field at all, so nothing but the gate in
/// `materialize_one` protects it. A write policy on the clone target must
/// still refuse the whole materialization before any key lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_write_policy_blocks_kv_materialize() {
    let server = TestServer::start().await;
    seed_kv_source(&server, "rlsgate_kv_src", 5).await;

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .exec("CLONE DATABASE rlsgate_kv_tgt FROM rlsgate_kv_src")
        .await
        .expect("clone database");

    server
        .exec("USE DATABASE rlsgate_kv_tgt")
        .await
        .expect("use clone target");
    server
        .exec("CREATE RLS POLICY rlsgate_kv_tgt_w ON rows FOR WRITE USING (val != '')")
        .await
        .expect("create target write policy");

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .expect_error(
            "ALTER DATABASE rlsgate_kv_tgt MATERIALIZE",
            "target write policy",
        )
        .await;

    server
        .exec("USE DATABASE rlsgate_kv_tgt")
        .await
        .expect("use clone target");
    let rows = server
        .query_rows("SELECT key FROM rows")
        .await
        .expect("select from clone target");
    assert_eq!(
        rows.len(),
        0,
        "a refused materialization must copy no rows: {rows:?}"
    );
}

/// With no RLS policy on either side, materialization still succeeds and
/// every source row is copied — the gate must not become a blanket refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_policies_either_side_materializes_every_row() {
    let server = TestServer::start().await;
    seed_columnar_source(&server, "rlsgate_col_free_src", 7).await;

    server
        .exec("USE DATABASE default")
        .await
        .expect("use default");
    server
        .exec("CLONE DATABASE rlsgate_col_free_tgt FROM rlsgate_col_free_src")
        .await
        .expect("clone database");
    server
        .exec("ALTER DATABASE rlsgate_col_free_tgt MATERIALIZE")
        .await
        .expect("materialize with no RLS policies must succeed");

    server
        .exec("USE DATABASE rlsgate_col_free_tgt")
        .await
        .expect("use clone target");
    let rows = server
        .query_rows("SELECT id FROM rows")
        .await
        .expect("select from clone target");
    assert_eq!(
        rows.len(),
        7,
        "every source row must be copied when no policy applies: {rows:?}"
    );
}
