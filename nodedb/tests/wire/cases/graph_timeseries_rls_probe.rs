// SPDX-License-Identifier: BUSL-1.1

//! A governed graph edge DELETE and a governed timeseries INSERT, in both
//! directions: a conforming write lands, a violating write is refused with
//! state unchanged.
//!
//! Both statements carry a compiled `RlsWriteCheck::Predicate` — graph because
//! a delete's image is the edge's stored property object, timeseries because an
//! ingest's rows exist only after the payload is rewritten into line protocol.
//! Neither predicate can be replicated: a follower has no writing identity to
//! evaluate `$auth.*` against, so `wal_replication::replicable_write` refuses
//! any plan still carrying one. `control/write_resolve` resolves both to a
//! decided form before proposing.
//!
//! That propose path is reachable only when `state.async_raft_proposer()` is
//! `Some`. This harness spawns a single-node server, where the predicate
//! instead reaches the Data Plane gate intact — so these tests pin the same
//! observable behaviour the resolved path must preserve: the conforming write
//! lands, the violating one does not, and the row count and value say which.

use nodedb::types::{DatabaseId, VShardId};

use crate::harness::TestServer;

const PASSWORD: &str = "graph-ts-rls-probe-secret-9";
const ROLE: &str = "readwrite";

/// Edge endpoints. The names are chosen for their HASHES: `VShardId::from_key`
/// maps both to the same vShard, which is what keeps every edge statement below
/// SINGLE-SHARD. A cross-shard edge is dual-homed through Calvin instead, and
/// on a single-node harness that submit is refused before the RLS gate is ever
/// reached — the test would then assert nothing about the policy.
const EDGE_SRC: &str = "a";
const EDGE_DST: &str = "xy";

/// The premise the edge tests rest on: one vShard owns both endpoints.
#[test]
fn edge_endpoints_are_co_resident() {
    assert_eq!(
        VShardId::from_key(EDGE_SRC.as_bytes()),
        VShardId::from_key(EDGE_DST.as_bytes()),
        "the edge tests in this file must exercise the SINGLE-SHARD path; \
         rename the endpoints until the two hashes agree again"
    );
}

/// Timeseries collections are collection-homed, so this only guards against a
/// future rename splitting the ingest off its own shard.
#[test]
fn the_timeseries_collection_homes_on_one_vshard() {
    assert!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "g_ts_probe_metrics").as_u32()
            < VShardId::COUNT
    );
}

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

async fn write_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy {policy}: {e}"));
}

/// Run `sql` as `user`, returning the server's error message on failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    drop(client);
    handle.abort();
    result
}

/// Edges reachable from [`EDGE_SRC`], as raw JSON rows.
async fn out_neighbors(server: &TestServer, collection: &str) -> Vec<serde_json::Value> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{EDGE_SRC}' DIRECTION out"
        ))
        .await
        .expect("read out-neighbors");
    rows.first()
        .map(|r| serde_json::from_str(r).unwrap_or_default())
        .unwrap_or_default()
}

/// A governed `GRAPH DELETE EDGE` that CONFORMS to a `FOR WRITE` owner policy
/// must succeed and remove the edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conforming_graph_edge_delete_succeeds_under_a_write_policy() {
    let server = TestServer::start().await;
    let user = "g_ts_probe_edge_user";
    let collection = "g_ts_probe_edges";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' \
             TYPE 'knows' PROPERTIES '{{\"owner\":\"{user}\"}}'"
        ))
        .await
        .expect("seed the edge owned by the probing user");
    create_user(&server, user).await;
    write_policy(&server, "g_ts_probe_edge_owner", collection).await;

    assert_eq!(
        out_neighbors(&server, collection).await.len(),
        1,
        "the seeded edge must exist before the delete"
    );

    run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' TYPE 'knows'"
        ),
    )
    .await
    .expect("a conforming edge delete must succeed, not be refused as an undecided predicate");

    assert!(
        out_neighbors(&server, collection).await.is_empty(),
        "the conformingly deleted edge must be gone"
    );
}

/// A governed `GRAPH DELETE EDGE` that VIOLATES the policy must be refused and
/// leave the edge in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn violating_graph_edge_delete_is_refused_and_leaves_the_edge() {
    let server = TestServer::start().await;
    let user = "g_ts_probe_edge_intruder";
    let collection = "g_ts_probe_edges_v";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' \
             TYPE 'knows' PROPERTIES '{{\"owner\":\"someone_else\"}}'"
        ))
        .await
        .expect("seed an edge owned by another principal");
    create_user(&server, user).await;
    write_policy(&server, "g_ts_probe_edge_owner_v", collection).await;

    let error = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' TYPE 'knows'"
        ),
    )
    .await
    .expect_err("a violating edge delete must be refused");
    assert!(
        !error.is_empty(),
        "the refusal must carry a message the client can act on"
    );

    let remaining = out_neighbors(&server, collection).await;
    assert_eq!(
        remaining.len(),
        1,
        "the refused delete must leave the edge in place, got: {remaining:?}"
    );
}

/// A governed timeseries `INSERT` that CONFORMS to a `FOR WRITE` owner policy
/// must succeed and be readable back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conforming_timeseries_insert_succeeds_under_a_write_policy() {
    let server = TestServer::start().await;
    let user = "g_ts_probe_ts_user";
    server
        .exec(
            "CREATE COLLECTION g_ts_probe_metrics \
             COLUMNS (ts BIGINT TIME_KEY, owner TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries collection");
    create_user(&server, user).await;
    write_policy(&server, "g_ts_probe_ts_owner", "g_ts_probe_metrics").await;

    run_as(
        &server,
        user,
        &format!("INSERT INTO g_ts_probe_metrics (ts, owner, value) VALUES (100, '{user}', 42.5)"),
    )
    .await
    .expect(
        "a conforming timeseries ingest must succeed, not be refused as an undecided predicate",
    );

    let rows = server
        .query_rows("SELECT ts, owner, value FROM g_ts_probe_metrics ORDER BY ts")
        .await
        .expect("read back g_ts_probe_metrics");
    assert_eq!(
        rows,
        vec![vec![
            "100".to_string(),
            user.to_string(),
            "42.5".to_string()
        ]],
        "the conforming row must be stored and readable back"
    );
}

/// A governed timeseries `INSERT` that VIOLATES the policy must be refused and
/// store nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn violating_timeseries_insert_is_refused_and_stores_nothing() {
    let server = TestServer::start().await;
    let user = "g_ts_probe_ts_intruder";
    server
        .exec(
            "CREATE COLLECTION g_ts_probe_metrics_v \
             COLUMNS (ts BIGINT TIME_KEY, owner TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries collection");
    create_user(&server, user).await;
    write_policy(&server, "g_ts_probe_ts_owner_v", "g_ts_probe_metrics_v").await;

    let error = run_as(
        &server,
        user,
        "INSERT INTO g_ts_probe_metrics_v (ts, owner, value) \
         VALUES (100, 'someone_else', 42.5)",
    )
    .await
    .expect_err("an ingest whose row the policy rejects must be refused");
    assert!(
        !error.is_empty(),
        "the refusal must carry a message the client can act on"
    );

    let rows = server
        .query_rows("SELECT ts, owner, value FROM g_ts_probe_metrics_v ORDER BY ts")
        .await
        .expect("read back g_ts_probe_metrics_v");
    assert!(
        rows.is_empty(),
        "the refused ingest must have stored nothing, got: {rows:?}"
    );
}

/// One violating row fails the whole batch: the conforming row ahead of it must
/// not become durable either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_timeseries_batch_stores_nothing() {
    let server = TestServer::start().await;
    let user = "g_ts_probe_ts_mixed";
    server
        .exec(
            "CREATE COLLECTION g_ts_probe_metrics_m \
             COLUMNS (ts BIGINT TIME_KEY, owner TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries collection");
    create_user(&server, user).await;
    write_policy(&server, "g_ts_probe_ts_owner_m", "g_ts_probe_metrics_m").await;

    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO g_ts_probe_metrics_m (ts, owner, value) \
             VALUES (100, '{user}', 42.5), (200, 'someone_else', 1.0)"
        ),
    )
    .await
    .expect_err("a batch holding one violating row must be refused whole");

    let rows = server
        .query_rows("SELECT ts, owner, value FROM g_ts_probe_metrics_m ORDER BY ts")
        .await
        .expect("read back g_ts_probe_metrics_m");
    assert!(
        rows.is_empty(),
        "no row of a refused batch may be durable, got: {rows:?}"
    );
}
