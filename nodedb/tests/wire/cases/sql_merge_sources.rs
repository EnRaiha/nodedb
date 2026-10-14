// SPDX-License-Identifier: BUSL-1.1

//! The `USING` relation of `MERGE` is read like a SELECT.
//!
//! A key-value or timeseries source merges through its own engine. A
//! subquery source keeps its WHERE. The source's read policy narrows what
//! merges. Two source rows on one join value are an error.

use crate::harness::TestServer;

const PASSWORD: &str = "merge-source-policy-3";

async fn create_target(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, score INT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!("INSERT INTO {name} (id, score) VALUES ('a', 1)"))
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
}

async fn scores(server: &TestServer, name: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT id, score FROM {name} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

fn rows(expected: &[(&str, &str)]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|(id, score)| vec![(*id).to_string(), (*score).to_string()])
        .collect()
}

const UPSERT_ARMS: &str = "WHEN MATCHED THEN UPDATE SET score = s.score \
                           WHEN NOT MATCHED THEN INSERT (id, score) VALUES (s.id, s.score)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_source_merges() {
    let server = TestServer::start().await;
    create_target(&server, "msrc_kv_t").await;
    server
        .exec("CREATE COLLECTION msrc_kv (key TEXT PRIMARY KEY, score INT) WITH (engine='kv')")
        .await
        .expect("create kv source");
    server
        .exec("INSERT INTO msrc_kv (key, score) VALUES ('a', 10), ('b', 20)")
        .await
        .expect("seed kv source");

    server
        .exec(&format!(
            "MERGE INTO msrc_kv_t t USING (SELECT key AS id, score FROM msrc_kv) s \
             ON t.id = s.id {UPSERT_ARMS}"
        ))
        .await
        .expect("merge from a kv source");
    assert_eq!(
        scores(&server, "msrc_kv_t").await,
        rows(&[("a", "10"), ("b", "20")])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_source_merges() {
    let server = TestServer::start().await;
    create_target(&server, "msrc_ts_t").await;
    server
        .exec(
            "CREATE COLLECTION msrc_ts (ts TIMESTAMP TIME_KEY, code TEXT, score INT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries source");
    server
        .exec("INSERT INTO msrc_ts (ts, code, score) VALUES (1000, 'a', 5), (2000, 'c', 7)")
        .await
        .expect("seed timeseries source");

    server
        .exec(&format!(
            "MERGE INTO msrc_ts_t t USING (SELECT code AS id, score FROM msrc_ts) s \
             ON t.id = s.id {UPSERT_ARMS}"
        ))
        .await
        .expect("merge from a timeseries source");
    assert_eq!(
        scores(&server, "msrc_ts_t").await,
        rows(&[("a", "5"), ("c", "7")])
    );
}

/// A source row the subquery's WHERE rejects neither updates nor inserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subquery_where_filters_the_source() {
    let server = TestServer::start().await;
    create_target(&server, "msrc_where_t").await;
    server
        .exec("CREATE COLLECTION msrc_where (id TEXT PRIMARY KEY, score INT)")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO msrc_where (id, score) VALUES ('a', -1), ('b', 2), ('c', -3)")
        .await
        .expect("seed source");

    server
        .exec(&format!(
            "MERGE INTO msrc_where_t t \
             USING (SELECT id, score FROM msrc_where WHERE score > 0) s \
             ON t.id = s.id {UPSERT_ARMS}"
        ))
        .await
        .expect("merge from a filtered subquery");
    assert_eq!(
        scores(&server, "msrc_where_t").await,
        rows(&[("a", "1"), ("b", "2")])
    );
}

/// Two source rows on one join value would both act on one target row. The
/// statement fails and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_source_join_value_is_an_error() {
    let server = TestServer::start().await;
    create_target(&server, "msrc_dup_t").await;
    server
        .exec("CREATE COLLECTION msrc_dup (row_id TEXT PRIMARY KEY, id TEXT, score INT)")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO msrc_dup (row_id, id, score) VALUES ('r1', 'a', 10), ('r2', 'a', 20)")
        .await
        .expect("seed source");

    server
        .expect_error(
            &format!("MERGE INTO msrc_dup_t t USING msrc_dup s ON t.id = s.id {UPSERT_ARMS}"),
            "more than one row",
        )
        .await;
    assert_eq!(scores(&server, "msrc_dup_t").await, rows(&[("a", "1")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_read_policy_narrows_the_merge() {
    let server = TestServer::start().await;
    let user = "msrc_reader";
    create_target(&server, "msrc_rls_t").await;
    server
        .exec("CREATE COLLECTION msrc_rls (id TEXT PRIMARY KEY, owner TEXT, score INT)")
        .await
        .expect("create source");
    server
        .exec(&format!(
            "INSERT INTO msrc_rls (id, owner, score) VALUES \
             ('a', '{user}', 10), ('b', 'someone_else', 20)"
        ))
        .await
        .expect("seed source");
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .expect("create user");
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY msrc_rls_owner ON msrc_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as reader");
    client
        .simple_query(&format!(
            "MERGE INTO msrc_rls_t t USING msrc_rls s ON t.id = s.id {UPSERT_ARMS}"
        ))
        .await
        .expect("merge as reader");
    drop(client);
    handle.abort();

    assert_eq!(
        scores(&server, "msrc_rls_t").await,
        rows(&[("a", "10")]),
        "only the policy-admitted source row may merge"
    );
}
