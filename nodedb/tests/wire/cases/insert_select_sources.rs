// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... SELECT` reads its source through the source's own engine.
//!
//! Every source engine copies its rows: key-value, columnar, spatial,
//! timeseries, and vector-primary sources are read as a SELECT over that
//! engine reads them. A target that is not a document collection is refused
//! at planning, by name, and copies nothing.

use crate::harness::TestServer;

/// Copy `source` into a fresh schemaless target with `select`, returning the
/// target's `(id, v)` rows ordered by `id`.
async fn copy_and_read(server: &TestServer, target: &str, select: &str) -> Vec<Vec<String>> {
    server
        .exec(&format!("CREATE COLLECTION {target}"))
        .await
        .unwrap_or_else(|e| panic!("create {target}: {e}"));
    server
        .exec(&format!("INSERT INTO {target} (id, v) {select}"))
        .await
        .unwrap_or_else(|e| panic!("copy into {target}: {e}"));
    server
        .query_rows(&format!("SELECT id, v FROM {target} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {target}: {e}"))
}

fn rows(expected: &[(&str, &str)]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|(id, v)| vec![(*id).to_string(), (*v).to_string()])
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_source_copies_every_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION iss_kv (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("create kv source");
    server
        .exec("INSERT INTO iss_kv (key, n) VALUES ('k1', 10), ('k2', 20)")
        .await
        .expect("seed kv source");

    let copied = copy_and_read(&server, "iss_kv_t", "SELECT key, n FROM iss_kv").await;
    assert_eq!(copied, rows(&[("k1", "10"), ("k2", "20")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_source_copies_every_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION iss_col (id TEXT PRIMARY KEY, v FLOAT) WITH (engine='columnar')")
        .await
        .expect("create columnar source");
    server
        .exec("INSERT INTO iss_col (id, v) VALUES ('c1', 1.5), ('c2', 2.5)")
        .await
        .expect("seed columnar source");

    let copied = copy_and_read(&server, "iss_col_t", "SELECT id, v FROM iss_col").await;
    assert_eq!(copied, rows(&[("c1", "1.5"), ("c2", "2.5")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_source_copies_every_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION iss_geo (id TEXT PRIMARY KEY, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .expect("create spatial source");
    for (id, name, lon) in [("p1", "SF", -122.4), ("p2", "LA", -118.2)] {
        server
            .exec(&format!(
                "INSERT INTO iss_geo (id, location, name) VALUES \
                 ('{id}', '{{\"type\":\"Point\",\"coordinates\":[{lon},37.8]}}', '{name}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed spatial {id}: {e}"));
    }

    let copied = copy_and_read(&server, "iss_geo_t", "SELECT id, name FROM iss_geo").await;
    assert_eq!(copied, rows(&[("p1", "SF"), ("p2", "LA")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_source_copies_every_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION iss_ts (ts TIMESTAMP TIME_KEY, v FLOAT) WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries source");
    server
        .exec("INSERT INTO iss_ts (ts, v) VALUES (1000, 1.5), (2000, 2.5)")
        .await
        .expect("seed timeseries source");
    server
        .exec("CREATE COLLECTION iss_ts_t")
        .await
        .expect("create target");
    server
        .exec("INSERT INTO iss_ts_t (v) SELECT v FROM iss_ts")
        .await
        .expect("copy timeseries rows");

    let copied = server
        .query_text("SELECT v FROM iss_ts_t ORDER BY v")
        .await
        .expect("read target");
    assert_eq!(copied, vec!["1.5".to_string(), "2.5".to_string()]);
}

/// A vector-primary source is read through its own engine, so the vector and
/// the payload both copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_source_copies_vector_and_payload() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION iss_vp (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary source");
    server
        .exec("INSERT INTO iss_vp (id, vec, owner) VALUES ('v1', ARRAY[1.0, 0.0, 0.0], 'alice')")
        .await
        .expect("seed vector-primary source");
    server
        .exec("CREATE COLLECTION iss_vp_t")
        .await
        .expect("create target");
    server
        .exec("CREATE VECTOR INDEX idx_iss_vp_t ON iss_vp_t METRIC cosine DIM 3")
        .await
        .expect("create target vector index");
    server
        .exec("INSERT INTO iss_vp_t (id, owner, embedding) SELECT id, owner, vec FROM iss_vp")
        .await
        .expect("copy vector-primary rows");

    let owners = server
        .query_text("SELECT owner FROM iss_vp_t WHERE id = 'v1'")
        .await
        .expect("read payload");
    assert_eq!(owners, vec!["alice".to_string()]);
    let nearest = server
        .query_text("SELECT id FROM iss_vp_t WHERE embedding <-> ARRAY[1.0, 0.0, 0.0] LIMIT 1")
        .await
        .expect("search copied vector");
    assert_eq!(nearest, vec!["v1".to_string()], "the vector must copy");
}

/// A key-value target is refused at planning, by name, and copies nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_target_is_refused_at_planning() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION iss_kvt (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("create kv target");
    server
        .exec("CREATE COLLECTION iss_kvt_src")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO iss_kvt_src (id, key, n) VALUES ('a', 'k1', 1)")
        .await
        .expect("seed source");
    server
        .expect_error(
            "INSERT INTO iss_kvt (key, n) SELECT key, n FROM iss_kvt_src",
            "must be a document collection",
        )
        .await;
    let landed = server
        .query_text("SELECT key FROM iss_kvt")
        .await
        .expect("read kv target");
    assert!(landed.is_empty(), "a refused copy wrote {landed:?}");
}

/// A vector-primary target is refused: the copy writes document bodies, and
/// a vector-primary collection stores rows through its vector index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_target_is_refused() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION iss_vpt (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3)",
        )
        .await
        .expect("create vector-primary target");
    server
        .exec("CREATE COLLECTION iss_vpt_src")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO iss_vpt_src (id, vec, owner) VALUES ('v1', ARRAY[1.0, 0.0, 0.0], 'a')")
        .await
        .expect("seed source");
    server
        .expect_error(
            "INSERT INTO iss_vpt (id, vec, owner) SELECT id, vec, owner FROM iss_vpt_src",
            "vector-primary",
        )
        .await;
}
