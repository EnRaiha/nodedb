// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Key-Value engine: set/get, upsert, delete,
//! secondary index scan, count, and WAL restart durability. KV collections
//! require a PRIMARY KEY column in the DDL.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_and_get_basic() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_basic (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();

    srv.exec("INSERT INTO kv_basic (key, value) VALUES ('k1', 'hello')")
        .await
        .unwrap();
    srv.exec("INSERT INTO kv_basic (key, value) VALUES ('k2', 'world')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT key, value FROM kv_basic WHERE key = 'k1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_replaces_value() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_upsert (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();

    srv.exec("INSERT INTO kv_upsert (key, v) VALUES ('u1', '1')")
        .await
        .unwrap();
    srv.exec("UPSERT INTO kv_upsert (key, v) VALUES ('u1', '99')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT v FROM kv_upsert WHERE key = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].parse::<i64>().unwrap(), 99);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_key() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_del (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();

    srv.exec("INSERT INTO kv_del (key, x) VALUES ('d1', '1')")
        .await
        .unwrap();
    srv.exec("INSERT INTO kv_del (key, x) VALUES ('d2', '2')")
        .await
        .unwrap();

    srv.exec("DELETE FROM kv_del WHERE key = 'd1'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT key FROM kv_del ORDER BY key")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "d2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secondary_index_scan() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_scan (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON kv_scan (bucket)").await.unwrap();

    srv.exec("INSERT INTO kv_scan (key, bucket, v) VALUES ('s1', 'A', '10')")
        .await
        .unwrap();
    srv.exec("INSERT INTO kv_scan (key, bucket, v) VALUES ('s2', 'B', '20')")
        .await
        .unwrap();
    srv.exec("INSERT INTO kv_scan (key, bucket, v) VALUES ('s3', 'A', '30')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT key FROM kv_scan WHERE bucket = 'A' ORDER BY key")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "s1");
    assert_eq!(rows[1][0], "s3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_all_keys() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_cnt (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();

    for i in 0..6u32 {
        srv.exec(&format!(
            "INSERT INTO kv_cnt (key, n) VALUES ('k{i}', '{i}')"
        ))
        .await
        .unwrap();
    }

    let rows = srv.query_rows("SELECT COUNT(*) FROM kv_cnt").await.unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_wal (key TEXT PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();
    srv.exec("INSERT INTO kv_wal (key, payload) VALUES ('w1', 'durable')")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT payload FROM kv_wal WHERE key = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "durable");
}
