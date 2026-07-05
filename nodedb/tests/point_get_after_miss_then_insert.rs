// SPDX-License-Identifier: BUSL-1.1

//! A `WHERE <pk> = X` that MISSES (key absent), followed by an INSERT that
//! creates key X, must then be visible to a subsequent `WHERE <pk> = X`: a
//! read that misses must never cache emptiness for the key such that a later
//! write stays invisible. Covered for the schemaless, strict, and key-value
//! engines, across point-get / compound-predicate / full-scan reads, in
//! autocommit.

mod common;
use common::pgwire_harness::TestServer;

async fn assert_miss_then_insert_then_hit(srv: &TestServer, create: &str, pk_col: &str) {
    srv.exec(create).await.expect("create collection");

    // 1. Point-get on an absent key — must miss (0 rows), and must NOT poison
    //    any subsequent read of the same key.
    let miss = srv
        .query_rows(&format!(
            "SELECT {pk_col} FROM poison WHERE {pk_col} = 'k1'"
        ))
        .await
        .expect("point-get miss");
    assert_eq!(
        miss.len(),
        0,
        "key k1 must be absent before insert, got: {miss:?}"
    );

    // 2. Insert the key that just missed.
    srv.exec(&format!(
        "INSERT INTO poison ({pk_col}, v) VALUES ('k1', 'hello')"
    ))
    .await
    .expect("insert k1");

    // 3. The same point-get must now see the row.
    let hit = srv
        .query_rows(&format!(
            "SELECT {pk_col}, v FROM poison WHERE {pk_col} = 'k1'"
        ))
        .await
        .expect("point-get hit after insert");
    assert_eq!(
        hit.len(),
        1,
        "key k1 must be visible after the insert that followed a miss (point-get poisoning), got: {hit:?}"
    );
    assert_eq!(hit[0][0], "k1");
    assert_eq!(hit[0][1], "hello");

    // 4. A compound predicate and a plain scan must also see the row (no read
    //    path may observe a stale emptiness for the key after the insert).
    let compound = srv
        .query_rows(&format!(
            "SELECT {pk_col} FROM poison WHERE {pk_col} = 'k1' AND v = 'hello'"
        ))
        .await
        .expect("compound-predicate read after insert");
    assert_eq!(
        compound.len(),
        1,
        "compound predicate must see key k1 after insert-following-miss, got: {compound:?}"
    );
    let scan = srv
        .query_rows("SELECT v FROM poison")
        .await
        .expect("full scan after insert");
    assert_eq!(
        scan.len(),
        1,
        "full scan must see the inserted row, got: {scan:?}"
    );

    srv.exec("DROP COLLECTION poison").await.expect("drop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_schemaless')",
        "id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict')",
        "id",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_point_get_after_miss_then_insert_is_visible() {
    let srv = TestServer::start().await;
    assert_miss_then_insert_then_hit(
        &srv,
        "CREATE COLLECTION poison (key STRING PRIMARY KEY, v STRING) \
         WITH (engine='kv')",
        "key",
    )
    .await;
}
