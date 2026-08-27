// SPDX-License-Identifier: BUSL-1.1

//! Restart durability of index drops: a dropped vector index must not be
//! resurrected by WAL replay of its create record, and sibling indexes on
//! other columns must survive both the drop and the restart.

use crate::harness::TestServer;

/// Index names listed by `SHOW INDEXES` for the current tenant.
async fn listed_indexes(server: &TestServer) -> Vec<String> {
    server
        .query_text("SHOW INDEXES")
        .await
        .expect("SHOW INDEXES must succeed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_vector_index_stays_dropped_across_restart() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx_restart (id TEXT PRIMARY KEY, embedding FLOAT[])")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX vidx_restart_idx ON vidx_restart (embedding) \
             METRIC cosine DIM 4",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO vidx_restart (id, embedding) VALUES ('a', ARRAY[1.0,0.0,0.0,0.0])")
        .await
        .unwrap();
    server.exec("DROP INDEX vidx_restart_idx").await.unwrap();

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (restarted, _dir) = TestServer::open_on_path(dir).await;

    let after = listed_indexes(&restarted).await;
    assert!(
        !after.iter().any(|n| n == "vidx_restart_idx"),
        "a dropped vector index must not come back on restart: {after:?}"
    );

    // The column must also be free to carry a differently configured index:
    // if the build parameters survived the restart, this is refused as a
    // duplicate.
    restarted
        .exec("CREATE VECTOR INDEX vidx_restart_idx ON vidx_restart (embedding) METRIC l2 DIM 8")
        .await
        .expect("after a restart the dropped column must accept a fresh vector index");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surviving_vector_index_still_works_after_a_sibling_is_dropped() {
    // The drop is index-scoped: dropping one embedding column's index must
    // leave the other column's index searchable, before and after a restart.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vidx_siblings")
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX vidx_siblings_text ON vidx_siblings (text_emb) \
             METRIC cosine DIM 4",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX vidx_siblings_image ON vidx_siblings (image_emb) \
             METRIC cosine DIM 4",
        )
        .await
        .unwrap();
    for (id, text, image) in [
        ("a", "1.0,0.0,0.0,0.0", "0.0,0.0,0.0,1.0"),
        ("b", "0.0,1.0,0.0,0.0", "0.0,0.0,1.0,0.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vidx_siblings (id, text_emb, image_emb) \
                 VALUES ('{id}', ARRAY[{text}], ARRAY[{image}])"
            ))
            .await
            .unwrap();
    }

    server
        .exec("DROP INDEX vidx_siblings_text")
        .await
        .expect("dropping one of two per-column vector indexes must succeed");

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (restarted, _dir) = TestServer::open_on_path(dir).await;

    let listed = listed_indexes(&restarted).await;
    assert!(
        !listed.iter().any(|n| n == "vidx_siblings_text"),
        "the dropped index must stay dropped: {listed:?}"
    );
    assert!(
        listed.iter().any(|n| n == "vidx_siblings_image"),
        "the sibling index must survive both the drop and the restart: {listed:?}"
    );

    let hits = restarted
        .query_text(
            "SELECT id FROM vidx_siblings \
             ORDER BY vector_distance(image_emb, ARRAY[0.0,0.0,0.0,1.0]) LIMIT 1",
        )
        .await
        .expect("search on the surviving index must succeed");
    assert_eq!(
        hits,
        vec!["a".to_string()],
        "the surviving index must still return its nearest row: {hits:?}"
    );
}
