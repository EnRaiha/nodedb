// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Vector engine.
//!
//! The vector engine is accessed via `WITH (engine='vector')` which maps to
//! the document_schemaless storage path with an HNSW index overlay.
//! Covers: index creation, ANN search via vector_distance, quantization option.

use crate::harness::TestServer;

#[tokio::test]
async fn create_vector_collection_and_insert() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_basic WITH (engine='vector')")
        .await
        .unwrap();

    srv.exec("INSERT INTO vec_basic { id: 'v1', embedding: [0.1, 0.2, 0.3], label: 'cat' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_basic { id: 'v2', embedding: [0.9, 0.8, 0.7], label: 'dog' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM vec_basic")
        .await
        .unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 2);
}

#[tokio::test]
async fn create_vector_index_and_ann_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_ann WITH (engine='vector')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_ann (embedding)")
        .await
        .unwrap();

    let vecs: &[(&str, [f32; 4])] = &[
        ("v1", [1.0, 0.0, 0.0, 0.0]),
        ("v2", [0.0, 1.0, 0.0, 0.0]),
        ("v3", [0.0, 0.0, 1.0, 0.0]),
        ("v4", [0.0, 0.0, 0.0, 1.0]),
        ("v5", [0.7, 0.7, 0.0, 0.0]),
    ];
    for (id, emb) in vecs {
        let arr = format!("[{},{},{},{}]", emb[0], emb[1], emb[2], emb[3]);
        srv.exec(&format!(
            "INSERT INTO vec_ann {{ id: '{id}', embedding: {arr} }}"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT id FROM vec_ann \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) \
             LIMIT 3",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"v1"), "v1 not in top-3: {ids:?}");
}

#[tokio::test]
async fn vector_distance_with_quantization_option() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_quant WITH (engine='vector')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_quant (vec)").await.unwrap();

    srv.exec("INSERT INTO vec_quant { id: 'q1', vec: [1.0, 2.0, 3.0, 4.0] }")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_quant { id: 'q2', vec: [4.0, 3.0, 2.0, 1.0] }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM vec_quant \
             ORDER BY vector_distance(vec, ARRAY[1.0,2.0,3.0,4.0], quantization => 'sq8') \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert!(!rows.is_empty(), "expected at least one result");
    assert_eq!(rows[0][0], "q1");
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_wal WITH (engine='vector')")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_wal { id: 'w1', label: 'persisted' }")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT label FROM vec_wal WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "persisted");
}

#[tokio::test]
async fn search_using_vector_dsl_lowers_to_canonical_form() {
    // The `SEARCH <coll> USING VECTOR(<field>, ARRAY[...], <k>)` shorthand
    // must lower to `ORDER BY vector_distance(...) LIMIT k` so users can pick
    // either surface.
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_search_dsl WITH (engine='vector')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_search_dsl (embedding)")
        .await
        .unwrap();

    let vecs: &[(&str, [f32; 4])] = &[
        ("a", [1.0, 0.0, 0.0, 0.0]),
        ("b", [0.0, 1.0, 0.0, 0.0]),
        ("c", [0.0, 0.0, 1.0, 0.0]),
        ("d", [0.0, 0.0, 0.0, 1.0]),
        ("e", [0.7, 0.7, 0.0, 0.0]),
    ];
    for (id, emb) in vecs {
        let arr = format!("[{},{},{},{}]", emb[0], emb[1], emb[2], emb[3]);
        srv.exec(&format!(
            "INSERT INTO vec_search_dsl {{ id: '{id}', embedding: {arr} }}"
        ))
        .await
        .unwrap();
    }

    let canonical = srv
        .query_rows(
            "SELECT * FROM vec_search_dsl \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 3",
        )
        .await
        .unwrap();
    let rows = srv
        .query_rows("SEARCH vec_search_dsl USING VECTOR(embedding, ARRAY[1.0,0.0,0.0,0.0], 3)")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        canonical.len(),
        "SEARCH USING VECTOR DSL must match canonical form: canonical={}, dsl={}",
        canonical.len(),
        rows.len()
    );
}
