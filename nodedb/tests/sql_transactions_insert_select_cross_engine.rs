// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine regression coverage for a TRANSACTIONAL `INSERT ... SELECT`
//! (`BEGIN; INSERT INTO <target> SELECT ... FROM <source>; COMMIT`).
//!
//! The in-transaction copy historically committed the buffered `InsertSelect`
//! plan by re-scanning the source on the Data Plane and writing each target row
//! under the SOURCE row's surrogate — which has no `(target_collection,
//! surrogate)→pk` catalog binding. A vector hit on the target therefore carried
//! the source's surrogate and could not resolve to the TARGET row's own primary
//! key (cross-engine-unresolvable), unlike the already-fixed autocommit path.
//!
//! The fix expands the staged `InsertSelect` into concrete, fresh-surrogate
//! writes in the Control-Plane COMMIT path before dispatch, so each copied row
//! owns its OWN registered surrogate. Both assertions below fail on the pre-fix
//! code (the target row is unresolvable through its vector index).

mod common;

use common::pgwire_harness::TestServer;

/// A row copied by a transactional `INSERT ... SELECT` must be resolvable
/// through the target's vector index to its OWN primary key — proving the
/// copied row carries a fresh, catalog-registered surrogate (not the source's,
/// which has no target binding).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_select_row_visible_to_vector_and_scan() {
    let server = TestServer::start().await;

    // Target: initially-empty document collection carrying a vector index.
    server.exec("CREATE COLLECTION istx_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_istx_target_emb ON istx_target METRIC cosine DIM 4")
        .await
        .unwrap();

    // Source: two rows, each with an embedding.
    server.exec("CREATE COLLECTION istx_source").await.unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO istx_source (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // Copy every source row into the target INSIDE a transaction.
    server.exec("BEGIN").await.unwrap();
    server
        .client
        .simple_query("INSERT INTO istx_target SELECT * FROM istx_source")
        .await
        .expect("in-tx insert-select should succeed at the statement");
    server.client.simple_query("COMMIT").await.unwrap();

    // Normal scan sees both copied rows.
    let scanned = server
        .query_text("SELECT id FROM istx_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "committed scan must return both copied rows; got {scanned:?}"
    );

    // Vector search near E1 must resolve the copied `alpha` to its target PK.
    // Pre-fix the row carried the source surrogate (unbound in the target), so
    // the hit could not resolve to `alpha`.
    let near_e1 = server
        .query_text(
            "SELECT id FROM istx_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search near E1 must resolve the copied 'alpha'; got {near_e1:?} \
         (pre-fix: source surrogate unbound in target → unresolvable)"
    );

    let near_e2 = server
        .query_text(
            "SELECT id FROM istx_target \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("beta"),
        "vector search near E2 must resolve the copied 'beta'; got {near_e2:?}"
    );
}

/// The row copied by a transactional `INSERT ... SELECT` must own a FRESH
/// identity, distinct from the source row: both collections are independently
/// vector-searchable and each resolves to its OWN primary key. If the target
/// reused the source's surrogate, the target hit would fail to resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_select_target_row_has_fresh_registered_identity() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION istf_source").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_istf_source_emb ON istf_source METRIC cosine DIM 4")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION istf_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_istf_target_emb ON istf_target METRIC cosine DIM 4")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO istf_source (id, embedding) VALUES \
             ('gamma', ARRAY[1.0, 0.0, 0.0, 0.0])",
        )
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .client
        .simple_query("INSERT INTO istf_target SELECT * FROM istf_source")
        .await
        .expect("in-tx insert-select should succeed at the statement");
    server.client.simple_query("COMMIT").await.unwrap();

    // Source row still resolves to its own id in the source collection.
    let src_hit = server
        .query_text(
            "SELECT id FROM istf_source \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        src_hit.first().map(String::as_str),
        Some("gamma"),
        "source row must resolve to its own id; got {src_hit:?}"
    );

    // The copied target row independently resolves to its own id — only possible
    // if it owns a fresh surrogate registered under (istf_target, 'gamma'). A
    // reused source surrogate has no target binding.
    let tgt_hit = server
        .query_text(
            "SELECT id FROM istf_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        tgt_hit.first().map(String::as_str),
        Some("gamma"),
        "committed target row must resolve to its own id via a fresh registered \
         surrogate; got {tgt_hit:?} (pre-fix: reused source surrogate → unresolvable)"
    );
}

/// A transactional `INSERT ... SELECT` whose SOURCE is a STRICT document
/// collection must normalize each scanned Binary Tuple to msgpack before the
/// copy: the copied rows persist AND resolve through the target's vector index
/// to their own PKs. Pre-normalization-fix the strict source's raw tuple bytes
/// were copied through unchanged — PK extraction and vector indexing both failed
/// (nothing persisted / unresolvable).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_select_from_strict_source_normalizes_and_resolves() {
    let server = TestServer::start().await;

    // Strict SOURCE: schema-enforced (Binary Tuple storage) with an embedding.
    server
        .exec(
            "CREATE COLLECTION iss_source \
             (id STRING NOT NULL PRIMARY KEY, embedding VECTOR(4)) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 0.0, 0.0, 1.0]),
    ] {
        server
            .exec(&format!(
                "INSERT INTO iss_source (id, embedding) VALUES \
                 ('{id}', ARRAY[{},{},{},{}])",
                v[0], v[1], v[2], v[3]
            ))
            .await
            .unwrap();
    }

    // Target: schemaless document collection carrying a vector index.
    server.exec("CREATE COLLECTION iss_target").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_iss_target_emb ON iss_target METRIC cosine DIM 4")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .client
        .simple_query("INSERT INTO iss_target SELECT * FROM iss_source")
        .await
        .expect("in-tx insert-select from strict source should succeed");
    server.client.simple_query("COMMIT").await.unwrap();

    // Copied rows persist (pre-fix: strict tuple bytes copied raw → PK unreadable
    // → nothing landed).
    let scanned = server
        .query_text("SELECT id FROM iss_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "committed copy from a strict source must persist both rows; got {scanned:?}"
    );

    // Vector search resolves the copied row to its OWN target PK.
    let near_e1 = server
        .query_text(
            "SELECT id FROM iss_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search must resolve the copied strict-source 'alpha'; got {near_e1:?}"
    );
}
