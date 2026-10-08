// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::text_search` returns real BM25-ranked
//! matches against indexed text content on a named field.
//!
//! A trait default that short-circuits to `Ok(Vec::new())` without
//! ever reaching the wire is the silent-wrong pattern this test guards
//! against — a fake "no matches" answer is indistinguishable from a
//! real one and lets callers proceed as if FTS were working.

use std::collections::HashSet;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::text_search::{QueryMode, TextSearchParams};

#[tokio::test]
async fn text_search_returns_real_matches() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Seed an FTS-indexed collection. The trait's `field` parameter
    // names which BM25 index to query; the harness must create both the
    // collection and the SEARCH INDEX on `body`, otherwise the planner
    // has nothing to match against.
    remote
        .execute_sql("CREATE COLLECTION docs", &[])
        .await
        .expect("CREATE COLLECTION docs");
    remote
        .execute_sql("CREATE SEARCH INDEX ON docs FIELDS body", &[])
        .await
        .expect("CREATE SEARCH INDEX on body field");

    let mut doc = nodedb_client::Document::new("d1");
    doc.set(
        "body",
        nodedb_client::Value::String("machine learning is everywhere".into()),
    );
    remote
        .document_put("docs", doc)
        .await
        .expect("seed document with indexed body field");

    // Spec: with indexed content matching the query, `text_search`
    // returns Ok with at least one BM25-ranked hit on the named field.
    //
    // An `Err("not implemented")` default is correct negative behavior
    // but not the spec — do not soften the assertion to accept `Err`;
    // that locks the gap in as the contract.
    let matches = remote
        .text_search(
            "docs",
            "body",
            "machine learning",
            10,
            TextSearchParams {
                mode: QueryMode::And,
                fuzzy: true,
            },
            None,
        )
        .await
        .expect("text_search must return Ok with real matches against indexed content");
    assert!(
        !matches.is_empty(),
        "text_search must return real BM25-ranked matches; got empty"
    );

    // A second document holds one of the two terms. `And` keeps only the
    // document holding both; `Or` returns both.
    let mut doc = nodedb_client::Document::new("d2");
    doc.set(
        "body",
        nodedb_client::Value::String("machine shop tools".into()),
    );
    remote
        .document_put("docs", doc)
        .await
        .expect("seed second document");
    let ids = |hits: &[nodedb_client::SearchResult]| -> Vec<String> {
        let mut ids: Vec<String> = hits.iter().map(|h| h.id.clone()).collect();
        ids.sort();
        ids
    };
    let all_terms = remote
        .text_search(
            "docs",
            "body",
            "machine learning",
            10,
            TextSearchParams {
                mode: QueryMode::And,
                fuzzy: false,
            },
            None,
        )
        .await
        .expect("And-mode text_search");
    assert_eq!(ids(&all_terms), vec!["d1".to_string()]);
    let any_term = remote
        .text_search(
            "docs",
            "body",
            "machine learning",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect("Or-mode text_search");
    assert_eq!(ids(&any_term), vec!["d1".to_string(), "d2".to_string()]);

    // A typo matches only through the fuzzy fallback.
    let exact = remote
        .text_search(
            "docs",
            "body",
            "machime",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect("non-fuzzy text_search");
    assert!(exact.is_empty(), "no exact match for a typo; got {exact:?}");
    let fuzzy = remote
        .text_search(
            "docs",
            "body",
            "machime",
            10,
            TextSearchParams {
                mode: QueryMode::Or,
                fuzzy: true,
            },
            None,
        )
        .await
        .expect("fuzzy text_search");
    assert_eq!(ids(&fuzzy), vec!["d1".to_string(), "d2".to_string()]);

    // `allowed_ids` restricts the candidates on the server.
    let only_other: std::collections::HashSet<String> =
        std::iter::once("other".to_string()).collect();
    let restricted = remote
        .text_search(
            "docs",
            "body",
            "machine learning",
            10,
            TextSearchParams {
                mode: QueryMode::And,
                fuzzy: true,
            },
            Some(&only_other),
        )
        .await
        .expect("text_search with allowed_ids");
    assert!(
        restricted.is_empty(),
        "d1 is not in allowed_ids, so no hit may return; got {restricted:?}"
    );

    server.graceful_shutdown().await;
}

/// The hits as `(id, score)` pairs, in rank order.
fn ranked(hits: &[nodedb_client::SearchResult]) -> Vec<(String, f32)> {
    hits.iter().map(|h| (h.id.clone(), h.distance)).collect()
}

/// The native client sends the statement the pgwire client sends, so the
/// two return the same ids with the same scores for every search.
#[tokio::test]
async fn native_text_search_matches_remote() {
    let server = TestServer::start().await;
    let remote = NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed");
    let native = NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ));

    remote
        .execute_sql("CREATE COLLECTION docs", &[])
        .await
        .expect("CREATE COLLECTION docs");
    remote
        .execute_sql("CREATE SEARCH INDEX ON docs FIELDS body", &[])
        .await
        .expect("CREATE SEARCH INDEX on body field");
    for (id, body) in [
        ("d1", "machine learning is everywhere"),
        ("d2", "machine shop tools"),
        ("d3", "learning to cook"),
    ] {
        let mut doc = nodedb_client::Document::new(id);
        doc.set("body", nodedb_client::Value::String(body.into()));
        native
            .document_put("docs", doc)
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }

    let only_d1: HashSet<String> = std::iter::once("d1".to_string()).collect();
    let cases: [(&str, TextSearchParams, Option<&HashSet<String>>); 4] = [
        ("any term", TextSearchParams::default(), None),
        (
            "all terms",
            TextSearchParams {
                mode: QueryMode::And,
                fuzzy: false,
            },
            None,
        ),
        (
            "fuzzy",
            TextSearchParams {
                mode: QueryMode::Or,
                fuzzy: true,
            },
            None,
        ),
        ("allowed ids", TextSearchParams::default(), Some(&only_d1)),
    ];
    for (case, params, allowed) in cases {
        let over_pgwire = remote
            .text_search("docs", "body", "machine learning", 10, params, allowed)
            .await
            .unwrap_or_else(|e| panic!("{case}: remote text_search: {e}"));
        let over_native = native
            .text_search("docs", "body", "machine learning", 10, params, allowed)
            .await
            .unwrap_or_else(|e| panic!("{case}: native text_search: {e}"));
        assert!(!over_native.is_empty(), "{case}: native found no hit");
        assert_eq!(
            ranked(&over_native),
            ranked(&over_pgwire),
            "{case}: both clients return the same ids and scores"
        );
    }

    server.graceful_shutdown().await;
}
