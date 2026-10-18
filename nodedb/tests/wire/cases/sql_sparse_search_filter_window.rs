// SPDX-License-Identifier: BUSL-1.1

//! A `WHERE` predicate beside `ORDER BY sparse_score(...)` narrows the
//! candidates before the top-k cut.
//!
//! The highest-scoring rows all fail the filter, and they outnumber the
//! search's initial candidate window. A filter applied to a fixed window
//! will return fewer than `k` rows. The search must return `k` rows, every
//! one matching the filter, highest score first.

use crate::harness::TestServer;

/// Rows that outscore every matching row and fail the filter. More than the
/// initial window of a `k = 2` filtered search.
const NEAR_ROWS: usize = 40;

/// The sparse query every statement ranks by.
const QUERY: &str = "{1: 1.0}";

/// Seed `sparse_filter_docs`: `NEAR_ROWS` high-scoring rows in category
/// `skip`, then three low-scoring rows in category `keep`.
async fn seed(srv: &TestServer) {
    srv.exec(
        "CREATE TABLE sparse_filter_docs \
         (id TEXT PRIMARY KEY, terms SPARSEVECTOR, category TEXT)",
    )
    .await
    .unwrap_or_else(|e| panic!("create sparse_filter_docs: {e}"));
    let near = (0..NEAR_ROWS).map(|i| (format!("near{i}"), 200.0 - i as f64, "skip"));
    let far = [("far1", 3.0), ("far2", 2.0), ("far3", 1.0)]
        .into_iter()
        .map(|(id, weight)| (id.to_string(), weight, "keep"));
    for (id, weight, category) in near.chain(far) {
        srv.exec(&format!(
            "INSERT INTO sparse_filter_docs (id, terms, category) \
             VALUES ('{id}', '{{1: {weight:.1}}}', '{category}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
}

/// The rows of `sql`, as text.
async fn rows(srv: &TestServer, sql: &str) -> Vec<Vec<String>> {
    srv.query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The first column of each row of `sql`.
async fn ids(srv: &TestServer, sql: &str) -> Vec<String> {
    rows(srv, sql)
        .await
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sparse_filter_narrows_candidates_before_top_k() {
    let srv = TestServer::start().await;
    seed(&srv).await;

    // Unfiltered, the near rows win: the filter below is what moves the
    // result to the far rows.
    let open = ids(
        &srv,
        &format!(
            "SELECT id FROM sparse_filter_docs \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert_eq!(open, vec!["near0", "near1"]);

    // Each hit carries its row, so a projected column reads its value.
    let columns = rows(
        &srv,
        &format!(
            "SELECT id, category FROM sparse_filter_docs \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert_eq!(
        columns,
        vec![
            vec!["near0".to_string(), "skip".to_string()],
            vec!["near1".to_string(), "skip".to_string()],
        ]
    );

    let keep = ids(
        &srv,
        &format!(
            "SELECT id FROM sparse_filter_docs WHERE category = 'keep' \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert_eq!(
        keep,
        vec!["far1", "far2"],
        "k matching rows, highest score first"
    );

    // A compound filter matches the next rows down.
    let far_two = ids(
        &srv,
        &format!(
            "SELECT id FROM sparse_filter_docs WHERE category = 'keep' AND id <> 'far1' \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert_eq!(far_two, vec!["far2", "far3"]);

    // A filter no row matches returns no row.
    let none = ids(
        &srv,
        &format!(
            "SELECT id FROM sparse_filter_docs WHERE category = 'absent' \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert!(none.is_empty(), "got {none:?}");
}

/// A computed item moves the search under a tail. The tail sees the rows the
/// filtered search returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sparse_filter_under_a_computed_item_keeps_filter_and_k() {
    let srv = TestServer::start().await;
    seed(&srv).await;

    let computed = rows(
        &srv,
        &format!(
            "SELECT id, upper(category) AS c FROM sparse_filter_docs \
             WHERE category = 'keep' \
             ORDER BY sparse_score(terms, '{QUERY}') DESC LIMIT 2"
        ),
    )
    .await;
    assert_eq!(
        computed,
        vec![
            vec!["far1".to_string(), "KEEP".to_string()],
            vec!["far2".to_string(), "KEEP".to_string()],
        ]
    );
}
