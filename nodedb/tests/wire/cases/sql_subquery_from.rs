// SPDX-License-Identifier: BUSL-1.1

//! Derived-table subqueries in the `FROM` clause: `FROM (SELECT ...) AS t`
//! must plan as a single source relation, not be rejected as multi-table
//! FROM.

use std::collections::HashMap;

use crate::harness::TestServer;

async fn create_items(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION items \
             COLUMNS (id TEXT PRIMARY KEY, category TEXT, qty INTEGER) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO items (id, category, qty) VALUES \
             ('i1', 'a', 1), \
             ('i2', 'a', 2), \
             ('i3', 'b', 3), \
             ('i4', 'b', 4), \
             ('i5', 'c', 5)",
        )
        .await
        .unwrap();
}

/// `FROM (SELECT ... WHERE ...) AS t` must plan as a derived table.
/// The pre-fix symptom was `unsupported: multi-table FROM without
/// JOIN` — the parenthesised SELECT was being misread as a second
/// base relation. With the fix, the inner filter applies and the
/// outer SELECT sees the filtered rows.
#[tokio::test]
async fn derived_with_inner_filter_is_supported() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows("SELECT category FROM (SELECT category FROM items WHERE qty > 2) AS t")
        .await
        .expect(
            "derived table `(SELECT ... WHERE ...) AS t` must plan as a single \
             source relation, not be rejected as multi-table FROM",
        );

    // Three rows satisfy qty > 2: i3=b, i4=b, i5=c.
    assert_eq!(
        rows.len(),
        3,
        "inner WHERE qty > 2 should yield 3 rows; got {rows:?}"
    );
    let cats: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    let mut sorted = cats.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["b", "b", "c"],
        "filtered categories should be {{b, b, c}}; got {cats:?}"
    );
}

/// `FROM (SELECT ... GROUP BY ...) AS agg` must plan as a derived
/// table over the grouped inner query. This is the canonical
/// "aggregate then post-process" pattern. The fix has to propagate
/// the inner GROUP BY's three output rows up to the outer SELECT
/// without the planner choking on the derived-table FROM.
#[tokio::test]
async fn derived_group_by_in_from_is_supported() {
    let srv = TestServer::start().await;
    create_items(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, total \
             FROM (SELECT category, SUM(qty) AS total \
                   FROM items GROUP BY category) AS agg",
        )
        .await
        .expect(
            "derived table over a grouped inner SELECT must plan as a single \
             relation — this is the canonical 'aggregate then post-process' \
             shape",
        );

    assert_eq!(rows.len(), 3, "three categories expected, got {rows:?}");

    // Assert correctness on contents, not order — ORDER BY after
    // GROUP BY is currently best-effort, so the inner rows can come
    // back in any order. Categories alphabetically: a (1+2=3), b
    // (3+4=7), c (5).
    let totals: HashMap<String, f64> = rows
        .iter()
        .map(|r| (r[0].clone(), r[1].parse::<f64>().unwrap()))
        .collect();
    assert_eq!(totals.get("a"), Some(&3.0), "category a total should be 3");
    assert_eq!(totals.get("b"), Some(&7.0), "category b total should be 7");
    assert_eq!(totals.get("c"), Some(&5.0), "category c total should be 5");
}
