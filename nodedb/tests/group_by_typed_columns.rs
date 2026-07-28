// SPDX-License-Identifier: BUSL-1.1

//! Regression guard: a `GROUP BY` with a SELECT-list alias on the
//! group KEY must preserve that alias as the output column name, keep the
//! SELECT-list order, and return non-null, correct aggregate values.
//!
//! Pre-fix, `SELECT k AS label, COUNT(*) AS n FROM t GROUP BY k` reported the
//! first column as `k` (the raw grouped column name) instead of the alias
//! `label` — the group-key `AS label` was dropped. The aggregate-result alias
//! (`n`) already worked; this guards the group-key alias, the column order,
//! and the non-null count (the value must still resolve under the raw grouped
//! column key).

mod common;

use common::pgwire_harness::TestServer;

/// `SELECT k AS label, COUNT(*) AS n FROM t GROUP BY k` must:
/// - return the aliases `label` and `n` as the column NAMES (not `k`/`count`),
/// - keep SELECT-list ORDER (`label` first, `n` second),
/// - return non-null, correct `COUNT(*)` values (guards the null risk:
///   the group-key value must still resolve under the raw grouped column key).
#[tokio::test]
async fn group_by_alias_and_order_preserved() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION t \
         COLUMNS (id TEXT PRIMARY KEY, k TEXT, v INTEGER) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE t");
    srv.exec(
        "INSERT INTO t (id, k, v) VALUES \
         ('1', 'a', 10), ('2', 'a', 20), ('3', 'b', 30)",
    )
    .await
    .expect("INSERT t");

    // Read via the simple protocol: column names come from the RowDescription
    // and cell values as text — enough to guard alias, order, and non-null.
    let rows = srv
        .client
        .simple_query("SELECT k AS label, COUNT(*) AS n FROM t GROUP BY k")
        .await
        .expect("GROUP BY with aliases must plan and execute");

    // Column NAMES are the SELECT-list aliases, in SELECT-list ORDER.
    let mut data_rows = Vec::new();
    for msg in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
            assert_eq!(
                names,
                vec!["label", "n"],
                "column names must be the SELECT-list aliases in order, got {names:?}"
            );
            let label = row.get("label").expect("group-key value must be non-null");
            let n = row.get("n").expect("COUNT(*) value must be non-null");
            data_rows.push((label.to_string(), n.to_string()));
        }
    }

    assert_eq!(
        data_rows.len(),
        2,
        "two distinct group keys expected, got {data_rows:?}"
    );

    // Counts are non-null and correct (guards the null risk).
    let mut counts = std::collections::HashMap::new();
    for (label, n) in data_rows {
        counts.insert(label, n);
    }
    assert_eq!(
        counts.get("a").map(String::as_str),
        Some("2"),
        "group `a` must count 2"
    );
    assert_eq!(
        counts.get("b").map(String::as_str),
        Some("1"),
        "group `b` must count 1"
    );
}
