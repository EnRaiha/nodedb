// SPDX-License-Identifier: BUSL-1.1

//! Regression: a `document_strict` collection created `WITH (bitemporal=true)`
//! carries three internal reserved temporal columns (`__system_from_ms`,
//! `__valid_from_ms`, `__valid_until_ms`) prepended into its physical schema.
//! These are engine bookkeeping — they must NOT leak into a user `SELECT *`.
//! Before the fix the strict decode path iterated every schema column
//! unconditionally, so `SELECT *` returned all three alongside the user's
//! columns (and, being prepended, shifted the user columns). This test asserts
//! the user-facing projection contains exactly the declared user columns.

mod common;
use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bitemporal_select_star_hides_reserved_columns() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION bt_strict (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    srv.exec("INSERT INTO bt_strict (id, value) VALUES ('r1', 'hello')")
        .await
        .expect("insert row");

    let rows = srv
        .query_named_rows("SELECT * FROM bt_strict")
        .await
        .expect("select * from strict bitemporal collection");

    assert_eq!(rows.len(), 1, "expected exactly one row, got: {rows:?}");
    let row = &rows[0];

    for reserved in ["__system_from_ms", "__valid_from_ms", "__valid_until_ms"] {
        assert!(
            !row.contains_key(reserved),
            "reserved temporal column '{reserved}' must not appear in a user SELECT *, got keys: {:?}",
            row.keys().collect::<Vec<_>>()
        );
    }

    assert_eq!(
        row.get("id").map(String::as_str),
        Some("r1"),
        "user column 'id' must be present and correct, got: {row:?}"
    );
    assert_eq!(
        row.get("value").map(String::as_str),
        Some("hello"),
        "user column 'value' must be present and correct (no value shift from leaked reserved columns), got: {row:?}"
    );
}
