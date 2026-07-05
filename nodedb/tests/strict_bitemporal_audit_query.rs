// SPDX-License-Identifier: BUSL-1.1

//! Regression: `AS OF SYSTEM TIME NULL` (the all-versions / audit-log query)
//! on a `document_strict` collection created `WITH (bitemporal=true)` used to
//! silently drop every user column. The audit-log scan handler treated the
//! stored row body as MessagePack, but strict bitemporal rows store a Binary
//! Tuple — decoding it as msgpack yields a non-object `Value`, which the old
//! code silently swallowed into an empty object carrying only the synthetic
//! `_ts_system` column. This test asserts the user columns survive and the
//! raw reserved bitemporal columns do not leak into the audit output.

mod common;
use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_bitemporal_audit_query_preserves_user_columns() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION bt_audit (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    srv.exec("INSERT INTO bt_audit (id, value) VALUES ('r1', 'hello')")
        .await
        .expect("insert row");

    srv.exec("UPDATE bt_audit SET value = 'world' WHERE id = 'r1'")
        .await
        .expect("update row to create a second system-time version");

    let rows = srv
        .query_named_rows("SELECT * FROM bt_audit AS OF SYSTEM TIME NULL")
        .await
        .expect("select audit-log (all versions) from strict bitemporal collection");

    assert_eq!(
        rows.len(),
        2,
        "expected both system-time versions of the row, got: {rows:?}"
    );

    for row in &rows {
        assert_eq!(
            row.get("id").map(String::as_str),
            Some("r1"),
            "user column 'id' must be present and correct in every audit-log row, got: {row:?}"
        );
        assert!(
            row.contains_key("value"),
            "user column 'value' must be present in every audit-log row, got: {row:?}"
        );
        for ts in ["_ts_system", "_ts_valid_from", "_ts_valid_until"] {
            assert!(
                row.contains_key(ts),
                "synthetic temporal column '{ts}' must be present, got: {row:?}"
            );
        }
        // Default insert carries no client valid-time, so the envelope stores
        // the unbounded sentinels; the audit query surfaces them raw (proving
        // valid-time comes from real storage, not a placeholder).
        assert_eq!(
            row.get("_ts_valid_from").map(String::as_str),
            Some(i64::MIN.to_string().as_str()),
            "_ts_valid_from must surface the raw i64::MIN unbounded sentinel, got: {row:?}"
        );
        assert_eq!(
            row.get("_ts_valid_until").map(String::as_str),
            Some(i64::MAX.to_string().as_str()),
            "_ts_valid_until must surface the raw i64::MAX unbounded sentinel, got: {row:?}"
        );
        for reserved in ["__system_from_ms", "__valid_from_ms", "__valid_until_ms"] {
            assert!(
                !row.contains_key(reserved),
                "reserved temporal column '{reserved}' must not leak into audit-log output, got keys: {:?}",
                row.keys().collect::<Vec<_>>()
            );
        }
    }

    let values: std::collections::HashSet<&str> = rows
        .iter()
        .filter_map(|r| r.get("value").map(String::as_str))
        .collect();
    assert!(
        values.contains("hello") && values.contains("world"),
        "expected both versions' 'value' payloads to survive decode, got: {values:?}"
    );
}
