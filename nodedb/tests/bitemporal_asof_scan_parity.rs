// SPDX-License-Identifier: BUSL-1.1

//! Regression: a bitemporal `AS OF SYSTEM TIME` / `AS OF VALID TIME` (and the
//! all-versions `AS OF SYSTEM TIME NULL` audit) Document read used to route
//! through a separate, stunted handler that DROPPED `ORDER BY`, computed /
//! generated columns and window functions — so temporal reads returned
//! unsorted rows with computed columns omitted, unlike a normal current-time
//! read. The read is now unified: the temporal slice differs only in the
//! row-fetch stage, and the same downstream (sort -> window -> computed ->
//! projection -> distinct) runs for every mode.
//!
//! These assertions FAIL on the pre-fix tree:
//! - `ORDER BY` is ignored by an `AS OF SYSTEM TIME <cutoff>` read (rows come
//!   back in insertion order).
//! - a `SELECT <expr> AS x` computed column is omitted from the `AS OF` output.
//! - a `ROW_NUMBER() OVER (...)` window column is absent from the `AS OF`
//!   output.
//! - `AS OF SYSTEM TIME NULL` (audit) ignores `ORDER BY`.
//! - the `AS OF`-at-current-time result diverges from the equivalent
//!   non-`AS OF` result.

mod common;
use common::pgwire_harness::TestServer;

/// Far-future system-time cutoff (year 2100, ms): includes every version
/// committed at real "now", so `AS OF SYSTEM TIME CUTOFF` is the current state.
const CUTOFF: &str = "4102444800000";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asof_strict_orders_computes_and_windows_like_current() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION bt (id STRING PRIMARY KEY, n INT) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    // Insert out of order so an ignored ORDER BY is observable.
    for (id, n) in [("c", 3), ("a", 1), ("b", 2)] {
        srv.exec(&format!("INSERT INTO bt (id, n) VALUES ('{id}', {n})"))
            .await
            .expect("insert row");
    }

    // ORDER BY parity: AS OF must return rows sorted, not in insertion order.
    let ordered = srv
        .query_rows(&format!(
            "SELECT n FROM bt AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of ordered scan");
    let ordered_flat: Vec<&str> = ordered.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        ordered_flat,
        vec!["1", "2", "3"],
        "AS OF SYSTEM TIME must honour ORDER BY (pre-fix: unsorted insertion order)"
    );

    // Computed-column parity: `n + 1 AS n1` must be present and correct.
    let computed = srv
        .query_named_rows(&format!(
            "SELECT n, n + 1 AS n1 FROM bt AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of computed-column scan");
    assert_eq!(computed.len(), 3, "expected three rows, got: {computed:?}");
    for row in &computed {
        let n: i64 = row
            .get("n")
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        assert_eq!(
            row.get("n1").map(String::as_str),
            Some((n + 1).to_string().as_str()),
            "AS OF must apply the computed column `n + 1 AS n1` (pre-fix: omitted), got: {row:?}"
        );
    }

    // Window-function parity: ROW_NUMBER() OVER (ORDER BY n) must be present
    // and correct. The outer `ORDER BY n` drives the scan's pre-sort that the
    // window evaluator consumes (the established pattern for every window
    // query — see `sql_window_functions.rs`); pre-fix the AS-OF handler
    // dropped BOTH the sort and the window, so `rn` came back absent/unsorted.
    let windowed = srv
        .query_named_rows(&format!(
            "SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn \
             FROM bt AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of window-function scan");
    assert_eq!(windowed.len(), 3, "expected three rows, got: {windowed:?}");
    let by_n: Vec<(i64, i64)> = windowed
        .iter()
        .map(|r| {
            (
                r.get("n").and_then(|s| s.parse().ok()).unwrap_or_default(),
                r.get("rn").and_then(|s| s.parse().ok()).unwrap_or(-1),
            )
        })
        .collect();
    assert_eq!(
        by_n,
        vec![(1, 1), (2, 2), (3, 3)],
        "AS OF must pre-sort by n and evaluate ROW_NUMBER() OVER (ORDER BY n) \
         (pre-fix: window column absent / rows unsorted)"
    );

    // Full parity: AS-OF-at-current-time equals the equivalent non-AS-OF read
    // for a query using ORDER BY + a computed column.
    let non_asof = srv
        .query_rows("SELECT id, n, n + 1 AS n1 FROM bt ORDER BY n")
        .await
        .expect("current-time scan");
    let asof = srv
        .query_rows(&format!(
            "SELECT id, n, n + 1 AS n1 FROM bt AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of-at-current-time scan");
    assert_eq!(
        asof, non_asof,
        "AS OF at current time must equal the non-AS OF read (same columns/order/values)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asof_audit_null_honours_order_by() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION bta (id STRING PRIMARY KEY, n INT) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    for (id, n) in [("c", 3), ("a", 1), ("b", 2)] {
        srv.exec(&format!("INSERT INTO bta (id, n) VALUES ('{id}', {n})"))
            .await
            .expect("insert row");
    }

    // Audit path still returns all versions AND now respects ORDER BY on a real
    // user column.
    let audit = srv
        .query_rows("SELECT n FROM bta AS OF SYSTEM TIME NULL ORDER BY n")
        .await
        .expect("audit scan with order by");
    let flat: Vec<&str> = audit.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        flat,
        vec!["1", "2", "3"],
        "AS OF SYSTEM TIME NULL must honour ORDER BY (pre-fix: unsorted)"
    );

    // And the synthetic `_ts_system` column is projectable + sortable.
    let by_ts = srv
        .query_rows("SELECT n FROM bta AS OF SYSTEM TIME NULL ORDER BY _ts_system, n")
        .await
        .expect("audit scan ordered by _ts_system");
    assert_eq!(
        by_ts.len(),
        3,
        "audit query must still return every version, got: {by_ts:?}"
    );
}

/// Regression: `AS OF SYSTEM TIME NULL` (all-versions audit) with
/// a `WHERE` predicate on a strict (`document_strict`) collection returned zero
/// rows. The audit fetch applied the predicate against the raw stored Binary
/// Tuple body via a MessagePack-only matcher (`matches_binary`), which never
/// matches a Binary Tuple — so every predicated audit query silently dropped
/// every version. The fetch now resolves the strict schema before matching
/// (`matches_with_resolved_schema`), mirroring the `AS OF <cutoff>` arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asof_audit_null_strict_filters_by_predicate() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION btp (id STRING PRIMARY KEY, v STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .expect("create strict bitemporal collection");

    srv.exec("INSERT INTO btp (id, v) VALUES ('a', 'one')")
        .await
        .expect("insert a=one");
    srv.exec("UPDATE btp SET v = 'two' WHERE id = 'a'")
        .await
        .expect("update a=two");
    srv.exec("INSERT INTO btp (id, v) VALUES ('b', 'other')")
        .await
        .expect("insert b=other");

    // Baseline: unfiltered audit returns every version — 'a' has two (one, two)
    // and 'b' has one, for three rows total.
    let all = srv
        .query_rows("SELECT * FROM btp AS OF SYSTEM TIME NULL")
        .await
        .expect("unfiltered audit scan");
    assert_eq!(
        all.len(),
        3,
        "audit must return all three versions (a x2, b x1), got: {all:?}"
    );

    // Predicated audit: WHERE id='a' must return both versions of 'a', each with
    // id='a' and a real synthetic `_ts_system` column. Pre-fix this returned 0.
    let filtered = srv
        .query_named_rows("SELECT * FROM btp AS OF SYSTEM TIME NULL WHERE id = 'a'")
        .await
        .expect("predicated audit scan");
    assert_eq!(
        filtered.len(),
        2,
        "WHERE id='a' must return both versions of 'a' (pre-fix: 0), got: {filtered:?}"
    );
    for row in &filtered {
        assert_eq!(
            row.get("id").map(String::as_str),
            Some("a"),
            "every filtered audit row must be id='a', got: {row:?}"
        );
        assert!(
            row.get("_ts_system")
                .and_then(|s| s.parse::<i64>().ok())
                .is_some(),
            "each audit row must carry a real `_ts_system` column, got: {row:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asof_schemaless_orders_and_computes_like_current() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION bts (id STRING PRIMARY KEY) \
         WITH (engine='document_schemaless', bitemporal=true)",
    )
    .await
    .expect("create schemaless bitemporal collection");

    for (id, n) in [("c", 3), ("a", 1), ("b", 2)] {
        srv.exec(&format!("INSERT INTO bts (id, n) VALUES ('{id}', {n})"))
            .await
            .expect("insert row");
    }

    let ordered = srv
        .query_rows(&format!(
            "SELECT n FROM bts AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of ordered scan");
    let flat: Vec<&str> = ordered.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        flat,
        vec!["1", "2", "3"],
        "schemaless AS OF must honour ORDER BY (pre-fix: unsorted)"
    );

    let non_asof = srv
        .query_rows("SELECT id, n, n + 1 AS n1 FROM bts ORDER BY n")
        .await
        .expect("current-time scan");
    let asof = srv
        .query_rows(&format!(
            "SELECT id, n, n + 1 AS n1 FROM bts AS OF SYSTEM TIME {CUTOFF} ORDER BY n"
        ))
        .await
        .expect("as-of-at-current-time scan");
    assert_eq!(
        asof, non_asof,
        "schemaless AS OF at current time must equal the non-AS OF read"
    );
}
