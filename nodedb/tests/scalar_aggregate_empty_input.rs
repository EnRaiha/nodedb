// SPDX-License-Identifier: BUSL-1.1

//! D1: a scalar aggregate (no GROUP BY) over zero input rows must
//! still emit exactly one identity row — `COUNT(*)` -> `0`,
//! `SUM`/`AVG`/`MIN`/`MAX` -> `NULL` — never an empty result set.
//!
//! Pre-fix, the streaming aggregate's `groups` map was empty whenever no
//! document matched, so `finalize_groups` emitted zero rows for both the
//! empty-collection case and the WHERE-matches-nothing case. A `GROUP BY`
//! aggregate over zero rows must keep emitting zero rows/groups — that
//! path is guarded here too so the fix doesn't regress it.

mod common;

use common::pgwire_harness::TestServer;

async fn create_table(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION t \
             COLUMNS (id TEXT PRIMARY KEY, v INTEGER) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
}

/// `COUNT(*)` on a completely empty collection must return exactly one
/// row containing `0`, not an empty result set.
#[tokio::test]
async fn count_star_on_empty_collection_returns_one_zero_row() {
    let srv = TestServer::start().await;
    create_table(&srv).await;

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM t")
        .await
        .expect("COUNT(*) over an empty collection must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar COUNT(*) over zero rows must emit exactly one identity row, got {rows:?}"
    );
    let cell = rows[0]
        .first()
        .expect("aggregate result must have at least one column");
    assert_eq!(
        cell, "0",
        "COUNT(*) over an empty collection must be 0, got `{cell}`"
    );
}

/// `COUNT(*) ... WHERE` that matches nothing (but the collection is
/// non-empty) must also emit exactly one `0` row — the "empty via
/// filter" path is the same identity case as "empty via no rows".
#[tokio::test]
async fn count_star_with_where_matching_nothing_returns_one_zero_row() {
    let srv = TestServer::start().await;
    create_table(&srv).await;
    srv.exec("INSERT INTO t (id, v) VALUES ('a', 1), ('b', 2)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM t WHERE v > 100")
        .await
        .expect("COUNT(*) with a non-matching WHERE must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar COUNT(*) with WHERE matching nothing must emit exactly one identity row, got {rows:?}"
    );
    let cell = rows[0]
        .first()
        .expect("aggregate result must have at least one column");
    assert_eq!(
        cell, "0",
        "COUNT(*) with WHERE matching nothing must be 0, got `{cell}`"
    );
}

/// Sanity: once rows actually match, COUNT(*) must return the true count
/// — exactly one row, no double-counting and no phantom rows introduced
/// by the empty-input identity-seeding fix.
#[tokio::test]
async fn count_star_with_matching_rows_returns_true_count() {
    let srv = TestServer::start().await;
    create_table(&srv).await;
    srv.exec("INSERT INTO t (id, v) VALUES ('a', 1), ('b', 2)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM t")
        .await
        .expect("COUNT(*) over a non-empty collection must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar COUNT(*) must always emit exactly one row, got {rows:?}"
    );
    let cell = rows[0]
        .first()
        .expect("aggregate result must have at least one column");
    assert_eq!(
        cell, "2",
        "COUNT(*) over 2 matching rows must be 2 (no double-count / phantom rows), got `{cell}`"
    );
}

/// Guard: a `GROUP BY` aggregate over zero input rows must keep emitting
/// zero rows (0 groups) — the empty-input identity fix must NOT touch
/// this path.
#[tokio::test]
async fn group_by_over_empty_collection_returns_zero_rows() {
    let srv = TestServer::start().await;
    create_table(&srv).await;

    let rows = srv
        .query_rows("SELECT v, COUNT(*) FROM t GROUP BY v")
        .await
        .expect("GROUP BY over an empty collection must plan and execute");

    assert_eq!(
        rows.len(),
        0,
        "GROUP BY over zero rows must yield zero groups/rows, got {rows:?}"
    );
}

/// `COUNT(*)` on an empty COLUMNAR collection must also emit one `0`
/// row — the columnar aggregate fast path must not early-return an empty
/// result on zero rows.
#[tokio::test]
async fn count_star_on_empty_columnar_collection_returns_one_zero_row() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION c \
         COLUMNS (id TEXT PRIMARY KEY, v INTEGER) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM c")
        .await
        .expect("COUNT(*) over an empty columnar collection must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar COUNT(*) over an empty columnar collection must emit one identity row, got {rows:?}"
    );
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("0"),
        "columnar COUNT(*) over zero rows must be 0, got {rows:?}"
    );
}

/// `COUNT(*)` on an empty TIMESERIES collection must also emit one `0` row.
#[tokio::test]
async fn count_star_on_empty_timeseries_collection_returns_one_zero_row() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts \
         COLUMNS (ts TIMESTAMP TIME_KEY, v INTEGER) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM ts")
        .await
        .expect("COUNT(*) over an empty timeseries collection must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar COUNT(*) over an empty timeseries collection must emit one identity row, got {rows:?}"
    );
    assert_eq!(
        rows[0].first().map(String::as_str),
        Some("0"),
        "timeseries COUNT(*) over zero rows must be 0, got {rows:?}"
    );
}

/// `SUM`/`MIN`/`MAX` over zero input rows are the non-COUNT scalar
/// identity case: SQL requires them to return `NULL`, in the single
/// identity row. Covered together with COUNT(*) in the same SELECT list
/// so the row count is asserted once for all four aggregates.
#[tokio::test]
async fn sum_min_max_on_empty_collection_return_one_null_row() {
    let srv = TestServer::start().await;
    create_table(&srv).await;

    let rows = srv
        .query_rows("SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t")
        .await
        .expect("SUM/MIN/MAX over an empty collection must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "scalar SUM/MIN/MAX over zero rows must emit exactly one identity row, got {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row.len(), 4, "expected 4 aggregate columns, got {row:?}");
    assert_eq!(row[0], "0", "COUNT(*) must be 0, got `{}`", row[0]);
    for (name, cell) in ["SUM", "MIN", "MAX"].iter().zip(&row[1..]) {
        assert!(
            cell.is_empty() || cell.eq_ignore_ascii_case("null"),
            "{name}(v) over zero rows must be NULL, got `{cell}`"
        );
    }
}
