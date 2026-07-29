// SPDX-License-Identifier: BUSL-1.1

//! Aggregate-function semantics — `COUNT(DISTINCT)`, `SUM(DISTINCT)`,
//! and the interaction between `LIMIT` and `GROUP BY`.
//!
//! Each test asserts the SQL spec for one aggregate construct that the
//! bench triage flagged as silently wrong. The `LIMIT`/`GROUP BY` tests
//! guard the worst-known failure mode: a top-N query is silently
//! widened to a server-internal page cap (~10 000 rows) instead of
//! being honoured at the planner level.

mod common;

use common::pgwire_harness::TestServer;

/// Build the standard `events` fixture used by the aggregate tests:
/// six rows with two distinct categories and one distinct user.
async fn create_events(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION events \
             COLUMNS (id TEXT PRIMARY KEY, category TEXT, user_id TEXT, amount INTEGER) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO events (id, category, user_id, amount) VALUES \
             ('e1', 'view',     'u1', 10), \
             ('e2', 'view',     'u1', 20), \
             ('e3', 'view',     'u2', 30), \
             ('e4', 'click',    'u2', 40), \
             ('e5', 'click',    'u3', 50), \
             ('e6', 'purchase', 'u1', 60)",
        )
        .await
        .unwrap();
}

/// `COUNT(DISTINCT user_id)` must return the count of distinct user_id
/// values across all six rows: `{u1, u2, u3}` → 3. The bug surfaced by
/// the bench triage is that the column comes back as NULL — a silent
/// wrong answer, not an error. Any future regression that returns NULL
/// or 0 must trip this guard.
#[tokio::test]
async fn count_distinct_returns_distinct_count_not_null() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows("SELECT COUNT(DISTINCT user_id) AS distinct_users FROM events")
        .await
        .expect("COUNT(DISTINCT user_id) must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "single-row aggregate must produce exactly one row, got {rows:?}"
    );

    // Specific regression guard: NULL or empty is the silent failure mode.
    let cell = rows[0]
        .first()
        .expect("aggregate result must have at least one column");
    assert!(
        !cell.is_empty(),
        "COUNT(DISTINCT user_id) returned NULL (empty cell). \
         Silent NULL is the original symptom — the test must fail here \
         until the planner wires DISTINCT through the aggregate."
    );

    let count: i64 = cell.parse().unwrap_or_else(|_| {
        panic!(
            "COUNT(DISTINCT ...) must return an integer; got non-integer \
             cell `{cell}`. Non-integer return is its own regression class."
        )
    });

    assert_eq!(
        count, 3,
        "expected 3 distinct user_ids (u1, u2, u3); got {count}"
    );
}

/// `COUNT(DISTINCT category)` must return 3 — `{view, click, purchase}`.
/// This is a sibling test against a different column to confirm the
/// fix isn't accidentally hard-wired to a single column path.
#[tokio::test]
async fn count_distinct_on_different_column_returns_correct_count() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows("SELECT COUNT(DISTINCT category) FROM events")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let count: i64 = rows[0][0]
        .parse()
        .expect("COUNT(DISTINCT category) must return an integer");
    assert_eq!(
        count, 3,
        "expected {{view, click, purchase}} = 3 distinct categories"
    );
}

/// `SUM(DISTINCT amount)` must add each distinct value once, even
/// when duplicate values are present. The fixture extension below
/// injects two extra rows with amounts that duplicate existing ones
/// (10 and 30) so a non-deduping SUM would return `350` and a
/// correct SUM(DISTINCT) returns `210`. Asserting on the deduped sum
/// guards both the silent-NULL bug and the silent "DISTINCT keyword
/// ignored" bug.
#[tokio::test]
async fn sum_distinct_returns_distinct_sum_not_null() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    // Two extra rows with duplicate amounts (10 and 30) so the
    // deduped sum (210) differs from the naïve sum (350).
    srv.exec(
        "INSERT INTO events (id, category, user_id, amount) VALUES \
         ('e7', 'view',  'u4', 10), \
         ('e8', 'click', 'u4', 30)",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT SUM(DISTINCT amount) FROM events")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let cell = &rows[0][0];
    assert!(
        !cell.is_empty(),
        "SUM(DISTINCT amount) returned NULL — silent failure across the \
         DISTINCT aggregate family, not just COUNT"
    );
    let sum: f64 = cell
        .parse()
        .unwrap_or_else(|_| panic!("SUM(DISTINCT amount) must return a number; got `{cell}`"));
    let expected = (10 + 20 + 30 + 40 + 50 + 60) as f64;
    assert!(
        (sum - expected).abs() < 0.001,
        "SUM(DISTINCT amount) must dedupe before summing — expected \
         {expected} (sum of distinct values), got {sum}. A result of \
         {} would indicate DISTINCT was silently dropped.",
        expected + 10.0 + 30.0,
    );
}

/// `LIMIT 1` on a `GROUP BY` query must return exactly one row, not
/// the entire grouped result set. Silent widening to the server-side
/// page cap breaks every top-N dashboard query. This is the failure
/// the bench triage flagged as worst — it produces correct-looking
/// data but the wrong row count.
#[tokio::test]
async fn limit_on_group_by_is_honoured() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, COUNT(*) AS n \
             FROM events \
             GROUP BY category \
             LIMIT 1",
        )
        .await
        .unwrap();

    // Regression guard: silent-truncation-at-page-cap means rows.len()
    // could be 3 (the full grouped result) when LIMIT 1 was asked for.
    assert_eq!(
        rows.len(),
        1,
        "LIMIT 1 on GROUP BY must yield exactly 1 row; got {} rows: {rows:?}. \
         Returning the full grouped result silently breaks every top-N query.",
        rows.len()
    );
}

/// `LIMIT 2` mid-cardinality on `GROUP BY` — same invariant for a
/// non-degenerate limit. There are 3 distinct categories; LIMIT 2 must
/// return 2 rows. Guards against the planner picking up LIMIT only
/// when LIMIT == 1.
#[tokio::test]
async fn limit_two_on_group_by_returns_two_rows() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, COUNT(*) AS n \
             FROM events \
             GROUP BY category \
             ORDER BY category \
             LIMIT 2",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        2,
        "LIMIT 2 on GROUP BY must yield exactly 2 rows; got {} rows: {rows:?}",
        rows.len()
    );
}

/// `SELECT DISTINCT col` must dedupe duplicate values. Surfaced
/// while building B2 derived-FROM coverage: the inner subquery
/// `SELECT DISTINCT category FROM items` returned every row
/// undeduplicated, which means DISTINCT is silently ignored at the
/// planner→executor handoff for `document_strict` (and very likely
/// every other engine that routes through the same scan path).
/// Silent NOT-deduplication is a data-correctness bug on every
/// `SELECT DISTINCT` query.
#[tokio::test]
async fn select_distinct_dedupes_rows() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows("SELECT DISTINCT category FROM events")
        .await
        .unwrap();
    let mut cats: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
    cats.sort();

    assert_eq!(
        cats,
        vec!["click", "purchase", "view"],
        "SELECT DISTINCT category must return exactly the distinct values \
         {{click, purchase, view}}; got {cats:?}. Returning duplicates is \
         a silent data-correctness bug — every downstream query that \
         relies on DISTINCT gets the wrong cardinality."
    );
}

/// `SELECT DISTINCT col1, col2` over a multi-column projection must
/// dedupe the (col1, col2) tuple, not just one column. Sibling
/// coverage to the single-column case so a partial fix (one-column
/// DISTINCT but not tuple-DISTINCT) is caught.
#[tokio::test]
async fn select_distinct_multicolumn_dedupes_tuples() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows("SELECT DISTINCT category, user_id FROM events")
        .await
        .unwrap();

    // Fixture distinct (category, user_id) pairs:
    //   (view, u1), (view, u2), (click, u2), (click, u3), (purchase, u1)
    // = 5 distinct tuples; the raw events table has 6 rows.
    assert_eq!(
        rows.len(),
        5,
        "SELECT DISTINCT category, user_id must return 5 distinct tuples, \
         got {} rows: {rows:?}",
        rows.len()
    );
}

/// `HAVING` filters groups on an aggregate predicate. The clause must keep the
/// groups that satisfy it — dropping every group instead returns an empty
/// result for a query that has matching data, a silent wrong answer rather
/// than an error.
#[tokio::test]
async fn having_keeps_groups_matching_the_aggregate_predicate() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    // Per-category totals: view = 10+20+30 = 60, click = 40+50 = 90,
    // purchase = 60. All three exceed 0, so all three groups must survive.
    let rows = srv
        .query_rows("SELECT category FROM events GROUP BY category HAVING SUM(amount) > 0")
        .await
        .expect("GROUP BY with HAVING must plan and execute");

    assert_eq!(
        rows.len(),
        3,
        "all three groups have SUM(amount) > 0 and must survive HAVING; \
         got {rows:?} (an empty result means every group was dropped)"
    );
}

/// `HAVING` must also *exclude* the groups that fail the predicate — a clause
/// that is parsed but never applied would return all three groups here.
#[tokio::test]
async fn having_excludes_groups_failing_the_aggregate_predicate() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    // Per-category totals: view = 60, click = 90, purchase = 60.
    // A threshold of 80 leaves exactly one group: click.
    let rows = srv
        .query_rows("SELECT category FROM events GROUP BY category HAVING SUM(amount) > 80")
        .await
        .expect("GROUP BY with HAVING must plan and execute");

    assert_eq!(
        rows.len(),
        1,
        "only the click group (SUM = 90) exceeds 80; got {rows:?}"
    );
    assert_eq!(
        rows[0][0], "click",
        "surviving group must be click: {rows:?}"
    );
}

// ── Post-aggregate ORDER BY ──────────────────────────────────────────────────
//
// After GROUP BY, an ORDER BY term may be an aggregate the query does not
// project, or an expression computed over one. Both address values that exist
// only once the groups are finalized.

/// `ORDER BY` an aggregate that is not in the SELECT list. The aggregate has
/// to be computed to sort by it, and must not become an output column the
/// client did not ask for.
#[tokio::test]
async fn order_by_unprojected_aggregate_sorts_groups() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    // Per-category totals: view = 60, click = 90, purchase = 60.
    // Descending by total: click (90), then view and purchase (60 each).
    let rows = srv
        .query_rows("SELECT category FROM events GROUP BY category ORDER BY SUM(amount) DESC")
        .await
        .expect("ORDER BY over an unprojected aggregate must plan and execute");

    assert_eq!(rows.len(), 3, "one row per category; got {rows:?}");
    assert_eq!(
        rows[0][0], "click",
        "click has the largest total (90) and must sort first; got {rows:?}"
    );
    assert_eq!(
        rows[0].len(),
        1,
        "the sort-only aggregate must not add an output column; got {rows:?}"
    );
}

/// `ORDER BY` an expression computed over an aggregate. The sort value exists
/// in no column of the group row, so it has to be evaluated after the groups
/// are finalized.
#[tokio::test]
async fn order_by_expression_over_aggregate_sorts_groups() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    // 1000 / SUM(amount): view = 16, click = 11, purchase = 16. Ascending by
    // that expression puts click first — the inverse of ordering by SUM
    // itself, so a sort that ignored the division cannot produce this.
    let rows = srv
        .query_rows("SELECT category FROM events GROUP BY category ORDER BY 1000 / SUM(amount)")
        .await
        .expect("ORDER BY over an aggregate expression must plan and execute");

    assert_eq!(rows.len(), 3, "one row per category; got {rows:?}");
    assert_eq!(
        rows[0][0], "click",
        "click has the largest total, so the smallest 1000/total; got {rows:?}"
    );
}

/// The same expression sort with the aggregate also projected under an alias.
/// The rename to the user alias happens before the sort, so the sort key must
/// still resolve.
#[tokio::test]
async fn order_by_expression_over_aliased_aggregate_sorts_groups() {
    let srv = TestServer::start().await;
    create_events(&srv).await;

    let rows = srv
        .query_rows(
            "SELECT category, SUM(amount) AS total FROM events \
             GROUP BY category ORDER BY 1000 / SUM(amount)",
        )
        .await
        .expect("ORDER BY over an aliased aggregate expression must plan and execute");

    assert_eq!(rows.len(), 3, "one row per category; got {rows:?}");
    assert_eq!(rows[0][0], "click", "click sorts first; got {rows:?}");
    let total: f64 = rows[0][1]
        .parse()
        .unwrap_or_else(|_| panic!("aliased total must be numeric; got {rows:?}"));
    assert!(
        (total - 90.0).abs() < 1e-9,
        "the aliased total must still be projected; got {rows:?}"
    );
}
