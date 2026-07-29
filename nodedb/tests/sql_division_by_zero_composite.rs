// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for division-by-zero in *composite* evaluation paths that
//! historically folded a zero-divisor to NULL and silently dropped the row
//! from the result instead of failing the statement.
//!
//! `sql_division_by_zero.rs` locks the row-scope paths (SELECT list, WHERE,
//! columnar scan). This file locks the paths that evaluate an expression
//! outside a single row's projection/filter and previously swallowed the
//! error:
//!
//! - **Aggregate argument** — `SUM(1/denom)` evaluates the argument per row in
//!   the streaming accumulator. A zero divisor used to exclude that row from
//!   the accumulation; it must now fail the statement with `22012`.
//! - **GROUP BY key** — `GROUP BY 10/denom` evaluates the key expression per
//!   row to build the group key. A zero divisor used to bucket the row under a
//!   `null` key; it must now fail with `22012`.
//! - **Window ORDER BY** — `... OVER (ORDER BY 1/denom)` evaluates the order
//!   key per row. A zero divisor used to fold to NULL; it must now fail.
//! - **Join residual ON predicate** — `JOIN ... ON a.grp = b.grp AND
//!   1/a.denom > 0` evaluates the non-equijoin residual per candidate pair in
//!   the hash-join probe. A zero divisor used to fold to "no match"; it must
//!   now fail.
//!
//! Every divisor is a stored column so the expression is never plan-time
//! constant-folded (see `sql_division_by_zero.rs`'s module doc for why), and
//! every collection is seeded with one `denom = 0` row so the error is
//! actually reachable.

mod common;

use common::pgwire_harness::TestServer;

/// Seed a schemaless collection with a zero-divisor row plus non-zero rows,
/// all sharing a `grp` value so GROUP BY / self-join produce multi-row groups.
async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (id, denom) in [("a", 2), ("b", 0), ("c", 4)] {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, grp, denom) VALUES ('{id}', 1, {denom})"
        ))
        .await
        .unwrap();
    }
}

/// `SUM` over a per-row expression argument that divides by a zero column.
#[tokio::test]
async fn aggregate_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_agg").await;

    srv.expect_error("SELECT SUM(1/denom) FROM divzero_agg", "22012")
        .await;
}

/// A computed GROUP BY key that divides by a zero column.
#[tokio::test]
async fn group_by_key_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_group").await;

    srv.expect_error(
        "SELECT COUNT(*) FROM divzero_group GROUP BY 10/denom",
        "22012",
    )
    .await;
}

/// A window ORDER BY key that divides by a zero column. `RANK()` (unlike a
/// pure `ROW_NUMBER()`, which numbers in partition order without evaluating the
/// ORDER BY expression) compares the ORDER BY value across rows to detect peer
/// groups, so it evaluates `1/denom` per row and must fail with `22012`.
#[tokio::test]
async fn window_order_by_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_window").await;

    srv.expect_error(
        "SELECT id, RANK() OVER (ORDER BY 1/denom) AS rnk FROM divzero_window",
        "22012",
    )
    .await;
}

/// A hash-join residual ON predicate that divides by a zero column.
#[tokio::test]
async fn join_residual_predicate_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_join").await;

    srv.expect_error(
        "SELECT a.id FROM divzero_join a JOIN divzero_join b \
         ON a.grp = b.grp AND 1/a.denom > 0",
        "22012",
    )
    .await;
}

/// Control: the same aggregate/group-by shapes over only non-zero divisors
/// still succeed — the fix must not turn valid division into an error.
#[tokio::test]
async fn valid_composite_division_still_succeeds() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION divzero_ok").await.unwrap();
    for (id, denom) in [("a", 2), ("c", 4)] {
        srv.exec(&format!(
            "INSERT INTO divzero_ok (id, grp, denom) VALUES ('{id}', 1, {denom})"
        ))
        .await
        .unwrap();
    }

    // SUM(10/denom) = 10/2 + 10/4 = 5 + 2 (integer division) = 7.
    let rows = srv
        .query_text("SELECT SUM(10/denom) FROM divzero_ok")
        .await
        .expect("aggregate over non-zero divisors must succeed");
    assert_eq!(rows.len(), 1);
}

// ── Aggregate accumulator family ─────────────────────────────────────────────
//
// `SUM` above proves one accumulator. Each aggregate kind feeds its argument
// through its own extraction helper (`extract_f64` / `extract_display_string` /
// `extract_value`) in `nodedb-query/src/msgpack_scan/aggregate_helpers.rs`, and
// each of those is a separate `Result` path back out to the statement. An
// accumulator that still folds returns a silently-wrong scalar, so every kind a
// client actually uses is locked independently.

/// `AVG` over a per-row expression argument that divides by a zero column.
/// A folded row would silently change both the numerator and the divisor of the
/// mean, so the returned average is wrong in a way no client can detect.
#[tokio::test]
async fn avg_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_avg").await;

    srv.expect_error("SELECT AVG(1/denom) FROM divzero_avg", "22012")
        .await;
}

/// `MIN` over a per-row expression argument that divides by a zero column.
#[tokio::test]
async fn min_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_min").await;

    srv.expect_error("SELECT MIN(1/denom) FROM divzero_min", "22012")
        .await;
}

/// `MAX` over a per-row expression argument that divides by a zero column.
#[tokio::test]
async fn max_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_max").await;

    srv.expect_error("SELECT MAX(1/denom) FROM divzero_max", "22012")
        .await;
}

/// `COUNT` over a per-row expression argument that divides by a zero column.
/// `COUNT(expr)` counts non-NULL argument values, so a folded row is not merely
/// skipped — it changes the reported cardinality.
#[tokio::test]
async fn count_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_count").await;

    srv.expect_error("SELECT COUNT(1/denom) FROM divzero_count", "22012")
        .await;
}

/// `COUNT(DISTINCT expr)` — the de-duplicating accumulator keeps its own value
/// set and is fed through a different extraction helper than the numeric
/// accumulators above.
#[tokio::test]
async fn distinct_aggregate_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_distinct_agg").await;

    srv.expect_error(
        "SELECT COUNT(DISTINCT 1/denom) FROM divzero_distinct_agg",
        "22012",
    )
    .await;
}

/// An aggregate evaluated per group under `GROUP BY`, rather than as a single
/// whole-table accumulator. The grouped path accumulates into one accumulator
/// per key, a distinct feed from the ungrouped one.
#[tokio::test]
async fn grouped_aggregate_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_grouped_agg").await;

    srv.expect_error(
        "SELECT grp, SUM(1/denom) FROM divzero_grouped_agg GROUP BY grp",
        "22012",
    )
    .await;
}

/// `HAVING` filters groups on an aggregate whose argument divides by zero. The
/// error must surface even though the offending group would have been filtered
/// out of the result — the aggregate is still evaluated to decide that.
///
/// `GROUP BY ... HAVING` currently returns zero groups for *any* predicate,
/// including one over a bare column, so this cannot pass until that is fixed;
/// `sql_aggregate_functions.rs`'s HAVING tests pin the underlying semantics.
#[tokio::test]
async fn having_aggregate_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_having").await;

    srv.expect_error(
        "SELECT grp FROM divzero_having GROUP BY grp HAVING SUM(1/denom) > 0",
        "22012",
    )
    .await;
}

// ── GROUP BY key family ──────────────────────────────────────────────────────

/// A computed GROUP BY key referenced through its SELECT-list alias rather than
/// repeated inline. Alias resolution rewrites the key before the group-key
/// builder ever sees it, so this reaches the builder by a different route than
/// `GROUP BY 10/denom`.
#[tokio::test]
async fn group_by_alias_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_group_alias").await;

    srv.expect_error(
        "SELECT 10/denom AS k, COUNT(*) FROM divzero_group_alias GROUP BY k",
        "22012",
    )
    .await;
}

/// A multi-key GROUP BY where only the second slot is a computed expression.
/// The key builder writes slots positionally and must not lose the error of a
/// later slot behind an earlier bare-column one.
#[tokio::test]
async fn group_by_second_key_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_group_multi").await;

    srv.expect_error(
        "SELECT COUNT(*) FROM divzero_group_multi GROUP BY grp, 10/denom",
        "22012",
    )
    .await;
}

// ── Window function family ───────────────────────────────────────────────────
//
// The existing `window_order_by_...` test covers the ORDER BY key of a ranking
// function. A window spec evaluates expressions in three other positions —
// PARTITION BY, the function argument, and the offset-function argument — and
// each is read by a different evaluator entry point.

/// A window `PARTITION BY` key that divides by a zero column. A folded key
/// silently merges the offending row into a `NULL` partition, changing every
/// window value computed for that partition.
#[tokio::test]
async fn window_partition_by_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_win_part").await;

    srv.expect_error(
        "SELECT id, COUNT(*) OVER (PARTITION BY 10/denom) AS c FROM divzero_win_part",
        "22012",
    )
    .await;
}

/// A window *aggregate* argument that divides by a zero column — the running
/// accumulator path, distinct from the ranking path the ORDER BY test covers.
#[tokio::test]
async fn window_aggregate_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_win_agg").await;

    srv.expect_error(
        "SELECT id, SUM(1/denom) OVER (ORDER BY id) AS s FROM divzero_win_agg",
        "22012",
    )
    .await;
}

/// A window *offset* function argument (`LAG`) that divides by a zero column.
/// Offset functions read the argument at another row's index, a separate
/// evaluator entry point from both the ranking and the aggregate paths.
#[tokio::test]
async fn window_offset_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_win_lag").await;

    srv.expect_error(
        "SELECT id, LAG(1/denom) OVER (ORDER BY id) AS prev FROM divzero_win_lag",
        "22012",
    )
    .await;
}

/// A window aggregate over an explicit frame. The frame evaluator recomputes
/// the argument per frame bound rather than streaming a single running total.
#[tokio::test]
async fn window_framed_aggregate_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_win_frame").await;

    srv.expect_error(
        "SELECT id, AVG(1/denom) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING \
         AND CURRENT ROW) AS a FROM divzero_win_frame",
        "22012",
    )
    .await;
}

// ── Join family ──────────────────────────────────────────────────────────────
//
// The existing `join_residual_predicate_...` test covers the inner hash-join
// probe. A join predicate that cannot be hashed into an equijoin key, and an
// outer join whose unmatched side is preserved, take different probe paths.

/// A non-equijoin predicate with no hashable key, which drives the nested-loop
/// probe rather than the hash probe.
///
/// An ON clause holding only an inequality currently matches nothing at all,
/// even over bare columns, so no candidate pair is ever evaluated and no error
/// can be raised; `sql_join_correctness.rs`'s inequality-only join test pins
/// the underlying semantics.
#[tokio::test]
async fn nested_loop_join_predicate_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_nl_join").await;

    srv.expect_error(
        "SELECT a.id FROM divzero_nl_join a JOIN divzero_nl_join b ON 1/a.denom > b.grp",
        "22012",
    )
    .await;
}

/// A residual ON predicate on a `LEFT JOIN`. The outer-join probe must not
/// convert a predicate error into "no match" and emit a NULL-extended row —
/// that turns a statement failure into a silently fabricated result row.
#[tokio::test]
async fn left_join_residual_predicate_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_left_join").await;

    srv.expect_error(
        "SELECT a.id FROM divzero_left_join a LEFT JOIN divzero_left_join b \
         ON a.grp = b.grp AND 1/a.denom > 0",
        "22012",
    )
    .await;
}

// ── Post-scan ordering and de-duplication ────────────────────────────────────

/// A top-level `ORDER BY` key that divides by a zero column. The sort key is
/// evaluated after projection, in the result-assembly path.
#[tokio::test]
async fn order_by_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_order").await;

    srv.expect_error("SELECT id FROM divzero_order ORDER BY 1/denom", "22012")
        .await;
}

/// `SELECT DISTINCT` over a computed column that divides by a zero column.
/// A folded row collapses into the `NULL` distinct bucket instead of failing.
#[tokio::test]
async fn distinct_projection_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_distinct").await;

    srv.expect_error("SELECT DISTINCT 10/denom FROM divzero_distinct", "22012")
        .await;
}

// ── Columnar engine composite paths ──────────────────────────────────────────
//
// `sql_division_by_zero.rs` locks the columnar row-scope scan (WHERE and
// computed projection). The columnar engine has its own aggregate and group-key
// path, separate from the Document-engine msgpack accumulator, and must reach
// the same SQLSTATE.

async fn seed_columnar(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, grp INT, denom INT) \
         WITH (engine='columnar')"
    ))
    .await
    .unwrap();
    for (id, denom) in [("a", 2), ("b", 0), ("c", 4)] {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, grp, denom) VALUES ('{id}', 1, {denom})"
        ))
        .await
        .unwrap();
    }
}

/// Columnar engine, aggregate argument dividing by a zero column.
#[tokio::test]
async fn columnar_aggregate_argument_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_agg").await;

    srv.expect_error("SELECT SUM(1/denom) FROM cdivzero_agg", "22012")
        .await;
}

/// Columnar engine, computed GROUP BY key dividing by a zero column.
#[tokio::test]
async fn columnar_group_by_key_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_group").await;

    srv.expect_error(
        "SELECT COUNT(*) FROM cdivzero_group GROUP BY 10/denom",
        "22012",
    )
    .await;
}
