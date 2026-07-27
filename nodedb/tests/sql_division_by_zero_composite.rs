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
