// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for division-by-zero handling: division and modulo by a
//! zero divisor must fail the statement at RUNTIME with SQLSTATE `22012`
//! (`division_by_zero`), not silently evaluate to a NULL row — the pre-fix
//! behavior. `nodedb-query/src/expr/binary.rs`'s `eval_binary_op` folded a
//! zero-divisor `/` or `%` to `Value::Null` for both the f64 and Decimal
//! arithmetic paths; `nodedb-query/src/functions/math.rs`'s `mod` scalar
//! function did the same. This is a runtime concern, distinct from unit 1's
//! plan-time `UndefinedFunction` gate: `1/0` is a perfectly well-typed,
//! well-formed expression — the error can only be known once the divisor
//! value is evaluated.
//!
//! ## Why every test here uses `FROM <collection>` with a column divisor
//!
//! A bare `SELECT <expr>` with NO `FROM` clause never reaches the runtime
//! evaluator this unit fixes: `nodedb-sql`'s planner special-cases a
//! from-less SELECT into a plan-time `SqlPlan::ConstantResult`
//! (`nodedb-sql/src/planner/select/select_stmt.rs`), computed via
//! `eval_constant_expr` (`nodedb-sql/src/planner/select/helpers.rs`), which
//! is `const_fold::fold_constant(expr, ..).unwrap_or(SqlValue::Null)` — ANY
//! expression the plan-time constant folder can't fully fold (not just
//! division-by-zero; integer overflow and every `CASE` expression hit the
//! exact same `unwrap_or(Null)`, see `sql_arithmetic_overflow.rs`'s hedged
//! assertions for the pre-existing overflow case) silently becomes NULL
//! there, before any row-scope `SqlExpr::eval` call exists. That is a
//! separate, pre-existing, general fold-coverage gap in `nodedb-sql`,
//! architecturally isolated from the `nodedb-query` row-scope evaluator this
//! unit modifies — out of scope here (fixing it would mean also changing
//! integer-overflow and `CASE` behavior for from-less SELECTs, well beyond
//! "division/modulo-by-zero only").
//!
//! A column reference is never constant-foldable (`fold_constant` has no
//! arm for `SqlExpr::Column`), so `FROM <collection>` with the divisor read
//! from a column guarantees the expression reaches the real row-scope
//! evaluator these tests are meant to exercise — the same path a real
//! `SELECT ... FROM t WHERE <predicate-referencing-a-column>` query takes.
//!
//! ## Why the float-path test divides an integer literal by a float column
//!
//! A decimal-point numeric literal (e.g. `1.5`) combined arithmetically with
//! a stored `FLOAT` column is, independently of this fix, always NULL in
//! this codebase — confirmed by direct diagnosis: `SELECT 4.0 + fdenom`
//! (fdenom = 2.0, a perfectly valid addition) already returns NULL with no
//! error on an unmodified evaluator, while `SELECT 4 + fdenom` (integer
//! literal) correctly returns `6`. That is a pre-existing, general
//! decimal-literal/float-column type-coercion gap, unrelated to division or
//! to zero divisors (it reproduces with `+`), and out of scope for a
//! division/modulo-by-zero-only fix. `2/fdenom` (integer literal ÷ float
//! column) sidesteps it while still exercising the f64 arithmetic branch of
//! `eval_binary_op` (neither operand is `Value::Decimal`, so the Decimal
//! branch is provably not what's under test here).

mod common;

use common::pgwire_harness::TestServer;

async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('a', 1, 1.0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('b', 0, 0.0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom, fdenom) VALUES ('c', 2, 2.0)"
    ))
    .await
    .unwrap();
}

/// Integer division by a zero-valued column.
#[tokio::test]
async fn integer_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_int_216").await;

    srv.expect_error(
        "SELECT 1/denom FROM divzero_int_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// Float division by a zero-valued column — the f64 arithmetic path in
/// `eval_binary_op`, distinct from the Decimal/Integer path (see the module
/// doc comment for why the dividend is an integer literal, not `1.5`).
#[tokio::test]
async fn float_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_float_216").await;

    srv.expect_error(
        "SELECT 2/fdenom FROM divzero_float_216 WHERE id = 'b'",
        "22012",
    )
    .await;

    // Control: the same expression over a non-zero float column still
    // succeeds with the correct value.
    let rows = srv
        .query_text("SELECT 2/fdenom FROM divzero_float_216 WHERE id = 'c'")
        .await
        .expect("2 / 2.0 must still succeed");
    assert_eq!(rows, vec!["1".to_string()]);
}

/// Modulo by a zero-valued column.
#[tokio::test]
async fn modulo_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_mod_216").await;

    srv.expect_error(
        "SELECT 5 % denom FROM divzero_mod_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// The WHERE-clause behavior flip (intended and Postgres-correct): a WHERE
/// predicate that divides by a column whose
/// value is zero for some row used to fold that row's predicate to
/// `Value::Null`/false and silently exclude it from the result — a
/// division-by-zero was indistinguishable from a legitimate non-match.
/// It must now fail the whole statement with `22012` instead, exactly like
/// evaluating the same expression in the SELECT list would.
#[tokio::test]
async fn where_clause_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_where_216").await;

    srv.expect_error(
        "SELECT * FROM divzero_where_216 WHERE 10 / denom > 1",
        "22012",
    )
    .await;
}

/// CASE laziness end-to-end: an untaken CASE branch containing a
/// division-by-zero expression must never be evaluated, so the statement
/// succeeds and returns the ELSE value. Uses a column reference (`denom`)
/// so the CASE expression is not plan-time constant-folded away — see the
/// module doc comment; `SqlExpr::Case` is never constant-foldable anyway,
/// so this exercises the row-scope evaluator's own CASE laziness
/// (`nodedb-query/src/expr/eval.rs`), which is exactly what's under test.
#[tokio::test]
async fn case_laziness_untaken_branch_succeeds() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_case_216").await;

    let rows = srv
        .query_text(
            "SELECT CASE WHEN false THEN 1/denom ELSE 42 END \
             FROM divzero_case_216 WHERE id = 'b'",
        )
        .await
        .expect("untaken CASE branch with 1/denom must not raise an error");
    assert_eq!(rows, vec!["42".to_string()]);
}

/// Control: a valid (non-zero-divisor) division still succeeds and returns
/// the correct value — the fix must not turn division itself into an error.
#[tokio::test]
async fn valid_division_still_succeeds() {
    let srv = TestServer::start().await;
    seed(&srv, "divzero_valid_216").await;

    let rows = srv
        .query_text("SELECT 10/denom FROM divzero_valid_216 WHERE id = 'c'")
        .await
        .expect("10 / 2 must still succeed");
    assert_eq!(rows, vec!["5".to_string()]);
}

// ── Columnar engine coverage (adversarial-review follow-up) ───────────────────
//
// The Document-engine tests above exercise `nodedb-query`'s row-scope
// evaluator directly. The columnar engine has its own scan handler
// (`nodedb/src/data/executor/handlers/columnar_read/scan.rs`) with its own
// error-plumbing from that same evaluator back out to a pgwire response —
// before this fix, four blocks there wrapped a division/modulo-by-zero
// `EvalError`/`crate::Error` as `ErrorCode::Internal { detail: e.to_string() }`
// instead of propagating the typed error, so a columnar query hit generic
// `XX000` instead of the correct `22012`. These tests prove the columnar
// path end-to-end, independent of the Document-engine coverage above.

async fn seed_columnar(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, denom INT) WITH (engine='columnar')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('a', 1)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('b', 0)"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, denom) VALUES ('c', 2)"
    ))
    .await
    .unwrap();
}

/// Columnar engine, WHERE-clause division by zero: `row_matches_filters`'s
/// `Err` arm in `execute_columnar_scan`'s live-memtable phase.
#[tokio::test]
async fn columnar_where_clause_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_where_216").await;

    srv.expect_error(
        "SELECT * FROM cdivzero_where_216 WHERE 10 / denom > 1",
        "22012",
    )
    .await;
}

/// Columnar engine, computed SELECT column division by zero:
/// `row_to_projected_json`'s `Err` arm in `execute_columnar_scan`'s
/// live-memtable phase (a real, non-empty `computed_cols`).
///
/// `denom` must be listed explicitly alongside the computed `ratio` column
/// (not just referenced inside the expression): `row_to_projected_json`
/// only includes a stored column in the row object it hands to the computed
/// expression evaluator when that column is itself in the projection list
/// (or force-included) — an explicit, non-computed `SELECT` entry for every
/// column a computed expression reads from is how a real client query would
/// request the same shape.
#[tokio::test]
async fn columnar_computed_column_division_by_zero_errors_22012() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_select_216").await;

    srv.expect_error(
        "SELECT id, denom, 10/denom AS ratio FROM cdivzero_select_216 WHERE id = 'b'",
        "22012",
    )
    .await;
}

/// Control: a valid (non-zero-divisor) columnar computed column still
/// succeeds — the fix must not turn columnar division itself into an error.
#[tokio::test]
async fn columnar_computed_column_valid_division_still_succeeds() {
    let srv = TestServer::start().await;
    seed_columnar(&srv, "cdivzero_valid_216").await;

    let rows = srv
        .query_named_rows(
            "SELECT id, denom, 10/denom AS ratio FROM cdivzero_valid_216 WHERE id = 'c'",
        )
        .await
        .expect("10 / 2 must still succeed on the columnar engine");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("ratio").map(String::as_str), Some("5"));
}
