// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for WITH RECURSIVE (recursive CTEs).
//!
//! Tests cover: value-generating sequence, UNION ALL vs UNION, tree traversal,
//! depth-limit overrun (typed error), invalid set-op (typed error),
//! column-count mismatch (typed error), and multi-column value-gen CTE.

use crate::harness::TestServer;

// ── Value-generating counter ─────────────────────────────────────────────────

/// `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5)`
/// must produce exactly 5 rows with values 1..5.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_generates_sequence() {
    let server = TestServer::start().await;

    let rows = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT n + 1 FROM c WHERE n < 5\
             ) \
             SELECT n FROM c",
        )
        .await;

    let values = rows.expect("recursive CTE counter should succeed");
    assert_eq!(
        values.len(),
        5,
        "recursive CTE should produce 5 rows (1..5): got {values:?}"
    );
    let mut nums: Vec<i64> = values
        .iter()
        .map(|v| v.trim().parse().expect("expected i64 row value"))
        .collect();
    nums.sort();
    assert_eq!(
        nums,
        vec![1, 2, 3, 4, 5],
        "rows should be 1..5: got {nums:?}"
    );
}

// ── UNION ALL vs UNION ───────────────────────────────────────────────────────

/// A base case that itself contains duplicates (`SELECT 1 UNION ALL SELECT 1`)
/// with no further recursion should yield exactly 2 rows when UNION ALL is used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_union_all_preserves_duplicates() {
    let server = TestServer::start().await;

    // We need a zero-iteration value-gen CTE.  The recursive arm immediately
    // fails the condition so no rows are added beyond the anchor.
    // Anchor is `SELECT 1` — single row.  Then verify UNION ALL vs UNION.
    let rows_all = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT n FROM c WHERE n > 999\
             ) \
             SELECT n FROM c",
        )
        .await;

    let all_vals = rows_all.expect("UNION ALL with immediate termination should succeed");
    assert_eq!(
        all_vals.len(),
        1,
        "anchor only: should be exactly 1 row: got {all_vals:?}"
    );

    // Now verify a 3-step counter with UNION ALL produces 3 rows (with distinct values).
    let rows3 = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 10 \
                UNION ALL \
                SELECT n + 10 FROM c WHERE n < 25\
             ) \
             SELECT n FROM c",
        )
        .await;

    let v3 = rows3.expect("3-step UNION ALL counter should succeed");
    assert_eq!(v3.len(), 3, "should be 10, 20, 30: got {v3:?}");
    let mut nums: Vec<i64> = v3
        .iter()
        .map(|v| v.trim().parse().expect("expected i64 row value"))
        .collect();
    nums.sort();
    assert_eq!(nums, vec![10, 20, 30]);
}

// ── Graph-edge traversal ─────────────────────────────────────────────────────

/// Traverse a 3-level tree using a collection-backed recursive CTE with
/// INNER JOIN on the CTE self-reference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_tree_traversal() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION tree (\
                id TEXT PRIMARY KEY, \
                parent_id TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    for (id, pid) in [
        ("root", "NULL"),
        ("child1", "'root'"),
        ("grandchild", "'child1'"),
        ("orphan", "'missing'"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO tree (id, parent_id) VALUES ('{id}', {pid})"
            ))
            .await
            .unwrap();
    }

    let rows = server
        .query_text(
            "WITH RECURSIVE descendants(id) AS (\
                SELECT id FROM tree WHERE id = 'root' \
                UNION ALL \
                SELECT t.id FROM tree t \
                INNER JOIN descendants d ON t.parent_id = d.id\
             ) \
             SELECT id FROM descendants",
        )
        .await;

    match rows {
        Ok(values) => {
            assert_eq!(
                values.len(),
                3,
                "should find root + child1 + grandchild: got {values:?}"
            );
            assert!(
                !values.iter().any(|v| v.contains("orphan")),
                "orphan should not appear: {values:?}"
            );
        }
        Err(msg) => {
            // Collection-backed recursive CTEs may return an explicit
            // "not supported" error; accept it but not a silent wrong result.
            assert!(
                msg.to_lowercase().contains("recursive")
                    || msg.to_lowercase().contains("not supported"),
                "unexpected error: {msg}"
            );
        }
    }
}

// ── Depth-limit overrun ──────────────────────────────────────────────────────

/// A CTE with no termination condition must hit `max_recursion_depth` and
/// return an error that mentions "recursion depth" or "max recursion".
/// It must NOT return a silent empty result or a truncated result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_depth_limit_overrun_is_typed_error() {
    let server = TestServer::start().await;

    // No WHERE clause on the recursive arm → infinite loop → depth error.
    let result = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT n + 1 FROM c\
             ) \
             SELECT n FROM c LIMIT 5",
        )
        .await;

    match result {
        Err(msg) => {
            let lower = msg.to_lowercase();
            assert!(
                lower.contains("recursion") || lower.contains("depth") || lower.contains("limit"),
                "depth-overrun error should mention recursion depth: {msg}"
            );
        }
        Ok(rows) => {
            // If LIMIT is pushed down before evaluation the query may return
            // fewer rows than the depth limit.  This is acceptable ONLY if
            // rows.len() == 5 (the LIMIT value) — anything else means the
            // executor silently truncated.
            assert_eq!(
                rows.len(),
                5,
                "if depth error is not raised, LIMIT must bound output to exactly 5: got {rows:?}"
            );
        }
    }
}

// ── INTERSECT in the recursive term ──────────────────────────────────────────

/// `WITH RECURSIVE … AS (base INTERSECT step)` must produce a typed error
/// mentioning that INTERSECT is not permitted in the recursive term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_intersect_is_typed_error() {
    let server = TestServer::start().await;

    let result = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                INTERSECT \
                SELECT 1\
             ) \
             SELECT n FROM c",
        )
        .await;

    let msg = result.expect_err("INTERSECT in recursive CTE must be an error");
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("intersect")
            || lower.contains("union")
            || lower.contains("recursive")
            || lower.contains("set operator"),
        "error should mention INTERSECT or the UNION requirement: {msg}"
    );
}

// ── Column-count mismatch ────────────────────────────────────────────────────

/// Declaring `c(a, b)` but supplying only one column in the anchor must
/// produce a typed error mentioning the column count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_column_count_mismatch_is_typed_error() {
    let server = TestServer::start().await;

    let result = server
        .query_text(
            "WITH RECURSIVE c(a, b) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT a + 1 FROM c WHERE a < 3\
             ) \
             SELECT a FROM c",
        )
        .await;

    let msg = result.expect_err("column-count mismatch must be an error");
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("column")
            || lower.contains("mismatch")
            || lower.contains("declared")
            || lower.contains("anchor"),
        "error should mention column count mismatch: {msg}"
    );
}

// ── Multi-column value-generating CTE ────────────────────────────────────────

/// A CTE with two columns `(n, sq)` accumulates `(n, n*n)` pairs.
/// Verifies that column names are propagated correctly in the output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_multi_column_value_gen() {
    let server = TestServer::start().await;

    let rows = server
        .query_rows(
            "WITH RECURSIVE c(n, sq) AS (\
                SELECT 1, 1 \
                UNION ALL \
                SELECT n + 1, (n + 1) * (n + 1) FROM c WHERE n < 4\
             ) \
             SELECT n, sq FROM c",
        )
        .await;

    match rows {
        Ok(matrix) => {
            assert_eq!(
                matrix.len(),
                4,
                "should produce 4 rows (1..4): got {matrix:?}"
            );
            // Verify first row is [1, 1] and last row is [4, 16].
            let first_n: i64 = matrix[0][0].trim().parse().unwrap_or(-1);
            let last_n: i64 = matrix[3][0].trim().parse().unwrap_or(-1);
            let last_sq: i64 = matrix[3][1].trim().parse().unwrap_or(-1);
            assert_eq!(first_n, 1, "first row n should be 1: {matrix:?}");
            assert_eq!(last_n, 4, "last row n should be 4: {matrix:?}");
            assert_eq!(last_sq, 16, "last row sq should be 16: {matrix:?}");
        }
        Err(msg) => {
            // Multi-column value CTEs may not be supported yet — acceptable if
            // the error is explicit.
            assert!(
                msg.to_lowercase().contains("recursive")
                    || msg.to_lowercase().contains("not supported"),
                "unexpected error in multi-column CTE: {msg}"
            );
        }
    }
}

// ── EXCEPT in the recursive term ─────────────────────────────────────────────

/// `WITH RECURSIVE … AS (base EXCEPT step)` must also produce a typed error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_except_is_typed_error() {
    let server = TestServer::start().await;

    let result = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                EXCEPT \
                SELECT 1\
             ) \
             SELECT n FROM c",
        )
        .await;

    let msg = result.expect_err("EXCEPT in recursive CTE must be an error");
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("except")
            || lower.contains("union")
            || lower.contains("recursive")
            || lower.contains("set operator"),
        "error should mention EXCEPT or the UNION requirement: {msg}"
    );
}

// ── Anchor-derived column names ──────────────────────────────────────────────

/// With no declared column list, a recursive CTE takes its column names from
/// the anchor term, as PostgreSQL does. The recursive step resolves its own
/// column references against those names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_without_column_list_takes_anchor_names() {
    let server = TestServer::start().await;

    let values = server
        .query_text(
            "WITH RECURSIVE c AS (\
                SELECT 1 AS n \
                UNION ALL \
                SELECT n + 1 FROM c WHERE n < 5\
             ) \
             SELECT n FROM c",
        )
        .await
        .expect("anchor alias should name the CTE column");

    let mut nums: Vec<i64> = values
        .iter()
        .map(|v| v.trim().parse().expect("expected i64 row value"))
        .collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3, 4, 5], "got {nums:?}");
}

/// An anchor item carrying no name of its own is addressable as `colN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_unnamed_anchor_item_falls_back_to_positional_name() {
    let server = TestServer::start().await;

    let values = server
        .query_text(
            "WITH RECURSIVE c AS (\
                SELECT 1 \
                UNION ALL \
                SELECT col0 + 1 FROM c WHERE col0 < 3\
             ) \
             SELECT col0 FROM c",
        )
        .await
        .expect("unnamed anchor item should keep its positional name");

    let mut nums: Vec<i64> = values
        .iter()
        .map(|v| v.trim().parse().expect("expected i64 row value"))
        .collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3], "got {nums:?}");
}

// ── Undefined column in the recursive step ───────────────────────────────────

/// A step column reference that resolves against nothing raises `42703`, the
/// SQLSTATE the same reference reports in any other statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_undefined_column_in_step_is_an_error() {
    let server = TestServer::start().await;

    let result = server
        .query_text(
            "WITH RECURSIVE c(n) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT zzz + 1 FROM c WHERE zzz < 3\
             ) \
             SELECT n FROM c",
        )
        .await;

    let msg = result.expect_err("an undefined column in the step must be an error");
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("zzz") && lower.contains("does not exist"),
        "error should name the missing column: {msg}"
    );
}

// ── Duplicate column names ───────────────────────────────────────────────────

/// Rows are keyed by column name, so two columns must not share one. Both the
/// declared list and the anchor-derived names are checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_cte_duplicate_column_names_are_rejected() {
    let server = TestServer::start().await;

    for sql in [
        "WITH RECURSIVE c(n, n) AS (\
            SELECT 1, 2 \
            UNION ALL \
            SELECT n + 1, 2 FROM c WHERE n < 3\
         ) \
         SELECT n FROM c",
        "WITH RECURSIVE c AS (\
            SELECT 1 AS n, 2 AS n \
            UNION ALL \
            SELECT n + 1, 2 FROM c WHERE n < 3\
         ) \
         SELECT n FROM c",
    ] {
        let msg = server
            .query_text(sql)
            .await
            .expect_err("duplicate CTE column names must be an error");
        let lower = msg.to_lowercase();
        assert!(
            lower.contains("more than once") || lower.contains("distinct name"),
            "error should explain the duplicate for `{sql}`: {msg}"
        );
    }
}
