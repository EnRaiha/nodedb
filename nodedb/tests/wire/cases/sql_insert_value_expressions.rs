// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for typed value expressions in `INSERT … VALUES (…)`.
//!
//! The planner lowers each `VALUES (…)` element through a hand-rolled
//! `expr_to_sql_value` walker. Anything it does not have an explicit arm
//! for is rejected at plan time with `unsupported: value expression`.
//! `<lit>::<type>` casts — and the equivalent `CAST(<lit> AS <type>)`
//! form — are a documented part of the SQL surface (`query-language.md`
//! lists both `CAST(expr AS type)` and `expr::type`), and the
//! `nodedb-client` `vector_insert(…, Some(meta))` path emits
//! `'{…}'::JSONB` for the metadata document. The same hand-rolled walker
//! also rejects any other constant-foldable expression in value position —
//! arithmetic (`60 * 60 * 24`), string concatenation (`'a' || 'b'`),
//! parenthesised literals — even though the canonical `convert_expr` +
//! `const_fold` pipeline that the projection path uses folds all of them.
//! These tests assert that a typed cast or a foldable expression in value
//! position behaves exactly as the equivalent literal would.

use crate::harness::TestServer;

// ── JSON / JSONB casts (the vector_insert metadata path) ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_jsonb_cast_matches_uncast_literal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION jsonb_cast_docs")
        .await
        .unwrap();

    server
        .exec(r#"INSERT INTO jsonb_cast_docs (id, fields) VALUES ('plain', '{"k":1}')"#)
        .await
        .unwrap();
    server
        .exec(r#"INSERT INTO jsonb_cast_docs (id, fields) VALUES ('cast', '{"k":1}'::JSONB)"#)
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT fields FROM jsonb_cast_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let cast = server
        .query_text_joined("SELECT fields FROM jsonb_cast_docs WHERE id = 'cast'")
        .await
        .unwrap();
    assert_eq!(cast.len(), 1, "::JSONB-cast row must be stored: {cast:?}");
    assert_eq!(
        cast, plain,
        "'{{...}}'::JSONB must store the same value as the bare '{{...}}' literal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_json_cast_matches_uncast_literal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION json_cast_docs")
        .await
        .unwrap();

    server
        .exec(r#"INSERT INTO json_cast_docs (id, fields) VALUES ('plain', '{"k":1}')"#)
        .await
        .unwrap();
    server
        .exec(r#"INSERT INTO json_cast_docs (id, fields) VALUES ('cast', '{"k":1}'::JSON)"#)
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT fields FROM json_cast_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let cast = server
        .query_text_joined("SELECT fields FROM json_cast_docs WHERE id = 'cast'")
        .await
        .unwrap();
    assert_eq!(cast.len(), 1, "::JSON-cast row must be stored: {cast:?}");
    assert_eq!(cast, plain, "'{{...}}'::JSON must match the bare literal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_cast_function_form_jsonb_matches_uncast_literal() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION cast_fn_docs").await.unwrap();

    server
        .exec(r#"INSERT INTO cast_fn_docs (id, fields) VALUES ('plain', '{"k":1}')"#)
        .await
        .unwrap();
    server
        .exec(r#"INSERT INTO cast_fn_docs (id, fields) VALUES ('cast', CAST('{"k":1}' AS JSONB))"#)
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT fields FROM cast_fn_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let cast = server
        .query_text_joined("SELECT fields FROM cast_fn_docs WHERE id = 'cast'")
        .await
        .unwrap();
    assert_eq!(
        cast.len(),
        1,
        "CAST(... AS JSONB) row must be stored: {cast:?}"
    );
    assert_eq!(
        cast, plain,
        "CAST('{{...}}' AS JSONB) must match the bare '{{...}}' literal"
    );
}

/// Regression guard against debug-formatted-AST leakage: the bug masks the
/// cast at plan time, but a careless fix could stringify the `Cast` AST node
/// (`'{...}'::JSONB`) and store *that* as the field value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_jsonb_cast_does_not_leak_ast_text() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION jsonb_leak_docs")
        .await
        .unwrap();

    server
        .exec(r#"INSERT INTO jsonb_leak_docs (id, fields) VALUES ('a', '{"k":42}'::JSONB)"#)
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT fields FROM jsonb_leak_docs WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("42"),
        "stored value should carry the data: {rows:?}"
    );
    assert!(
        !rows[0].contains("JSONB") && !rows[0].contains("::") && !rows[0].contains("Cast"),
        "field value must not contain the cast AST text: {rows:?}"
    );
}

// ── Numeric casts on literals in value position ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_integer_cast_matches_uncast_literal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION int_cast_docs")
        .await
        .unwrap();

    server
        .exec("INSERT INTO int_cast_docs (id, n) VALUES ('plain', 42)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO int_cast_docs (id, n) VALUES ('cast', '42'::INTEGER)")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT n FROM int_cast_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let cast = server
        .query_text_joined("SELECT n FROM int_cast_docs WHERE id = 'cast'")
        .await
        .unwrap();
    assert_eq!(cast.len(), 1, "'42'::INTEGER row must be stored: {cast:?}");
    assert_eq!(
        cast, plain,
        "'42'::INTEGER must store the integer 42, identical to the bare literal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_numeric_cast_matches_uncast_literal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION num_cast_docs")
        .await
        .unwrap();

    server
        .exec("INSERT INTO num_cast_docs (id, n) VALUES ('plain', 3.5)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO num_cast_docs (id, n) VALUES ('cast', '3.5'::NUMERIC)")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT n FROM num_cast_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let cast = server
        .query_text_joined("SELECT n FROM num_cast_docs WHERE id = 'cast'")
        .await
        .unwrap();
    assert_eq!(cast.len(), 1, "'3.5'::NUMERIC row must be stored: {cast:?}");
    assert_eq!(
        cast, plain,
        "'3.5'::NUMERIC must store the decimal 3.5, identical to the bare literal"
    );
}

// ── Other constant-foldable expressions in value position ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_arithmetic_folds_to_result() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION arith_docs").await.unwrap();

    server
        .exec("INSERT INTO arith_docs (id, ttl) VALUES ('plain', 86400)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO arith_docs (id, ttl) VALUES ('expr', 60 * 60 * 24)")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT ttl FROM arith_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let expr = server
        .query_text_joined("SELECT ttl FROM arith_docs WHERE id = 'expr'")
        .await
        .unwrap();
    assert_eq!(
        expr.len(),
        1,
        "arithmetic-valued row must be stored: {expr:?}"
    );
    assert_eq!(
        expr, plain,
        "60 * 60 * 24 in value position must fold to 86400"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_string_concat_folds() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION concat_docs").await.unwrap();

    server
        .exec("INSERT INTO concat_docs (id, label) VALUES ('plain', 'foobar')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO concat_docs (id, label) VALUES ('expr', 'foo' || 'bar')")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT label FROM concat_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let expr = server
        .query_text_joined("SELECT label FROM concat_docs WHERE id = 'expr'")
        .await
        .unwrap();
    assert_eq!(expr.len(), 1, "concat-valued row must be stored: {expr:?}");
    assert_eq!(
        expr, plain,
        "'foo' || 'bar' in value position must fold to 'foobar'"
    );
}

// ── Unary negation, routed through `convert_expr` + `const_fold` rather than
//    a `UnaryOp { Minus, .. }` special-case ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_negative_integer_literal() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION neg_int_docs").await.unwrap();

    server
        .exec("INSERT INTO neg_int_docs (id, n) VALUES ('plain', -42)")
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT n FROM neg_int_docs WHERE id = 'plain'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "negative-integer row must be stored: {rows:?}"
    );
    assert_eq!(rows[0], "-42", "expected -42, got {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_negative_float_literal() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION neg_float_docs")
        .await
        .unwrap();

    server
        .exec("INSERT INTO neg_float_docs (id, n) VALUES ('plain', -3.5)")
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT n FROM neg_float_docs WHERE id = 'plain'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "negative-float row must be stored: {rows:?}");
    assert!(rows[0].starts_with("-3.5"), "expected -3.5, got {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_negation_in_arithmetic() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION neg_arith_docs")
        .await
        .unwrap();

    server
        .exec("INSERT INTO neg_arith_docs (id, n) VALUES ('plain', -3600)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO neg_arith_docs (id, n) VALUES ('expr', 60 * -60)")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT n FROM neg_arith_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let expr = server
        .query_text_joined("SELECT n FROM neg_arith_docs WHERE id = 'expr'")
        .await
        .unwrap();
    assert_eq!(expr.len(), 1, "60 * -60 row must be stored: {expr:?}");
    assert_eq!(expr, plain, "60 * -60 must fold to -3600");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_parenthesized_literal() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION paren_docs").await.unwrap();

    server
        .exec("INSERT INTO paren_docs (id, n) VALUES ('plain', 7)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO paren_docs (id, n) VALUES ('expr', (7))")
        .await
        .unwrap();

    let plain = server
        .query_text_joined("SELECT n FROM paren_docs WHERE id = 'plain'")
        .await
        .unwrap();
    let expr = server
        .query_text_joined("SELECT n FROM paren_docs WHERE id = 'expr'")
        .await
        .unwrap();
    assert_eq!(
        expr.len(),
        1,
        "parenthesised-literal row must be stored: {expr:?}"
    );
    assert_eq!(
        expr, plain,
        "(7) in value position must equal the bare literal 7"
    );
}
