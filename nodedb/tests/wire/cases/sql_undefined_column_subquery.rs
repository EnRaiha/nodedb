// SPDX-License-Identifier: BUSL-1.1

//! Unknown column references inside subquery, EXISTS, and LATERAL scopes
//! raise SQLSTATE `42703`, and the derived-alias column set is inferred
//! rather than treated as open.

use crate::harness::TestServer;

async fn seed_strict(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (a INT4 PRIMARY KEY, b INT8) WITH (engine = 'document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} (a, b) VALUES (1, 10)"))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} (a, b) VALUES (2, 20)"))
        .await
        .unwrap();
}

#[tokio::test]
async fn uncorrelated_exists_unknown_inner_where_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_exists_unc_o").await;
    seed_strict(&srv, "sqs_exists_unc_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_exists_unc_o \
         WHERE EXISTS (SELECT 1 FROM sqs_exists_unc_i WHERE nonexistent_col = 1)",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn correlated_exists_unknown_outer_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_exists_outer_o").await;
    seed_strict(&srv, "sqs_exists_outer_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_exists_outer_o AS o \
         WHERE EXISTS (SELECT 1 FROM sqs_exists_outer_i AS i WHERE i.a = o.nonexistent_col)",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn correlated_exists_unknown_inner_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_exists_inner_o").await;
    seed_strict(&srv, "sqs_exists_inner_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_exists_inner_o AS o \
         WHERE EXISTS (SELECT 1 FROM sqs_exists_inner_i AS i WHERE i.nonexistent_col = o.a)",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn not_exists_unknown_inner_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_notexists_o").await;
    seed_strict(&srv, "sqs_notexists_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_notexists_o \
         WHERE NOT EXISTS (SELECT 1 FROM sqs_notexists_i WHERE nonexistent_col = 1)",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn in_subquery_unknown_inner_where_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_in_o").await;
    seed_strict(&srv, "sqs_in_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_in_o WHERE a IN (SELECT a FROM sqs_in_i WHERE nonexistent_col = 1)",
        "42703",
    )
    .await;
}

/// The LATERAL derived alias `x` projects only `a`. Selecting an
/// unqualified name outside that set must not resolve as open.
#[tokio::test]
async fn lateral_alias_unknown_projected_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_lat_unk_o").await;
    seed_strict(&srv, "sqs_lat_unk_i").await;
    srv.expect_error(
        "SELECT x.nonexistent_col FROM sqs_lat_unk_o AS o, \
         LATERAL (SELECT i.a FROM sqs_lat_unk_i AS i WHERE i.a = o.a) x",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn lateral_correlated_predicate_unknown_outer_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_lat_outerunk_o").await;
    seed_strict(&srv, "sqs_lat_outerunk_i").await;
    srv.expect_error(
        "SELECT o.a FROM sqs_lat_outerunk_o AS o, \
         LATERAL (SELECT i.a FROM sqs_lat_outerunk_i AS i WHERE i.a = o.nonexistent_col) x",
        "42703",
    )
    .await;
}

/// The non-LATERAL derived alias `t` projects only `a`. Selecting an
/// unqualified name outside that set must not resolve as open.
#[tokio::test]
async fn derived_subquery_alias_unknown_projected_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_derived_unk_src").await;
    srv.expect_error(
        "SELECT t.nonexistent_col FROM (SELECT a FROM sqs_derived_unk_src) AS t",
        "42703",
    )
    .await;
}

/// Positive control: an uncorrelated EXISTS on a real column still guards
/// the outer scan — this must not be a false-positive `42703`.
#[tokio::test]
async fn uncorrelated_exists_known_column_filters_correctly() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_exists_pos_o").await;
    seed_strict(&srv, "sqs_exists_pos_i").await;
    let rows = srv
        .query_rows("SELECT a FROM sqs_exists_pos_o WHERE EXISTS (SELECT 1 FROM sqs_exists_pos_i WHERE b = 20) ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
}

/// Positive control: a correlated EXISTS on real columns on both sides
/// returns exactly the outer rows that have a matching inner row.
#[tokio::test]
async fn correlated_exists_known_columns_returns_matching_rows() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_exists_corr_o").await;
    srv.exec(
        "CREATE COLLECTION sqs_exists_corr_i (a INT4 PRIMARY KEY, b INT8) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO sqs_exists_corr_i (a, b) VALUES (1, 100)")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT o.a FROM sqs_exists_corr_o AS o \
             WHERE EXISTS (SELECT 1 FROM sqs_exists_corr_i AS i WHERE i.a = o.a) \
             ORDER BY o.a",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"]]);
}

/// Positive control: NOT EXISTS on the same setup returns the complementary
/// row set.
#[tokio::test]
async fn not_exists_known_columns_returns_complementary_rows() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_notexists_pos_o").await;
    srv.exec(
        "CREATE COLLECTION sqs_notexists_pos_i (a INT4 PRIMARY KEY, b INT8) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO sqs_notexists_pos_i (a, b) VALUES (1, 100)")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT o.a FROM sqs_notexists_pos_o AS o \
             WHERE NOT EXISTS (SELECT 1 FROM sqs_notexists_pos_i AS i WHERE i.a = o.a) \
             ORDER BY o.a",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["2"]]);
}

/// Positive control: a LATERAL alias projecting a real column still
/// selects and returns the expected value.
#[tokio::test]
async fn lateral_alias_known_projected_column_returns_values() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_lat_pos_o").await;
    seed_strict(&srv, "sqs_lat_pos_i").await;
    let rows = srv
        .query_rows(
            "SELECT x.a FROM sqs_lat_pos_o AS o, \
             LATERAL (SELECT i.a FROM sqs_lat_pos_i AS i WHERE i.a = o.a) x \
             ORDER BY x.a",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
}

/// Positive control: a non-LATERAL derived alias projecting a real column
/// still selects and returns the expected values.
#[tokio::test]
async fn derived_subquery_alias_known_projected_column_returns_values() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_derived_pos_src").await;
    let rows = srv
        .query_rows("SELECT t.a FROM (SELECT a FROM sqs_derived_pos_src) AS t ORDER BY t.a")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
}

/// The schemaless engine accepts undeclared fields on write
/// (`docs/documents.md:43`). A derived alias that projects `*` over an
/// open-schema source carries that openness through: an unqualified name
/// outside the declared fields still resolves to NULL, never errors.
#[tokio::test]
async fn schemaless_derived_subquery_unknown_column_resolves_to_null() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION sqs_schemaless_src (id INT PRIMARY KEY, x INT)")
        .await
        .unwrap();
    srv.exec("INSERT INTO sqs_schemaless_src (id, x) VALUES (1, 10)")
        .await
        .unwrap();
    srv.exec("INSERT INTO sqs_schemaless_src (id, x) VALUES (2, 20)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT t.nonexistent_col FROM (SELECT * FROM sqs_schemaless_src) AS t")
        .await
        .expect("an unknown identifier over a schemaless source must not error");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(row[0].is_empty(), "expected NULL, got {row:?}");
    }
}

/// The `IN (SELECT ...)` rewrite consumes the whole predicate, so the outer
/// operand never reaches the expression converter. An unknown outer column
/// must still raise `42703`.
#[tokio::test]
async fn in_subquery_unknown_outer_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_in_outer_o").await;
    seed_strict(&srv, "sqs_in_outer_i").await;
    srv.expect_error(
        "SELECT a FROM sqs_in_outer_o WHERE nonexistent_col IN (SELECT a FROM sqs_in_outer_i)",
        "42703",
    )
    .await;
}

/// The same rewrite with a qualified outer operand.
#[tokio::test]
async fn in_subquery_unknown_qualified_outer_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "sqs_in_qual_o").await;
    seed_strict(&srv, "sqs_in_qual_i").await;
    srv.expect_error(
        "SELECT o.a FROM sqs_in_qual_o AS o \
         WHERE o.nonexistent_col IN (SELECT a FROM sqs_in_qual_i)",
        "42703",
    )
    .await;
}
