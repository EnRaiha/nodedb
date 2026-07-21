// SPDX-License-Identifier: BUSL-1.1

//! PostgreSQL-compatible `regclass` input and output semantics.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn quoted_relation_name_resolves_like_the_unquoted_form() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION quoted_regclass_target \
         (id BIGINT PRIMARY KEY, name TEXT DEFAULT 'x', qty INT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");

    let mut bare = srv
        .query_text(
            "SELECT attname FROM pg_attribute \
             WHERE attrelid = 'quoted_regclass_target'::regclass",
        )
        .await
        .expect("bare regclass lookup");
    let mut quoted = srv
        .query_text(
            "SELECT attname FROM pg_attribute \
             WHERE attrelid = '\"quoted_regclass_target\"'::regclass",
        )
        .await
        .expect("quoted regclass lookup");
    bare.sort();
    quoted.sort();

    assert_eq!(bare, vec!["id", "name", "qty"]);
    assert_eq!(
        quoted, bare,
        "identifier quotes inside a regclass string must be normalized before lookup"
    );
}

#[tokio::test]
async fn default_schema_qualifier_resolves_the_same_relation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION qualified_regclass_target \
         (id BIGINT PRIMARY KEY, name TEXT DEFAULT 'x', qty INT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");

    let mut qualified = srv
        .query_text(
            "SELECT attname FROM pg_attribute \
             WHERE attrelid = 'public.\"qualified_regclass_target\"'::regclass",
        )
        .await
        .expect("schema-qualified regclass lookup");
    qualified.sort();

    assert_eq!(
        qualified,
        vec!["id", "name", "qty"],
        "the default schema qualifier must resolve through the same relation identity"
    );
}

#[tokio::test]
async fn unknown_regclass_cast_reports_undefined_relation() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT 'missing_regclass_target'::regclass", "42P01")
        .await;
}

#[tokio::test]
async fn regclass_validation_covers_update_and_delete_filters() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION regclass_dml_target (id BIGINT PRIMARY KEY, value TEXT)")
        .await
        .expect("create collection");
    srv.expect_error(
        "UPDATE regclass_dml_target SET value = 'x' \
         WHERE 'missing_update_relation'::regclass = 1",
        "42P01",
    )
    .await;
    srv.expect_error(
        "DELETE FROM regclass_dml_target \
         WHERE 'missing_delete_relation'::regclass = 1",
        "42P01",
    )
    .await;
}

#[tokio::test]
async fn regclass_validation_covers_projection_and_aggregate_expressions() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION regclass_expr_target (id BIGINT PRIMARY KEY)")
        .await
        .expect("create collection");
    srv.expect_error(
        "SELECT 'missing_projection_relation'::regclass FROM regclass_expr_target",
        "42P01",
    )
    .await;
    srv.expect_error(
        "SELECT COUNT('missing_aggregate_relation'::regclass) FROM regclass_expr_target",
        "42P01",
    )
    .await;
}

#[tokio::test]
async fn direct_regclass_cast_renders_the_relation_name() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION rendered_regclass_target (id BIGINT PRIMARY KEY)")
        .await
        .expect("create collection");

    let bare = srv
        .query_text("SELECT 'rendered_regclass_target'::regclass")
        .await
        .expect("render bare regclass");
    let quoted = srv
        .query_text("SELECT '\"rendered_regclass_target\"'::regclass")
        .await
        .expect("render quoted regclass");

    assert_eq!(bare, vec!["rendered_regclass_target"]);
    assert_eq!(quoted, bare);
    assert!(
        bare.iter().all(|value| !value.is_empty()),
        "resolved regclass values must never render as empty cells"
    );
}
