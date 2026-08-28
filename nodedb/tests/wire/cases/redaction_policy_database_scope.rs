// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: a column-redaction policy created inside a
//! non-default database must actually be enforced.
//!
//! Before the fix, `db_qualified` prefixes every physical-plan op's
//! `collection` field with the owning database ID for any non-default
//! database, but `CREATE REDACTION POLICY` stored (and looked up) the bare
//! collection name. The two keys only coincided in `default`, so a policy
//! created anywhere else was silently inert: the masking hook found no
//! policy and shipped the protected column in the clear, and the
//! aggregate/graph fail-closed refusal never fired either, since it consults
//! the same store under the same mismatched key.

use crate::harness::TestServer;

const PASSWORD: &str = "redact-db-scope-secret-1";
const ROLE: &str = "readwrite";

async fn query_ok(server: &TestServer, sql: &str) {
    server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nsql: {sql}"));
}

/// Create `user`, grant it write access to the role and to `database`.
async fn create_scoped_user(server: &TestServer, user: &str, database: &str) {
    query_ok(
        server,
        &format!("CREATE USER {user} WITH PASSWORD '{PASSWORD}' ROLE {ROLE}"),
    )
    .await;
    query_ok(
        server,
        &format!("GRANT ALL ON DATABASE {database} TO {user}"),
    )
    .await;
}

/// Run `sql` as `user` against `database` via a fresh connection, returning
/// the single text column of the first row, or the server's SQLSTATE on
/// failure.
async fn query_first_col_as(
    server: &TestServer,
    user: &str,
    database: &str,
    sql: &str,
) -> Result<String, String> {
    let (client, handle) = server
        .connect_as_database(user, PASSWORD, database)
        .await
        .unwrap_or_else(|e| panic!("connect as {user} on {database}: {e}"));
    let result = match client.simple_query(sql).await {
        Ok(messages) => {
            let value = messages.iter().find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(|s| s.to_string()),
                _ => None,
            });
            value.ok_or_else(|| "no row returned".to_string())
        }
        Err(error) => Err(error
            .as_db_error()
            .map(|db_error| db_error.code().code().to_string())
            .unwrap_or_else(|| error.to_string())),
    };
    drop(client);
    handle.abort();
    result
}

/// A redaction policy created against a collection living in a non-default
/// database must mask the protected column, exactly as it does in `default`.
///
/// This is the regression: pre-fix, the policy was stored under the bare
/// collection name while the masking hook looked it up under the database-
/// qualified name, so the two never matched and `ssn` shipped in the clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redaction_policy_in_non_default_database_masks_the_column() {
    let (server, db) = TestServer::with_database("redact_scope_db").await;
    let user = "redact_scope_nondefault_user";

    query_ok(
        &server,
        "CREATE COLLECTION redact_docs (\
             id TEXT PRIMARY KEY, name TEXT, ssn TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO redact_docs (id, name, ssn) VALUES ('r1', 'alice', '123-45-6789')",
    )
    .await;
    create_scoped_user(&server, user, &db).await;
    query_ok(
        &server,
        "CREATE REDACTION POLICY mask_ssn ON redact_docs FOR ROLE readwrite \
         (ssn MASK '***-**-****')",
    )
    .await;

    let ssn = query_first_col_as(
        &server,
        user,
        &db,
        "SELECT ssn FROM redact_docs WHERE id = 'r1'",
    )
    .await
    .unwrap_or_else(|e| panic!("SELECT ssn: {e}"));

    assert_eq!(
        ssn, "***-**-****",
        "a redaction policy on a non-default-database collection must mask the \
         protected column, got the stored value unmasked: {ssn}"
    );
}

/// An aggregate over a redacted column in a non-default database must be
/// refused fail-closed, not silently answer over the unmasked stored values.
///
/// Column redaction cannot rewrite a Data-Plane-computed scalar, so
/// `refuse_unredactable_plan` refuses the statement outright while a rule
/// applies. Pre-fix, the lookup missed the qualified-stored policy in a
/// non-default database, so the aggregate ran unrefused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_redacted_column_in_non_default_database_is_refused() {
    let (server, db) = TestServer::with_database("redact_scope_agg_db").await;
    let user = "redact_scope_agg_user";

    query_ok(
        &server,
        "CREATE COLLECTION redact_agg_docs (\
             id TEXT PRIMARY KEY, ssn TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO redact_agg_docs (id, ssn) VALUES ('r1', '123-45-6789')",
    )
    .await;
    create_scoped_user(&server, user, &db).await;
    query_ok(
        &server,
        "CREATE REDACTION POLICY mask_agg_ssn ON redact_agg_docs FOR ROLE readwrite \
         (ssn MASK '***-**-****')",
    )
    .await;

    let result =
        query_first_col_as(&server, user, &db, "SELECT MAX(ssn) FROM redact_agg_docs").await;

    let sqlstate = result.expect_err(
        "an aggregate over a redacted column in a non-default database must be refused, \
         not answer over the unmasked stored values",
    );
    assert_eq!(
        sqlstate, "42601",
        "expected the redaction refusal's SQLSTATE (42601, mapped from PlanError), got: {sqlstate}"
    );
}

/// `SHOW REDACTION POLICIES` on a non-default-database collection must
/// render the collection name as the user wrote it, never the database-
/// qualified storage key (`"<db_id>/<collection>"`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_redaction_policies_displays_the_bare_collection_name() {
    let (server, _db) = TestServer::with_database("redact_scope_show_db").await;

    query_ok(
        &server,
        "CREATE COLLECTION redact_show_docs (\
             id TEXT PRIMARY KEY, ssn TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        "CREATE REDACTION POLICY show_mask_ssn ON redact_show_docs FOR ROLE readwrite \
         (ssn MASK '***-**-****')",
    )
    .await;

    let rows = server
        .query_rows("SHOW REDACTION POLICIES")
        .await
        .unwrap_or_else(|e| panic!("SHOW REDACTION POLICIES: {e}"));

    let found = rows
        .iter()
        .any(|row| row.iter().any(|c| c == "redact_show_docs"));
    assert!(
        found,
        "SHOW REDACTION POLICIES must display the bare collection name \
         'redact_show_docs', got: {rows:?}"
    );
    let leaked_qualified = rows
        .iter()
        .any(|row| row.iter().any(|c| c.contains("/redact_show_docs")));
    assert!(
        !leaked_qualified,
        "SHOW REDACTION POLICIES must never display the database-qualified \
         storage key, got: {rows:?}"
    );
}
