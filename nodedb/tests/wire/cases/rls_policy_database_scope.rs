// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: an RLS write policy created inside a non-default
//! database must actually be enforced.
//!
//! Before the fix, `db_qualified` prefixes every physical-plan op's
//! `collection` field with the owning database ID for any non-default
//! database, but `CREATE RLS POLICY` stored (and looked up) the bare
//! collection name. The two keys only coincided in `default`, so a policy
//! created anywhere else was silently unenforced while `SHOW RLS POLICIES`
//! still reported it enabled.

use crate::harness::TestServer;

const PASSWORD: &str = "rls-db-scope-secret-1";
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

/// Run `sql` as `user` against `database`; returns `Err` on failure.
async fn try_exec_as(
    server: &TestServer,
    user: &str,
    database: &str,
    sql: &str,
) -> Result<(), String> {
    let (client, handle) = server
        .connect_as_database(user, PASSWORD, database)
        .await
        .unwrap_or_else(|e| panic!("connect as {user} on {database}: {e}"));
    let result = client
        .simple_query(sql)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    drop(client);
    handle.abort();
    result
}

/// Every `(id, owner, note)` row read back on the session's current
/// database, as the superuser — who holds no restricting policy, so this is
/// the true stored state.
async fn stored(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT id, owner, note FROM {collection} ORDER BY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// A write policy created against a collection living in a non-default
/// database must reject a violating write, exactly as it does in `default`.
///
/// This is the regression: pre-fix, the policy was stored under the bare
/// collection name while enforcement looked it up under the database-
/// qualified name, so the two never matched and the write below went
/// through unopposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_policy_in_non_default_database_is_enforced() {
    let (server, db) = TestServer::with_database("rls_scope_db").await;
    let user = "rls_scope_nondefault_user";

    query_ok(
        &server,
        "CREATE COLLECTION scoped_docs (\
             id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        &format!("INSERT INTO scoped_docs (id, owner, note) VALUES ('r_mine', '{user}', 'before')"),
    )
    .await;
    create_scoped_user(&server, user, &db).await;

    query_ok(
        &server,
        "CREATE RLS POLICY scoped_docs_owner ON scoped_docs FOR WRITE \
         USING (owner = $auth.username)",
    )
    .await;

    let before = stored(&server, "scoped_docs").await;

    // Handing the row to someone else is exactly what the policy forbids.
    let result = try_exec_as(
        &server,
        user,
        &db,
        "UPDATE scoped_docs SET owner = 'mallory' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "a write policy on a non-default-database collection must reject a \
         violating post-image, got: {result:?}"
    );
    assert_eq!(
        stored(&server, "scoped_docs").await,
        before,
        "a rejected write in a non-default database must leave storage untouched"
    );

    // A conforming write still applies — the policy is a real predicate, not
    // a blanket ban that happens to also reject everything.
    try_exec_as(
        &server,
        user,
        &db,
        "UPDATE scoped_docs SET note = 'touched' WHERE id = 'r_mine'",
    )
    .await
    .expect("a conforming write under the policy must apply");
    assert_eq!(
        stored(&server, "scoped_docs").await,
        vec![vec![
            "r_mine".to_string(),
            user.to_string(),
            "touched".to_string(),
        ]],
        "the conforming write must be persisted"
    );
}

/// The same scenario in `default` must keep working — this is the path the
/// fix must not regress, since `db_qualified` is a no-op for `default` and
/// the pre-fix code happened to key both sides identically there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_policy_in_default_database_is_still_enforced() {
    let server = TestServer::start().await;
    let user = "rls_scope_default_user";

    query_ok(
        &server,
        "CREATE COLLECTION default_scoped_docs (\
             id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        &format!(
            "INSERT INTO default_scoped_docs (id, owner, note) VALUES ('r_mine', '{user}', 'before')"
        ),
    )
    .await;
    query_ok(
        &server,
        &format!("CREATE USER {user} WITH PASSWORD '{PASSWORD}' ROLE {ROLE}"),
    )
    .await;

    query_ok(
        &server,
        "CREATE RLS POLICY default_scoped_docs_owner ON default_scoped_docs FOR WRITE \
         USING (owner = $auth.username)",
    )
    .await;

    let before = stored(&server, "default_scoped_docs").await;
    let result = try_exec_as(
        &server,
        user,
        "default",
        "UPDATE default_scoped_docs SET owner = 'mallory' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "a write policy in the default database must still reject a violating post-image"
    );
    assert_eq!(
        stored(&server, "default_scoped_docs").await,
        before,
        "a rejected write in the default database must leave storage untouched"
    );
}

/// `SHOW RLS POLICIES` on a non-default-database collection must render the
/// collection name as the user wrote it, never the database-qualified
/// storage key (`"<db_id>/<collection>"`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_rls_policies_displays_the_bare_collection_name() {
    let (server, _db) = TestServer::with_database("rls_scope_show_db").await;

    query_ok(
        &server,
        "CREATE COLLECTION show_scoped_docs (\
             id TEXT PRIMARY KEY, owner TEXT) \
         WITH (engine='document_strict')",
    )
    .await;
    query_ok(
        &server,
        "CREATE RLS POLICY show_scoped_owner ON show_scoped_docs FOR WRITE \
         USING (owner = $auth.username)",
    )
    .await;

    let rows = server
        .query_rows("SHOW RLS POLICIES")
        .await
        .unwrap_or_else(|e| panic!("SHOW RLS POLICIES: {e}"));

    let found = rows
        .iter()
        .any(|row| row.iter().any(|c| c == "show_scoped_docs"));
    assert!(
        found,
        "SHOW RLS POLICIES must display the bare collection name 'show_scoped_docs', got: {rows:?}"
    );
    let leaked_qualified = rows
        .iter()
        .any(|row| row.iter().any(|c| c.contains("/show_scoped_docs")));
    assert!(
        !leaked_qualified,
        "SHOW RLS POLICIES must never display the database-qualified storage key, got: {rows:?}"
    );
}
