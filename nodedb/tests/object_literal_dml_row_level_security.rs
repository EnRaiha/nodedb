// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over the object-literal DML entry point.
//!
//! `INSERT INTO c { … }` and `UPSERT INTO c { … }` are rewritten to standard
//! SQL and planned through the protocol-neutral DML handler rather than the
//! pgwire statement path. That handler is a client-reachable transport like any
//! other, so the same `FOR WRITE` / `FOR ALL` policies that govern
//! `INSERT … VALUES` have to govern it — otherwise the object-literal form is a
//! way to write rows a policy forbids simply by spelling the statement
//! differently.
//!
//! What these tests pin:
//!
//! - An object-literal INSERT whose row violates the policy is rejected; a
//!   conforming one applies.
//! - The same for the UPSERT form, which reaches the identical handler.
//! - A non-Document engine through the same entry point, so the coverage is the
//!   transport's — not one engine's. Key-value is used because its object
//!   literal is already the documented form for that engine.
//! - What the path actually RETURNS, established rather than assumed: a write
//!   with no `RETURNING` answers with a command status and no rows, so a read
//!   policy has no row set to narrow and the write gate is the whole of the
//!   control that applies. An ordinary `SELECT` against the same collection is
//!   still filtered, which is asserted alongside so the empty result is pinned
//!   as the statement's shape rather than as rows a policy silently removed.
//! - A write that DOES ask for rows is answered on its row, not on its clause:
//!   a conforming row comes back, a violating one is refused as a policy denial
//!   and writes nothing. `RETURNING` must not become a way around the write
//!   gate, and a conforming row must never be reported as a policy denial.
//! - A collection with no policy is unaffected on every one of those forms.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "objlit-rls-secret-42";

/// The least privilege that can run the DML under test, so a denial is the
/// policy's doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

/// Restrict writes on `collection` to rows the authenticated principal owns.
async fn write_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy {policy}: {e}"));
}

/// Run `sql` as `user`, returning one string per row with every column joined,
/// and the server's error message on failure.
///
/// Columns are joined rather than only the first taken, because what a
/// `RETURNING` write answers with differs by statement form — one JSON column on
/// the neutral handler, the projected document columns on the SQL path — and an
/// assertion about what the row does or does not disclose must not depend on
/// which of those served it. Single-column results are unaffected.
///
/// The error message is read off the attached `DbError`, never off the
/// `tokio_postgres::Error` wrapper: that wrapper's `Display` is the fixed string
/// "db error", so asserting on it would make every refusal below
/// indistinguishable from every other failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    let rows = result.map(|messages| {
        messages
            .into_iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    let joined = (0..row.len())
                        .map(|i| row.get(i).unwrap_or(""))
                        .collect::<Vec<_>>()
                        .join("\t");
                    Some(joined)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    });
    drop(client);
    handle.abort();
    rows
}

/// Assert a statement was refused BY THE POLICY rather than by some unrelated
/// failure that would make the test pass for the wrong reason.
fn assert_rls_denied(result: Result<Vec<String>, String>, what: &str) {
    match result {
        Ok(rows) => panic!("{what} must be refused, but it succeeded: {rows:?}"),
        Err(message) => assert!(
            message.contains("RLS"),
            "{what} must be refused by the RLS policy, got: {message}"
        ),
    }
}

/// Every row of `collection` read back as the superuser — who holds no
/// restricting policy, so this is the true stored state.
async fn stored(server: &TestServer, collection: &str, key: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT {key}, owner FROM {collection} ORDER BY {key}"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// The object-literal INSERT is rewritten to standard SQL and planned through
/// the neutral DML handler; the write policy decides the row it carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_object_literal_insert_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "objlit_ins_user";
    server
        .exec("CREATE COLLECTION objlit_ins")
        .await
        .expect("create document collection");
    create_user(&server, user).await;
    write_policy(&server, "objlit_ins_owner", "objlit_ins").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO objlit_ins { id: 'd_bad', owner: 'alice', note: 'x' }",
        )
        .await,
        "an object-literal insert handing the row to another owner",
    );

    run_as(
        &server,
        user,
        &format!("INSERT INTO objlit_ins {{ id: 'd_ok', owner: '{user}', note: 'x' }}"),
    )
    .await
    .expect("an object-literal insert whose row satisfies the policy must apply");

    assert_eq!(
        stored(&server, "objlit_ins", "id").await,
        vec![vec!["d_ok".to_string(), user.to_string()]],
        "exactly the conforming insert may be stored"
    );
}

/// `UPSERT INTO c { … }` reaches the same handler, so it is decided the same
/// way — a row that leaves the policy's scope is refused rather than overwriting
/// what is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_object_literal_upsert_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "objlit_ups_user";
    server
        .exec("CREATE COLLECTION objlit_ups")
        .await
        .expect("create document collection");
    server
        .exec(&format!(
            "INSERT INTO objlit_ups {{ id: 'u1', owner: '{user}', note: 'before' }}"
        ))
        .await
        .expect("seed the row the upsert will target");
    create_user(&server, user).await;
    write_policy(&server, "objlit_ups_owner", "objlit_ups").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "UPSERT INTO objlit_ups { id: 'u1', owner: 'alice', note: 'taken' }",
        )
        .await,
        "an object-literal upsert whose post-image leaves the policy's scope",
    );

    run_as(
        &server,
        user,
        &format!("UPSERT INTO objlit_ups {{ id: 'u1', owner: '{user}', note: 'after' }}"),
    )
    .await
    .expect("an object-literal upsert whose post-image satisfies the policy must apply");

    let rows = server
        .query_rows("SELECT id, owner, note FROM objlit_ups")
        .await
        .expect("read back objlit_ups");
    assert_eq!(
        rows,
        vec![vec![
            "u1".to_string(),
            user.to_string(),
            "after".to_string()
        ]],
        "only the conforming upsert may be stored"
    );
}

/// The fix is the transport's, not one engine's: the same object literal into a
/// key-value collection is decided by the key-value write gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_object_literal_insert_is_rejected_on_a_kv_collection() {
    let server = TestServer::start().await;
    let user = "objlit_kv_user";
    server
        .exec(
            "CREATE COLLECTION objlit_kv (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create kv collection");
    create_user(&server, user).await;
    write_policy(&server, "objlit_kv_owner", "objlit_kv").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO objlit_kv { key: 'k_bad', owner: 'alice', note: 'x' }",
        )
        .await,
        "an object-literal key-value insert handing the row to another owner",
    );

    run_as(
        &server,
        user,
        &format!("INSERT INTO objlit_kv {{ key: 'k_ok', owner: '{user}', note: 'x' }}"),
    )
    .await
    .expect("an object-literal key-value insert satisfying the policy must apply");

    assert_eq!(
        stored(&server, "objlit_kv", "key").await,
        vec![vec!["k_ok".to_string(), user.to_string()]],
        "exactly the conforming key-value insert may be stored"
    );
}

/// The object-literal form returns NO rows, so a read policy has nothing to
/// narrow on it.
///
/// The rewrite that turns `INSERT INTO c { … }` into standard SQL reconstructs
/// the statement from the parsed fields, so this form answers with a command
/// status rather than a row set. That makes the write gate the whole of the
/// control that applies here. An ordinary `SELECT` against the same collection
/// is still filtered — asserted below so the absence of rows above is pinned as
/// "this statement returns nothing", not "the read policy silently ate them".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_returns_no_rows_so_a_read_policy_has_nothing_to_narrow() {
    let server = TestServer::start().await;
    let user = "objlit_ret_user";
    server
        .exec("CREATE COLLECTION objlit_ret")
        .await
        .expect("create document collection");
    server
        .exec("INSERT INTO objlit_ret { id: 'theirs', owner: 'alice', note: 'secret' }")
        .await
        .expect("seed a row owned by someone else");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY objlit_ret_read ON objlit_ret FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    let returned = run_as(
        &server,
        user,
        &format!("INSERT INTO objlit_ret {{ id: 'mine', owner: '{user}', note: 'plain' }}"),
    )
    .await
    .expect("a read policy alone must not block a write");
    assert!(
        returned.is_empty(),
        "the object-literal form answers with a command status, not rows: {returned:?}"
    );

    // The write itself applied — so the empty result above is the statement's
    // shape, not a swallowed failure.
    assert_eq!(
        server
            .query_rows("SELECT id FROM objlit_ret WHERE id = 'mine'")
            .await
            .expect("read back objlit_ret"),
        vec![vec!["mine".to_string()]],
        "the write must have applied"
    );

    let visible = run_as(&server, user, "SELECT id FROM objlit_ret ORDER BY id")
        .await
        .expect("select under a read policy must run");
    assert_eq!(
        visible,
        vec!["mine".to_string()],
        "an ordinary select IS filtered by the read policy: {visible:?}"
    );
}

/// A write that asks for rows is answered on its own merits: a conforming row
/// comes back, a violating one is refused BY THE POLICY.
///
/// The distinction pinned here is WHICH outcome the caller gets, and that the
/// two are never confused. A conforming write must not be refused at all — an
/// operator told "policy denied" for a row the policy admits goes hunting
/// through their RLS rules for something that is not there. A violating write
/// must be refused as a policy denial and must not write, whatever clause it
/// carries: `RETURNING` must not become a way around the write gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_insert_is_answered_on_its_row_not_on_its_clause() {
    let server = TestServer::start().await;
    let user = "objlit_echo_user";
    server
        .exec("CREATE COLLECTION objlit_echo")
        .await
        .expect("create document collection");
    create_user(&server, user).await;
    write_policy(&server, "objlit_echo_owner", "objlit_echo").await;

    // Conforming rows: the only thing that could refuse these is the policy,
    // and it admits them — so each answers with its own stored row.
    for (id, sql) in [
        (
            "mine",
            format!("INSERT INTO objlit_echo (id, owner) VALUES ('mine', '{user}') RETURNING id"),
        ),
        (
            "mine2",
            format!("INSERT INTO objlit_echo {{ id: 'mine2', owner: '{user}' }} RETURNING id"),
        ),
    ] {
        let rows = run_as(&server, user, &sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` conforms to the policy and must apply: {e}"));
        assert_eq!(
            rows,
            vec![id.to_string()],
            "`{sql}` must answer with the row it stored"
        );
    }

    // Violating rows: refused by the POLICY, not by anything about the clause.
    for sql in [
        "INSERT INTO objlit_echo (id, owner) VALUES ('theirs', 'alice') RETURNING id".to_string(),
        "INSERT INTO objlit_echo { id: 'theirs2', owner: 'alice' } RETURNING id".to_string(),
    ] {
        assert_rls_denied(run_as(&server, user, &sql).await, &sql);
    }

    // Only the conforming rows exist: asking for rows back never loosened the
    // write gate.
    assert_eq!(
        server
            .query_rows("SELECT id FROM objlit_echo ORDER BY id")
            .await
            .expect("read back objlit_echo"),
        vec![vec!["mine".to_string()], vec!["mine2".to_string()]],
        "a policy-denied statement must not have written its row"
    );
}

/// An ungoverned collection pays nothing: every object-literal form the policed
/// collections above refuse must still apply here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_without_a_write_policy_are_unaffected() {
    let server = TestServer::start().await;
    let user = "objlit_free_user";
    server
        .exec("CREATE COLLECTION objlit_free")
        .await
        .expect("create document collection");
    server
        .exec(
            "CREATE COLLECTION objlit_free_kv (key TEXT PRIMARY KEY, owner TEXT) \
               WITH (engine='kv')",
        )
        .await
        .expect("create kv collection");
    create_user(&server, user).await;

    for sql in [
        "INSERT INTO objlit_free { id: 'f1', owner: 'alice' }",
        "UPSERT INTO objlit_free { id: 'f1', owner: 'bob' }",
        "INSERT INTO objlit_free_kv { key: 'f1', owner: 'alice' }",
    ] {
        run_as(&server, user, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must apply with no write policy: {e}"));
    }

    assert_eq!(
        stored(&server, "objlit_free", "id").await,
        vec![vec!["f1".to_string(), "bob".to_string()]],
        "the ungoverned upsert must have overwritten the insert"
    );
    assert_eq!(
        stored(&server, "objlit_free_kv", "key").await,
        vec![vec!["f1".to_string(), "alice".to_string()]],
        "the ungoverned key-value insert must be stored"
    );
}
