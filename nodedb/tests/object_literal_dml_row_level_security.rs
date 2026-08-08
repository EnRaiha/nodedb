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
//! - What the path actually RETURNS, established rather than assumed: nothing.
//!   An INSERT answers with a command status, and `RETURNING` is unsupported on
//!   INSERT product-wide — no insert operation carries a `returning` slot on any
//!   engine — so both this form and `(cols) VALUES (…)` refuse the clause rather
//!   than dropping it. A read policy therefore has no row set to narrow on a
//!   write through this entry point; the write gate is the whole of the control
//!   that applies. An ordinary `SELECT` against the same collection is still
//!   filtered, which is asserted alongside so the empty result is pinned as the
//!   statement's shape rather than as rows a policy silently removed. The
//!   refusals themselves live in `pgwire_returning_dml.rs` and
//!   `object_literal_trailing_clause.rs`; what is pinned here is that a
//!   conforming row is refused on the CLAUSE, not reported as a policy denial.
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

/// No INSERT form returns rows, and asking for them is refused as a RETURNING
/// limitation — never mistaken for a policy denial.
///
/// `RETURNING` is unsupported on INSERT across the whole product: no insert
/// operation carries a `returning` slot on any engine, so both the object
/// literal and the `(cols) VALUES (…)` form refuse it. The distinction pinned
/// here is WHICH refusal the caller gets. A conforming write that asked for
/// rows must fail on the clause, not on the policy — reporting a policy denial
/// for a statement the policy admits would send an operator hunting through
/// their RLS rules for a parser limitation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asking_an_insert_for_rows_is_refused_on_the_clause_not_the_policy() {
    let server = TestServer::start().await;
    let user = "objlit_echo_user";
    server
        .exec("CREATE COLLECTION objlit_echo")
        .await
        .expect("create document collection");
    create_user(&server, user).await;
    write_policy(&server, "objlit_echo_owner", "objlit_echo").await;

    // The row CONFORMS to the write policy, so the only thing wrong with the
    // statement is the clause.
    for sql in [
        format!("INSERT INTO objlit_echo (id, owner) VALUES ('mine', '{user}') RETURNING *"),
        format!("INSERT INTO objlit_echo {{ id: 'mine2', owner: '{user}' }} RETURNING *"),
    ] {
        match run_as(&server, user, &sql).await {
            Ok(rows) => panic!("`{sql}` must be refused, but it succeeded: {rows:?}"),
            Err(message) => {
                assert!(
                    message.contains("RETURNING"),
                    "the refusal must name the clause; sql = {sql}, got: {message}"
                );
                assert!(
                    !message.contains("RLS"),
                    "a conforming row must not be reported as a policy denial; sql = {sql}, \
                     got: {message}"
                );
            }
        }
    }

    // …and neither refused statement wrote anything.
    assert!(
        server
            .query_rows("SELECT id FROM objlit_echo")
            .await
            .expect("read back objlit_echo")
            .is_empty(),
        "a refused statement must not have written its row"
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
