// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over timeseries ingest.
//!
//! A timeseries row exists only once the line-protocol parser has produced it,
//! so the Data Plane is where the policy can decide it — every ingest format,
//! including the raw line-protocol listener's, normalizes into parsed lines
//! before anything is appended, and the gate sits at that single funnel. A SQL
//! `INSERT` is the exception: its rows are carried in the plan in full, so the
//! statement fails before dispatch.
//!
//! What these tests pin:
//!
//! - An ingest whose row violates the policy is refused and nothing is stored.
//! - A conforming ingest applies: the gate is not a blanket ingest ban.
//! - A collection with no write policy behaves exactly as before.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "ts-write-rls-secret-42";

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

/// A timeseries collection plus the user the tests authenticate as.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             COLUMNS (ts TIMESTAMP TIME_KEY, owner VARCHAR, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    create_user(server, user).await;
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

/// Run `sql` as `user`, returning the server's error message on failure.
///
/// The message is read off the attached `DbError`, never off the
/// `tokio_postgres::Error` wrapper, whose `Display` is the fixed string
/// "db error".
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    drop(client);
    handle.abort();
    result
}

/// Assert a statement was refused BY THE POLICY rather than by some unrelated
/// failure that would make the test pass for the wrong reason.
fn assert_rls_denied(result: Result<(), String>, what: &str) {
    match result {
        Ok(()) => panic!("{what} must be refused, but it succeeded"),
        Err(message) => assert!(
            message.contains("RLS"),
            "{what} must be refused by the RLS policy, got: {message}"
        ),
    }
}

/// The stored `owner` of every row, read as the superuser — who holds no
/// restricting policy, so this is the true stored state.
async fn stored_owners(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT owner FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// A SQL ingest carries its rows in the plan, so a violating row fails the
/// statement before dispatch and nothing reaches the memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_ingest_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "ts_rls_user";
    seed(&server, "ts_rls", user).await;
    write_policy(&server, "ts_rls_owner", "ts_rls").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO ts_rls (ts, owner, value) \
             VALUES (1700000000000, 'alice', 1.0)",
        )
        .await,
        "an ingest handing the row to another owner",
    );
    assert!(
        stored_owners(&server, "ts_rls").await.is_empty(),
        "the refused ingest must store nothing"
    );

    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO ts_rls (ts, owner, value) \
             VALUES (1700000000000, '{user}', 1.0)"
        ),
    )
    .await
    .expect("an ingest whose row satisfies the policy must apply");

    assert_eq!(
        stored_owners(&server, "ts_rls").await.len(),
        1,
        "the conforming ingest must land — the gate is not a blanket ingest ban"
    );
}

/// A batch is decided as a whole: one violating row refuses it, and the rows
/// ahead of the offending one must not survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_violating_row_rejects_the_whole_batch() {
    let server = TestServer::start().await;
    let user = "ts_rls_batch_user";
    seed(&server, "ts_rls_batch", user).await;
    write_policy(&server, "ts_rls_batch_owner", "ts_rls_batch").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!(
                "INSERT INTO ts_rls_batch (ts, owner, value) \
                 VALUES (1700000000000, '{user}', 1.0), \
                        (1700000001000, 'alice', 2.0)"
            ),
        )
        .await,
        "a batch holding one row owned by someone else",
    );
    assert!(
        stored_owners(&server, "ts_rls_batch").await.is_empty(),
        "the rejected batch must apply nothing at all, not even its conforming row"
    );
}

/// Without a write policy nothing changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_with_no_write_policy_is_unaffected() {
    let server = TestServer::start().await;
    let user = "ts_rls_free_user";
    seed(&server, "ts_rls_free", user).await;

    run_as(
        &server,
        user,
        "INSERT INTO ts_rls_free (ts, owner, value) \
         VALUES (1700000000000, 'alice', 1.0)",
    )
    .await
    .expect("ingest must apply with no policy");

    assert_eq!(
        stored_owners(&server, "ts_rls_free").await.len(),
        1,
        "an ungoverned collection must ingest exactly as before"
    );
}

/// A policy predicating on a column whose TYPE the ingest rewrites must be
/// decided against the stored value, not the submitted one.
///
/// The SQL parser routes a decimal literal through `SqlValue::Decimal`, which
/// the MessagePack writer encodes as a string; the ingest recovers the numeric
/// type on the way to the memtable, so the collection stores `1.5` as a float.
/// A decision taken on the submitted image compares a float predicate against
/// the string `"1.5"`, which cannot match — so this conforming row was refused
/// before the decision moved to the one point that sees the normalized row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_whose_type_normalization_rewrites_is_admitted_on_its_stored_value() {
    let server = TestServer::start().await;
    let user = "ts_rls_norm_user";
    seed(&server, "ts_rls_norm", user).await;
    server
        .exec(
            "CREATE RLS POLICY ts_rls_norm_reading ON ts_rls_norm FOR WRITE \
             USING (value = 1.5)",
        )
        .await
        .expect("create write policy on a normalized column");

    run_as(
        &server,
        user,
        "INSERT INTO ts_rls_norm (ts, owner, value) \
         VALUES (1700000000000, 'alice', 1.5)",
    )
    .await
    .expect("the stored value satisfies the policy, so the ingest must be admitted");

    assert_eq!(
        stored_owners(&server, "ts_rls_norm").await.len(),
        1,
        "the conforming ingest must land"
    );

    // …and the same policy still refuses a row whose stored value differs.
    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO ts_rls_norm (ts, owner, value) \
             VALUES (1700000001000, 'alice', 2.5)",
        )
        .await,
        "an ingest whose stored value violates the policy",
    );
    assert_eq!(
        stored_owners(&server, "ts_rls_norm").await.len(),
        1,
        "the refused ingest must store nothing"
    );
}
