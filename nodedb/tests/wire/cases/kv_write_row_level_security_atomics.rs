// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over key-value writes.
//!
//! The key-value engine is often described as storing opaque values, and that
//! is only half true. A multi-column SQL write encodes its columns as a
//! MessagePack map, field-addressable exactly like a document body, so a write
//! policy decides it the same way. Only a single column literally named `value`
//! — the RESP `SET k v` shape — is a bare scalar, and that case needs no
//! special handling: it carries no field the predicate can name, so the same
//! evaluation that admits a field-addressed row rejects it.
//!
//! What these tests pin:
//!
//! - A write whose row violates the policy is rejected and storage is
//!   untouched; a conforming one applies. The gate is not a blanket write ban.
//! - `DELETE` is decided against the row it removes — the only image it has.
//! - An opaque scalar fails CLOSED. "No field to test" must never mean "allow".
//! - The atomics (`KV_INCR`, `KV_CAS`, `KV_GETSET`) and `TRANSFER_ITEM` are
//!   gated too: each derives or supplies an image, and each is decided before
//!   the value becomes durable.
//! - `KV_GETSET` is a read as much as a write: an old value the READ policy
//!   excludes comes back absent rather than being disclosed by the write that
//!   replaced it.
//! - `TRUNCATE` keeps refusing. It removes every row without reading one, so
//!   there is no image to decide — and a policy restricting which rows this
//!   identity may write is precisely a statement that it may not remove all of
//!   them.
//! - A collection with no write policy behaves exactly as before.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "kv-write-rls-secret-42";

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

/// A field-addressed key-value collection seeded with one row owned by `user`
/// and one owned by `alice`, plus a probing user that no policy restricts yet.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                 key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (key, owner) in [("r_mine", user), ("r_theirs", "alice")] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (key, owner, note) \
                 VALUES ('{key}', '{owner}', 'before')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{key}: {e}"));
    }
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

/// Run `sql` as `user`, returning the rows' first column on success and the
/// server's error message on failure.
///
/// The message is read off the attached `DbError`, never off the
/// `tokio_postgres::Error` wrapper: that wrapper's `Display` is the fixed
/// string "db error", so asserting on it would make every refusal below
/// indistinguishable from every other failure — a test that cannot fail for
/// the reason it claims to check.
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
                    Some(row.get(0).unwrap_or("").to_string())
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

/// Every `(key, owner, note)` row read back as the superuser — who holds no
/// restricting policy, so this is the true stored state.
async fn stored(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT key, owner, note FROM {collection} ORDER BY key"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// The JSON payload a `SELECT KV_*(...)` call returns in its single column.
fn payload(rows: &[String]) -> serde_json::Value {
    serde_json::from_str(&rows[0]).unwrap_or_else(|e| panic!("KV_* result must be JSON: {e}"))
}

/// A multi-column row is a MessagePack map, so the write policy decides it
/// field by field: the conforming insert lands, the violating one does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_insert_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "kv_rls_ins_user";
    seed(&server, "kv_rls_ins", user).await;
    write_policy(&server, "kv_rls_ins_owner", "kv_rls_ins").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO kv_rls_ins (key, owner, note) VALUES ('r_new', 'alice', 'x')",
        )
        .await,
        "an insert handing the row to another owner",
    );

    run_as(
        &server,
        user,
        &format!("INSERT INTO kv_rls_ins (key, owner, note) VALUES ('r_ok', '{user}', 'x')"),
    )
    .await
    .expect("an insert whose row satisfies the policy must apply");

    let rows = stored(&server, "kv_rls_ins").await;
    assert_eq!(
        rows.len(),
        3,
        "exactly the conforming insert must have landed: {rows:?}"
    );
}

/// A `DELETE` is decided against the row it removes. Before this enforcement
/// existed the key-value delete read nothing at all, so there was no image to
/// decide — the pre-read exists precisely to produce one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_row_the_policy_excludes_is_rejected() {
    let server = TestServer::start().await;
    let user = "kv_rls_del_user";
    seed(&server, "kv_rls_del", user).await;
    write_policy(&server, "kv_rls_del_owner", "kv_rls_del").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "DELETE FROM kv_rls_del WHERE key = 'r_theirs'",
        )
        .await,
        "deleting a row outside the write policy",
    );
    assert_eq!(
        stored(&server, "kv_rls_del").await.len(),
        2,
        "the excluded row must survive"
    );

    run_as(&server, user, "DELETE FROM kv_rls_del WHERE key = 'r_mine'")
        .await
        .expect("deleting an owned row must apply");
    assert_eq!(
        stored(&server, "kv_rls_del").await.len(),
        1,
        "the owned row must be gone"
    );
}

/// A single column named `value` stores one bare scalar. There is no field for
/// the predicate to name, so the write is rejected rather than admitted by
/// omission — the failure mode this test exists to prevent is "no field to
/// test" being read as "nothing to enforce".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_opaque_scalar_value_is_rejected_under_a_write_policy() {
    let server = TestServer::start().await;
    let user = "kv_rls_opaque_user";
    server
        .exec(
            "CREATE COLLECTION kv_rls_opaque (key TEXT PRIMARY KEY, value TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create opaque collection");
    create_user(&server, user).await;
    write_policy(&server, "kv_rls_opaque_owner", "kv_rls_opaque").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO kv_rls_opaque (key, value) VALUES ('k1', 'anything')",
        )
        .await,
        "writing an opaque scalar under a write policy",
    );
    assert!(
        server
            .query_rows("SELECT key FROM kv_rls_opaque")
            .await
            .expect("read back opaque collection")
            .is_empty(),
        "the rejected scalar write must leave the collection empty"
    );
}

/// An `UPDATE` on a key-value row merges fields into the stored map, so the
/// image the policy decides only exists after that merge: moving `owner` out of
/// scope is refused, touching another column is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_column_row_is_gated_by_field() {
    let server = TestServer::start().await;
    let user = "kv_rls_upd_user";
    seed(&server, "kv_rls_upd", user).await;
    write_policy(&server, "kv_rls_upd_owner", "kv_rls_upd").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "UPDATE kv_rls_upd SET owner = 'alice' WHERE key = 'r_mine'",
        )
        .await,
        "an update whose post-image leaves the policy's scope",
    );

    run_as(
        &server,
        user,
        "UPDATE kv_rls_upd SET note = 'touched' WHERE key = 'r_mine'",
    )
    .await
    .expect("an update whose post-image satisfies the policy must apply");

    assert_eq!(
        stored(&server, "kv_rls_upd").await,
        vec![
            vec![
                "r_mine".to_string(),
                user.to_string(),
                "touched".to_string()
            ],
            vec![
                "r_theirs".to_string(),
                "alice".to_string(),
                "before".to_string()
            ],
        ],
        "only the conforming field change may be stored"
    );
}

/// `KV_INCR` computes the value it stores inside the engine, so the gate is the
/// only place that row can be decided — and a counter is a bare scalar, so it
/// fails closed. `KV_CAS` and `KV_GETSET` supply their image directly and are
/// decided before the engine is entered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_kv_atomics_are_gated_by_the_write_policy() {
    let server = TestServer::start().await;
    let user = "kv_rls_atomic_user";
    server
        .exec(
            "CREATE COLLECTION kv_rls_atomic (key TEXT PRIMARY KEY, value TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create atomics collection");
    create_user(&server, user).await;
    write_policy(&server, "kv_rls_atomic_owner", "kv_rls_atomic").await;

    for (what, sql) in [
        ("KV_INCR", "SELECT KV_INCR('kv_rls_atomic', 'ctr', 1)"),
        (
            "KV_CAS",
            "SELECT KV_CAS('kv_rls_atomic', 'ctr', '', 'taken')",
        ),
        (
            "KV_GETSET",
            "SELECT KV_GETSET('kv_rls_atomic', 'ctr', 'taken')",
        ),
    ] {
        assert_rls_denied(run_as(&server, user, sql).await, what);
    }

    assert!(
        server
            .query_rows("SELECT key FROM kv_rls_atomic")
            .await
            .expect("read back atomics collection")
            .is_empty(),
        "no refused atomic may leave a row behind"
    );
}

/// `KV_GETSET` hands back the value it replaced, which is a read. A row the
/// READ policy hides must come back absent — indistinguishable from a key that
/// never existed — rather than being disclosed by the write that replaced it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn getset_does_not_disclose_an_old_value_the_read_policy_excludes() {
    let server = TestServer::start().await;
    let user = "kv_rls_getset_user";
    seed(&server, "kv_rls_getset", user).await;
    server
        .exec(
            "CREATE RLS POLICY kv_rls_getset_read ON kv_rls_getset FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    let rows = run_as(
        &server,
        user,
        "SELECT KV_GETSET('kv_rls_getset', 'r_theirs', 'replacement')",
    )
    .await
    .expect("a read policy alone must not block the write half");

    assert_eq!(
        payload(&rows)["old_value"],
        serde_json::Value::Null,
        "the replaced row belongs to another owner, so it must read back absent"
    );
}

/// `TRANSFER_ITEM` moves bytes between two key-value locations, and the same
/// bytes are two images to two policies: the row leaving the source and the row
/// arriving at the destination. Both are decided before either half runs, so a
/// refused move never deletes what it could not deliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_item_is_gated_on_both_sides() {
    let server = TestServer::start().await;
    let user = "kv_rls_move_user";
    server
        .exec(
            "CREATE COLLECTION kv_rls_move (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create move collection");
    // `TRANSFER_ITEM` keys a row as `<owner>:<item>`, so the source row is
    // seeded under the key the function will look for.
    server
        .exec(
            "INSERT INTO kv_rls_move (key, owner, note) \
             VALUES ('alice:sword', 'alice', 'before')",
        )
        .await
        .expect("seed the moved row");
    create_user(&server, user).await;
    write_policy(&server, "kv_rls_move_owner", "kv_rls_move").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!(
                "SELECT TRANSFER_ITEM('kv_rls_move', 'kv_rls_move', 'sword', 'alice', '{user}')"
            ),
        )
        .await,
        "moving a row the write policy excludes",
    );

    let rows = stored(&server, "kv_rls_move").await;
    assert_eq!(
        rows,
        vec![vec![
            "alice:sword".to_string(),
            "alice".to_string(),
            "before".to_string()
        ]],
        "a refused move must leave the source row exactly where it was"
    );
}

/// A truncate removes every row without reading one, so there is no image the
/// policy could be evaluated against — and a policy restricting which rows this
/// identity may write says it may not remove all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_still_refuses_under_a_write_policy() {
    let server = TestServer::start().await;
    let user = "kv_rls_trunc_user";
    seed(&server, "kv_rls_trunc", user).await;
    write_policy(&server, "kv_rls_trunc_owner", "kv_rls_trunc").await;

    let result = run_as(&server, user, "TRUNCATE kv_rls_trunc").await;
    assert!(
        result.is_err(),
        "a truncate under a write policy must be refused"
    );
    assert_eq!(
        stored(&server, "kv_rls_trunc").await.len(),
        2,
        "the refused truncate must leave every row in place"
    );
}

/// An ungoverned collection pays nothing for the gate: every write shape that
/// the policed collections above refuse must still apply here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_without_a_write_policy_is_unaffected() {
    let server = TestServer::start().await;
    let user = "kv_rls_free_user";
    seed(&server, "kv_rls_free", user).await;

    for sql in [
        "INSERT INTO kv_rls_free (key, owner, note) VALUES ('r_new', 'alice', 'x')",
        "UPDATE kv_rls_free SET owner = 'alice' WHERE key = 'r_mine'",
        "DELETE FROM kv_rls_free WHERE key = 'r_theirs'",
    ] {
        run_as(&server, user, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must apply with no write policy: {e}"));
    }

    let rows = stored(&server, "kv_rls_free").await;
    assert_eq!(
        rows,
        vec![
            vec![
                "r_mine".to_string(),
                "alice".to_string(),
                "before".to_string()
            ],
            vec!["r_new".to_string(), "alice".to_string(), "x".to_string()],
        ],
        "every write must have applied untouched: {rows:?}"
    );
}
