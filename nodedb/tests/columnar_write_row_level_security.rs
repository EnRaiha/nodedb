// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over writes to the columnar storage core.
//!
//! The columnar family used to refuse every write on a governed collection
//! outright, on the reasoning that no point in the plan held a row image. Half
//! of that is wrong: a plain `INSERT` carries every row it will persist, so the
//! policy decides those rows before anything is dispatched. The other half is
//! real but solvable — an update's post-image, a delete's pre-image and an
//! upsert's merged body only exist inside the handler, so the compiled
//! predicate travels with the plan and the handler decides the actual rows.
//!
//! What these tests pin:
//!
//! - A violating `INSERT` is refused and the statement applies NOTHING, not
//!   even the conforming rows sharing the batch.
//! - An `UPDATE` is decided against its POST-image, and a refusal leaves the
//!   stored row exactly as it was.
//! - A `DELETE` is decided against the row it removes.
//! - `ON CONFLICT DO UPDATE` is decided against the MERGED row, which is the
//!   only image that describes what will exist.
//! - A spatial-engine collection's user DML is gated: it routes through the
//!   columnar ops, so it inherits the same enforcement.
//! - A collection with no write policy behaves exactly as before.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "columnar-write-rls-secret-42";

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

/// A columnar collection seeded with one row owned by `user` and one owned by
/// `alice`, plus the user the tests authenticate as.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, owner) in [("r_mine", user), ("r_theirs", "alice")] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, note) \
                 VALUES ('{id}', '{owner}', 'before')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
    }
    create_user(server, user).await;
}

/// Run `sql` as `user`, returning the server's error message on failure.
///
/// The message is read off the attached `DbError`, never off the
/// `tokio_postgres::Error` wrapper: that wrapper's `Display` is the fixed
/// string "db error", so asserting on it would make every refusal below
/// indistinguishable from every other failure.
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

/// Every `(id, owner, note)` row read back as the superuser — who holds no
/// restricting policy, so this is the true stored state.
async fn stored(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT id, owner, note FROM {collection} ORDER BY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// The plan carries every row a plain insert persists, so the policy decides
/// them before dispatch. The batch below mixes a conforming row with a
/// violating one: the conforming row must NOT survive, because the statement
/// failed as a whole and a caller told "rejected" must not find half of it
/// applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_insert_rejects_the_whole_batch() {
    let server = TestServer::start().await;
    let user = "col_rls_ins_user";
    seed(&server, "col_rls_ins", user).await;
    write_policy(&server, "col_rls_ins_owner", "col_rls_ins").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!(
                "INSERT INTO col_rls_ins (id, owner, note) \
                 VALUES ('r_ok', '{user}', 'x'), ('r_bad', 'alice', 'x')"
            ),
        )
        .await,
        "a batch holding one row owned by someone else",
    );

    let rows = stored(&server, "col_rls_ins").await;
    assert_eq!(
        rows.len(),
        2,
        "the rejected batch must apply nothing at all, not even its conforming \
         row: {rows:?}"
    );

    run_as(
        &server,
        user,
        &format!("INSERT INTO col_rls_ins (id, owner, note) VALUES ('r_ok', '{user}', 'x')"),
    )
    .await
    .expect("an insert whose rows satisfy the policy must apply");

    assert_eq!(
        stored(&server, "col_rls_ins").await.len(),
        3,
        "the conforming insert must land — the gate is not a blanket write ban"
    );
}

/// The image an update is governed by is the row that will exist afterwards, so
/// moving `owner` out of scope is refused even though the row started in scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_whose_post_image_violates_is_rejected() {
    let server = TestServer::start().await;
    let user = "col_rls_upd_user";
    seed(&server, "col_rls_upd", user).await;
    write_policy(&server, "col_rls_upd_owner", "col_rls_upd").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "UPDATE col_rls_upd SET owner = 'alice' WHERE id = 'r_mine'",
        )
        .await,
        "an update handing the row to another owner",
    );

    let rows = stored(&server, "col_rls_upd").await;
    let mine = rows
        .iter()
        .find(|row| row[0] == "r_mine")
        .unwrap_or_else(|| panic!("the owned row must still exist: {rows:?}"));
    assert_eq!(
        (mine[1].as_str(), mine[2].as_str()),
        (user, "before"),
        "the refused update must leave the stored row untouched: {rows:?}"
    );

    run_as(
        &server,
        user,
        "UPDATE col_rls_upd SET note = 'after' WHERE id = 'r_mine'",
    )
    .await
    .expect("an update whose post-image satisfies the policy must apply");

    let rows = stored(&server, "col_rls_upd").await;
    let mine = rows
        .iter()
        .find(|row| row[0] == "r_mine")
        .unwrap_or_else(|| panic!("the owned row must still exist: {rows:?}"));
    assert_eq!(mine[2], "after", "the conforming update must apply");
}

/// A delete is decided against the row it removes — the only image it has.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_row_the_policy_excludes_is_rejected() {
    let server = TestServer::start().await;
    let user = "col_rls_del_user";
    seed(&server, "col_rls_del", user).await;
    write_policy(&server, "col_rls_del_owner", "col_rls_del").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "DELETE FROM col_rls_del WHERE id = 'r_theirs'",
        )
        .await,
        "deleting a row outside the write policy",
    );
    assert_eq!(
        stored(&server, "col_rls_del").await.len(),
        2,
        "the excluded row must survive"
    );

    run_as(&server, user, "DELETE FROM col_rls_del WHERE id = 'r_mine'")
        .await
        .expect("deleting an owned row must apply");
    assert_eq!(
        stored(&server, "col_rls_del").await.len(),
        1,
        "the owned row must be gone"
    );
}

/// The row an upsert persists on conflict is the stored row with the
/// assignments applied, and that merge exists only inside the handler. Deciding
/// the incoming body instead would clear a write whose real post-image the
/// policy never saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_on_conflict_update_whose_merged_row_violates_is_rejected() {
    let server = TestServer::start().await;
    let user = "col_rls_upsert_user";
    seed(&server, "col_rls_upsert", user).await;
    write_policy(&server, "col_rls_upsert_owner", "col_rls_upsert").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!(
                "INSERT INTO col_rls_upsert (id, owner, note) \
                 VALUES ('r_mine', '{user}', 'incoming') \
                 ON CONFLICT (id) DO UPDATE SET owner = 'alice'"
            ),
        )
        .await,
        "an upsert whose merged row moves out of policy scope",
    );

    let rows = stored(&server, "col_rls_upsert").await;
    let mine = rows
        .iter()
        .find(|row| row[0] == "r_mine")
        .unwrap_or_else(|| panic!("the owned row must still exist: {rows:?}"));
    assert_eq!(
        (mine[1].as_str(), mine[2].as_str()),
        (user, "before"),
        "the refused upsert must leave the stored row untouched: {rows:?}"
    );
}

/// A spatial-engine collection stores its rows in the columnar core, and user
/// DML against it is planned as the columnar ops — so it inherits exactly the
/// same enforcement rather than needing its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_engine_user_dml_is_gated() {
    let server = TestServer::start().await;
    let user = "spa_rls_user";
    server
        .exec(
            "CREATE COLLECTION spa_rls \
             (id TEXT PRIMARY KEY, owner TEXT, loc GEOMETRY SPATIAL_INDEX) \
             WITH (engine='spatial')",
        )
        .await
        .expect("create spatial collection");
    server
        .exec(
            "INSERT INTO spa_rls (id, owner, loc) \
             VALUES ('r_theirs', 'alice', ST_Point(2.3522, 48.8566))",
        )
        .await
        .expect("seed spatial row");
    create_user(&server, user).await;
    write_policy(&server, "spa_rls_owner", "spa_rls").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO spa_rls (id, owner, loc) \
             VALUES ('r_new', 'alice', ST_Point(0.0, 0.0))",
        )
        .await,
        "a spatial insert handing the row to another owner",
    );
    assert_rls_denied(
        run_as(
            &server,
            user,
            "UPDATE spa_rls SET owner = 'alice' WHERE id = 'r_theirs'",
        )
        .await,
        "a spatial update outside the write policy",
    );
    assert_rls_denied(
        run_as(&server, user, "DELETE FROM spa_rls WHERE id = 'r_theirs'").await,
        "a spatial delete outside the write policy",
    );

    let rows = server
        .query_rows("SELECT id, owner FROM spa_rls ORDER BY id")
        .await
        .expect("read back spatial collection");
    assert_eq!(
        rows.len(),
        1,
        "no refused spatial statement may have changed storage: {rows:?}"
    );
    assert_eq!(rows[0][1], "alice", "the seeded row must be untouched");
}

/// Without a write policy nothing changes: the same statements that the gate
/// refuses above all apply here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_with_no_write_policy_is_unaffected() {
    let server = TestServer::start().await;
    let user = "col_rls_free_user";
    seed(&server, "col_rls_free", user).await;

    run_as(
        &server,
        user,
        "INSERT INTO col_rls_free (id, owner, note) VALUES ('r_new', 'alice', 'x')",
    )
    .await
    .expect("insert must apply with no policy");
    run_as(
        &server,
        user,
        "UPDATE col_rls_free SET owner = 'alice' WHERE id = 'r_mine'",
    )
    .await
    .expect("update must apply with no policy");
    run_as(
        &server,
        user,
        "DELETE FROM col_rls_free WHERE id = 'r_theirs'",
    )
    .await
    .expect("delete must apply with no policy");

    let rows = stored(&server, "col_rls_free").await;
    assert_eq!(
        rows.len(),
        2,
        "one inserted, one deleted, one updated in place: {rows:?}"
    );
}
