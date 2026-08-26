// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over KV writes whose post-image is resolved on the
//! Control Plane before the mutation is proposed, since these ops have no
//! post-image until the stored value is read. Every test asserts both
//! directions: a conforming write succeeds with the correct state, a
//! violating write is refused with state unchanged.

use crate::harness::TestServer;

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
/// Reads the message off the attached `DbError`, since
/// `tokio_postgres::Error`'s own `Display` is always "db error".
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

/// Every `(id, owner, note)` row, read back as the superuser — who holds no
/// restricting policy, so this is the true stored state.
async fn stored(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT id, owner, note FROM {collection} ORDER BY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// A KV collection seeded with one row owned by `user` and one owned by
/// `alice`, plus the probing user with no policy yet.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')"
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

/// A literal `UPDATE` lowers to `KvOp::FieldSet`, whose merged body is
/// decided against the write policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn field_set_update_is_gated_both_directions() {
    let server = TestServer::start().await;
    let user = "kv_rls_fs_user";
    seed(&server, "kv_rls_fs", user).await;
    write_policy(&server, "kv_rls_fs_owner", "kv_rls_fs").await;

    let before = stored(&server, "kv_rls_fs").await;
    let result = run_as(
        &server,
        user,
        "UPDATE kv_rls_fs SET owner = 'alice' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "a FieldSet post-image handing the row to someone else must be refused"
    );
    assert_eq!(
        stored(&server, "kv_rls_fs").await,
        before,
        "a refused FieldSet update must leave every stored row exactly as it was"
    );

    run_as(
        &server,
        user,
        "UPDATE kv_rls_fs SET note = 'touched' WHERE id = 'r_mine'",
    )
    .await
    .expect("a FieldSet post-image that stays inside the policy must apply");
    assert_eq!(
        stored(&server, "kv_rls_fs").await,
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
        "the conforming row must be written and the other row left alone"
    );
}

/// `KvOp::Delete` is decided against the row it removes — the only image a
/// delete has. Deleting a row the policy excludes is refused and the row
/// survives; deleting an owned row applies and removes only that row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_is_gated_both_directions() {
    let server = TestServer::start().await;
    let user = "kv_rls_del_user";
    seed(&server, "kv_rls_del", user).await;
    write_policy(&server, "kv_rls_del_owner", "kv_rls_del").await;

    let result = run_as(
        &server,
        user,
        "DELETE FROM kv_rls_del WHERE id = 'r_theirs'",
    )
    .await;
    assert!(
        result.is_err(),
        "deleting a row outside the write policy must be refused"
    );
    let rows = stored(&server, "kv_rls_del").await;
    assert_eq!(rows.len(), 2, "the excluded row must survive: {rows:?}");

    run_as(&server, user, "DELETE FROM kv_rls_del WHERE id = 'r_mine'")
        .await
        .expect("deleting an owned row must apply");
    assert_eq!(
        stored(&server, "kv_rls_del").await,
        vec![vec![
            "r_theirs".to_string(),
            "alice".to_string(),
            "before".to_string()
        ]],
        "only the owned row must be removed"
    );
}

/// Arithmetic `ON CONFLICT DO UPDATE` (`KvOp::InsertOnConflictUpdate`) merges
/// on the Control Plane before the policy is decided. Checks both the verdict
/// and the resolved value, not just the verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_update_arithmetic_is_gated_and_computed_correctly() {
    let server = TestServer::start().await;
    let user = "kv_rls_incr_user";
    server
        .exec(
            "CREATE COLLECTION kv_rls_incr \
             (id TEXT PRIMARY KEY, owner TEXT, n INT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create kv_rls_incr");
    for (id, owner, n) in [("r_mine", user, 5), ("r_theirs", "alice", 100)] {
        server
            .exec(&format!(
                "INSERT INTO kv_rls_incr (id, owner, n) VALUES ('{id}', '{owner}', {n})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed kv_rls_incr/{id}: {e}"));
    }
    create_user(&server, user).await;
    write_policy(&server, "kv_rls_incr_owner", "kv_rls_incr").await;

    // The assignment touches only `n`, so the merged owner stays alice's
    // regardless of the incoming row's `owner` column.
    let result = run_as(
        &server,
        user,
        "INSERT INTO kv_rls_incr (id, owner, n) VALUES ('r_theirs', 'alice', 0) \
         ON CONFLICT (id) DO UPDATE SET n = n + 1",
    )
    .await;
    assert!(
        result.is_err(),
        "an arithmetic ON CONFLICT update on a row outside the write policy must be refused"
    );
    let n_theirs: Vec<Vec<String>> = server
        .query_rows("SELECT n FROM kv_rls_incr WHERE id = 'r_theirs'")
        .await
        .expect("read back r_theirs");
    assert_eq!(
        n_theirs,
        vec![vec!["100".to_string()]],
        "a refused arithmetic update must leave the stored value untouched"
    );

    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO kv_rls_incr (id, owner, n) VALUES ('r_mine', '{user}', 0) \
             ON CONFLICT (id) DO UPDATE SET n = n + 1"
        ),
    )
    .await
    .expect("an arithmetic ON CONFLICT update on an owned row must apply");
    let n_mine: Vec<Vec<String>> = server
        .query_rows("SELECT n FROM kv_rls_incr WHERE id = 'r_mine'")
        .await
        .expect("read back r_mine");
    assert_eq!(
        n_mine,
        vec![vec!["6".to_string()]],
        "the resolved arithmetic must apply against the stored value (5 + 1 = 6), \
         not the incoming row's literal"
    );
}

/// `ON CONFLICT DO UPDATE` decides the policy against the MERGED row, not the
/// incoming one: an incoming `owner` that would pass must still be refused
/// when the assignment list never writes `owner`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_update_is_decided_against_the_merged_row_not_the_incoming_one() {
    let server = TestServer::start().await;
    let user = "kv_rls_merge_user";
    seed(&server, "kv_rls_merge", user).await;
    write_policy(&server, "kv_rls_merge_owner", "kv_rls_merge").await;

    // `r_theirs` is alice's. The incoming `owner = user` would pass, but the
    // update list only writes `note`, so the merged owner stays alice's.
    let result = run_as(
        &server,
        user,
        &format!(
            "INSERT INTO kv_rls_merge (id, owner, note) VALUES ('r_theirs', '{user}', 'merged') \
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note"
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "a merge whose RESOLVED owner violates the policy must be refused even though \
         the incoming row's own owner column would have passed"
    );
    assert_eq!(
        stored(&server, "kv_rls_merge").await,
        vec![
            vec!["r_mine".to_string(), user.to_string(), "before".to_string()],
            vec![
                "r_theirs".to_string(),
                "alice".to_string(),
                "before".to_string()
            ],
        ],
        "a refused merge must leave every stored row exactly as it was"
    );

    // Same statement against the owned row: merged owner conforms, note updates.
    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO kv_rls_merge (id, owner, note) VALUES ('r_mine', '{user}', 'merged') \
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note"
        ),
    )
    .await
    .expect("a merge whose resolved owner satisfies the policy must apply");
    assert_eq!(
        stored(&server, "kv_rls_merge").await,
        vec![
            vec!["r_mine".to_string(), user.to_string(), "merged".to_string()],
            vec![
                "r_theirs".to_string(),
                "alice".to_string(),
                "before".to_string()
            ],
        ],
        "the conforming merge must apply and the other row must stay untouched"
    );
}

/// The gate is keyed on write policies: a collection without one writes
/// unrestricted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_without_a_write_policy_is_unaffected() {
    let server = TestServer::start().await;
    let user = "kv_rls_none_user";
    seed(&server, "kv_rls_none", user).await;

    run_as(&server, user, "UPDATE kv_rls_none SET note = 'touched'")
        .await
        .expect("no write policy exists — every row must accept the update");
    assert_eq!(
        stored(&server, "kv_rls_none").await,
        vec![
            vec![
                "r_mine".to_string(),
                user.to_string(),
                "touched".to_string()
            ],
            vec![
                "r_theirs".to_string(),
                "alice".to_string(),
                "touched".to_string()
            ],
        ],
        "both rows must be written when the collection carries no write policy"
    );

    run_as(
        &server,
        user,
        "DELETE FROM kv_rls_none WHERE id = 'r_theirs'",
    )
    .await
    .expect("no write policy exists — the delete must apply to any row");
    assert_eq!(
        stored(&server, "kv_rls_none").await,
        vec![vec![
            "r_mine".to_string(),
            user.to_string(),
            "touched".to_string()
        ]],
        "the ungated delete must remove the targeted row"
    );
}

/// A predicate `UPDATE` with no `WHERE` on a governed collection is refused
/// WHOLE if it would touch any row the identity does not own — the owned
/// row is not written either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_update_is_gated_both_directions() {
    let server = TestServer::start().await;
    let user = "kv_rls_pu_user";
    seed(&server, "kv_rls_pu", user).await;
    write_policy(&server, "kv_rls_pu_owner", "kv_rls_pu").await;

    let before = stored(&server, "kv_rls_pu").await;
    let result = run_as(&server, user, "UPDATE kv_rls_pu SET note = 'touched'").await;
    assert!(
        result.is_err(),
        "an unqualified UPDATE reaching a row the policy excludes must be refused"
    );
    assert_eq!(
        stored(&server, "kv_rls_pu").await,
        before,
        "a refused predicate update must leave every stored row exactly as it was"
    );

    run_as(
        &server,
        user,
        &format!("UPDATE kv_rls_pu SET note = 'touched' WHERE owner = '{user}'"),
    )
    .await
    .expect("a predicate matching only owned rows must apply");
    assert_eq!(
        stored(&server, "kv_rls_pu").await,
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
        "the conforming rows must be written and the excluded row left alone"
    );
}

/// A predicate `DELETE` on a governed collection: decided against the
/// pre-image of every row it would remove, in both directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_delete_is_gated_both_directions() {
    let server = TestServer::start().await;
    let user = "kv_rls_pd_user";
    seed(&server, "kv_rls_pd", user).await;
    write_policy(&server, "kv_rls_pd_owner", "kv_rls_pd").await;

    let before = stored(&server, "kv_rls_pd").await;
    let result = run_as(&server, user, "DELETE FROM kv_rls_pd WHERE note = 'before'").await;
    assert!(
        result.is_err(),
        "a predicate reaching a row the policy excludes must be refused"
    );
    assert_eq!(
        stored(&server, "kv_rls_pd").await,
        before,
        "a refused predicate delete must leave every stored row exactly as it was"
    );

    run_as(
        &server,
        user,
        &format!("DELETE FROM kv_rls_pd WHERE owner = '{user}'"),
    )
    .await
    .expect("a predicate matching only owned rows must apply");
    assert_eq!(
        stored(&server, "kv_rls_pd").await,
        vec![vec![
            "r_theirs".to_string(),
            "alice".to_string(),
            "before".to_string()
        ]],
        "only the owned row may be removed"
    );
}
