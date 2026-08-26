// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over governed document writes whose row image is
//! decided where the row is persisted: `UPSERT`, bulk `UPDATE`, bulk
//! `DELETE`. Every test asserts both directions and row count/value, not
//! just absence of an error — a blanket refusal would pass a one-sided
//! check. Under Raft these resolve on the Control Plane before proposing,
//! since a follower has no writing identity to decide the predicate against.

use crate::harness::TestServer;

const PASSWORD: &str = "deferred-rls-secret-7";

/// Least privilege that can run the DML under test, so a denial is the policy's
/// doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

/// Create `collection` with `r_mine` owned by `user` and `r_theirs` owned by
/// `alice`, a probing user, and a `FOR WRITE` owner policy.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                 id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='document_strict')"
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
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
    server
        .exec(&format!(
            "CREATE RLS POLICY {collection}_owner ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy on {collection}: {e}"));
}

/// Run `sql` as `user`, returning the server's error message on failure.
async fn try_exec_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client
        .simple_query(sql)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
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

fn row(id: &str, owner: &str, note: &str) -> Vec<String> {
    vec![id.to_string(), owner.to_string(), note.to_string()]
}

/// `UPSERT`, both branches and both directions. The merge branch's image
/// exists only after the stored row is read and `ON CONFLICT` runs; the
/// insert branch's only after the row is found absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_is_gated_in_both_directions() {
    let server = TestServer::start().await;
    let user = "w_rls_ups_user";
    seed(&server, "w_rls_ups", user).await;

    // Conforming MERGE branch: the row stays owned, so the policy admits it.
    try_exec_as(
        &server,
        user,
        &format!(
            "INSERT INTO w_rls_ups (id, owner, note) VALUES ('r_mine', '{user}', 'seed') \
             ON CONFLICT (id) DO UPDATE SET note = 'merged'"
        ),
    )
    .await
    .expect("an upsert whose merged image satisfies the policy must apply");
    assert_eq!(
        stored(&server, "w_rls_ups").await,
        vec![
            row("r_mine", user, "merged"),
            row("r_theirs", "alice", "before"),
        ],
        "the conforming upsert must land the merged row and leave the other alone"
    );

    // Conforming INSERT branch: a fresh owned row.
    try_exec_as(
        &server,
        user,
        &format!(
            "INSERT INTO w_rls_ups (id, owner, note) VALUES ('r_new', '{user}', 'fresh') \
             ON CONFLICT (id) DO UPDATE SET note = 'merged'"
        ),
    )
    .await
    .expect("an upsert inserting a row the policy admits must apply");
    assert_eq!(
        stored(&server, "w_rls_ups").await,
        vec![
            row("r_mine", user, "merged"),
            row("r_new", user, "fresh"),
            row("r_theirs", "alice", "before"),
        ],
        "the conforming insert branch must land exactly one new row"
    );

    // Violating MERGE branch: handing the row away is what the policy forbids.
    let before = stored(&server, "w_rls_ups").await;
    let result = try_exec_as(
        &server,
        user,
        &format!(
            "INSERT INTO w_rls_ups (id, owner, note) VALUES ('r_mine', '{user}', 'seed') \
             ON CONFLICT (id) DO UPDATE SET owner = 'alice'"
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "an upsert whose merged image violates the write policy must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_ups").await,
        before,
        "a rejected upsert must leave every stored row exactly as it was"
    );

    // Violating INSERT branch: a fresh row owned by someone else.
    let result = try_exec_as(
        &server,
        user,
        "INSERT INTO w_rls_ups (id, owner, note) VALUES ('r_other', 'alice', 'fresh') \
         ON CONFLICT (id) DO UPDATE SET note = 'merged'",
    )
    .await;
    assert!(
        result.is_err(),
        "an upsert inserting a row the policy excludes must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_ups").await,
        before,
        "a rejected upsert insert must add no row"
    );
}

/// A predicate `UPDATE`, both directions. Row set is known only after
/// committed state is scanned, post-image only after assignments run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_update_is_gated_in_both_directions() {
    let server = TestServer::start().await;
    let user = "w_rls_bupd_user";
    seed(&server, "w_rls_bupd", user).await;

    // Conforming: the predicate matches only the owned row, whose post-image
    // stays in scope.
    try_exec_as(
        &server,
        user,
        &format!("UPDATE w_rls_bupd SET note = 'bulk' WHERE owner = '{user}'"),
    )
    .await
    .expect("a predicate update whose matched post-images satisfy the policy must apply");
    assert_eq!(
        stored(&server, "w_rls_bupd").await,
        vec![
            row("r_mine", user, "bulk"),
            row("r_theirs", "alice", "before"),
        ],
        "the conforming bulk update must rewrite exactly the matched row"
    );

    // Violating: an unfiltered update spans the excluded row, so it must fail
    // WHOLE rather than rewriting the owned row and stopping.
    let before = stored(&server, "w_rls_bupd").await;
    let result = try_exec_as(&server, user, "UPDATE w_rls_bupd SET note = 'wide'").await;
    assert!(
        result.is_err(),
        "a predicate update spanning a row the policy excludes must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_bupd").await,
        before,
        "a rejected bulk update must leave every stored row exactly as it was"
    );

    // Violating a second way: every matched post-image leaves scope.
    let result = try_exec_as(
        &server,
        user,
        &format!("UPDATE w_rls_bupd SET owner = 'alice' WHERE owner = '{user}'"),
    )
    .await;
    assert!(
        result.is_err(),
        "a predicate update whose post-image leaves the policy's scope must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_bupd").await,
        before,
        "the rejected post-image must leave storage untouched"
    );
}

/// A predicate `DELETE`, both directions. The pre-deletion image is the only
/// image a delete has, so every matched row is decided against its stored bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_delete_is_gated_in_both_directions() {
    let server = TestServer::start().await;
    let user = "w_rls_bdel_user";
    seed(&server, "w_rls_bdel", user).await;

    // Violating first, so the conforming delete below runs against the full
    // seed: an unfiltered delete spans the excluded row.
    let before = stored(&server, "w_rls_bdel").await;
    let result = try_exec_as(&server, user, "DELETE FROM w_rls_bdel").await;
    assert!(
        result.is_err(),
        "a predicate delete spanning a row the policy excludes must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_bdel").await,
        before,
        "a rejected bulk delete must remove nothing"
    );

    // Conforming: the predicate matches only rows the policy admits.
    try_exec_as(
        &server,
        user,
        &format!("DELETE FROM w_rls_bdel WHERE owner = '{user}'"),
    )
    .await
    .expect("a predicate delete matching only admitted rows must apply");
    assert_eq!(
        stored(&server, "w_rls_bdel").await,
        vec![row("r_theirs", "alice", "before")],
        "the conforming bulk delete must remove exactly the admitted row"
    );
}
