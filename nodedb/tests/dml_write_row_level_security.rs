// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over document writes whose row image is built where the
//! row is persisted.
//!
//! An `INSERT` carries its whole new row in the plan, so a write policy decides
//! it before dispatch. `UPDATE`, `DELETE`, `UPSERT`, and `MERGE` do not: the
//! post-image of an update exists only after the stored row is read and the
//! assignments are applied, and a delete's image only after the row being
//! removed is read. The compiled predicate therefore travels with the plan and
//! is evaluated in the storage engine against the exact bytes about to be
//! written.
//!
//! What these tests pin:
//!
//! - A rejected row fails the WHOLE statement and leaves storage untouched. A
//!   silently skipped row would report an affected count for a write that never
//!   happened, and would leave a multi-row statement half applied.
//! - The gate is the WRITE policy, not the read policy. A collection carrying
//!   only a `FOR READ` policy must keep writing exactly as before.
//! - `FOR ALL` decides both halves, from one `USING` clause.
//! - An unresolvable `$auth.*` reference denies rather than compiling to an
//!   allow-everything predicate.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "write-rls-secret-42";

/// The role the probing user holds. `readwrite` is the least privilege that can
/// run the DML under test, so a denial here is the policy's doing and not the
/// RBAC layer's.
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

/// Create `collection` with two rows — `r_mine` owned by `user`, `r_theirs`
/// owned by `alice` — and a probing user with no policy yet.
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
    create_user(server, user).await;
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

/// Rows `user` sees from `sql`, each row's columns joined by `|`.
async fn rows_as(server: &TestServer, user: &str, sql: &str) -> Vec<String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{user} runs {sql}: {e}"));
    let mut out = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            let mut cells = Vec::new();
            for i in 0..row.len() {
                cells.push(row.get(i).unwrap_or("").to_string());
            }
            out.push(cells.join("|"));
        }
    }
    drop(client);
    handle.abort();
    out
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

/// An `UPDATE` whose post-image leaves the policy's scope is rejected, and the
/// stored row is untouched. This is the case a plan-time check cannot make: the
/// row that violates the predicate does not exist until the assignment runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_producing_a_violating_row_is_rejected_and_writes_nothing() {
    let server = TestServer::start().await;
    let user = "w_rls_upd_user";
    seed(&server, "w_rls_upd", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_upd_owner ON w_rls_upd FOR WRITE \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create write policy");

    let before = stored(&server, "w_rls_upd").await;

    // Handing the row to someone else is precisely what the policy forbids.
    let result = try_exec_as(
        &server,
        user,
        "UPDATE w_rls_upd SET owner = 'alice' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "an update whose post-image violates the write policy must be rejected"
    );

    assert_eq!(
        stored(&server, "w_rls_upd").await,
        before,
        "a rejected update must leave every stored row exactly as it was"
    );
}

/// An `UPDATE` whose post-image stays inside the policy's scope applies
/// normally — the gate must not turn into a blanket write ban.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conforming_update_succeeds() {
    let server = TestServer::start().await;
    let user = "w_rls_ok_user";
    seed(&server, "w_rls_ok", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_ok_owner ON w_rls_ok FOR WRITE \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create write policy");

    try_exec_as(
        &server,
        user,
        "UPDATE w_rls_ok SET note = 'touched' WHERE id = 'r_mine'",
    )
    .await
    .expect("an update whose post-image satisfies the policy must apply");

    assert_eq!(
        stored(&server, "w_rls_ok").await,
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

/// A `DELETE` is decided against the row it removes — the only image a delete
/// has. Deleting a row the policy excludes is rejected and the row survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_row_the_policy_excludes_is_rejected() {
    let server = TestServer::start().await;
    let user = "w_rls_del_user";
    seed(&server, "w_rls_del", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_del_owner ON w_rls_del FOR WRITE \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create write policy");

    let result = try_exec_as(&server, user, "DELETE FROM w_rls_del WHERE id = 'r_theirs'").await;
    assert!(
        result.is_err(),
        "deleting a row outside the write policy must be rejected"
    );

    let rows = stored(&server, "w_rls_del").await;
    assert_eq!(rows.len(), 2, "the excluded row must survive: {rows:?}");

    // The unfiltered predicate DELETE matches both rows, so it must also fail
    // whole rather than removing the owned row and stopping.
    let result = try_exec_as(&server, user, "DELETE FROM w_rls_del").await;
    assert!(
        result.is_err(),
        "a predicate delete spanning an excluded row must fail whole"
    );
    let rows = stored(&server, "w_rls_del").await;
    assert_eq!(
        rows.len(),
        2,
        "a rejected predicate delete must remove nothing: {rows:?}"
    );
}

/// A `MERGE` writes through every arm, so the target's write policy gates each
/// one. A NOT-MATCHED insert whose row the policy excludes fails the statement
/// and leaves the target as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_write_arms_are_gated_by_the_target_policy() {
    let server = TestServer::start().await;
    let user = "w_rls_merge_user";

    for name in ["w_rls_merge_tgt", "w_rls_merge_src"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (\
                     id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    server
        .exec(&format!(
            "INSERT INTO w_rls_merge_tgt (id, owner, note) \
             VALUES ('m_mine', '{user}', 'before')"
        ))
        .await
        .expect("seed merge target");
    // `m_mine` updates the owned target row; `m_theirs` is a NOT-MATCHED insert
    // of a row the target's policy excludes.
    for (id, owner) in [("m_mine", user), ("m_theirs", "alice")] {
        server
            .exec(&format!(
                "INSERT INTO w_rls_merge_src (id, owner, note) \
                 VALUES ('{id}', '{owner}', 'merged')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed merge source {id}: {e}"));
    }
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_merge_owner ON w_rls_merge_tgt FOR WRITE \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create write policy on merge target");

    let before = stored(&server, "w_rls_merge_tgt").await;
    let result = try_exec_as(
        &server,
        user,
        "MERGE INTO w_rls_merge_tgt t USING w_rls_merge_src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET note = s.note \
         WHEN NOT MATCHED THEN INSERT (id, owner, note) VALUES (s.id, s.owner, s.note)",
    )
    .await;
    assert!(
        result.is_err(),
        "a MERGE arm writing a row the target's policy excludes must be rejected"
    );
    assert_eq!(
        stored(&server, "w_rls_merge_tgt").await,
        before,
        "a rejected MERGE must leave the target untouched — no arm applies"
    );
}

/// `FOR ALL` compiles one `USING` clause into both halves: the read filter that
/// bounds what a `SELECT` returns and the write predicate that bounds what may
/// be persisted. Neither half may be silently dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_for_all_policy_gates_both_reads_and_writes() {
    let server = TestServer::start().await;
    let user = "w_rls_all_user";
    seed(&server, "w_rls_all", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_all_owner ON w_rls_all FOR ALL \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create FOR ALL policy");

    // Read half: only the owned row is visible.
    assert_eq!(
        rows_as(&server, user, "SELECT id FROM w_rls_all ORDER BY id").await,
        vec!["r_mine".to_string()],
        "the read half must hide the row the policy excludes"
    );

    // Write half: the same predicate rejects a post-image that leaves scope.
    let before = stored(&server, "w_rls_all").await;
    let result = try_exec_as(
        &server,
        user,
        "UPDATE w_rls_all SET owner = 'alice' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "the write half of a FOR ALL policy must reject the violating post-image"
    );
    assert_eq!(
        stored(&server, "w_rls_all").await,
        before,
        "the rejected write must leave storage untouched"
    );

    // …and a conforming write still applies, so the write half is a predicate
    // and not a blanket ban.
    try_exec_as(
        &server,
        user,
        "UPDATE w_rls_all SET note = 'touched' WHERE id = 'r_mine'",
    )
    .await
    .expect("a conforming write under FOR ALL must apply");
}

/// A write predicate naming a session variable the identity does not CARRY
/// cannot be resolved. `$auth.org_id` is a valid variable — the policy parses —
/// but a password-created user has no organization, so it resolves to nothing
/// at query time. That must deny, not compile to an empty (allow-everything)
/// gate: the fail-closed rule the read path already follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unresolvable_auth_reference_denies_the_write() {
    let server = TestServer::start().await;
    let user = "w_rls_auth_user";
    seed(&server, "w_rls_auth", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_auth_missing ON w_rls_auth FOR WRITE \
             USING (owner = $auth.org_id)",
        )
        .await
        .expect("create write policy on an org-scoped variable");

    let before = stored(&server, "w_rls_auth").await;
    let result = try_exec_as(
        &server,
        user,
        "UPDATE w_rls_auth SET note = 'touched' WHERE id = 'r_mine'",
    )
    .await;
    assert!(
        result.is_err(),
        "an unresolvable $auth reference must deny the write, not admit it"
    );
    assert_eq!(
        stored(&server, "w_rls_auth").await,
        before,
        "the denied write must leave storage untouched"
    );
}

/// A collection with no write policy — or with a read policy only — writes
/// exactly as it did before the gate existed. The gate is keyed on write
/// policies, so a `FOR READ` policy must not start rejecting writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collection_without_a_write_policy_is_unaffected() {
    let server = TestServer::start().await;
    let user = "w_rls_none_user";
    seed(&server, "w_rls_none", user).await;
    server
        .exec(
            "CREATE RLS POLICY w_rls_none_read ON w_rls_none FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read-only policy");

    // Every row is rewritten, including the one the READ policy hides: a read
    // policy bounds what is shown, never what is written.
    try_exec_as(&server, user, "UPDATE w_rls_none SET note = 'touched'")
        .await
        .expect("a read-only policy must not gate writes");

    assert_eq!(
        stored(&server, "w_rls_none").await,
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
        "both rows must be written under a read-only policy"
    );

    try_exec_as(&server, user, "DELETE FROM w_rls_none")
        .await
        .expect("a read-only policy must not gate deletes");
    assert!(
        stored(&server, "w_rls_none").await.is_empty(),
        "the ungated delete must remove every row"
    );
}
