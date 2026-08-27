// SPDX-License-Identifier: BUSL-1.1

//! A write the server REFUSED must not come back after a restart.
//!
//! The write funnel appends a write's redo record BEFORE the Data Plane decides
//! whether to accept it, so any refusal decided inside the storage engine —
//! notably a row-level-security WRITE policy, which is evaluated against the
//! row image about to be persisted — arrives with the record already in the
//! log. Replay is indifferent to why a record is there, so without an explicit
//! cancellation it re-applies the refused write and recovery silently undoes
//! the refusal.
//!
//! Both tests here refuse a `DELETE`, and that is not incidental. A delete's
//! image is the row it removes, which only the storage engine can produce, so
//! the verdict is necessarily downstream of the append. It is also one of the
//! few document shapes that appends anything on the forward path at all:
//! `PointUpdate`, `Upsert`, `BulkUpdate`, `BulkDelete`, `Merge` and
//! `UpdateFromJoin` journal NOTHING before dispatch (see
//! `wal_dispatch::document`) — their redo is minted after apply and only on
//! success — so a refusal on those leaves no record to resurrect.
//!
//! What each engine loses if the record replays differs, and each test asserts
//! the thing its engine actually loses:
//!
//! * **Key-value** — the rows live only in an in-memory hash table, so WAL
//!   replay is their sole recovery path and a replayed delete removes the row
//!   outright.
//! * **Document** — the row is shielded by redb's synchronous commit at apply
//!   time, but its secondary vector index is not: that HNSW has no durable
//!   backing of its own and `CoreLoop::replay_document_vector_wal` rebuilds it
//!   from these very records, its delete arm dropping the vector node named by
//!   each `Delete`. A replayed refused delete therefore strips the vector of a
//!   row storage still holds — readable by primary key, vanished from vector
//!   search, and only after a restart.
//!
//! The restart is a real one: `graceful_shutdown` releases every WAL and redb
//! handle and `open_on_path` reopens the same directory, so the engines come
//! back up through WAL replay. The non-RLS half of this invariant — a
//! constraint violation refused after the same append — is covered against the
//! real server binary under `kill -9` in
//! `crash_refused_write_not_resurrected.rs`.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "restart-refused-write-secret-3";

/// The least privilege that can run the DML under test, so a refusal is the
/// write policy's doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

async fn create_probing_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

/// Run `sql` as `user`, returning the rows' first column on success and the
/// server's error message on failure.
///
/// The message is read off the attached `DbError`: the
/// `tokio_postgres::Error` wrapper's own `Display` is the fixed string "db
/// error", so asserting on it would make a policy refusal indistinguishable
/// from every other failure.
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

/// Refuse `sql` as `user`, and prove the refusal came from the write policy.
///
/// Before probing, this asserts the identity can actually SEE `visible_probe`'s
/// row. A statement that matches zero rows also succeeds, so without that
/// precondition an identity that cannot see the row at all is indistinguishable
/// from a policy that failed to fire — the test would report "the server
/// accepted a write it must refuse" while nothing had been asked of the gate.
async fn refuse_as(server: &TestServer, user: &str, visible_probe: &str, sql: &str) {
    let visible = run_as(server, user, visible_probe)
        .await
        .unwrap_or_else(|e| panic!("visibility precondition query failed for {user}: {e}"));
    assert_eq!(
        visible.len(),
        1,
        "test setup: {user} must be able to see the row it is about to be refused on, \
         or the statement under test matches nothing and refuses nothing (got {visible:?})"
    );

    match run_as(server, user, sql).await {
        Ok(rows) => panic!("the server accepted a statement it must refuse: {sql} (got {rows:?})"),
        Err(message) => assert!(
            message.contains("RLS"),
            "the statement must be refused BY THE WRITE POLICY, not by an unrelated failure \
             that would make this test pass for the wrong reason: {message}"
        ),
    }
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

/// The single nearest neighbour of `axis` in `collection`, by id.
async fn nearest(server: &TestServer, collection: &str, axis: &str) -> String {
    let rows = server
        .query_rows(&format!(
            "SELECT id FROM {collection} ORDER BY vector_distance(embedding, {axis}) LIMIT 1"
        ))
        .await
        .unwrap_or_else(|e| panic!("vector query on {axis}: {e}"));
    assert_eq!(rows.len(), 1, "nearest-neighbour query must return one row");
    rows[0][0].clone()
}

/// The x-axis, where the governed document row sits.
const X_AXIS: &str = "ARRAY[1.0, 0.0, 0.0, 0.0]";

/// Deleting a document row the write policy excludes is refused in the storage
/// engine, after the funnel already appended the delete's `Delete` record. On
/// restart the secondary vector index is rebuilt from those records, so the
/// refused delete must not be among them.
///
/// `r_theirs` sits exactly ON the x-axis and `x_anchor` slightly off it, so
/// `r_theirs` is the unique nearest neighbour of an x-axis query while its
/// vector node exists — and `x_anchor` takes its place the moment a replayed
/// delete removes it. That asymmetry is what makes the assertion discriminating
/// rather than a coin flip between tied candidates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_refused_document_delete_is_not_replayed_into_the_vector_index() {
    let srv = TestServer::start().await;
    let user = "refused_vec_user";

    srv.exec("CREATE COLLECTION refused_vec TYPE document")
        .await
        .expect("create collection");
    srv.exec("CREATE VECTOR INDEX idx_refused_vec ON refused_vec (embedding) METRIC cosine DIM 4")
        .await
        .expect("create vector index");

    let rows: &[(&str, &str, [f32; 4])] = &[
        // The row under test: owned by someone else, so the probing user may
        // not delete it. Exactly on the x-axis.
        ("r_theirs", "alice", [1.0, 0.0, 0.0, 0.0]),
        // Near the x-axis but not on it: the runner-up that wins only if
        // `r_theirs` loses its vector node.
        ("x_anchor", "alice", [0.9, 0.1, 0.0, 0.0]),
        // The probing user's own row, so the policy is not a blanket ban.
        ("r_mine", user, [0.0, 0.0, 1.0, 0.0]),
    ];
    for (id, owner, emb) in rows {
        srv.exec(&format!(
            "INSERT INTO refused_vec (id, owner, embedding) VALUES \
             ('{id}', '{owner}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    create_probing_user(&srv, user).await;
    write_policy(&srv, "refused_vec_owner", "refused_vec").await;

    assert_eq!(
        nearest(&srv, "refused_vec", X_AXIS).await,
        "r_theirs",
        "test setup: the governed row must start as the x-axis nearest neighbour"
    );

    refuse_as(
        &srv,
        user,
        "SELECT id FROM refused_vec WHERE id = 'r_theirs'",
        "DELETE FROM refused_vec WHERE id = 'r_theirs'",
    )
    .await;

    // Pre-restart: nothing moved. Rules out a false positive where the refusal
    // failed to protect the live state either, which would leave the
    // post-restart assertion proving nothing about replay.
    assert_eq!(
        nearest(&srv, "refused_vec", X_AXIS).await,
        "r_theirs",
        "a refused delete must leave the live vector index untouched"
    );

    // WAL-only restart against the same data directory. The vector index has no
    // durable backing of its own, so this rebuilds it purely from WAL records.
    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // The row itself is still in storage — redb never applied the refused
    // delete — so any post-restart disagreement is the vector index's alone.
    let stored = srv2
        .query_rows("SELECT owner FROM refused_vec WHERE id = 'r_theirs'")
        .await
        .expect("read back the governed row");
    assert_eq!(
        stored,
        vec![vec!["alice".to_string()]],
        "a refused delete must leave the stored row in place"
    );

    // Without the abort record, replay feeds the refused `Delete` to
    // `replay_document_vector_wal`, which drops this row's vector node and
    // hands the x-axis to `x_anchor`.
    assert_eq!(
        nearest(&srv2, "refused_vec", X_AXIS).await,
        "r_theirs",
        "a delete the server REFUSED was replayed after a restart and stripped the row's \
         entry from the secondary vector index, so vector search no longer returns a row \
         storage still holds"
    );
}

/// The same defect on the key-value engine, whose rows live only in an
/// in-memory hash table: WAL replay is their sole recovery path, so a refused
/// `DELETE` that replays removes a row the server said it would keep.
///
/// The collection shape mirrors `kv_write_row_level_security.rs` so the refusal
/// under test is the one that fixture already proves, differing only in what
/// happens afterwards: a restart against the same data directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rls_refused_kv_delete_is_not_resurrected_by_replay() {
    let srv = TestServer::start().await;
    let user = "refused_kv_user";

    srv.exec(
        "CREATE COLLECTION refused_kv (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
         WITH (engine='kv')",
    )
    .await
    .expect("create collection");
    for (key, owner) in [("r_mine", user), ("r_theirs", "alice")] {
        srv.exec(&format!(
            "INSERT INTO refused_kv (key, owner, note) VALUES ('{key}', '{owner}', 'before')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {key}: {e}"));
    }
    create_probing_user(&srv, user).await;
    write_policy(&srv, "refused_kv_owner", "refused_kv").await;

    let before = srv
        .query_rows("SELECT key, owner, note FROM refused_kv ORDER BY key")
        .await
        .expect("read back stored rows");
    assert_eq!(before.len(), 2, "both seed rows must be stored: {before:?}");

    refuse_as(
        &srv,
        user,
        "SELECT key FROM refused_kv WHERE key = 'r_theirs'",
        "DELETE FROM refused_kv WHERE key = 'r_theirs'",
    )
    .await;

    assert_eq!(
        srv.query_rows("SELECT key, owner, note FROM refused_kv ORDER BY key")
            .await
            .expect("read back stored rows"),
        before,
        "a refused delete must leave storage untouched even before any restart"
    );

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        srv2.query_rows("SELECT key, owner, note FROM refused_kv ORDER BY key")
            .await
            .expect("read back stored rows after restart"),
        before,
        "a delete the server refused was replayed after a restart and removed the row"
    );
}
