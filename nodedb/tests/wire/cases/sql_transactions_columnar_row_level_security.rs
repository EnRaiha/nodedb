// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over columnar writes issued inside a `BEGIN..COMMIT`
//! block.
//!
//! An in-transaction columnar write is executed at STATEMENT time by staging it
//! into the per-transaction overlay, and only replayed durably at COMMIT. The
//! write policy therefore has to decide the row where that image is produced —
//! at staging. Deferring it to COMMIT alone would let the statement report a
//! successful `{"affected": N}` and make the refused image readable by the
//! transaction's own scans, right up until COMMIT failed.
//!
//! What these tests pin:
//!
//! - A violating `UPDATE` / `DELETE` inside a transaction fails AT THE
//!   STATEMENT. Pre-fix both returned a successful affected count and only
//!   COMMIT refused them.
//! - A conforming write still stages and is visible to the transaction's own
//!   read, so the gate is not a blanket in-transaction write ban.
//! - Nothing a refused statement touched survives the transaction.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

const PASSWORD: &str = "col-tx-rls-secret-42";

/// The least privilege that can run the DML under test, so a denial is the
/// policy's doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

/// A columnar collection seeded with one row owned by `user` and one owned by
/// `alice`, a user to authenticate as, and a write policy scoping writes to the
/// authenticated principal.
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

/// The server's error message for a failed statement, read off the attached
/// `DbError` — the `tokio_postgres::Error` wrapper's `Display` is the fixed
/// string "db error", so asserting on it would make every refusal
/// indistinguishable from every other failure.
fn db_message(error: tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string())
}

fn rows_of(messages: &[SimpleQueryMessage], column: &str) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(column).map(str::to_string),
            _ => None,
        })
        .collect()
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

/// The post-image an in-transaction `UPDATE` produces is staged at the
/// statement, so the policy refuses the statement itself. Pre-fix the staging
/// path dropped the compiled predicate entirely: the `UPDATE` reported
/// `{"affected": 1}`, the transaction's own scan saw the refused row, and only
/// COMMIT failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_transaction_violating_update_is_refused_at_the_statement() {
    let server = TestServer::start().await;
    let user = "col_tx_rls_upd_user";
    seed(&server, "col_tx_rls_upd", user).await;

    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the policed user");
    client.simple_query("BEGIN").await.expect("begin");

    let error = client
        .simple_query("UPDATE col_tx_rls_upd SET owner = 'alice' WHERE id = 'r_mine'")
        .await
        .expect_err("the staged post-image violates the policy, so the STATEMENT must fail");
    let message = db_message(error);
    assert!(
        message.contains("RLS"),
        "the statement must be refused by the RLS policy, got: {message}"
    );

    client.simple_query("ROLLBACK").await.expect("rollback");
    drop(client);
    handle.abort();

    let rows = stored(&server, "col_tx_rls_upd").await;
    let mine = rows
        .iter()
        .find(|row| row[0] == "r_mine")
        .unwrap_or_else(|| panic!("the owned row must still exist: {rows:?}"));
    assert_eq!(
        (mine[1].as_str(), mine[2].as_str()),
        (user, "before"),
        "no part of the refused statement may survive: {rows:?}"
    );
}

/// A `DELETE` is decided against the row it removes, and that decision also
/// happens at the statement rather than at COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_transaction_violating_delete_is_refused_at_the_statement() {
    let server = TestServer::start().await;
    let user = "col_tx_rls_del_user";
    seed(&server, "col_tx_rls_del", user).await;

    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the policed user");
    client.simple_query("BEGIN").await.expect("begin");

    let error = client
        .simple_query("DELETE FROM col_tx_rls_del WHERE id = 'r_theirs'")
        .await
        .expect_err("deleting a row outside the policy must fail at the STATEMENT");
    let message = db_message(error);
    assert!(
        message.contains("RLS"),
        "the statement must be refused by the RLS policy, got: {message}"
    );

    client.simple_query("ROLLBACK").await.expect("rollback");
    drop(client);
    handle.abort();

    assert_eq!(
        stored(&server, "col_tx_rls_del").await.len(),
        2,
        "the excluded row must survive the refused in-transaction delete"
    );
}

/// The gate must not become a blanket in-transaction write ban: a conforming
/// update still stages, still reports its affected count at the statement, and
/// is still visible to the transaction's own read before COMMIT.
///
/// This is also what makes the refusals above meaningful — staged writes ARE
/// visible to same-transaction reads, so a refused row that had been staged
/// would have been readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conforming_in_transaction_update_stages_and_is_visible() {
    let server = TestServer::start().await;
    let user = "col_tx_rls_ok_user";
    seed(&server, "col_tx_rls_ok", user).await;

    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the policed user");
    client.simple_query("BEGIN").await.expect("begin");

    client
        .simple_query("UPDATE col_tx_rls_ok SET note = 'after' WHERE id = 'r_mine'")
        .await
        .expect("an update whose post-image satisfies the policy must stage");

    let seen = client
        .simple_query("SELECT note FROM col_tx_rls_ok WHERE id = 'r_mine'")
        .await
        .expect("read-your-own-writes inside the transaction");
    assert_eq!(
        rows_of(&seen, "note"),
        vec!["after"],
        "the staged post-image must be visible to the transaction's own read"
    );

    client.simple_query("COMMIT").await.expect("commit");
    drop(client);
    handle.abort();

    let rows = stored(&server, "col_tx_rls_ok").await;
    let mine = rows
        .iter()
        .find(|row| row[0] == "r_mine")
        .unwrap_or_else(|| panic!("the owned row must still exist: {rows:?}"));
    assert_eq!(mine[2], "after", "the committed update must be durable");
}
