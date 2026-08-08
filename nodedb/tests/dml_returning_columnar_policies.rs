// SPDX-License-Identifier: BUSL-1.1

//! Row-level security and redaction over columnar `INSERT ... RETURNING`.
//!
//! A `RETURNING` clause is a read, so its output is gated on the SELECT policy:
//! an `INSERT ... RETURNING *` can never surface a row that `SELECT *` would
//! hide from the same principal. The write is unaffected — every row still
//! lands and is still counted; only the rows shipped back shrink.
//!
//! The two gates are independent and must not be conflated. `rls_write_check`
//! decides whether a row may be written; `rls_filters` decides what may be
//! shown back. These tests use a collection with a READ policy and no write
//! policy, which is exactly the case where treating one as the other would go
//! unnoticed: the write must stay unrestricted while the row set shrinks.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "columnar-returning-secret-33";

/// The role the probing user holds, which is what a redaction policy's
/// `FOR ROLE` binds against.
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

/// Create a columnar `collection` with an `owner`-keyed READ policy and a
/// probing user who will own only some of the rows it inserts.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                 id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    create_user(server, user).await;
    server
        .exec(&format!(
            "CREATE RLS POLICY {collection}_owner ON {collection} FOR READ \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create read policy on {collection}: {e}"));
}

/// The read policy filters the returned rows while every row still lands.
///
/// The statement inserts two rows, one owned by the caller and one by someone
/// else. Only the caller's row may come back — but both must be stored, which
/// the superuser `SELECT` at the end proves. A gate that refused the write, or
/// one that returned both rows, fails a different half of this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_policy_filters_returned_rows_while_every_row_still_lands() {
    let server = TestServer::start().await;
    let user = "col_ret_rls_user";
    seed(&server, "col_ret_rls", user).await;

    let returned = rows_as(
        &server,
        user,
        &format!(
            "INSERT INTO col_ret_rls (id, owner, note) VALUES \
             ('r_visible', '{user}', 'mine'), ('r_hidden', 'alice', 'theirs') \
             RETURNING id, owner"
        ),
    )
    .await;

    assert_eq!(
        returned,
        vec![format!("r_visible|{user}")],
        "only the row the read policy admits may be shown back: {returned:?}"
    );

    // The policy is FOR READ only, so the write was never restricted. Read as
    // the superuser, which no policy filters, to prove both rows are stored.
    let stored = server
        .query_rows("SELECT id FROM col_ret_rls ORDER BY id")
        .await
        .expect("superuser read")
        .into_iter()
        .map(|r| r.join("|"))
        .collect::<Vec<_>>();
    assert_eq!(
        stored,
        vec!["r_hidden".to_string(), "r_visible".to_string()],
        "both rows must have landed — the READ policy must not have gated the write: {stored:?}"
    );
}

/// Row filtering and column redaction compose: the policy decides which rows
/// survive, redaction masks the columns of whichever rows do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redaction_policy_masks_a_field_in_the_returned_rows() {
    let server = TestServer::start().await;
    let user = "col_ret_redact_user";
    seed(&server, "col_ret_redact", user).await;
    server
        .exec(&format!(
            "CREATE REDACTION POLICY col_mask_note ON col_ret_redact FOR ROLE {ROLE} \
             (note MASK '***')"
        ))
        .await
        .expect("create redaction policy");

    let returned = rows_as(
        &server,
        user,
        &format!(
            "INSERT INTO col_ret_redact (id, owner, note) \
             VALUES ('r_visible', '{user}', 'secret') RETURNING id, note"
        ),
    )
    .await;

    assert_eq!(
        returned,
        vec!["r_visible|***".to_string()],
        "the returned row must have its ruled column masked: {returned:?}"
    );

    // The stored value is untouched — redaction shapes the response, it does
    // not rewrite the row. Asserting this keeps a masking bug from being
    // mistaken for a write bug, and vice versa.
    let stored = server
        .query_rows("SELECT note FROM col_ret_redact WHERE id = 'r_visible'")
        .await
        .expect("superuser read")
        .into_iter()
        .map(|r| r.join("|"))
        .collect::<Vec<_>>();
    assert_eq!(
        stored,
        vec!["secret".to_string()],
        "redaction must not have altered what was stored: {stored:?}"
    );
}
