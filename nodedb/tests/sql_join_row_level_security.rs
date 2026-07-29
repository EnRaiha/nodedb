// SPDX-License-Identifier: BUSL-1.1

//! Row-level security across a join.
//!
//! A join reads two collections. Both sides are reads on the caller's behalf,
//! so both carry the caller's read policies — whether the side arrives from a
//! resolved child plan or is scanned locally by the join handler.
//!
//! The filters must apply per side *before* the join, not after it. A row the
//! policy excludes must neither match a partner nor survive as a null-extended
//! outer row, and a post-join predicate cannot express either.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "probe-secret-99";

/// Two collections joined on `owner_id`, seeded so that every row belongs to
/// `alice` — nothing the probing principal may read.
async fn seed(server: &TestServer, left: &str, right: &str, user: &str) {
    for collection in [left, right] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (\
                     id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {collection}: {e}"));
        for i in 1..=3 {
            server
                .exec(&format!(
                    "INSERT INTO {collection} (id, owner, note) \
                     VALUES ('k{i}', 'alice', 'secret-{i}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("seed {collection} row {i}: {e}"));
        }
    }
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Run `sql` as `user` and return the rows it sees.
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

/// An inner join must not surface rows the read policy excludes on either side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inner_join_excludes_policy_filtered_rows() {
    let server = TestServer::start().await;
    seed(&server, "join_rls_left", "join_rls_right", "join_rls_user").await;
    server
        .exec(
            "CREATE RLS POLICY left_owner ON join_rls_left FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create left policy");

    let rows = rows_as(
        &server,
        "join_rls_user",
        "SELECT l.note FROM join_rls_left l \
         JOIN join_rls_right r ON l.id = r.id",
    )
    .await;

    assert!(
        rows.is_empty(),
        "join surfaced rows the read policy excludes: {rows:?}"
    );
}

/// A policy on the right side alone must still exclude its rows — the join is
/// filtered per side, not only on whichever side the planner drives from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_applies_policy_on_the_probe_side() {
    let server = TestServer::start().await;
    seed(
        &server,
        "join_rls_probe_l",
        "join_rls_probe_r",
        "join_probe_user",
    )
    .await;
    server
        .exec(
            "CREATE RLS POLICY right_owner ON join_rls_probe_r FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create right policy");

    let rows = rows_as(
        &server,
        "join_probe_user",
        "SELECT r.note FROM join_rls_probe_l l \
         JOIN join_rls_probe_r r ON l.id = r.id",
    )
    .await;

    assert!(
        rows.is_empty(),
        "join surfaced probe-side rows the read policy excludes: {rows:?}"
    );
}

/// A LEFT JOIN must not leak excluded rows as null-extended output either: the
/// policy removes the row from the driving side entirely, so it produces no
/// output row at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn left_join_does_not_null_extend_excluded_rows() {
    let server = TestServer::start().await;
    seed(
        &server,
        "join_rls_outer_l",
        "join_rls_outer_r",
        "join_outer_user",
    )
    .await;
    server
        .exec(
            "CREATE RLS POLICY outer_owner ON join_rls_outer_l FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create outer policy");

    let rows = rows_as(
        &server,
        "join_outer_user",
        "SELECT l.id FROM join_rls_outer_l l \
         LEFT JOIN join_rls_outer_r r ON l.id = r.id",
    )
    .await;

    assert!(
        rows.is_empty(),
        "LEFT JOIN null-extended rows the read policy excludes: {rows:?}"
    );
}

/// The policy filters rather than blanket-denying: a principal the policy
/// admits still sees its own rows through the join.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_returns_rows_the_policy_admits() {
    let server = TestServer::start().await;
    seed(
        &server,
        "join_rls_ok_l",
        "join_rls_ok_r",
        "join_rls_ok_user",
    )
    .await;
    // Rows owned by the probing principal, alongside the seeded `alice` rows.
    for collection in ["join_rls_ok_l", "join_rls_ok_r"] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, note) \
                 VALUES ('mine', 'join_rls_ok_user', 'visible')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed own row in {collection}: {e}"));
    }
    server
        .exec(
            "CREATE RLS POLICY ok_owner ON join_rls_ok_l FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let rows = rows_as(
        &server,
        "join_rls_ok_user",
        "SELECT l.note FROM join_rls_ok_l l \
         JOIN join_rls_ok_r r ON l.id = r.id",
    )
    .await;

    assert_eq!(
        rows,
        vec!["visible".to_string()],
        "the policy should admit the principal's own row and exclude the rest"
    );
}
