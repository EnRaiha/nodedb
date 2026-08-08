// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over DML `RETURNING` output.
//!
//! A `RETURNING` clause is a read. PostgreSQL gates that output on the SELECT
//! policy, so `UPDATE ... RETURNING *` can never surface a row that
//! `SELECT * FROM t` would hide from the same principal.
//!
//! The write is unaffected: the statement still writes every matched row and
//! still counts every row it wrote. Only the rows shipped back shrink. The
//! filter must also be evaluated on the full stored row rather than on the
//! projected one, because a policy routinely predicates on a column the
//! `RETURNING` list never mentions.

mod common;

use common::pgwire_harness::TestServer;

const PASSWORD: &str = "returning-secret-71";

/// The role the probing user holds, which is what a redaction policy's
/// `FOR ROLE` binds against.
const ROLE: &str = "readwrite";

/// Create `collection` with an `owner`-keyed read policy and a probing user
/// that owns exactly one of its two rows.
///
/// `r_hidden` belongs to `alice`, `r_visible` belongs to `user`, so any
/// statement touching both rows writes twice and may show back only one.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                 id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, owner) in [("r_hidden", "alice"), ("r_visible", user)] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, note) \
                 VALUES ('{id}', '{owner}', 'before')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
    }
    create_user(server, user).await;
    server
        .exec(&format!(
            "CREATE RLS POLICY {collection}_owner ON {collection} FOR READ \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create read policy on {collection}: {e}"));
}

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

/// The row count `user`'s statement reports in its command tag.
async fn affected_as(server: &TestServer, user: &str, sql: &str) -> u64 {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{user} runs {sql}: {e}"));
    let mut affected = None;
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = message {
            affected = Some(n);
        }
    }
    drop(client);
    handle.abort();
    affected.unwrap_or_else(|| panic!("{user}'s statement reported no command tag: {sql}"))
}

/// `UPDATE ... RETURNING` shows only the rows the read policy admits, while
/// still writing — and counting — the rows it hides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_returning_hides_rows_the_read_policy_excludes() {
    let server = TestServer::start().await;
    seed(&server, "ret_rls_update", "ret_rls_update_user").await;

    let rows = rows_as(
        &server,
        "ret_rls_update_user",
        "UPDATE ret_rls_update SET note = 'touched' RETURNING id, note",
    )
    .await;

    assert_eq!(
        rows,
        vec!["r_visible|touched".to_string()],
        "only the row the read policy admits may be returned"
    );

    // The hidden row was still written — read it back as the superuser, who
    // holds no restricting policy.
    let stored = server
        .query_rows("SELECT id, note FROM ret_rls_update ORDER BY id")
        .await
        .expect("read back as superuser");
    assert_eq!(
        stored,
        vec![
            vec!["r_hidden".to_string(), "touched".to_string()],
            vec!["r_visible".to_string(), "touched".to_string()],
        ],
        "the write must reach every matched row, hidden or not: {stored:?}"
    );

    // …and the write still counts what it wrote, not what it showed.
    let affected = affected_as(
        &server,
        "ret_rls_update_user",
        "UPDATE ret_rls_update SET note = 'again'",
    )
    .await;
    assert_eq!(affected, 2, "the update must count both rows it wrote");
}

/// A policy predicating on a column absent from the `RETURNING` list still
/// filters. Testing the projected row instead of the stored one would leave
/// the predicate with nothing to evaluate and leak the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_list_without_the_policy_column_still_filters() {
    let server = TestServer::start().await;
    seed(&server, "ret_rls_proj", "ret_rls_proj_user").await;

    // `owner` is the policy's column and appears nowhere in the projection.
    let rows = rows_as(
        &server,
        "ret_rls_proj_user",
        "UPDATE ret_rls_proj SET note = 'touched' RETURNING note",
    )
    .await;

    assert_eq!(
        rows,
        vec!["touched".to_string()],
        "the policy must be evaluated against the stored row, not the projection"
    );
}

/// `DELETE ... RETURNING` shows only the admitted row while removing both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_returning_hides_rows_the_read_policy_excludes() {
    let server = TestServer::start().await;
    seed(&server, "ret_rls_delete", "ret_rls_delete_user").await;

    let rows = rows_as(
        &server,
        "ret_rls_delete_user",
        "DELETE FROM ret_rls_delete RETURNING id",
    )
    .await;

    assert_eq!(
        rows,
        vec!["r_visible".to_string()],
        "only the row the read policy admits may be returned"
    );

    let stored = server
        .query_rows("SELECT id FROM ret_rls_delete")
        .await
        .expect("read back as superuser");
    assert!(
        stored.is_empty(),
        "the delete must remove every matched row, hidden or not: {stored:?}"
    );
}

/// `MERGE ... RETURNING` gates its output on the TARGET's read policy: the
/// merge still updates and inserts every arm's row, but shows back only rows
/// the principal may read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_returning_hides_rows_the_read_policy_excludes() {
    let server = TestServer::start().await;
    let user = "ret_rls_merge_user";

    for name in ["ret_rls_merge_tgt", "ret_rls_merge_src"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (\
                     id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    // Target holds one row owned by `alice`; the source both updates it and
    // introduces a NOT-MATCHED row owned by the probing user.
    server
        .exec(
            "INSERT INTO ret_rls_merge_tgt (id, owner, note) \
             VALUES ('m_hidden', 'alice', 'before')",
        )
        .await
        .expect("seed merge target");
    for (id, owner) in [("m_hidden", "alice"), ("m_visible", user)] {
        server
            .exec(&format!(
                "INSERT INTO ret_rls_merge_src (id, owner, note) \
                 VALUES ('{id}', '{owner}', 'merged')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed merge source {id}: {e}"));
    }
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY merge_tgt_owner ON ret_rls_merge_tgt FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy on merge target");

    let rows = rows_as(
        &server,
        user,
        "MERGE INTO ret_rls_merge_tgt t USING ret_rls_merge_src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET note = s.note \
         WHEN NOT MATCHED THEN INSERT (id, owner, note) VALUES (s.id, s.owner, s.note) \
         RETURNING id, note",
    )
    .await;

    assert_eq!(
        rows,
        vec!["m_visible|merged".to_string()],
        "only the target row the read policy admits may be returned"
    );

    let stored = server
        .query_rows("SELECT id, note FROM ret_rls_merge_tgt ORDER BY id")
        .await
        .expect("read back as superuser");
    assert_eq!(
        stored,
        vec![
            vec!["m_hidden".to_string(), "merged".to_string()],
            vec!["m_visible".to_string(), "merged".to_string()],
        ],
        "both the matched update and the not-matched insert must land: {stored:?}"
    );
}

/// Row filtering and column redaction compose: the policy decides which rows
/// survive, redaction masks the columns of whichever rows do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redaction_still_masks_the_rows_that_survive_filtering() {
    let server = TestServer::start().await;
    seed(&server, "ret_rls_redact", "ret_rls_redact_user").await;
    server
        .exec(&format!(
            "CREATE REDACTION POLICY mask_note ON ret_rls_redact FOR ROLE {ROLE} \
             (note MASK '***')"
        ))
        .await
        .expect("create redaction policy");

    let rows = rows_as(
        &server,
        "ret_rls_redact_user",
        "UPDATE ret_rls_redact SET note = 'touched' RETURNING id, note",
    )
    .await;

    assert_eq!(
        rows,
        vec!["r_visible|***".to_string()],
        "the surviving row must be returned with its ruled column masked"
    );
}

/// `INSERT ... RETURNING` is a read of the rows it wrote: the read policy must
/// filter what comes back while every row still lands.
///
/// The write is unaffected — no write policy exists on this collection, so the
/// principal may insert a row it will not be shown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_hides_rows_the_read_policy_excludes() {
    let server = TestServer::start().await;
    let user = "ret_rls_insert_user";
    server
        .exec(
            "CREATE COLLECTION ret_rls_insert (\
                 id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create ret_rls_insert");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY ret_rls_insert_owner ON ret_rls_insert FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy on ret_rls_insert");

    let rows = rows_as(
        &server,
        user,
        &format!(
            "INSERT INTO ret_rls_insert (id, owner, note) \
             VALUES ('i_hidden', 'alice', 'a'), ('i_visible', '{user}', 'b') \
             RETURNING id, note"
        ),
    )
    .await;

    assert_eq!(
        rows,
        vec!["i_visible|b".to_string()],
        "only the row the read policy admits may be returned"
    );

    // Both rows were written — read them back as the superuser, who holds no
    // restricting policy.
    let stored = server
        .query_rows("SELECT id FROM ret_rls_insert ORDER BY id")
        .await
        .expect("read back as superuser");
    assert_eq!(
        stored,
        vec![vec!["i_hidden".to_string()], vec!["i_visible".to_string()]],
        "the insert must write every row, hidden or not: {stored:?}"
    );
}

/// A read policy predicating on a column the `RETURNING` list omits still
/// filters an insert's output — the filter runs on the stored row, before
/// projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_filters_on_a_column_outside_the_projection() {
    let server = TestServer::start().await;
    let user = "ret_rls_insert_proj_user";
    server
        .exec(
            "CREATE COLLECTION ret_rls_insert_proj (\
                 id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create ret_rls_insert_proj");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY ret_rls_insert_proj_owner ON ret_rls_insert_proj FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy on ret_rls_insert_proj");

    let rows = rows_as(
        &server,
        user,
        &format!(
            "INSERT INTO ret_rls_insert_proj (id, owner, note) \
             VALUES ('p_hidden', 'alice', 'hidden'), ('p_visible', '{user}', 'shown') \
             RETURNING note"
        ),
    )
    .await;

    assert_eq!(
        rows,
        vec!["shown".to_string()],
        "the policy must be evaluated against the stored row, not the projection"
    );
}

/// The key-value engine goes through the same read gate: `INSERT ... RETURNING`
/// on a KV collection shows only the rows the read policy admits, while every
/// row still lands.
///
/// KV is asserted separately from the document engines because its row shape is
/// built by a different helper (`{key, value…}` rather than `{id, …}`), and the
/// filter has to be evaluated against that shape — a gate that silently matched
/// nothing on a KV row would return everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_hides_rows_the_read_policy_excludes() {
    let server = TestServer::start().await;
    let user = "ret_rls_kv_user";
    server
        .exec(
            "CREATE COLLECTION ret_rls_kv (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create ret_rls_kv");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY ret_rls_kv_owner ON ret_rls_kv FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy on ret_rls_kv");

    let rows = rows_as(
        &server,
        user,
        &format!(
            "INSERT INTO ret_rls_kv (key, owner, note) \
             VALUES ('k_hidden', 'alice', 'a'), ('k_visible', '{user}', 'b') \
             RETURNING key, note"
        ),
    )
    .await;

    assert_eq!(
        rows,
        vec!["k_visible|b".to_string()],
        "only the row the read policy admits may be returned"
    );

    // Both rows were written — read them back as the superuser, who holds no
    // restricting policy.
    let stored = server
        .query_rows("SELECT key FROM ret_rls_kv ORDER BY key")
        .await
        .expect("read back as superuser");
    assert_eq!(
        stored,
        vec![vec!["k_hidden".to_string()], vec!["k_visible".to_string()]],
        "the insert must write every row, hidden or not: {stored:?}"
    );
}
