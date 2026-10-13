// SPDX-License-Identifier: BUSL-1.1

//! The source of `INSERT ... SELECT` is read as the statement's identity.
//!
//! A read policy on the source narrows the copy to the rows a SELECT by the
//! same identity returns. A redaction rule on the source refuses the copy,
//! since the copied values never pass the masking hook.

use crate::harness::TestServer;

const PASSWORD: &str = "insert-select-policy-9";

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Run `sql` as `user`, returning `Err((sqlstate, message))` on refusal.
async fn exec_as(server: &TestServer, user: &str, sql: &str) -> Result<(), (String, String)> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    result.map(|_| ()).map_err(|e| {
        e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_read_policy_narrows_the_copy() {
    let server = TestServer::start().await;
    let user = "isp_reader";
    server
        .exec("CREATE COLLECTION isp_src (id TEXT PRIMARY KEY, owner TEXT, n INT)")
        .await
        .expect("create source");
    server
        .exec(&format!(
            "INSERT INTO isp_src (id, owner, n) VALUES \
             ('mine', '{user}', 1), ('theirs', 'someone_else', 2)"
        ))
        .await
        .expect("seed source");
    server
        .exec("CREATE COLLECTION isp_dst")
        .await
        .expect("create target");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY isp_src_owner ON isp_src FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    exec_as(
        &server,
        user,
        "INSERT INTO isp_dst (id, n) SELECT id, n FROM isp_src",
    )
    .await
    .unwrap_or_else(|(code, message)| panic!("copy as {user}: {code} {message}"));

    let copied = server
        .query_text("SELECT id FROM isp_dst ORDER BY id")
        .await
        .expect("read target");
    assert_eq!(
        copied,
        vec!["mine".to_string()],
        "the copy must hold only the rows the read policy admits"
    );
}

/// A read policy on a key-value source narrows the copy too: the policy rides
/// the source's own read plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_source_read_policy_narrows_the_copy() {
    let server = TestServer::start().await;
    let user = "isp_kv_reader";
    server
        .exec("CREATE COLLECTION isp_kv (key TEXT PRIMARY KEY, owner TEXT) WITH (engine='kv')")
        .await
        .expect("create kv source");
    server
        .exec(&format!(
            "INSERT INTO isp_kv (key, owner) VALUES ('k1', '{user}'), ('k2', 'someone_else')"
        ))
        .await
        .expect("seed kv source");
    server
        .exec("CREATE COLLECTION isp_kv_dst")
        .await
        .expect("create target");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE RLS POLICY isp_kv_owner ON isp_kv FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create read policy");

    exec_as(
        &server,
        user,
        "INSERT INTO isp_kv_dst (id) SELECT key FROM isp_kv",
    )
    .await
    .unwrap_or_else(|(code, message)| panic!("copy as {user}: {code} {message}"));

    let copied = server
        .query_text("SELECT id FROM isp_kv_dst ORDER BY id")
        .await
        .expect("read target");
    assert_eq!(copied, vec!["k1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redacted_source_is_refused() {
    let server = TestServer::start().await;
    let user = "isp_redacted";
    server
        .exec("CREATE COLLECTION isp_secret (id TEXT PRIMARY KEY, ssn TEXT)")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO isp_secret (id, ssn) VALUES ('r1', '123-45-6789')")
        .await
        .expect("seed source");
    server
        .exec("CREATE COLLECTION isp_leak")
        .await
        .expect("create target");
    create_user(&server, user).await;
    server
        .exec(
            "CREATE REDACTION POLICY isp_mask ON isp_secret FOR ROLE readwrite \
             (ssn MASK '***-**-****')",
        )
        .await
        .expect("create redaction policy");

    let refused = exec_as(
        &server,
        user,
        "INSERT INTO isp_leak (id, ssn) SELECT id, ssn FROM isp_secret",
    )
    .await;
    match refused {
        Err((_, message)) => assert!(
            message.contains("isp_secret") && message.contains("redaction"),
            "the refusal must name the source and the redaction rule: {message}"
        ),
        Ok(()) => panic!("a copy out of a redacted source must be refused"),
    }
    let leaked = server
        .query_text("SELECT ssn FROM isp_leak")
        .await
        .expect("read target");
    assert!(leaked.is_empty(), "a refused copy wrote {leaked:?}");
}
