// SPDX-License-Identifier: BUSL-1.1

//! A permission tree lives in the database its DDL ran in.
//!
//! Both the default database and a named database hold a collection
//! `pt_docs` with rows `d1` and `d2`. `ALTER COLLECTION ... SET
//! PERMISSION_TREE` runs in the named database. The tree must govern that
//! database's `pt_docs` only: a probe there sees the granted row, and a probe
//! in the default database sees every row. `DROP PERMISSION_TREE` in the named
//! database lifts the filter there.

use crate::harness::TestServer;

use super::permission_tree_support::{PROBE_PASSWORD, PROBE_USER, SELECT_DOCS, select_ids};

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Open a connection as the probe user on `database`.
async fn connect_probe_to(
    server: &TestServer,
    database: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .connect_as_database(PROBE_USER, PROBE_PASSWORD, database)
        .await
        .unwrap_or_else(|e| panic!("connect as {PROBE_USER} on {database}: {e}"))
}

async fn create_docs(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION pt_docs (id TEXT PRIMARY KEY, title TEXT) \
         WITH (engine='document_strict')",
        "INSERT INTO pt_docs (id, title) VALUES ('d1', 'Doc One')",
        "INSERT INTO pt_docs (id, title) VALUES ('d2', 'Doc Two')",
    ] {
        exec(server, sql).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_tree_ddl_acts_on_the_session_database() {
    let (server, db) = TestServer::with_database("pt_scope_db").await;

    // The default database's same-name collection, which carries no tree.
    exec(&server, "USE DATABASE default").await;
    create_docs(&server).await;

    exec(&server, &format!("USE DATABASE {db}")).await;
    create_docs(&server).await;
    for sql in [
        "CREATE COLLECTION pt_grants",
        "ALTER COLLECTION pt_docs SET PERMISSION_TREE = '{\
            \"resource_column\":\"id\",\
            \"graph_index\":\"pt_docs_tree\",\
            \"permission_table\":\"pt_grants\"\
         }'",
        "CREATE ROLE pt_role",
        "CREATE USER pt_probe PASSWORD 'pt-probe-password-7'",
        "GRANT ROLE readwrite TO pt_probe",
        "GRANT ROLE pt_role TO pt_probe",
        "INSERT INTO pt_grants (resource_id, grantee, level, inherited) \
         VALUES ('d1', 'pt_role', 'viewer', false)",
    ] {
        exec(&server, sql).await;
    }
    exec(
        &server,
        &format!("GRANT ALL ON DATABASE {db} TO {PROBE_USER}"),
    )
    .await;

    let (in_db, in_db_handle) = connect_probe_to(&server, &db).await;
    assert_eq!(
        select_ids(&in_db, SELECT_DOCS).await,
        vec!["d1".to_string()],
        "the tree set in the named database must filter that database's collection"
    );

    let (in_default, in_default_handle) = connect_probe_to(&server, "default").await;
    assert_eq!(
        select_ids(&in_default, SELECT_DOCS).await,
        vec!["d1".to_string(), "d2".to_string()],
        "the default database's same-name collection must carry no tree"
    );

    exec(&server, "ALTER COLLECTION pt_docs DROP PERMISSION_TREE").await;
    assert_eq!(
        select_ids(
            &in_db,
            "SELECT id FROM pt_docs WHERE 'dropped' = 'dropped' ORDER BY id"
        )
        .await,
        vec!["d1".to_string(), "d2".to_string()],
        "dropping the tree in the named database must lift its filter"
    );

    drop(in_db);
    drop(in_default);
    in_db_handle.abort();
    in_default_handle.abort();
}
