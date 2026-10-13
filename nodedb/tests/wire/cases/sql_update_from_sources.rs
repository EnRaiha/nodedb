// SPDX-License-Identifier: BUSL-1.1

//! The `FROM` relation of `UPDATE ... FROM` is read like a SELECT.
//!
//! A key-value source joins through its own engine. A WHERE conjunct over
//! the source alone filters the source before the join. A conjunct that
//! mixes a source column with a target column outside the join is refused.

use crate::harness::TestServer;

async fn create_target(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (id, name) VALUES ('a', 'old_a'), ('b', 'old_b')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
}

async fn names(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT name FROM {name} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {name}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_value_source_updates_the_target() {
    let server = TestServer::start().await;
    create_target(&server, "ufs_kv_t").await;
    server
        .exec("CREATE COLLECTION ufs_kv (key TEXT PRIMARY KEY, label TEXT) WITH (engine='kv')")
        .await
        .expect("create kv source");
    server
        .exec("INSERT INTO ufs_kv (key, label) VALUES ('a', 'new_a')")
        .await
        .expect("seed kv source");

    server
        .exec(
            "UPDATE ufs_kv_t SET name = ufs_kv.label FROM ufs_kv \
             WHERE ufs_kv_t.id = ufs_kv.key",
        )
        .await
        .expect("update from a kv source");
    assert_eq!(names(&server, "ufs_kv_t").await, vec!["new_a", "old_b"]);
}

/// A WHERE conjunct over the source alone drops source rows before the
/// join, so a target row whose only source row it rejects keeps its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_only_predicate_filters_the_source() {
    let server = TestServer::start().await;
    create_target(&server, "ufs_where_t").await;
    server
        .exec("CREATE COLLECTION ufs_where (id TEXT PRIMARY KEY, label TEXT, ok BOOL)")
        .await
        .expect("create source");
    server
        .exec(
            "INSERT INTO ufs_where (id, label, ok) VALUES \
             ('a', 'new_a', true), ('b', 'new_b', false)",
        )
        .await
        .expect("seed source");

    server
        .exec(
            "UPDATE ufs_where_t SET name = ufs_where.label FROM ufs_where \
             WHERE ufs_where_t.id = ufs_where.id AND ufs_where.ok = true",
        )
        .await
        .expect("update with a source-only predicate");
    assert_eq!(names(&server, "ufs_where_t").await, vec!["new_a", "old_b"]);
}

/// A conjunct that compares a source column with a target column, other than
/// the join itself, is refused rather than evaluated against one side only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_predicate_is_refused() {
    let server = TestServer::start().await;
    create_target(&server, "ufs_mix_t").await;
    server
        .exec("CREATE COLLECTION ufs_mix (id TEXT PRIMARY KEY, label TEXT)")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO ufs_mix (id, label) VALUES ('a', 'old_a')")
        .await
        .expect("seed source");

    let refused = server
        .exec(
            "UPDATE ufs_mix_t SET name = 'changed' FROM ufs_mix \
             WHERE ufs_mix_t.id = ufs_mix.id AND ufs_mix.label = ufs_mix_t.name",
        )
        .await;
    assert!(refused.is_err(), "a mixed conjunct must be refused");
    assert_eq!(names(&server, "ufs_mix_t").await, vec!["old_a", "old_b"]);
}
