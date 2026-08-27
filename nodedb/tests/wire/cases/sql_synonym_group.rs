// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the `CREATE SYNONYM GROUP` / `DROP SYNONYM GROUP` /
//! `SHOW SYNONYM GROUPS` DDL surface and end-to-end FTS query expansion.
//!
//! Scenarios covered:
//! 1. Create a synonym group and verify SHOW lists it.
//! 2. Drop a synonym group (unconditional) — SHOW no longer lists it.
//! 3. DROP IF EXISTS on a non-existent group — no error.
//! 4. Duplicate CREATE — returns an error.
//! 5. Query-time synonym expansion: querying one term matches documents
//!    that contain a synonym term.
//! 6. Drop removes expansion — post-drop query no longer expands.
//! 7. Restart durability: groups survive server restart.

use crate::harness::TestServer;

/// Create a synonym group and verify SHOW SYNONYM GROUPS reflects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_show_synonym_group() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP colors AS ('red', 'crimson', 'scarlet')")
        .await
        .expect("create synonym group");

    let rows = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show synonym groups");

    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names.contains(&"colors"),
        "colors group must appear in SHOW SYNONYM GROUPS: {names:?}"
    );

    let colors_row = rows.iter().find(|r| r[0] == "colors").unwrap();
    let terms_str = &colors_row[1];
    assert!(
        terms_str.contains("red"),
        "terms must contain 'red': {terms_str}"
    );
    assert!(
        terms_str.contains("crimson"),
        "terms must contain 'crimson': {terms_str}"
    );
    assert!(
        terms_str.contains("scarlet"),
        "terms must contain 'scarlet': {terms_str}"
    );
}

/// Drop a synonym group and verify SHOW no longer lists it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_synonym_group_removes_from_show() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP mammals AS ('cat', 'feline', 'kitty')")
        .await
        .expect("create synonym group");

    srv.exec("DROP SYNONYM GROUP mammals")
        .await
        .expect("drop synonym group");

    let rows = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show synonym groups");

    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        !names.contains(&"mammals"),
        "mammals group must be absent after drop: {names:?}"
    );
}

/// DROP IF EXISTS on a group that does not exist must succeed (no error).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_if_exists_nonexistent_group_is_ok() {
    let srv = TestServer::start().await;

    srv.exec("DROP SYNONYM GROUP IF EXISTS nonexistent_group_xyz")
        .await
        .expect("DROP IF EXISTS on nonexistent group must not error");
}

/// Creating a synonym group with a name that already exists must return an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_create_synonym_group_errors() {
    let srv = TestServer::start().await;

    srv.exec("CREATE SYNONYM GROUP dupes AS ('alpha', 'beta')")
        .await
        .expect("first create must succeed");

    let result = srv
        .exec("CREATE SYNONYM GROUP dupes AS ('gamma', 'delta')")
        .await;

    assert!(
        result.is_err(),
        "second CREATE with same name must return an error"
    );
}

/// Query-time synonym expansion: document containing a synonym term is
/// returned when the query uses a different term in the same group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fts_query_expands_synonym_terms() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION syn_docs WITH (engine='document_schemaless')")
        .await
        .expect("create collection");

    // Insert documents using one term from each synonym pair.
    srv.exec("INSERT INTO syn_docs { id: 'a1', body: 'automobile engine repair' }")
        .await
        .expect("insert a1");
    srv.exec("INSERT INTO syn_docs { id: 'a2', body: 'car maintenance guide' }")
        .await
        .expect("insert a2");
    srv.exec("INSERT INTO syn_docs { id: 'a3', body: 'cooking recipe for pasta' }")
        .await
        .expect("insert a3");

    // Before adding synonym group: querying 'automobile' should NOT match 'car'.
    let rows_before = srv
        .query_rows("SELECT id FROM syn_docs WHERE text_match(body, 'automobile') ORDER BY id")
        .await
        .expect("query before synonyms");
    let ids_before: Vec<&str> = rows_before.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_before.contains(&"a1"),
        "a1 must match 'automobile' before synonyms: {ids_before:?}"
    );
    assert!(
        !ids_before.contains(&"a2"),
        "a2 must NOT match 'automobile' before synonyms: {ids_before:?}"
    );

    // Add synonym group: automobile = car.
    srv.exec("CREATE SYNONYM GROUP vehicles AS ('automobile', 'car')")
        .await
        .expect("create synonym group");

    // After adding synonym group: querying 'automobile' should also match 'car'.
    let rows_after = srv
        .query_rows("SELECT id FROM syn_docs WHERE text_match(body, 'automobile') ORDER BY id")
        .await
        .expect("query after synonyms");
    let ids_after: Vec<&str> = rows_after.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_after.contains(&"a1"),
        "a1 must still match 'automobile': {ids_after:?}"
    );
    assert!(
        ids_after.contains(&"a2"),
        "a2 must now match via synonym expansion: {ids_after:?}"
    );
    assert!(
        !ids_after.contains(&"a3"),
        "a3 (unrelated) must not match: {ids_after:?}"
    );
}

/// Drop removes expansion: after dropping a synonym group, querying one term
/// no longer returns documents that only contain a synonym term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fts_expansion_disabled_after_drop() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION syn_drop_docs WITH (engine='document_schemaless')")
        .await
        .expect("create collection");

    srv.exec("INSERT INTO syn_drop_docs { id: 'b1', body: 'physician visits hospital' }")
        .await
        .expect("insert b1");
    srv.exec("INSERT INTO syn_drop_docs { id: 'b2', body: 'doctor appointments clinic' }")
        .await
        .expect("insert b2");

    srv.exec("CREATE SYNONYM GROUP medical AS ('physician', 'doctor')")
        .await
        .expect("create synonym group");

    // Confirm expansion works.
    let rows_with = srv
        .query_rows("SELECT id FROM syn_drop_docs WHERE text_match(body, 'physician') ORDER BY id")
        .await
        .expect("query with synonyms");
    let ids_with: Vec<&str> = rows_with.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_with.contains(&"b2"),
        "b2 must match via synonym before drop: {ids_with:?}"
    );

    // Drop synonym group.
    srv.exec("DROP SYNONYM GROUP medical")
        .await
        .expect("drop synonym group");

    // After drop, expansion must no longer work.
    let rows_without = srv
        .query_rows("SELECT id FROM syn_drop_docs WHERE text_match(body, 'physician') ORDER BY id")
        .await
        .expect("query without synonyms");
    let ids_without: Vec<&str> = rows_without.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_without.contains(&"b1"),
        "b1 must still match 'physician': {ids_without:?}"
    );
    assert!(
        !ids_without.contains(&"b2"),
        "b2 must NOT match after drop (no expansion): {ids_without:?}"
    );
}

/// Restart durability: synonym groups survive a server restart and expansion
/// still works after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synonym_group_survives_restart() {
    let srv = TestServer::start().await;

    srv.exec("CREATE COLLECTION durable_fts WITH (engine='document_schemaless')")
        .await
        .expect("create collection");

    srv.exec("INSERT INTO durable_fts { id: 'c1', body: 'laptop computer hardware' }")
        .await
        .expect("insert c1");
    srv.exec("INSERT INTO durable_fts { id: 'c2', body: 'notebook computing device' }")
        .await
        .expect("insert c2");

    srv.exec("CREATE SYNONYM GROUP computing AS ('laptop', 'notebook')")
        .await
        .expect("create synonym group");

    // Verify it's there before restart.
    let rows_pre = srv
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show pre-restart");
    let names_pre: Vec<&str> = rows_pre.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names_pre.contains(&"computing"),
        "computing must be listed pre-restart: {names_pre:?}"
    );

    // Capture data dir and restart.
    let (srv, data_dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _data_dir) = TestServer::open_on_path(data_dir).await;

    // Verify group still listed after restart.
    let rows_post = srv2
        .query_rows("SHOW SYNONYM GROUPS")
        .await
        .expect("show post-restart");
    let names_post: Vec<&str> = rows_post.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names_post.contains(&"computing"),
        "computing must survive restart: {names_post:?}"
    );

    // Verify expansion still works after restart.
    let rows_exp = srv2
        .query_rows("SELECT id FROM durable_fts WHERE text_match(body, 'laptop') ORDER BY id")
        .await
        .expect("fts query post-restart");
    let ids_exp: Vec<&str> = rows_exp.iter().map(|r| r[0].as_str()).collect();
    assert!(
        ids_exp.contains(&"c1"),
        "c1 must match 'laptop': {ids_exp:?}"
    );
    assert!(
        ids_exp.contains(&"c2"),
        "c2 must match via synonym after restart: {ids_exp:?}"
    );
}
