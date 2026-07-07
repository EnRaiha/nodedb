// SPDX-License-Identifier: BUSL-1.1

//! Background materializer sweep integration test.
//!
//! Clones a small KV database, fires one manual sweep tick, and verifies
//! every cloned collection transitions to `Materialized` and every source
//! row is readable through the (now self-contained) clone.

mod common;

use std::sync::atomic::AtomicBool;

use common::pgwire_harness::TestServer;
use nodedb::control::maintenance::clone_materializer::run_scheduled_sweep;
use nodedb_types::CloneStatus;

#[tokio::test(flavor = "multi_thread")]
async fn background_sweep_materializes_clone_without_ddl() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE bg_sweep_src")
        .await
        .expect("CREATE DATABASE bg_sweep_src");
    client
        .simple_query("USE DATABASE bg_sweep_src")
        .await
        .expect("USE bg_sweep_src");
    client
        .simple_query(
            "CREATE COLLECTION records \
             (key STRING PRIMARY KEY, val STRING) \
             WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION records");
    for i in 0..50u32 {
        client
            .simple_query(&format!(
                "INSERT INTO records (key, val) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT k{i}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE bg_sweep_clone FROM bg_sweep_src")
        .await
        .expect("CLONE DATABASE");

    // The sweep dispatches through SPSC and uses `Handle::block_on`
    // internally, so it must run via `spawn_blocking`, mirroring the
    // production background loop.
    let shared = server.shared.clone();
    tokio::task::spawn_blocking(move || {
        let cancel = AtomicBool::new(false);
        let catalog = shared.credentials.catalog();
        run_scheduled_sweep(&shared, catalog, &cancel)
    })
    .await
    .expect("spawn_blocking join")
    .expect("run_scheduled_sweep must succeed");

    let catalog = server.shared.credentials.catalog();
    let db_id = catalog
        .get_database_id_by_name("bg_sweep_clone")
        .expect("catalog lookup")
        .expect("bg_sweep_clone must exist");
    let colls = catalog
        .load_all_collections(db_id)
        .expect("load collections");
    assert!(
        !colls.is_empty(),
        "bg_sweep_clone must have collections after sweep"
    );
    for coll in &colls {
        assert!(
            matches!(coll.clone_status, CloneStatus::Materialized),
            "collection '{}' must be Materialized after sweep, got {:?}",
            coll.name,
            coll.clone_status
        );
    }

    client
        .simple_query("USE DATABASE bg_sweep_clone")
        .await
        .expect("USE bg_sweep_clone");
    let rows = client
        .simple_query("SELECT key FROM records")
        .await
        .expect("SELECT from bg_sweep_clone");
    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 50, "all 50 source rows must be readable post-sweep");
}
