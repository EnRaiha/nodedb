// SPDX-License-Identifier: BUSL-1.1

//! Regression test: single-node, non-cluster.
//!
//! When an Array (`CREATE ARRAY`) collection's schema is synced onto a node
//! that has no Raft group configured, `OriginArrayInbound::handle_schema`
//! takes the direct-import branch (no `raft_proposer` installed). Before
//! this fix, that branch called `OriginSchemaRegistry::import_snapshot` and
//! returned — it never registered an `ArrayCatalogEntry`, so the array was
//! invisible to `array_catalog` and, transitively, to `SHOW COLLECTIONS`
//! (which — also before this fix — never consulted `array_catalog` at all).
//!
//! This test drives `handle_schema` exactly as the WebSocket listener would
//! on a single-node deployment, then asserts the array is visible via
//! `SHOW COLLECTIONS`. It fails on the pre-fix tree because (a) the
//! single-node branch never called the catalog-registration helper and
//! (b) `show_collections` never merged in `array_catalog` entries.

mod common;

use std::sync::Arc;

use common::array_sync::build_schema_snapshot;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::array_sync::{OriginApplyEngine, OriginArrayInbound};
use nodedb::control::security::identity::AuthenticatedIdentity;
use nodedb::control::server::shared::ddl::neutral::collection::show_collections;
use nodedb::control::server::shared::ddl::result::DdlResult;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;
use nodedb_types::DatabaseId;

fn build_test_state() -> Arc<SharedState> {
    let dir = tempfile::tempdir().expect("tmpdir");
    let wal_path = dir.path().join("test.wal");
    std::mem::forget(dir); // outlive SharedState, mirrors other test harnesses in this crate

    let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    SharedState::new(dispatcher, wal).unwrap()
}

fn superuser_identity() -> AuthenticatedIdentity {
    nodedb_test_support::pgwire_auth_helpers::superuser()
}

/// Extract the `name` column values from a `SHOW COLLECTIONS` result.
fn row_names(results: &[DdlResult]) -> Vec<String> {
    results
        .iter()
        .filter_map(|r| match r {
            DdlResult::Rows(shaped) => Some(shaped),
            _ => None,
        })
        .flat_map(|shaped| shaped.rows.iter())
        .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect()
}

#[tokio::test]
async fn synced_array_schema_is_visible_in_system_catalog_single_node() {
    let shared = build_test_state();
    // No `raft_proposer` installed => `handle_schema` takes the single-node
    // direct-import branch (mirrors an embedded / single-node deployment).
    assert!(
        shared.raft_proposer.get().is_none(),
        "test assumes single-node (no raft_proposer installed)"
    );

    let engine = Arc::new(OriginApplyEngine::new(
        Arc::clone(&shared.array_sync_schemas),
        Arc::clone(&shared.array_sync_op_log),
    ));
    let inbound = OriginArrayInbound::new(
        engine,
        Arc::clone(&shared.array_sync_schemas),
        Arc::clone(&shared),
        nodedb_test_support::pgwire_auth_helpers::superuser(),
    );

    let array_name = "genome_tiles";
    let (snapshot_payload, schema_hlc) = build_schema_snapshot(array_name);
    let mut schema_hlc_bytes = [0u8; 18];
    schema_hlc_bytes.copy_from_slice(&schema_hlc.to_bytes());

    let msg = nodedb_types::sync::wire::array::ArraySchemaSyncMsg {
        array: array_name.to_string(),
        replica_id: 1,
        snapshot_payload,
        schema_hlc_bytes,
    };

    inbound
        .handle_schema(&msg)
        .await
        .expect("single-node direct-import schema handling must succeed");

    // Sanity: the array_catalog itself must now carry the entry (this is the
    // Data-Plane-openability half of the bug).
    {
        let cat = shared.array_catalog.read().expect("array_catalog lock");
        assert!(
            cat.lookup_by_name(array_name).is_some(),
            "array_catalog must be registered by the single-node direct-import path"
        );
    }

    // The actual reported gap: SHOW COLLECTIONS must list the array.
    let identity = superuser_identity();
    let results =
        show_collections(&shared, &identity, DatabaseId::DEFAULT).expect("show_collections");
    let names = row_names(&results);
    assert!(
        names.contains(&array_name.to_string()),
        "synced Array collection '{array_name}' must be visible in SHOW COLLECTIONS \
         (system catalog introspection); got rows: {names:?}"
    );
}
