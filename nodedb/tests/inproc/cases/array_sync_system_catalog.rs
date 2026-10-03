// SPDX-License-Identifier: BUSL-1.1

//! A synced Array schema is visible in the system catalog.
//!
//! When an Array (`CREATE ARRAY`) collection's schema is synced onto a node,
//! `OriginArrayInbound::handle_schema` proposes it through the metadata
//! group. Its apply registers an `ArrayCatalogEntry`, so the array is visible
//! to `array_catalog` and, through it, to `SHOW COLLECTIONS`.
//!
//! This test drives `handle_schema` exactly as the WebSocket listener would
//! on a one-node cluster, then asserts the array is visible via
//! `SHOW COLLECTIONS`.

use std::sync::Arc;

use nodedb::control::array_sync::{OriginApplyEngine, OriginArrayInbound};
use nodedb::control::security::identity::AuthenticatedIdentity;
use nodedb::control::server::shared::ddl::neutral::collection::show_collections;
use nodedb::control::server::shared::ddl::result::DdlResult;
use nodedb_test_support::array_sync::build_schema_snapshot;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::DatabaseId;

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
async fn synced_array_schema_is_visible_in_system_catalog() {
    // A full one-node cluster: `handle_schema` proposes the array through its
    // metadata group, and the catalog entry opens the array on its
    // Data-Plane cores.
    let server = TestServer::start().await;
    let shared = Arc::clone(&server.shared);

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
        .expect("schema handling on a one-node cluster must succeed");

    // The array_catalog mirror and the durable catalog carry the entry, so the
    // Data Plane can open the array.
    {
        let cat = shared.array_catalog.read().expect("array_catalog lock");
        assert!(
            cat.all_entries().iter().any(|e| e.name == array_name),
            "array_catalog must be registered once the schema entry applies"
        );
    }
    assert!(
        shared
            .credentials
            .catalog()
            .load_all_arrays()
            .expect("catalog read")
            .iter()
            .any(|e| e.name == array_name),
        "the synced array's catalog row must be durable"
    );

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
