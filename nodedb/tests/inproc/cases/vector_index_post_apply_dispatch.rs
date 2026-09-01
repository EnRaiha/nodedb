// SPDX-License-Identifier: BUSL-1.1

//! A replicated vector-index entry must reach the Data Plane of every node
//! that applies it, not only the node that ran the statement.
//!
//! These tests drive the follower path directly — catalog apply, the
//! synchronous post-apply, then the async post-apply lane — and assert
//! through the engine's own dimension gate, which answers only once
//! `VectorOp::SetParams` has installed the declared width on the cores.
//! Asserting the catalog row instead would pass while the defect is live.

use std::sync::Arc;

use nodedb::control::catalog_entry::post_apply::{
    apply_post_apply_side_effects_sync, spawn_post_apply_async_side_effects,
};
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::StoredVectorIndexParams;

const DATABASE: u64 = 0;
const TENANT: u64 = 1;
const COLLECTION: &str = "follower_vec";
const FIELD: &str = "embedding";
const DECLARED_DIM: usize = 8;

fn params() -> StoredVectorIndexParams {
    StoredVectorIndexParams {
        database_id: DATABASE,
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        field_name: FIELD.to_string(),
        dim: DECLARED_DIM,
        metric: "cosine".to_string(),
        m: 32,
        ef_construction: 400,
        index_type: "hnsw".to_string(),
        pq_m: 0,
        ivf_cells: 0,
        ivf_nprobe: 0,
    }
}

/// Apply an entry the way a node applying a committed raft entry does:
/// durable write, synchronous install, then the async dispatch lane.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
    spawn_post_apply_async_side_effects(entry.clone(), Arc::clone(&server.shared), 0);
}

/// SQL for one row carrying an `embedding` array of `dim` components.
fn insert_sql(id: &str, dim: usize) -> String {
    let components: Vec<String> = (0..dim).map(|i| format!("{}.0", i + 1)).collect();
    format!(
        "INSERT INTO {COLLECTION} (id, {FIELD}) VALUES ('{id}', ARRAY[{}])",
        components.join(",")
    )
}

/// A `PutVectorIndexParams` applied without ever running the DDL must leave
/// this node enforcing the declared width, which only the Data Plane knows.
///
/// The pre-entry insert is what makes the post-entry rejection meaningful:
/// it shows the same statement is accepted while no index is registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_apply_installs_the_declared_dimension_on_this_node() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");

    server
        .exec(&insert_sql("before", 4))
        .await
        .expect("a 4-wide embedding is accepted while no vector index is registered");

    apply_entry(
        &server,
        &CatalogEntry::PutVectorIndexParams(Box::new(params())),
    );

    let rejected = server
        .exec(&insert_sql("after", 4))
        .await
        .expect_err("the applied entry must install DIM 8 on this node's cores");
    assert!(
        rejected.contains("dimension"),
        "the rejection must name the width the index declares, got: {rejected}"
    );

    server
        .exec(&insert_sql("matching", DECLARED_DIM))
        .await
        .expect("a row at the declared width is still accepted");
}

/// A `DeleteVectorIndexParams` applied without ever running the DDL must
/// tear the index down on this node too, so the width it declared stops
/// being enforced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_apply_drops_the_index_on_this_node() {
    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("CREATE COLLECTION");

    apply_entry(
        &server,
        &CatalogEntry::PutVectorIndexParams(Box::new(params())),
    );
    server
        .exec(&insert_sql("gated", 4))
        .await
        .expect_err("the index must be registered before the drop is observable");

    apply_entry(
        &server,
        &CatalogEntry::DeleteVectorIndexParams {
            database_id: DATABASE,
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            field_name: FIELD.to_string(),
        },
    );

    server
        .exec(&insert_sql("after_drop", 4))
        .await
        .expect("the dropped index must stop enforcing its declared width on this node");
}
