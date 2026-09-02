// SPDX-License-Identifier: BUSL-1.1

//! Vector model metadata and vector-index build parameters are replicated
//! catalog state, not node-local index bookkeeping.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up with the same durable row. These tests drive
//! that follower path directly — apply, then the synchronous post-apply — and
//! assert through the catalog readers the vector SQL and the boot seed use.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{StoredVectorIndexParams, VectorModelEntry, VectorModelMetadata};

const DATABASE: u64 = 0;
const TENANT: u64 = 1;
const COLLECTION: &str = "documents";
const FIELD: &str = "embedding";

fn model() -> VectorModelEntry {
    VectorModelEntry {
        database_id: DATABASE,
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        column: FIELD.to_string(),
        metadata: VectorModelMetadata {
            model: "all-MiniLM-L6-v2".to_string(),
            dimensions: 384,
            created_at: "2026-01-01".to_string(),
            strict_dimensions: true,
        },
    }
}

fn params() -> StoredVectorIndexParams {
    StoredVectorIndexParams {
        database_id: DATABASE,
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        field_name: FIELD.to_string(),
        dim: 384,
        metric: "cosine".to_string(),
        m: 32,
        ef_construction: 400,
        index_type: "hnsw".to_string(),
        pq_m: 0,
        ivf_cells: 0,
        ivf_nprobe: 0,
    }
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// A replicated `PutVectorModel` makes the embedding-model row durable on a
/// node that never parsed the `ALTER COLLECTION` statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_vector_model() {
    let server = TestServer::start().await;
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .get_vector_model(DATABASE, TENANT, COLLECTION, FIELD)
            .expect("read vector model")
            .is_none(),
        "no vector model is stored before the entry applies"
    );

    apply_entry(&server, &CatalogEntry::PutVectorModel(Box::new(model())));

    let stored = server
        .shared
        .credentials
        .catalog()
        .get_vector_model(DATABASE, TENANT, COLLECTION, FIELD)
        .expect("read the vector model back")
        .expect("apply must write the durable row on this node");
    assert_eq!(stored.metadata.model, "all-MiniLM-L6-v2");
    assert_eq!(stored.metadata.dimensions, 384);
    assert_eq!(stored.metadata.created_at, "2026-01-01");
    assert!(stored.metadata.strict_dimensions);
}

/// A replicated `PutVectorIndexParams` reaches the same boot-seed list the
/// executing node builds its index from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_vector_index_params() {
    let server = TestServer::start().await;

    apply_entry(
        &server,
        &CatalogEntry::PutVectorIndexParams(Box::new(params())),
    );

    let stored = server
        .shared
        .credentials
        .catalog()
        .get_vector_index_params(DATABASE, TENANT, COLLECTION, FIELD)
        .expect("read the params back")
        .expect("apply must write the durable row on this node");
    assert_eq!(stored.dim, 384);
    assert_eq!(stored.metric, "cosine");
    assert_eq!(stored.m, 32);
    assert_eq!(stored.ef_construction, 400);

    let seeded = server
        .shared
        .credentials
        .catalog()
        .list_all_vector_index_params()
        .expect("list the boot seed");
    assert!(
        seeded
            .iter()
            .any(|e| e.database_id == DATABASE && e.collection == COLLECTION),
        "the replicated row is what this node's next boot seeds the core from"
    );
}

/// A replicated `DeleteVectorIndexParams` drops the row on every node, and a
/// re-delivery of it is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_vector_index_params() {
    let server = TestServer::start().await;
    apply_entry(
        &server,
        &CatalogEntry::PutVectorIndexParams(Box::new(params())),
    );
    let delete = CatalogEntry::DeleteVectorIndexParams {
        database_id: DATABASE,
        tenant_id: TENANT,
        collection: COLLECTION.to_string(),
        field_name: FIELD.to_string(),
    };

    apply_entry(&server, &delete);
    apply_entry(&server, &delete);

    assert!(
        server
            .shared
            .credentials
            .catalog()
            .get_vector_index_params(DATABASE, TENANT, COLLECTION, FIELD)
            .expect("read the params back")
            .is_none(),
        "the dropped index leaves no parameters for the next boot to re-seed"
    );
}
