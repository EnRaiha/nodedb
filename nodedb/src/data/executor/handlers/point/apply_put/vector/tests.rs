// SPDX-License-Identifier: BUSL-1.1

//! Vector side-effect tests for the point-put path.

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::VectorIndexPutParams;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_types::{Surrogate, Value};

/// Holds the bridge endpoints + tempdir alive for the core's lifetime.
/// The tests drive `apply_point_put_vector_indexes` directly and never
/// tick the event loop, so the far ends are unused — they just must not
/// be dropped.
struct CoreHarness {
    core: CoreLoop,
    _req_tx: Producer<BridgeRequest>,
    _resp_rx: Consumer<BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn make_core() -> CoreHarness {
    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

/// Register a bare (default-"embedding") schemaless vector field so the put
/// path's schemaless indexing branch fires for it.
fn register_bare_field(core: &mut CoreLoop, db_id: u64, tid: u64, collection: &str) {
    core.vector_params.insert(
        (
            nodedb_types::DatabaseId::new(db_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        ),
        crate::engine::vector::hnsw::HnswParams::default(),
    );
}

/// Register a named schemaless vector field (`{collection}:{field}`).
fn register_named_field(core: &mut CoreLoop, db_id: u64, tid: u64, collection: &str, field: &str) {
    core.vector_params.insert(
        (
            nodedb_types::DatabaseId::new(db_id),
            crate::types::TenantId::new(tid),
            format!("{collection}:{field}"),
        ),
        crate::engine::vector::hnsw::HnswParams::default(),
    );
}

/// A schemaless document body carrying the named vector fields.
fn doc_with_vectors(fields: &[(&str, &[f32])]) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    for (name, vector) in fields {
        obj.insert(
            (*name).to_string(),
            Value::Array(vector.iter().map(|f| Value::Float(*f as f64)).collect()),
        );
    }
    nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode doc")
}

fn live_count(core: &CoreLoop, db_id: u64, tid: u64, collection: &str, field: &str) -> usize {
    let key = CoreLoop::vector_index_key(db_id, tid, collection, field);
    core.vector_collections
        .get(&key)
        .map(|c| c.live_count())
        .unwrap_or(0)
}

fn physical_len(core: &CoreLoop, db_id: u64, tid: u64, collection: &str, field: &str) -> usize {
    let key = CoreLoop::vector_index_key(db_id, tid, collection, field);
    core.vector_collections
        .get(&key)
        .map(|c| c.len())
        .unwrap_or(0)
}

/// Regression for the latent HNSW duplicate-node bug: a second `PointPut`
/// for the same surrogate — a live overwrite, or a replayed duplicate WAL
/// record — must replace the surrogate's prior vector node rather than
/// append a second one that keeps scoring in KNN forever.
#[test]
fn second_put_for_same_surrogate_replaces_not_duplicates_vector_node() {
    let mut harness = make_core();
    let core = &mut harness.core;

    let db_id = 0u64;
    let tid = 1u64;
    let collection = "docs";
    let surrogate = Surrogate::new(1);
    let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);

    register_bare_field(core, db_id, tid, collection);

    let first = doc_with_vectors(&[("embedding", &[1.0, 0.0, 0.0])]);
    core.apply_point_put_vector_indexes(VectorIndexPutParams {
        database_id: db_id,
        tid,
        collection,
        document_id: &row_key,
        surrogate,
        value: &first,
        wal_lsn: 0,
    })
    .expect("vector indexing must accept this fixture");

    let second = doc_with_vectors(&[("embedding", &[0.0, 1.0, 0.0])]);
    core.apply_point_put_vector_indexes(VectorIndexPutParams {
        database_id: db_id,
        tid,
        collection,
        document_id: &row_key,
        surrogate,
        value: &second,
        wal_lsn: 0,
    })
    .expect("vector indexing must accept this fixture");

    assert_eq!(
        physical_len(core, db_id, tid, collection, "embedding"),
        2,
        "both puts must have physically indexed (guards against a silent no-op false pass)"
    );
    assert_eq!(
        live_count(core, db_id, tid, collection, "embedding"),
        1,
        "second put for the same surrogate must replace the prior node, not append a duplicate"
    );
}

/// Regression for the multi-vector-field case: a single put of a document
/// carrying TWO vector fields must leave exactly one live node in EACH
/// field's index. A whole-doc remove-before-insert inside the per-field
/// loop would delete the first field's just-inserted node while processing
/// the second, wiping every field but the last — breaking MetaEmbed /
/// ColBERT multi-vector collections on every put.
#[test]
fn single_put_with_two_vector_fields_keeps_one_live_node_each() {
    let mut harness = make_core();
    let core = &mut harness.core;

    let db_id = 0u64;
    let tid = 1u64;
    let collection = "docs";
    let surrogate = Surrogate::new(1);
    let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);

    register_named_field(core, db_id, tid, collection, "embedding");
    register_named_field(core, db_id, tid, collection, "title_vec");

    let doc = doc_with_vectors(&[
        ("embedding", &[1.0, 0.0, 0.0]),
        ("title_vec", &[0.0, 1.0, 0.0, 0.0]),
    ]);
    core.apply_point_put_vector_indexes(VectorIndexPutParams {
        database_id: db_id,
        tid,
        collection,
        document_id: &row_key,
        surrogate,
        value: &doc,
        wal_lsn: 0,
    })
    .expect("vector indexing must accept this fixture");

    assert_eq!(
        live_count(core, db_id, tid, collection, "embedding"),
        1,
        "the `embedding` field must keep its live node — not be wiped by the sibling field's put"
    );
    assert_eq!(
        live_count(core, db_id, tid, collection, "title_vec"),
        1,
        "the `title_vec` field must have exactly one live node"
    );
}
