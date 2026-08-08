// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the vector engine (`primary='vector'` collections).
//!
//! A vector-primary row lives in two stores: the vector itself in the HNSW
//! graph, and every other column in a sparse-store sidecar keyed by the row's
//! surrogate in hex. Only the sidecar is readable as a row — `attach_body`
//! fetches it by that key and the response translator flattens it — so the
//! sidecar is what a `RETURNING` projection must report.
//!
//! The sidecar holds `zerompk` TAGGED bytes (`Value::String(s)` encodes as
//! `[4,"…"]`), stored verbatim by the upsert handler. Decoding them as an
//! ordinary document body yields tag arrays instead of values, which is the
//! same failure that once made a stored `"v1"` read back as the integer 118.

mod common;

use common::pgwire_harness::TestServer;

/// Every row in `collection` with its FULL column set, rendered as sorted
/// `name=value` pairs.
///
/// The same shape-capture the timeseries agreement test uses. For this engine
/// it answers a question that cannot be settled by reading the flatten path
/// with confidence: which columns a `SELECT *` on a vector-primary collection
/// actually produces — whether the vector field is projectable at all, and
/// whether the surrogate surfaces as a column or stays internal identity.
/// `RETURNING *` has to mean exactly what `SELECT *` means, so this is the
/// definition, not a convenience.
async fn full_rows(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_named_rows(&format!("SELECT * FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT * FROM {collection}: {e}"))
        .into_iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            format!("{{{}}}", cells.join(", "))
        })
        .collect()
}

async fn create_vector_primary(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// A vector-primary payload column must read back as its VALUE, not as the
/// zerompk tag array it is stored as.
///
/// This is a regression guard for a live read-path defect, not a shape probe.
/// The sidecar holds `zerompk::to_msgpack_vec(&HashMap<String, Value>)` —
/// tagged form — written verbatim by the upsert handler. The scan path
/// normalizes sparse rows through `doc_format::json_to_msgpack`, whose
/// "already standard MessagePack?" guard inspects only the OUTER container:
/// a tagged map is a valid msgpack map, so the bytes pass through untouched
/// and the tagged VALUES reach the client as `[4,"alice"]`.
///
/// That makes a vector-primary collection unreadable in any useful sense, and
/// it is independent of `RETURNING`. The full column set is reported on failure
/// because the same output settles what `RETURNING *` must mean for this
/// engine: which columns exist, whether the vector is projectable, and whether
/// the surrogate surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_primary_payload_column_reads_back_as_its_value_not_a_tag_array() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_shape").await;

    server
        .exec(
            "INSERT INTO vec_shape (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert must succeed");

    let shape = full_rows(&server, "vec_shape").await;
    assert_eq!(
        shape.len(),
        1,
        "one stored row: {shape:?}\n\
         (if this is empty, a vector-primary collection is not scannable without a \
          vector search and RETURNING has no SELECT to agree with)"
    );

    let row = &shape[0];
    // The payload columns must survive the tagged-encoding round trip. A body
    // decoded as an ordinary document would render these as `[4,"alice"]`
    // rather than `alice`, so this assertion is the tag-decode check as much as
    // a storage check.
    assert!(
        row.contains("owner=alice"),
        "a payload column must read back as its value, not as a zerompk tag array: {shape:?}"
    );
    assert!(
        row.contains("id=r1"),
        "the declared primary-key column must read back: {shape:?}"
    );
}

/// The vector-primary marker must be rebuilt from the durable catalog at boot,
/// not only installed by the live `CREATE COLLECTION`.
///
/// The marker is what tells the read path that this collection's sparse rows
/// are tagged sidecars. Deriving it only on the live-DDL path would leave a
/// collection readable until its first restart and unreadable after it — the
/// same "decoder given the wrong format" defect, one layer over. Reading the
/// rows back through a real restart is the only check that both the live path
/// and the boot seed carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_primary_payload_column_still_reads_back_after_restart() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_restart").await;

    server
        .exec(
            "INSERT INTO vec_restart (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert must succeed");

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let shape = full_rows(&server, "vec_restart").await;
    assert_eq!(
        shape.len(),
        1,
        "the stored row must survive restart: {shape:?}"
    );
    assert!(
        shape[0].contains("owner=alice"),
        "the vector-primary marker must be re-seeded from the catalog at boot, \
         or payload columns come back as tag arrays after a restart: {shape:?}"
    );
    assert!(
        shape[0].contains("id=r1"),
        "the declared primary-key column must read back after restart: {shape:?}"
    );
}

/// A CLASSIC collection with a vector index over a document field must be
/// unaffected by the vector-primary sidecar decoding.
///
/// Its rows are ordinary document bodies, and its crash-recovery rebuild reads
/// them raw to extract the vector field out of the body — a vector-primary
/// sidecar has no vector field at all. Decoding these rows as sidecars would
/// both corrupt the scan and break the rebuild, so this pins the boundary from
/// the other side: after a restart the documents still read back as values and
/// the rebuilt index still ranks them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_classic_vector_indexed_collection_survives_restart_unchanged() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vec_classic TYPE document")
        .await
        .expect("create vec_classic");
    server
        .exec(
            "CREATE VECTOR INDEX idx_vec_classic ON vec_classic (embedding) \
             METRIC cosine DIM 3",
        )
        .await
        .expect("create vector index");

    for (id, owner, emb) in [
        ("c1", "alice", "ARRAY[1.0, 0.0, 0.0]"),
        ("c2", "bob", "ARRAY[0.0, 1.0, 0.0]"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vec_classic (id, owner, embedding) \
                 VALUES ('{id}', '{owner}', {emb})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let shape = full_rows(&server, "vec_classic").await;
    assert_eq!(
        shape.len(),
        2,
        "both documents must survive restart: {shape:?}"
    );
    assert!(
        shape.iter().any(|r| r.contains("owner=alice")),
        "a classic document column must still read back as its value: {shape:?}"
    );

    // The index rebuild reads the same rows RAW; if that path were rerouted
    // through the sidecar normalizer it would extract no vector and this search
    // would rank nothing.
    let nearest = server
        .query_rows(
            "SELECT id FROM vec_classic \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .expect("vector search after restart");
    assert_eq!(
        nearest[0][0], "c1",
        "the rebuilt index must still rank the classic collection's vectors: {nearest:?}"
    );
}
