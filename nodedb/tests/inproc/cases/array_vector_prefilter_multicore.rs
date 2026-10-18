// SPDX-License-Identifier: BUSL-1.1

//! A vector search prefiltered by an `ARRAY_SLICE` on a 4-core server.
//!
//! Document `g{i}` holds the embedding `[i + 1, 0, 0]`. Its surrogate is
//! bound to the array cell `(chrom = i / 5, pos = i % 5)` before the cell is
//! written, so cell and document share one identity. The ten one-cell tiles
//! spread over the cores, and the search's own core holds only some of them.
//! The slice `chrom = 0` admits `g0`..`g4`, and the search must rank exactly
//! those five.

use nodedb_array::types::coord::value::CoordValue;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{CollectionKey, DatabaseId, TenantId};

const CORES: usize = 4;
const DOCS: u32 = 10;
/// The tenant the harness superuser writes as.
const TENANT: TenantId = TenantId::new(1);

fn coord(i: u32) -> (i64, i64) {
    (i64::from(i / 5), i64::from(i % 5))
}

async fn prepare(srv: &TestServer) {
    srv.exec("CREATE COLLECTION genes TYPE document")
        .await
        .expect("CREATE COLLECTION genes");
    srv.exec("CREATE VECTOR INDEX idx_genes_emb ON genes (embedding) METRIC l2 DIM 3")
        .await
        .expect("CREATE VECTOR INDEX");
    for i in 0..DOCS {
        srv.exec(&format!(
            "INSERT INTO genes (id, embedding) VALUES ('g{i}', ARRAY[{}.0, 0.0, 0.0])",
            i + 1
        ))
        .await
        .unwrap_or_else(|e| panic!("INSERT g{i}: {e}"));
    }
    srv.exec(
        "CREATE ARRAY genome \
         DIMS (chrom INT64 [0..1], pos INT64 [0..4]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (1, 1)",
    )
    .await
    .expect("CREATE ARRAY genome");

    let assigner = &srv.shared.surrogate_assigner;
    let genes = CollectionKey::from_bare(DatabaseId::DEFAULT, "genes");
    let genome = CollectionKey::from_bare(DatabaseId::DEFAULT, "genome");
    for i in 0..DOCS {
        let id = format!("g{i}");
        let surrogate = assigner
            .lookup_bound(genes, TENANT, id.as_bytes())
            .expect("surrogate lookup")
            .unwrap_or_else(|| panic!("document {id} has no surrogate in tenant {TENANT:?}"));
        let (chrom, pos) = coord(i);
        let pk = zerompk::to_msgpack_vec(&vec![CoordValue::Int64(chrom), CoordValue::Int64(pos)])
            .expect("coord pk encode");
        let bound = assigner
            .bind(genome, TENANT, &pk, surrogate)
            .expect("bind the cell to the document's surrogate");
        assert_eq!(bound, surrogate, "cell ({chrom}, {pos}) was bound already");
    }
    let cells: Vec<String> = (0..DOCS)
        .map(|i| {
            let (chrom, pos) = coord(i);
            format!("COORDS ({chrom}, {pos}) VALUES ({i}.0)")
        })
        .collect();
    srv.exec(&format!("INSERT INTO ARRAY genome {}", cells.join(", ")))
        .await
        .expect("INSERT INTO ARRAY genome");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_slice_prefilter_admits_every_core_s_cells() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv).await;

    let ids = srv
        .query_text(
            "SELECT id FROM genes \
             JOIN ARRAY_SLICE('genome', '{chrom: [0, 0], pos: [0, 4]}') AS s \
               ON id = s.qual \
             ORDER BY vector_distance(embedding, [5.5, 0.0, 0.0]) \
             LIMIT 10",
        )
        .await
        .expect("fused vector + array prefilter query");
    assert_eq!(
        ids,
        vec!["g4", "g3", "g2", "g1", "g0"],
        "the slice admits g0..g4, ranked by distance to 5.5"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_cell_leaves_the_prefilter() {
    let srv = TestServer::start_multicores(CORES).await;
    prepare(&srv).await;
    srv.exec("DELETE FROM ARRAY genome WHERE COORDS IN ((0, 4), (0, 2))")
        .await
        .expect("DELETE FROM ARRAY");

    let ids = srv
        .query_text(
            "SELECT id FROM genes \
             JOIN ARRAY_SLICE('genome', '{chrom: [0, 0], pos: [0, 4]}') AS s \
               ON id = s.qual \
             ORDER BY vector_distance(embedding, [5.5, 0.0, 0.0]) \
             LIMIT 10",
        )
        .await
        .expect("fused vector + array prefilter query");
    assert_eq!(
        ids,
        vec!["g3", "g1", "g0"],
        "the deleted cells no longer admit g4 and g2"
    );
}
