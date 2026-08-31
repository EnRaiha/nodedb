// SPDX-License-Identifier: BUSL-1.1

//! Per-database isolation of durable vector-index parameters.
//!
//! The catalog keys a vector index by its database. Two databases can hold a
//! collection of the same name with a vector index on the same field, and each
//! must keep its own build parameters across a restart.
//!
//! A database-blind key collides across databases: the second
//! `CREATE VECTOR INDEX` is refused as a duplicate (`42710`) even though it
//! names a different database.

use crate::harness::TestServer;

#[tokio::test]
async fn two_databases_keep_their_own_vector_index_params_across_restart() {
    let srv = TestServer::start().await;

    // Same collection name and same vector field in both databases, differing
    // only in dimension — the field a shared key would clobber.
    for (db, dim) in [("vecdb_a", 4usize), ("vecdb_b", 8usize)] {
        srv.exec(&format!("CREATE DATABASE {db}")).await.unwrap();
        srv.exec(&format!("USE DATABASE {db}")).await.unwrap();
        srv.exec("CREATE COLLECTION shared_name TYPE document")
            .await
            .unwrap();
        srv.exec(&format!(
            "CREATE VECTOR INDEX idx_shared ON shared_name (embedding) METRIC cosine DIM {dim}"
        ))
        .await
        .unwrap();
    }

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv2, _dir) = TestServer::open_on_path(dir).await;

    // Each database must still enforce the dimension it declared. Sharing one
    // catalog entry leaves one of them enforcing the other's.
    srv2.exec("USE DATABASE vecdb_a").await.unwrap();
    srv2.exec("INSERT INTO shared_name (id, embedding) VALUES ('a', ARRAY[1.0,0.0,0.0,0.0])")
        .await
        .expect("vecdb_a declared DIM 4 and must still accept a 4-element vector");

    srv2.exec("USE DATABASE vecdb_b").await.unwrap();
    srv2.exec(
        "INSERT INTO shared_name (id, embedding) VALUES \
         ('b', ARRAY[1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])",
    )
    .await
    .expect("vecdb_b declared DIM 8 and must still accept an 8-element vector");
}
