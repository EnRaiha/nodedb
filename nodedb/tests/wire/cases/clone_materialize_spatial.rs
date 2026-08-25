// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE clone MATERIALIZE` for the Spatial engine.
//!
//! Verifies that a CLONE of a spatial collection can be fully materialized:
//! all 5 source rows are readable from the clone after `MATERIALIZE`.
//!
//! ## Design note: WKB round-trip
//!
//! Spatial rows are stored as `GEOMETRY` (WKB internally). The materializer
//! encodes the source row as a `Value::Object` via `value_to_msgpack` and
//! writes it to target via `ColumnarOp::Insert`. The `Value::Geometry` variant
//! round-trips through msgpack via zerompk without loss because zerompk uses
//! the `Geometry` extension type tag defined in `nodedb-types`. The
//! `GEOMETRY` column is re-inserted with the same WKB bytes.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn spatial_clone_materialize_copies_all_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Create source database with a spatial collection.
    client
        .simple_query("CREATE DATABASE sp_mat_src")
        .await
        .expect("CREATE DATABASE sp_mat_src");
    client
        .simple_query("USE DATABASE sp_mat_src")
        .await
        .expect("USE sp_mat_src");
    client
        .simple_query(
            "CREATE COLLECTION places \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .expect("CREATE COLLECTION places");

    // Seed 5 point geometries.
    let points = [
        ("p1", -122.4, 37.8, "SF"),
        ("p2", -118.2, 34.0, "LA"),
        ("p3", -87.6, 41.9, "Chicago"),
        ("p4", -73.9, 40.7, "NYC"),
        ("p5", -95.4, 29.8, "Houston"),
    ];
    for (id, lon, lat, name) in points {
        client
            .simple_query(&format!(
                "INSERT INTO places (id, location, name) \
                 VALUES ('{id}', ST_Point({lon}, {lat}), '{name}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT {id}: {e}"));
    }

    // Clone the source database.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE sp_mat_clone FROM sp_mat_src")
        .await
        .expect("CLONE DATABASE");

    // Materialize: bulk-copy all source rows into the clone.
    client
        .simple_query("ALTER DATABASE sp_mat_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE sp_mat_clone MATERIALIZE");

    // All 5 rows must be readable from the materialized clone.
    client
        .simple_query("USE DATABASE sp_mat_clone")
        .await
        .expect("USE sp_mat_clone");

    let rows = client
        .simple_query("SELECT id FROM places")
        .await
        .expect("SELECT id FROM places in clone");

    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 5,
        "all 5 source rows must be readable in the materialized spatial clone"
    );
}
