// SPDX-License-Identifier: BUSL-1.1

//! Spatial query from the SQL planner (CP) through to Data Plane: inserts a
//! GeoJSON point, issues `ST_DWithin`, and verifies the typed `Geometry`
//! travels through the SPSC bridge and returns the correct row.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_sql_query_returns_correct_row() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION geo_places (\
                id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();

    // Insert a point near Times Square, NYC.
    server
        .exec(
            "INSERT INTO geo_places (id, location, name) \
             VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')",
        )
        .await
        .unwrap();

    // Insert a point far away — should not match.
    server
        .exec(
            "INSERT INTO geo_places (id, location, name) \
             VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')",
        )
        .await
        .unwrap();

    // Query: rows within ~5 km of Times Square using a GeoJSON literal.
    let rows = server
        .query_text(
            "SELECT name FROM geo_places WHERE \
             ST_DWithin(location, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)",
        )
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row within 5 km of Times Square"
    );
    assert!(
        rows[0].contains("Times Square"),
        "returned row should be Times Square"
    );
}
