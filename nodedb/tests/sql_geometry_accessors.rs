// SPDX-License-Identifier: BUSL-1.1

//! Geometry accessor and renderer functions in scalar / projection position.
//!
//! Covers the `ST_*` accessor surface applied to a stored GEOMETRY column
//! (`ST_AsText`, `ST_X`/`ST_Y`, `ST_GeometryType`, `ST_NPoints`, `ST_IsValid`,
//! `ST_AsGeoJSON`, `ST_Length`, `ST_Perimeter`) and geometry constructors used
//! inside a projection expression rather than in an INSERT value.

mod common;
use common::pgwire_harness::TestServer;

/// Create a spatial collection with a single geometry row built from WKT.
async fn seeded(srv: &TestServer, collection: &str, wkt: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, loc) VALUES ('a', ST_GeomFromText('{wkt}'))"
    ))
    .await
    .unwrap();
}

// ── Accessors on a stored geometry column ───────────────────────────────────

#[tokio::test]
async fn st_astext_renders_stored_geometry_as_wkt() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_astext", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT ST_AsText(loc) FROM sp_astext WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let wkt = rows[0][0].to_uppercase();
    assert!(
        wkt.starts_with("POINT") && wkt.contains('1') && wkt.contains('2'),
        "ST_AsText must render the stored point as WKT, got: {}",
        rows[0][0]
    );
}

#[tokio::test]
async fn st_x_and_st_y_read_stored_point_ordinates() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_xy", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT ST_X(loc), ST_Y(loc) FROM sp_xy WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let x: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("ST_X must return a number, got {:?}", rows[0][0]));
    let y: f64 = rows[0][1]
        .parse()
        .unwrap_or_else(|_| panic!("ST_Y must return a number, got {:?}", rows[0][1]));
    assert!((x - 1.0).abs() < 1e-9, "ST_X expected 1, got {x}");
    assert!((y - 2.0).abs() < 1e-9, "ST_Y expected 2, got {y}");
}

/// The accessor capability itself exists under the internal `geo_*` spelling;
/// only the standard `ST_*` name is absent. This anchors the accessor tests
/// above to a naming gap rather than a missing implementation.
#[tokio::test]
async fn geo_x_accessor_reads_stored_point_ordinate() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_geo_x", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT geo_x(loc) FROM sp_geo_x WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let x: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("geo_x must return a number, got {:?}", rows[0][0]));
    assert!((x - 1.0).abs() < 1e-9, "geo_x expected 1, got {x}");
}

#[tokio::test]
async fn st_geometrytype_names_stored_geometry() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_geomtype", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT ST_GeometryType(loc) FROM sp_geomtype WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    assert!(
        rows[0][0].to_uppercase().contains("POINT"),
        "ST_GeometryType must name the stored geometry, got {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn st_npoints_counts_stored_linestring_vertices() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_npoints", "LINESTRING(0 0, 1 1, 2 2)").await;

    let rows = srv
        .query_rows("SELECT ST_NPoints(loc) FROM sp_npoints WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    assert_eq!(
        rows[0][0], "3",
        "ST_NPoints must count the three stored vertices"
    );
}

#[tokio::test]
async fn st_isvalid_reports_stored_geometry_validity() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_isvalid", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT ST_IsValid(loc) FROM sp_isvalid WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    assert!(
        matches!(rows[0][0].to_lowercase().as_str(), "t" | "true"),
        "ST_IsValid must report the stored point as valid, got {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn st_asgeojson_renders_stored_geometry_as_geojson() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_asgeojson", "POINT(1 2)").await;

    let rows = srv
        .query_rows("SELECT ST_AsGeoJSON(loc) FROM sp_asgeojson WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let json = &rows[0][0];
    assert!(
        json.contains("\"Point\"") && json.contains("coordinates"),
        "ST_AsGeoJSON must render GeoJSON, got {json:?}"
    );
}

#[tokio::test]
async fn st_length_measures_stored_linestring() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_length", "LINESTRING(0 0, 0 1)").await;

    let rows = srv
        .query_rows("SELECT ST_Length(loc) FROM sp_length WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let len: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("ST_Length must return a number, got {:?}", rows[0][0]));
    assert!(len > 0.0, "ST_Length must be positive, got {len}");
}

#[tokio::test]
async fn st_perimeter_measures_stored_polygon() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_perimeter", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))").await;

    let rows = srv
        .query_rows("SELECT ST_Perimeter(loc) FROM sp_perimeter WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let perimeter: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("ST_Perimeter must return a number, got {:?}", rows[0][0]));
    assert!(
        perimeter > 0.0,
        "ST_Perimeter must be positive, got {perimeter}"
    );
}

// ── Geometry constructors inside a projection expression ────────────────────
// Constructors resolve today only in INSERT value position and in the two
// hardcoded cases of the spatial-predicate argument path. In a projection they
// reach the scalar-function resolver, which has no entry for them.

#[tokio::test]
async fn st_astext_renders_st_geomfromtext_literal() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION sp_scalar_wkt WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'))")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let wkt = rows[0][0].to_uppercase();
    assert!(
        wkt.starts_with("POINT") && wkt.contains('1') && wkt.contains('2'),
        "ST_AsText(ST_GeomFromText(...)) must round-trip the WKT, got {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn st_distance_accepts_st_geomfromtext_arguments() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION sp_scalar_dist WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT ST_Distance(\
               ST_GeomFromText('POINT(-122.4 37.8)'), \
               ST_GeomFromText('POINT(-87.6 41.8)')\
             )",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let dist: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("ST_Distance must return a number, got {:?}", rows[0][0]));
    assert!(dist > 0.0, "distance should be positive, got {dist}");
}

#[tokio::test]
async fn st_makepoint_resolves_in_projection() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION sp_scalar_mkpt WITH (engine='document_schemaless')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT ST_X(ST_MakePoint(1, 2)), ST_Y(ST_MakePoint(1, 2))")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
    let x: f64 = rows[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("ST_X must return a number, got {:?}", rows[0][0]));
    let y: f64 = rows[0][1]
        .parse()
        .unwrap_or_else(|_| panic!("ST_Y must return a number, got {:?}", rows[0][1]));
    assert!((x - 1.0).abs() < 1e-9, "ST_X expected 1, got {x}");
    assert!((y - 2.0).abs() < 1e-9, "ST_Y expected 2, got {y}");
}
