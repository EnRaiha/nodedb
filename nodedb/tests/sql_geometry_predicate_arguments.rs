// SPDX-License-Identifier: BUSL-1.1

//! Geometry arguments to spatial predicates in WHERE.
//!
//! `ST_DWithin` / `ST_Within` / `ST_Contains` / `ST_Intersects` take a query
//! geometry in their second argument. Any geometry-valued expression accepted
//! elsewhere in SQL — every geometry constructor, and geometry-returning
//! operations such as `ST_Buffer` — must resolve there to the same geometry it
//! produces in INSERT value position.

mod common;
use common::pgwire_harness::TestServer;

/// Create a spatial collection holding `POINT(1 2)` plus a far-away control row
/// so a predicate that degenerates into match-all is distinguishable from one
/// that actually filters.
async fn seeded(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, loc) VALUES ('near', ST_GeomFromText('POINT(1 2)'))"
    ))
    .await
    .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, loc) VALUES ('far', ST_GeomFromText('POINT(120 -40)'))"
    ))
    .await
    .unwrap();
}

/// Assert a predicate selected exactly the seeded near row — never the control
/// row, so a match-all degeneration fails rather than passing by accident.
fn assert_only_near(rows: &[Vec<String>]) {
    assert_eq!(
        rows.len(),
        1,
        "predicate must select exactly the near row, got {rows:?}"
    );
    assert_eq!(rows[0][0], "near", "wrong row selected: {rows:?}");
}

// ── Constructors in query-geometry position ─────────────────────────────────

#[tokio::test]
async fn st_dwithin_accepts_st_geomfromtext_query_geometry() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_dw_wkt").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_dw_wkt \
             WHERE ST_DWithin(loc, ST_GeomFromText('POINT(1 2)'), 1000)",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

#[tokio::test]
async fn st_dwithin_accepts_st_makepoint_query_geometry() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_dw_mkpt").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_dw_mkpt \
             WHERE ST_DWithin(loc, ST_MakePoint(1, 2), 1000)",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

#[tokio::test]
async fn st_dwithin_accepts_st_geomfromwkb_query_geometry() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_dw_wkb").await;

    // WKB for POINT(1 2): little-endian (01), type 1 point, x=1.0, y=2.0.
    let rows = srv
        .query_rows(
            "SELECT id FROM sp_dw_wkb \
             WHERE ST_DWithin(loc, \
               ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 1000)",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

/// A geometry-returning operation, not just a constructor, is a valid query
/// geometry — a buffered search area is the common way to express a radius
/// against a polygon predicate.
#[tokio::test]
async fn st_within_accepts_st_buffer_query_geometry() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_within_buffer").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_within_buffer \
             WHERE ST_Within(loc, ST_Buffer(ST_Point(1, 2), 1000))",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

#[tokio::test]
async fn st_within_accepts_st_geomfromtext_polygon() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_within_wkt").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_within_wkt \
             WHERE ST_Within(loc, ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

#[tokio::test]
async fn st_intersects_accepts_st_geomfromtext_polygon() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_intersects_wkt").await;

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_intersects_wkt \
             WHERE ST_Intersects(loc, ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))",
        )
        .await
        .unwrap();
    assert_only_near(&rows);
}

#[tokio::test]
async fn st_contains_accepts_st_geomfromtext_polygon() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION sp_contains_wkt \
         COLUMNS (id TEXT, loc GEOMETRY) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO sp_contains_wkt (id, loc) \
         VALUES ('zone', ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO sp_contains_wkt (id, loc) \
         VALUES ('elsewhere', ST_GeomFromText('POLYGON((100 100, 105 100, 105 105, 100 105, 100 100))'))",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM sp_contains_wkt \
             WHERE ST_Contains(loc, ST_GeomFromText('POINT(1 2)'))",
        )
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "predicate must select exactly the containing zone, got {rows:?}"
    );
    assert_eq!(rows[0][0], "zone", "wrong row selected: {rows:?}");
}

// ── Rejection of a genuinely non-geometry argument ──────────────────────────
// An unresolvable query geometry must fail as an unsupported geometry
// expression. It must never be Display-formatted back into a string and fed to
// the GeoJSON parser: the resulting error names a JSON column offset in the
// SQL source text, which tells the caller nothing about what was wrong and
// masks whether the argument was a valid geometry expression the planner
// simply did not know how to resolve.

#[tokio::test]
async fn non_geometry_predicate_argument_reports_geometry_error() {
    let srv = TestServer::start().await;
    seeded(&srv, "sp_bad_arg").await;

    let err = srv
        .query_rows("SELECT id FROM sp_bad_arg WHERE ST_DWithin(loc, 42, 1000)")
        .await
        .expect_err("a numeric query geometry must be rejected");

    assert!(
        !err.contains("Invalid JSON value"),
        "geometry rejection must not surface as a raw JSON parse failure, got: {err}"
    );
    assert!(
        !err.contains("line 1 column 1"),
        "geometry rejection must not report a JSON offset into the SQL text, got: {err}"
    );
    assert!(
        err.to_lowercase().contains("geometry"),
        "error must name the geometry argument as the problem, got: {err}"
    );
}
