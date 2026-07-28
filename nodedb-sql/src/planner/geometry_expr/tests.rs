// SPDX-License-Identifier: Apache-2.0

use nodedb_types::geometry::Geometry;

use super::{fold_geometry_function, resolve_geometry_expr};
use crate::types::SqlValue;

/// Parse a scalar SQL expression out of `SELECT <expr>`.
fn expr(sql: &str) -> sqlparser::ast::Expr {
    use sqlparser::ast::{SetExpr, Statement};
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, &format!("SELECT {sql}"))
        .expect("test expression must parse");
    let Statement::Query(query) = statements.remove(0) else {
        panic!("expected a query");
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("expected a select");
    };
    match select.projection.into_iter().next() {
        Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) => e,
        other => panic!("expected an unnamed expression, got {other:?}"),
    }
}

fn resolved(sql: &str) -> Geometry {
    resolve_geometry_expr(&expr(sql))
        .unwrap_or_else(|e| panic!("`{sql}` must resolve to a geometry: {e}"))
}

// ── Predicate geometry position ─────────────────────────────────────────────

#[test]
fn every_constructor_resolves_in_geometry_position() {
    let point = Geometry::point(1.0, 2.0);
    assert_eq!(resolved("ST_Point(1, 2)"), point);
    assert_eq!(resolved("ST_MakePoint(1, 2)"), point);
    assert_eq!(resolved("ST_GeomFromText('POINT(1 2)')"), point);
    assert_eq!(
        resolved(r#"ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2]}')"#),
        point
    );
    assert_eq!(
        resolved("ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040')"),
        point
    );
}

/// The resolver folds recursively, so a geometry-returning operation wrapping
/// a constructor is itself a valid query geometry.
#[test]
fn nested_geometry_operations_resolve() {
    let buffered = resolved("ST_Buffer(ST_Point(1, 2), 1000)");
    assert!(
        matches!(buffered, Geometry::Polygon { .. }),
        "ST_Buffer must resolve to a polygon, got {buffered:?}"
    );
    assert!(matches!(
        resolved("ST_Envelope(ST_GeomFromText('LINESTRING(0 0, 1 1)'))"),
        Geometry::Polygon { .. }
    ));
    assert_eq!(
        resolved("ST_Centroid(ST_GeomFromText('LINESTRING(0 0, 0 0)'))"),
        Geometry::point(0.0, 0.0)
    );
}

/// A bare literal in geometry position is a geometry, in either interchange
/// format — PostGIS reads an unknown-typed literal as WKT.
#[test]
fn wkt_and_geojson_literals_resolve() {
    let point = Geometry::point(1.0, 2.0);
    assert_eq!(resolved("'POINT(1 2)'"), point);
    assert_eq!(resolved(r#"'{"type":"Point","coordinates":[1,2]}'"#), point);
}

/// The reported failure: the argument must never be Display-formatted back
/// into a string and handed to the GeoJSON parser. That produced a JSON
/// offset into the SQL source text, which names neither the argument nor the
/// problem.
#[test]
fn non_geometry_argument_reports_a_geometry_error() {
    for sql in ["42", "'not a geometry'", "TRUE"] {
        let err = resolve_geometry_expr(&expr(sql))
            .expect_err("`{sql}` must not resolve to a geometry")
            .to_string();
        assert!(
            err.to_lowercase().contains("geometry"),
            "error for `{sql}` must name the geometry position, got: {err}"
        );
        assert!(
            !err.contains("Invalid JSON value") && !err.contains("line 1 column 1"),
            "error for `{sql}` must not surface as a JSON parse failure, got: {err}"
        );
    }
}

// ── Inserted-value position ─────────────────────────────────────────────────

#[test]
fn geometry_functions_fold_to_their_stored_geojson_form() {
    let sqlparser::ast::Expr::Function(func) = expr("ST_GeomFromText('POINT(1 2)')") else {
        panic!("expected a function");
    };
    let Some(Ok(SqlValue::String(geojson))) = fold_geometry_function(&func) else {
        panic!("a geometry constructor must fold to its stored form");
    };
    assert_eq!(
        nodedb_types::geometry::from_geojson_str(&geojson),
        Some(Geometry::point(1.0, 2.0)),
        "stored form must parse back through the spatial read path"
    );
}

/// A non-geometry call falls through so the generic constant folder handles
/// it; claiming it here would break every other scalar in value position.
#[test]
fn non_geometry_function_falls_through() {
    let sqlparser::ast::Expr::Function(func) = expr("ST_Area(ST_Point(1, 2))") else {
        panic!("expected a function");
    };
    assert!(fold_geometry_function(&func).is_none());
}

/// Storing NULL for a malformed geometry would drop the row's location with
/// no signal to the writer.
#[test]
fn malformed_geometry_in_value_position_is_an_error_not_null() {
    for sql in ["ST_GeomFromText('POINT(')", "ST_GeomFromText('nonsense')"] {
        let sqlparser::ast::Expr::Function(func) = expr(sql) else {
            panic!("expected a function");
        };
        let Some(result) = fold_geometry_function(&func) else {
            panic!("`{sql}` is a geometry constructor and must be claimed");
        };
        assert!(
            result.is_err(),
            "`{sql}` must fail rather than fold to NULL, got {result:?}"
        );
    }
}
