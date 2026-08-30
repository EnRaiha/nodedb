// SPDX-License-Identifier: Apache-2.0

//! Resolution of a SQL expression that denotes a geometry.
//!
//! Every syntactic position that expects a geometry — an inserted GEOMETRY
//! column value, a spatial predicate's query-geometry argument — resolves it
//! here, and this module resolves it by constant-folding through the same
//! evaluator that runs the expression at row scope. There is no second
//! implementation of "what `ST_GeomFromText(...)` means", so a constructor
//! cannot work in one position and be unknown in another.

use sqlparser::ast;

use nodedb_query::geo_functions;
use nodedb_types::geometry::Geometry;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Fold a function call that denotes a geometry into its stored form.
///
/// Returns `None` when `func` is not a geometry-producing geospatial call, so
/// the caller falls through to the shared constant-folding pipeline.
///
/// The stored form is a GeoJSON string, which is what the spatial read path
/// parses back. A call whose arguments do not describe a valid geometry is an
/// error rather than a NULL: silently storing NULL for a malformed
/// `ST_GeomFromText('POINT(')` would lose the row's geometry with no signal.
pub(crate) fn fold_geometry_function(func: &ast::Function) -> Option<Result<SqlValue>> {
    let name = function_name(func);
    if !geo_functions::returns_geometry(&name) {
        return None;
    }
    let expr = ast::Expr::Function(func.clone());
    Some(match resolve(&expr) {
        Ok(Some(geom)) => serialize(&geom, &name),
        Ok(None) => Err(invalid_geometry(&name)),
        Err(e) => Err(e),
    })
}

/// Resolve any expression in geometry position to a concrete geometry.
///
/// Accepts geospatial calls, nested geometry-returning operations, and WKT or
/// GeoJSON string literals. Anything that does not denote a geometry is a
/// typed error naming the offending expression — never a Display-formatted
/// AST handed to a parser, which reports a JSON offset into SQL source text
/// and tells the caller nothing about what was actually wrong.
pub(crate) fn resolve_geometry_expr(expr: &ast::Expr) -> Result<Geometry> {
    match resolve(expr)? {
        Some(geom) => Ok(geom),
        None => Err(SqlError::InvalidFunction {
            detail: format!(
                "expression in geometry position does not resolve to a geometry: {expr}"
            ),
        }),
    }
}

/// Constant-fold `expr` and read a geometry out of the result.
///
/// `Ok(None)` means the expression folded but is not a geometry, or could not
/// be folded at plan time at all.
fn resolve(expr: &ast::Expr) -> Result<Option<Geometry>> {
    let sql_expr = crate::resolver::expr::convert_expr(expr)?;
    let Some(value) = crate::planner::const_fold::fold_constant_default(&sql_expr)? else {
        return Ok(None);
    };
    Ok(geometry_from_value(&value))
}

/// Geometry travels through `SqlValue` as its GeoJSON string form, which is
/// also how it is stored; WKT literals are accepted here for the same reason
/// they are accepted by the evaluator.
fn geometry_from_value(value: &SqlValue) -> Option<Geometry> {
    match value {
        SqlValue::String(text) => geo_functions::geometry_from_text(text),
        _ => None,
    }
}

fn serialize(geom: &Geometry, name: &str) -> Result<SqlValue> {
    sonic_rs::to_string(geom)
        .map(SqlValue::String)
        .map_err(|e| SqlError::InvalidFunction {
            detail: format!("{name}: failed to serialize geometry: {e}"),
        })
}

fn invalid_geometry(name: &str) -> SqlError {
    SqlError::InvalidFunction {
        detail: format!("{name}: arguments do not describe a valid geometry"),
    }
}

/// Lowercased, dot-joined function name.
fn function_name(func: &ast::Function) -> String {
    func.name
        .0
        .iter()
        .map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use nodedb_types::geometry::Geometry;

    use super::{fold_geometry_function, resolve_geometry_expr};
    use crate::types::SqlValue;

    /// Parse a scalar SQL expression out of `SELECT <expr>`.
    fn expr(sql: &str) -> sqlparser::ast::Expr {
        use sqlparser::ast::{SetExpr, Statement};
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut statements =
            sqlparser::parser::Parser::parse_sql(&dialect, &format!("SELECT {sql}"))
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
}
