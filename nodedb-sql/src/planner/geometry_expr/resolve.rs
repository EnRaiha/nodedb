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
    let Some(value) = crate::planner::const_fold::fold_constant_default(&sql_expr) else {
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
