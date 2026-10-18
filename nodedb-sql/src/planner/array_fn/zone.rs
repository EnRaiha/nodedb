// SPDX-License-Identifier: Apache-2.0

//! The polygon zone argument pair of `ARRAY_AGG`: a WKT `POLYGON` or
//! `MULTIPOLYGON` and the two numeric dims it is drawn over, x then y.

use sqlparser::ast;

use nodedb_types::geometry::Geometry;

use super::helpers::{expect_string_array, expect_string_literal};
use crate::catalog::ArrayCatalogView;
use crate::error::{Result, SqlError};
use crate::types_array::{ArrayDimType, ArrayZoneAst};

/// Parse `zone_wkt` and `zone_dims` of an `ARRAY_AGG` over array `name`.
pub(super) fn parse_zone_args(
    wkt_expr: &ast::Expr,
    dims_expr: &ast::Expr,
    name: &str,
    view: &ArrayCatalogView,
) -> Result<ArrayZoneAst> {
    let wkt = expect_string_literal(wkt_expr, "ARRAY_AGG zone")?;
    let geometry =
        nodedb_spatial::geometry_from_wkt(&wkt).ok_or_else(|| SqlError::InvalidFunction {
            detail: format!("ARRAY_AGG: zone is not valid WKT: {wkt}"),
        })?;
    let issues = nodedb_spatial::validate::validate_geometry(&geometry);
    if !issues.is_empty() {
        return Err(SqlError::InvalidFunction {
            detail: format!("ARRAY_AGG: invalid zone: {}", issues.join("; ")),
        });
    }
    let polygons = match geometry {
        Geometry::Polygon { coordinates } => vec![coordinates],
        Geometry::MultiPolygon { coordinates } => coordinates,
        other => {
            return Err(SqlError::InvalidFunction {
                detail: format!(
                    "ARRAY_AGG: zone must be a POLYGON or MULTIPOLYGON, got {}",
                    other.geometry_type()
                ),
            });
        }
    };

    let dims = expect_string_array(dims_expr, "ARRAY_AGG zone dims")?;
    let [x_dim, y_dim] = <[String; 2]>::try_from(dims).map_err(|dims| SqlError::Unsupported {
        detail: format!(
            "ARRAY_AGG: zone dims must name two dims, x then y; got {}",
            dims.len()
        ),
    })?;
    if x_dim == y_dim {
        return Err(SqlError::Unsupported {
            detail: format!("ARRAY_AGG: zone dims must be two different dims; got '{x_dim}' twice"),
        });
    }
    for dim in [&x_dim, &y_dim] {
        let spec =
            view.dims
                .iter()
                .find(|d| &d.name == dim)
                .ok_or_else(|| SqlError::Unsupported {
                    detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
                })?;
        match spec.dtype {
            ArrayDimType::Int64 | ArrayDimType::Float64 => {}
            ArrayDimType::TimestampMs | ArrayDimType::String => {
                return Err(SqlError::TypeMismatch {
                    detail: format!(
                        "ARRAY_AGG: zone dim '{dim}' is {:?}; a zone needs an INT64 or FLOAT64 dim",
                        spec.dtype
                    ),
                });
            }
        }
    }
    Ok(ArrayZoneAst {
        polygons,
        x_dim,
        y_dim,
    })
}
