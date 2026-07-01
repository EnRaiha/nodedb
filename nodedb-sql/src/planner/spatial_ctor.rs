// SPDX-License-Identifier: Apache-2.0

//! Spatial geometry constructors that appear in value position (INSERT
//! `VALUES`, WHERE equality) and synthesise a GeoJSON string literal directly
//! rather than resolving as registered scalar functions.
//!
//! All constructors produce a `SqlValue::String` holding GeoJSON in the exact
//! shape the spatial read path parses (`extract_geometry` →
//! `sonic_rs::from_str::<Geometry>`), so an INSERTed geometry round-trips
//! through a subsequent SELECT of the GEOMETRY column.

use sqlparser::ast;

use super::dml_helpers::expr_to_sql_value;
use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Attempt to interpret `func` as a spatial constructor. Returns `None` for
/// any non-spatial function so the caller can fall through to the shared
/// constant-folding pipeline.
pub(super) fn try_spatial_constructor(func: &ast::Function) -> Option<Result<SqlValue>> {
    SpatialConstructor::from_function(func).map(|ctor| spatial_constructor_to_value(ctor, func))
}

/// Spatial constructors that synthesise a GeoJSON string literal directly
/// in value position (rather than going through the registered scalar
/// evaluator). Closed set — adding a new constructor requires a new variant,
/// which forces handling in `spatial_constructor_to_value`.
#[derive(Copy, Clone)]
enum SpatialConstructor {
    Point,
    MakePoint,
    GeomFromGeoJson,
    GeomFromText,
    GeomFromWkb,
}

impl SpatialConstructor {
    fn from_function(func: &ast::Function) -> Option<Self> {
        let name = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".")
            .to_lowercase();
        match name.as_str() {
            "st_point" => Some(Self::Point),
            "st_makepoint" => Some(Self::MakePoint),
            "st_geomfromgeojson" => Some(Self::GeomFromGeoJson),
            "st_geomfromtext" => Some(Self::GeomFromText),
            "st_geomfromwkb" => Some(Self::GeomFromWkb),
            _ => None,
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Point => "ST_Point",
            Self::MakePoint => "ST_MakePoint",
            Self::GeomFromGeoJson => "ST_GeomFromGeoJSON",
            Self::GeomFromText => "ST_GeomFromText",
            Self::GeomFromWkb => "ST_GeomFromWKB",
        }
    }
}

fn spatial_constructor_to_value(
    ctor: SpatialConstructor,
    func: &ast::Function,
) -> Result<SqlValue> {
    let args = super::select::extract_func_args(func)?;
    match ctor {
        SpatialConstructor::Point => {
            if args.len() < 2 {
                return Err(SqlError::InvalidFunction {
                    detail: format!(
                        "{} requires 2 arguments (longitude, latitude), got {}",
                        ctor.display_name(),
                        args.len()
                    ),
                });
            }
            let lon = super::select::extract_float(&args[0])?;
            let lat = super::select::extract_float(&args[1])?;
            Ok(SqlValue::String(format!(
                r#"{{"type":"Point","coordinates":[{lon},{lat}]}}"#
            )))
        }
        SpatialConstructor::MakePoint => {
            if args.len() < 2 {
                return Err(SqlError::InvalidFunction {
                    detail: format!(
                        "{} requires at least 2 arguments (x, y[, z]), got {}",
                        ctor.display_name(),
                        args.len()
                    ),
                });
            }
            let x = super::select::extract_float(&args[0])?;
            let y = super::select::extract_float(&args[1])?;
            // 3-arg form carries an explicit z; preserve it rather than drop it.
            if args.len() >= 3 {
                let z = super::select::extract_float(&args[2])?;
                Ok(SqlValue::String(format!(
                    r#"{{"type":"Point","coordinates":[{x},{y},{z}]}}"#
                )))
            } else {
                Ok(SqlValue::String(format!(
                    r#"{{"type":"Point","coordinates":[{x},{y}]}}"#
                )))
            }
        }
        SpatialConstructor::GeomFromText => {
            if args.is_empty() {
                return Err(SqlError::InvalidFunction {
                    detail: format!("{} requires a WKT string argument", ctor.display_name()),
                });
            }
            // SRID (optional 2nd arg) is ignored, consistent with ST_Point.
            let wkt = super::select::extract_string_literal(&args[0])?;
            let geom = nodedb_spatial::geometry_from_wkt(&wkt).ok_or_else(|| {
                SqlError::InvalidFunction {
                    detail: format!("{}: invalid WKT geometry '{wkt}'", ctor.display_name()),
                }
            })?;
            geometry_to_geojson_value(geom, ctor)
        }
        SpatialConstructor::GeomFromWkb => {
            if args.is_empty() {
                return Err(SqlError::InvalidFunction {
                    detail: format!("{} requires a WKB bytes argument", ctor.display_name()),
                });
            }
            // SRID (optional 2nd arg) is ignored, consistent with ST_Point.
            let bytes = match expr_to_sql_value(&args[0])? {
                SqlValue::Bytes(b) => b,
                // A hex string (rather than an `X'...'` literal) is trivially
                // decodable to the same bytes.
                SqlValue::String(s) => hex_string_to_bytes(&s, ctor)?,
                other => {
                    return Err(SqlError::InvalidFunction {
                        detail: format!(
                            "{} requires a bytes argument, got {other:?}",
                            ctor.display_name()
                        ),
                    });
                }
            };
            let geom = nodedb_spatial::geometry_from_wkb(&bytes).ok_or_else(|| {
                SqlError::InvalidFunction {
                    detail: format!("{}: invalid WKB geometry", ctor.display_name()),
                }
            })?;
            geometry_to_geojson_value(geom, ctor)
        }
        SpatialConstructor::GeomFromGeoJson => {
            if args.is_empty() {
                return Err(SqlError::InvalidFunction {
                    detail: format!(
                        "{} requires 1 argument (GeoJSON string)",
                        ctor.display_name()
                    ),
                });
            }
            let s = super::select::extract_string_literal(&args[0])?;
            Ok(SqlValue::String(s))
        }
    }
}

/// Serialize a parsed `Geometry` to the GeoJSON string form the spatial read
/// path (`extract_geometry` → `sonic_rs::from_str`) parses back. `Geometry`
/// derives serde with `tag = "type"`, so this yields the same
/// `{"type":...,"coordinates":...}` shape ST_Point synthesises by hand.
fn geometry_to_geojson_value(
    geom: nodedb_types::geometry::Geometry,
    ctor: SpatialConstructor,
) -> Result<SqlValue> {
    let json = sonic_rs::to_string(&geom).map_err(|e| SqlError::InvalidFunction {
        detail: format!("{}: failed to serialize geometry: {e}", ctor.display_name()),
    })?;
    Ok(SqlValue::String(json))
}

/// Decode an even-length ASCII hex string into bytes (for a hex-string WKB
/// argument passed as a plain string rather than an `X'...'` literal).
/// Delegates the byte-parsing to the shared helper in `resolver::expr::value`
/// so the two hex-decode surfaces don't drift.
fn hex_string_to_bytes(s: &str, ctor: SpatialConstructor) -> Result<Vec<u8>> {
    crate::resolver::expr::value::parse_hex_bytes(s).map_err(|reason| SqlError::InvalidFunction {
        detail: format!("{}: hex WKB {reason}", ctor.display_name()),
    })
}
