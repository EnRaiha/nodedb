// SPDX-License-Identifier: BUSL-1.1

//! Shared coercion helpers for array coordinate and attribute literals.

use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::cell_value::NarrowError;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_sql::types_array::{ArrayAttrLiteral, ArrayCoordLiteral};

pub(super) fn coerce_coords(
    coords: &[ArrayCoordLiteral],
    schema: &ArraySchema,
) -> crate::Result<Vec<CoordValue>> {
    if coords.len() != schema.dims.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "coord arity {} does not match dim count {}",
                coords.len(),
                schema.dims.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(coords.len());
    for (i, c) in coords.iter().enumerate() {
        let dim = &schema.dims[i];
        let v = match (c, dim.dtype) {
            (ArrayCoordLiteral::Int64(n), EngineDimType::Int64) => CoordValue::Int64(*n),
            (ArrayCoordLiteral::Int64(n), EngineDimType::TimestampMs) => {
                CoordValue::TimestampMs(*n)
            }
            (ArrayCoordLiteral::Int64(n), EngineDimType::Float64) => CoordValue::Float64(*n as f64),
            (ArrayCoordLiteral::Float64(f), EngineDimType::Float64) => CoordValue::Float64(*f),
            (ArrayCoordLiteral::String(s), EngineDimType::String) => CoordValue::String(s.clone()),
            (got, want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "coord literal for dim `{}`: got {got:?}, expected dim type {want:?}",
                        dim.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}

pub(super) fn coerce_attrs(
    attrs: &[ArrayAttrLiteral],
    schema: &ArraySchema,
) -> crate::Result<Vec<CellValue>> {
    if attrs.len() != schema.attrs.len() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "attr arity {} does not match attr count {}",
                attrs.len(),
                schema.attrs.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(attrs.len());
    for (i, a) in attrs.iter().enumerate() {
        let spec = &schema.attrs[i];
        // A value past the attr's width is SQLSTATE 22003. A literal of a
        // kind the attr never holds is a plan error.
        let narrow = |r: Result<CellValue, NarrowError>| -> crate::Result<CellValue> {
            r.map_err(|e| {
                let detail = format!("attr `{}`: {e}", spec.name);
                if e.is_out_of_range() {
                    crate::Error::NumericValueOutOfRange { detail }
                } else {
                    crate::Error::PlanError { detail }
                }
            })
        };
        let v = match (a, spec.dtype) {
            (ArrayAttrLiteral::Null, _) if spec.nullable => CellValue::Null,
            (ArrayAttrLiteral::Null, _) => {
                return Err(crate::Error::PlanError {
                    detail: format!("attr `{}` is NOT NULL", spec.name),
                });
            }
            (ArrayAttrLiteral::Int64(n), dtype) => narrow(CellValue::from_int_literal(*n, dtype))?,
            (ArrayAttrLiteral::Float64(f), dtype) => {
                narrow(CellValue::from_float_literal(*f, dtype))?
            }
            (ArrayAttrLiteral::String(s), EngineAttrType::String) => CellValue::String(s.clone()),
            (ArrayAttrLiteral::Bytes(b), EngineAttrType::Bytes) => CellValue::Bytes(b.clone()),
            (got @ (ArrayAttrLiteral::String(_) | ArrayAttrLiteral::Bytes(_)), want) => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "attr literal for `{}`: got {got:?}, expected attr type {want:?}",
                        spec.name
                    ),
                });
            }
        };
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, DimSpec};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "y",
                EngineDimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("b1", EngineAttrType::UInt16, true))
            .attr(AttrSpec::new("ndvi", EngineAttrType::Float32, true))
            .tile_extents(vec![16])
            .build()
            .expect("schema")
    }

    #[test]
    fn literals_narrow_to_the_declared_type() {
        let cells = coerce_attrs(
            &[
                ArrayAttrLiteral::Int64(65535),
                ArrayAttrLiteral::Float64(0.1),
            ],
            &schema(),
        )
        .expect("in range");
        assert_eq!(
            cells,
            vec![CellValue::UInt16(65535), CellValue::Float32(0.1)]
        );
    }

    #[test]
    fn a_value_past_the_width_is_22003() {
        let err = coerce_attrs(
            &[ArrayAttrLiteral::Int64(65536), ArrayAttrLiteral::Null],
            &schema(),
        )
        .expect_err("past UINT16");
        assert!(matches!(err, crate::Error::NumericValueOutOfRange { .. }));
        let err = coerce_attrs(
            &[ArrayAttrLiteral::Float64(1.5), ArrayAttrLiteral::Null],
            &schema(),
        )
        .expect_err("float into UINT16");
        assert!(matches!(err, crate::Error::PlanError { .. }));
    }
}
