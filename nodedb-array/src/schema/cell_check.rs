// SPDX-License-Identifier: Apache-2.0

//! Checking a live cell's attribute values against the schema.
//!
//! Every write that stores cells passes this gate, so a stored column
//! holds one value type plus `Null`, and the column codec can rely on it.

use crate::error::{ArrayError, ArrayResult};
use crate::schema::array_schema::ArraySchema;
use crate::types::cell_value::value::CellValue;

impl ArraySchema {
    /// Check one live cell's attrs: one value per attr, each `Null` on a
    /// nullable attr or of the attr's exact declared type.
    pub fn check_cell(&self, attrs: &[CellValue]) -> ArrayResult<()> {
        if attrs.len() != self.attrs.len() {
            return Err(ArrayError::InvalidOp {
                detail: format!(
                    "a cell of '{}' carries {} attrs, the schema has {}",
                    self.name,
                    attrs.len(),
                    self.attrs.len()
                ),
            });
        }
        for (value, spec) in attrs.iter().zip(&self.attrs) {
            match value.attr_type() {
                None if spec.nullable => {}
                None => {
                    return Err(ArrayError::CellTypeMismatch {
                        array: self.name.clone(),
                        attr: spec.name.clone(),
                        detail: "the attribute is NOT NULL".to_string(),
                    });
                }
                Some(got) if got == spec.dtype => {}
                Some(got) => {
                    return Err(ArrayError::CellTypeMismatch {
                        array: self.name.clone(),
                        attr: spec.name.clone(),
                        detail: format!(
                            "declared {}, the cell holds {}",
                            spec.dtype.sql_name(),
                            got.sql_name()
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    /// Check every cell of a write with [`Self::check_cell`]. The first
    /// refused cell refuses the whole write.
    pub fn check_cells<'a>(
        &self,
        cells: impl IntoIterator<Item = &'a [CellValue]>,
    ) -> ArrayResult<()> {
        cells
            .into_iter()
            .try_for_each(|attrs| self.check_cell(attrs))
    }
}

#[cfg(test)]
mod tests {
    use crate::error::ArrayError;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::cell_value::value::CellValue;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> crate::schema::ArraySchema {
        ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("b1", AttrType::UInt16, false))
            .attr(AttrSpec::new("ndvi", AttrType::Float32, true))
            .tile_extents(vec![16])
            .build()
            .expect("schema")
    }

    #[test]
    fn typed_cells_pass() {
        let s = schema();
        let a = [CellValue::UInt16(7), CellValue::Float32(0.5)];
        let b = [CellValue::UInt16(8), CellValue::Null];
        assert!(s.check_cells([&a[..], &b[..]]).is_ok());
    }

    #[test]
    fn a_wider_value_is_refused() {
        let s = schema();
        let a = [CellValue::Int64(7), CellValue::Null];
        assert!(matches!(
            s.check_cell(&a),
            Err(ArrayError::CellTypeMismatch { attr, .. }) if attr == "b1"
        ));
    }

    #[test]
    fn null_on_not_null_and_arity_are_refused() {
        let s = schema();
        assert!(matches!(
            s.check_cell(&[CellValue::Null, CellValue::Null]),
            Err(ArrayError::CellTypeMismatch { .. })
        ));
        assert!(matches!(
            s.check_cell(&[CellValue::UInt16(1)]),
            Err(ArrayError::InvalidOp { .. })
        ));
    }
}
