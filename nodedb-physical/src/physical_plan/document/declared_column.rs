// SPDX-License-Identifier: Apache-2.0

//! A declared numeric column of a schemaless document or KV collection.
//!
//! These engines store a row as a MessagePack map. The Data Plane re-types
//! every value written to one of these columns by the rule the strict encoder
//! applies, declared integer or float width included. The Control Plane
//! derives the list from the catalog and ships it on `DocumentOp::Register`:
//! from the declared text for a schemaless collection, from the typed schema
//! for a KV collection.

use nodedb_types::columnar::{ColumnDef, ColumnType, FloatWidth, IntWidth};

/// One declared column whose stored value the write path re-types.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DeclaredColumn {
    /// The field name, as the catalog records it.
    pub name: String,
    /// `Int64`, `Float64`, or `Decimal(Some(typmod))`.
    pub column_type: ColumnType,
    /// The declared integer width. `SMALLINT` and `INTEGER` bound the value.
    pub int_width: Option<IntWidth>,
    /// The declared float width. `REAL` refuses a finite value past `f32`.
    pub float_width: Option<FloatWidth>,
}

impl DeclaredColumn {
    /// The column `name` declared as `declared`, the DDL type text with its
    /// modifiers.
    ///
    /// `Some` only for a type that fixes the stored numeric value: an
    /// integer, a float, or a `DECIMAL(p,s)`. Every other declared type is
    /// stored as given and returns `None`.
    pub fn from_declared(name: &str, declared: &str) -> Option<Self> {
        let column_type = ColumnType::from_declared_type(declared)?;
        if !fixes_stored_value(&column_type) {
            return None;
        }
        Some(Self {
            name: name.to_string(),
            column_type,
            int_width: IntWidth::from_declared_type(declared),
            float_width: FloatWidth::from_declared_type(declared),
        })
    }

    /// The typed schema column `column`, with its declared width.
    ///
    /// `Some` only for a column type that fixes the stored numeric value, as
    /// [`Self::from_declared`] decides.
    pub fn from_column_def(column: &ColumnDef) -> Option<Self> {
        fixes_stored_value(&column.column_type).then(|| Self {
            name: column.name.clone(),
            column_type: column.column_type,
            int_width: column.int_width,
            float_width: column.float_width,
        })
    }
}

/// Whether a column of `column_type` fixes the numeric value it stores: an
/// integer, a float, or a `DECIMAL(p,s)`.
fn fixes_stored_value(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Int64 | ColumnType::Float64 | ColumnType::Decimal(Some(_))
    )
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::DecimalTypmod;

    use super::*;

    #[test]
    fn numeric_declarations_resolve_with_their_width() {
        let small = DeclaredColumn::from_declared("n", "SMALLINT NOT NULL").expect("smallint");
        assert_eq!(small.column_type, ColumnType::Int64);
        assert_eq!(small.int_width, Some(IntWidth::I16));

        let real = DeclaredColumn::from_declared("r", "REAL").expect("real");
        assert_eq!(real.column_type, ColumnType::Float64);
        assert_eq!(real.float_width, Some(FloatWidth::F32));

        let typmod = DecimalTypmod::new(5, 2).expect("valid typmod");
        let dec = DeclaredColumn::from_declared("d", "DECIMAL(5, 2)").expect("decimal");
        assert_eq!(dec.column_type, ColumnType::Decimal(Some(typmod)));
    }

    #[test]
    fn schema_column_keeps_its_declared_width() {
        let small = ColumnDef::nullable("n", ColumnType::Int64).with_declared_width("SMALLINT");
        let declared = DeclaredColumn::from_column_def(&small).expect("integer column");
        assert_eq!(declared.int_width, Some(IntWidth::I16));
        assert_eq!(
            declared,
            DeclaredColumn::from_declared("n", "SMALLINT").expect("smallint")
        );
        assert!(
            DeclaredColumn::from_column_def(&ColumnDef::nullable("t", ColumnType::String))
                .is_none()
        );
    }

    #[test]
    fn other_declarations_are_not_retyped() {
        for declared in [
            "TEXT",
            "DECIMAL",
            "BOOL",
            "TIMESTAMP",
            "VECTOR(3)",
            "unknown",
        ] {
            assert!(
                DeclaredColumn::from_declared("c", declared).is_none(),
                "{declared}"
            );
        }
    }
}
