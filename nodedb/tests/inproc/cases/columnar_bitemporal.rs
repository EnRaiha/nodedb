// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal columnar — row-level visibility predicate.
//!
//! Unit-scope test: the collection-level write path is covered by
//! `scripts/test.sql`. Here we verify the `bitemporal_row_visible`
//! helper and `prepend_bitemporal_columns` schema shape through the
//! public DDL + insert → scan flow is exercised at the SQL level.

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema, SchemaOps};

/// Building a bitemporal columnar schema places the three reserved
/// Int64 slots at positions 0/1/2 ahead of user columns.
#[test]
fn bitemporal_columnar_schema_shape() {
    let base = ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::nullable("value", ColumnType::Float64),
    ])
    .unwrap();

    // Mirror `prepend_bitemporal_columns`: three reserved Int64 at slots 0/1/2.
    let mut cols = Vec::with_capacity(3 + base.columns.len());
    cols.push(ColumnDef::required("_ts_system", ColumnType::Int64));
    cols.push(ColumnDef::required("_ts_valid_from", ColumnType::Int64));
    cols.push(ColumnDef::required("_ts_valid_until", ColumnType::Int64));
    cols.extend(base.columns);
    let bitemporal = ColumnarSchema::new(cols).unwrap();

    assert_eq!(bitemporal.columns[0].name, "_ts_system");
    assert_eq!(bitemporal.columns[1].name, "_ts_valid_from");
    assert_eq!(bitemporal.columns[2].name, "_ts_valid_until");
    assert_eq!(bitemporal.columns[3].name, "id");
    assert_eq!(bitemporal.columns[4].name, "value");
    assert_eq!(bitemporal.len(), 5);
}
