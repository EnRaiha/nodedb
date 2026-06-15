// SPDX-License-Identifier: BUSL-1.1

//! `pg_type` materializer and the canonical built-in type table.

use std::collections::HashMap;

use pgwire::error::PgWireResult;

use crate::control::server::pgwire::pg_catalog::vquery::VTable;
use crate::control::server::pgwire::pg_catalog::vquery::value::{VColumn, VType, VValue};

/// One built-in PostgreSQL type row.
struct PgTypeRow {
    oid: i64,
    name: &'static str,
    len: i32,
    byval: bool,
    /// `b` = base, `A`-category arrays are also `b` in `typtype`.
    typtype: &'static str,
    /// `typcategory`: N numeric, S string, B boolean, D datetime, T timespan,
    /// U user/other, A array.
    category: &'static str,
    /// Element type OID (0 for scalars; the base type OID for arrays).
    elem: i64,
    /// Array type OID whose element is this type (0 for array types).
    array: i64,
}

/// Canonical built-in types (base types followed by their array types). OIDs
/// match PostgreSQL so `::regtype` and driver type caches interoperate.
const TYPES: &[PgTypeRow] = &[
    row(16, "bool", 1, true, "B", 0, 1000),
    row(17, "bytea", -1, false, "U", 0, 1001),
    row(18, "char", 1, true, "Z", 0, 1002),
    row(19, "name", 64, false, "S", 0, 1003),
    row(20, "int8", 8, true, "N", 0, 1016),
    row(21, "int2", 2, true, "N", 0, 1005),
    row(23, "int4", 4, true, "N", 0, 1007),
    row(25, "text", -1, false, "S", 0, 1009),
    row(26, "oid", 4, true, "N", 0, 1028),
    row(114, "json", -1, false, "U", 0, 199),
    row(700, "float4", 4, true, "N", 0, 1021),
    row(701, "float8", 8, true, "N", 0, 1022),
    row(1042, "bpchar", -1, false, "S", 0, 1014),
    row(1043, "varchar", -1, false, "S", 0, 1015),
    row(1082, "date", 4, true, "D", 0, 1182),
    row(1083, "time", 8, true, "D", 0, 1183),
    row(1114, "timestamp", 8, true, "D", 0, 1115),
    row(1184, "timestamptz", 8, true, "D", 0, 1185),
    row(1186, "interval", 16, false, "T", 0, 1187),
    row(1700, "numeric", -1, false, "N", 0, 1231),
    row(2950, "uuid", 16, false, "U", 0, 2951),
    row(3802, "jsonb", -1, false, "U", 0, 3807),
    // Array types.
    row(1000, "_bool", -1, false, "A", 16, 0),
    row(1001, "_bytea", -1, false, "A", 17, 0),
    row(1002, "_char", -1, false, "A", 18, 0),
    row(1003, "_name", -1, false, "A", 19, 0),
    row(1005, "_int2", -1, false, "A", 21, 0),
    row(1007, "_int4", -1, false, "A", 23, 0),
    row(1009, "_text", -1, false, "A", 25, 0),
    row(1016, "_int8", -1, false, "A", 20, 0),
    row(1028, "_oid", -1, false, "A", 26, 0),
    row(199, "_json", -1, false, "A", 114, 0),
    row(1021, "_float4", -1, false, "A", 700, 0),
    row(1022, "_float8", -1, false, "A", 701, 0),
    row(1014, "_bpchar", -1, false, "A", 1042, 0),
    row(1015, "_varchar", -1, false, "A", 1043, 0),
    row(1182, "_date", -1, false, "A", 1082, 0),
    row(1183, "_time", -1, false, "A", 1083, 0),
    row(1115, "_timestamp", -1, false, "A", 1114, 0),
    row(1185, "_timestamptz", -1, false, "A", 1184, 0),
    row(1187, "_interval", -1, false, "A", 1186, 0),
    row(1231, "_numeric", -1, false, "A", 1700, 0),
    row(2951, "_uuid", -1, false, "A", 2950, 0),
    row(3807, "_jsonb", -1, false, "A", 3802, 0),
];

const fn row(
    oid: i64,
    name: &'static str,
    len: i32,
    byval: bool,
    category: &'static str,
    elem: i64,
    array: i64,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        name,
        len,
        byval,
        typtype: "b",
        category,
        elem,
        array,
    }
}

pub fn columns() -> Vec<VColumn> {
    vec![
        VColumn::new("oid", VType::Int8),
        VColumn::new("typname", VType::Text),
        VColumn::new("typnamespace", VType::Int8),
        VColumn::new("typlen", VType::Int4),
        VColumn::new("typbyval", VType::Bool),
        VColumn::new("typtype", VType::Text),
        VColumn::new("typcategory", VType::Text),
        VColumn::new("typispreferred", VType::Bool),
        VColumn::new("typisdefined", VType::Bool),
        VColumn::new("typdelim", VType::Text),
        VColumn::new("typrelid", VType::Int8),
        VColumn::new("typelem", VType::Int8),
        VColumn::new("typarray", VType::Int8),
        VColumn::new("typnotnull", VType::Bool),
    ]
}

pub fn pg_type() -> PgWireResult<VTable> {
    let mut t = VTable::new(columns());
    for r in TYPES {
        t.push(vec![
            VValue::Int8(r.oid),
            VValue::Text(r.name.into()),
            VValue::Int8(11),
            VValue::Int4(r.len),
            VValue::Bool(r.byval),
            VValue::Text(r.typtype.into()),
            VValue::Text(r.category.into()),
            VValue::Bool(false),
            VValue::Bool(true),
            VValue::Text(",".into()),
            VValue::Int8(0),
            VValue::Int8(r.elem),
            VValue::Int8(r.array),
            VValue::Bool(false),
        ]);
    }
    Ok(t)
}

/// Name → OID map for `::regtype` resolution, including common aliases.
pub fn type_oid_map() -> HashMap<String, i64> {
    let mut m: HashMap<String, i64> = TYPES.iter().map(|r| (r.name.to_string(), r.oid)).collect();
    for (alias, oid) in [
        ("integer", 23),
        ("int", 23),
        ("bigint", 20),
        ("smallint", 21),
        ("boolean", 16),
        ("real", 700),
        ("float", 701),
        ("double precision", 701),
        ("double", 701),
        ("character varying", 1043),
        ("character", 1042),
        ("timestamp without time zone", 1114),
        ("timestamp with time zone", 1184),
        ("time without time zone", 1083),
    ] {
        m.insert(alias.to_string(), oid);
    }
    m
}
