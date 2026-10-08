// SPDX-License-Identifier: BUSL-1.1

//! The text `FieldInfo` builder for fixed text result columns, and the
//! NodeDB-type-name to pgwire `Type` mapping. Typed result columns build
//! their fields through `wire_type::result_field`.

use nodedb_types::columnar::ColumnType;
use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;

/// Build a FieldInfo for a text column in query results.
pub fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::TEXT, FieldFormat::Text)
}

/// Map a NodeDB field type name to a pgwire `Type`.
///
/// Uses `ColumnType::from_str` + `ColumnType::to_pg_oid` as the single
/// authoritative OID mapping. Falls back to `Type::TEXT` only for names that
/// cannot be parsed as a known `ColumnType` (e.g. DataFusion aliases like
/// `"int4"` or `"float8[]"`).
pub fn type_name_to_pgwire(type_name: &str) -> Type {
    // Try to parse via the canonical ColumnType mapping first.
    if let Ok(ct) = type_name.parse::<ColumnType>() {
        return Type::from_oid(ct.to_pg_oid()).unwrap_or(Type::TEXT);
    }
    // Handle DataFusion / legacy aliases that ColumnType::from_str doesn't cover.
    match type_name.to_lowercase().as_str() {
        "int" | "int4" | "integer" => Type::INT4,
        "int2" | "smallint" => Type::INT2,
        "float4" | "real" => Type::FLOAT4,
        "float8" | "double" | "double precision" => Type::FLOAT8,
        "varchar" => Type::VARCHAR,
        "timestamptz" => Type::TIMESTAMPTZ,
        s if s.starts_with("float4[]") => Type::FLOAT4_ARRAY,
        "float8[]" => Type::FLOAT8_ARRAY,
        _ => Type::TEXT,
    }
}
