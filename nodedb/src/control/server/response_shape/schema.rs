// SPDX-License-Identifier: BUSL-1.1

//! Planner-authoritative output schema types, plus the type mapping from the
//! planner's `SqlDataType` to the response shaper's wire-facing `DdlColType`.
//!
//! Nothing in this module is consumed by existing call sites yet; it is a
//! purely additive foundation for later threading the planner's resolved
//! output schema into response shaping (replacing the SQL-string re-parse
//! path).

/// One output column of a resolved query, as known by the planner.
///
/// `display_name` is the client-facing column label; `lookup_key` is the key
/// used to find the value in the flat row object emitted by the Data Plane
/// (for qualified `table.column` refs this is the full dot-joined form the
/// join executor prefixes). `ty` is the column's resolved type.
#[derive(Clone, Debug)]
pub struct OutputColumn {
    pub display_name: String,
    pub lookup_key: String,
    pub ty: super::types::DdlColType,
}

/// The authoritative output schema of a query, resolved by the planner.
///
/// `columns` is the ordered projected column list. `is_star` marks a
/// `SELECT *` whose concrete columns are only known from the returned rows
/// (id-first union derivation still applies for that case).
#[derive(Clone, Debug, Default)]
pub struct OutputSchema {
    pub columns: Vec<OutputColumn>,
    pub is_star: bool,
}

/// Maps the planner's resolved SQL column type to the response shaper's
/// protocol-neutral wire type.
///
/// Variants with no dedicated wire type yet (`Decimal`, `Uuid`, `Vector`,
/// `Geometry`) fall back to `DdlColType::Text`, preserving today's
/// all-TEXT behavior for those types until a dedicated wire type exists.
pub fn sql_data_type_to_ddl_col_type(
    ty: &nodedb_sql::types_expr::SqlDataType,
) -> super::types::DdlColType {
    use super::types::DdlColType;
    use nodedb_sql::types_expr::SqlDataType;

    match ty {
        SqlDataType::Int64 => DdlColType::Int8,
        SqlDataType::Float64 => DdlColType::Float8,
        SqlDataType::String => DdlColType::Text,
        SqlDataType::Bool => DdlColType::Bool,
        SqlDataType::Bytes => DdlColType::Bytea,
        SqlDataType::Timestamp => DdlColType::Timestamp,
        SqlDataType::Timestamptz => DdlColType::Timestamptz,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Decimal => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Uuid => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Vector(_) => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Geometry => DdlColType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::DdlColType;
    use super::*;
    use nodedb_sql::types_expr::SqlDataType;

    #[test]
    fn maps_every_sql_data_type_variant() {
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Int64),
            DdlColType::Int8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Float64),
            DdlColType::Float8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::String),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Bool),
            DdlColType::Bool
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Bytes),
            DdlColType::Bytea
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Timestamp),
            DdlColType::Timestamp
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Timestamptz),
            DdlColType::Timestamptz
        );
        // Fallback variants: no dedicated wire type, all map to Text.
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Decimal),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Uuid),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Vector(3)),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Geometry),
            DdlColType::Text
        );
    }
}
