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

/// Like [`sql_data_type_to_ddl_col_type`], but narrows an `Int8` result to the
/// column's declared integer width (`ColumnInfo::int_width`) when the catalog
/// recorded one.
///
/// # Why only `Int8` is narrowed
///
/// `SqlDataType::Int64` is the single planner-facing type for every integer
/// width — `SMALLINT`/`INT2`, `INTEGER`/`INT4`, and `BIGINT`/`INT8` all resolve
/// to it (see `catalog_adapter::type_convert::parse_type_str` and
/// `nodedb_types::columnar::ColumnType::from_str`), because nodedb's storage —
/// columnar, strict, and kv alike — always keeps integers as a full `i64`. The
/// declared width therefore carries no storage meaning; it is authoritative for
/// the wire contract: a client that declared `SMALLINT` expects OID 21 and, in
/// binary format, exactly two bytes. Silently widening every integer to
/// `BIGINT`'s OID 20 breaks ORMs and typed client libraries that trust the
/// advertised OID (issue #217). Every other `SqlDataType` variant passes
/// through unchanged with `width` ignored — there is no other wire-ambiguous
/// case to resolve.
///
/// # Why this takes a resolved width rather than a type string
///
/// The declared width is also enforced on the write path, and the two must
/// agree exactly or the narrowing here would be advertising a contract writes
/// do not honour. Both sides read the same
/// [`IntWidth`](nodedb_types::columnar::IntWidth), resolved once at the catalog
/// boundary by `catalog_adapter::type_convert`, so disagreement is not
/// representable. A `None` width means the catalog has no record of the
/// declared type (for example a planner-synthesized column) and leaves the base
/// `Int8` — the widest, and so the only lossless, fallback.
pub fn sql_data_type_to_ddl_col_type_with_width(
    ty: &nodedb_sql::types_expr::SqlDataType,
    width: Option<nodedb_types::columnar::IntWidth>,
) -> super::types::DdlColType {
    use super::types::DdlColType;
    use nodedb_types::columnar::IntWidth;

    let base = sql_data_type_to_ddl_col_type(ty);
    let (DdlColType::Int8, Some(width)) = (base, width) else {
        return base;
    };

    match width {
        IntWidth::I16 => DdlColType::Int2,
        IntWidth::I32 => DdlColType::Int4,
        IntWidth::I64 => DdlColType::Int8,
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::DdlColType;
    use super::*;
    use nodedb_sql::types_expr::SqlDataType;
    use nodedb_types::columnar::IntWidth;

    /// Each declared width narrows the `Int8` base to its own wire type, and
    /// the mapping matches the OIDs `IntWidth` itself advertises — locking the
    /// two representations of "how wide is this column" against drift.
    #[test]
    fn narrows_int8_to_each_declared_width() {
        let cases: &[(IntWidth, DdlColType, u32)] = &[
            (IntWidth::I16, DdlColType::Int2, 21),
            (IntWidth::I32, DdlColType::Int4, 23),
            (IntWidth::I64, DdlColType::Int8, 20),
        ];
        for (width, expected, expected_oid) in cases {
            assert_eq!(
                sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Int64, Some(*width)),
                *expected,
                "declared width {width:?} must narrow to {expected:?}"
            );
            assert_eq!(
                width.pg_oid(),
                *expected_oid,
                "declared width {width:?} must advertise OID {expected_oid}"
            );
        }
    }

    /// `width = None` — a planner-synthesized column, or one whose declared
    /// type the catalog never recorded — stays at the base `Int8`. `BIGINT` is
    /// the widest integer wire type, so it is the only fallback that cannot
    /// truncate a stored value.
    #[test]
    fn no_declared_width_stays_int8() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Int64, None),
            DdlColType::Int8
        );
    }

    /// Only an `Int8` base is eligible for narrowing — every other
    /// `SqlDataType` passes through `sql_data_type_to_ddl_col_type` exactly,
    /// with `width` ignored even when one is supplied. A width can never
    /// disagree with the planner's resolved `SqlDataType` in practice, but
    /// this proves narrowing is never misapplied to a non-integer column.
    #[test]
    fn passes_through_non_int8_types_untouched() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::String, Some(IntWidth::I16)),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Float64, Some(IntWidth::I32)),
            DdlColType::Float8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Bool, None),
            DdlColType::Bool
        );
    }

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
