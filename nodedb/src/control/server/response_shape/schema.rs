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

/// Like [`sql_data_type_to_ddl_col_type`], but refines an `Int8` result using
/// the catalog's declared raw type string (`ColumnInfo::raw_type`), when
/// present.
///
/// # Why only `Int8` is refined
///
/// `SqlDataType::Int64` is the single planner-facing type for every integer
/// width — `SMALLINT`/`INT2`, `INTEGER`/`INT4`, and `BIGINT`/`INT8` all parse
/// to it (see `catalog_adapter::type_convert::parse_type_str` and
/// `nodedb_types::columnar::ColumnType::from_str`), because nodedb's
/// storage — columnar, strict, and kv alike — always keeps integers as a
/// full `i64`. The declared width therefore carries no storage or
/// computation meaning; it is authoritative for exactly one thing: the wire
/// type a pgwire client expects in `RowDescription`. A client that declared
/// `SMALLINT` expects OID 21 (and, for binary-format results, a 2-byte
/// value) — silently widening every integer to `BIGINT`'s OID 20 breaks ORMs
/// and typed client libraries that trust the advertised OID (nodedb upstream
/// issue #217). So this function narrows *display width only*, strictly
/// downstream of `sql_data_type_to_ddl_col_type`'s own type mapping: every
/// other `SqlDataType` variant (`String`, `Float64`, `Bool`, ...) passes
/// through unchanged, with `raw` ignored, because there is no other
/// wire-ambiguous case to resolve.
///
/// # Matching
///
/// `raw` is matched case-insensitively by keyword, using the same
/// prefix-with-boundary logic as
/// `pgwire::catalog::tables::collections::field_type_to_oid` (the
/// already-correct reference implementation this mirrors): `BIGINT`/`INT8`
/// are checked *first* to mirror that reference implementation's ordering,
/// but the order isn't load-bearing — the boundary check itself already
/// rejects `int` as a prefix match of `int8`, since the next character
/// (`8`) is neither `(` nor whitespace. An unrecognized or absent `raw`
/// leaves the base `Int8` untouched — refinement is best-effort, never a
/// hard error.
pub fn sql_data_type_to_ddl_col_type_with_raw(
    ty: &nodedb_sql::types_expr::SqlDataType,
    raw: Option<&str>,
) -> super::types::DdlColType {
    use super::types::DdlColType;

    let base = sql_data_type_to_ddl_col_type(ty);
    let (DdlColType::Int8, Some(raw)) = (base, raw) else {
        return base;
    };

    let normalized = raw.trim().to_ascii_lowercase();
    let starts_with_type = |name: &str| {
        normalized == name
            || normalized.strip_prefix(name).is_some_and(|rest| {
                rest.starts_with('(') || rest.chars().next().is_some_and(char::is_whitespace)
            })
    };

    if starts_with_type("bigint") || starts_with_type("int8") {
        DdlColType::Int8
    } else if starts_with_type("smallint") || starts_with_type("int2") {
        DdlColType::Int2
    } else if starts_with_type("integer") || starts_with_type("int4") || starts_with_type("int") {
        DdlColType::Int4
    } else {
        DdlColType::Int8
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::DdlColType;
    use super::*;
    use nodedb_sql::types_expr::SqlDataType;

    /// `sql_data_type_to_ddl_col_type_with_raw` must narrow an `Int64`
    /// (base `Int8`) column to `Int4`/`Int2` when the catalog's declared raw
    /// type string says the DDL author wrote a narrower integer keyword.
    /// `BIGINT`/`INT8` stay `Int8`. Case-insensitive, matching every other
    /// raw-type match in this codebase (e.g. `field_type_to_oid`).
    #[test]
    fn refine_narrows_int8_by_raw_integer_keyword() {
        let cases: &[(&str, DdlColType)] = &[
            ("BIGINT", DdlColType::Int8),
            ("INT8", DdlColType::Int8),
            ("INTEGER", DdlColType::Int4),
            ("INT", DdlColType::Int4),
            ("INT4", DdlColType::Int4),
            ("SMALLINT", DdlColType::Int2),
            ("INT2", DdlColType::Int2),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                sql_data_type_to_ddl_col_type_with_raw(&SqlDataType::Int64, Some(raw)),
                *expected,
                "raw type {raw:?} must refine to {expected:?}"
            );
            // Case-insensitivity.
            assert_eq!(
                sql_data_type_to_ddl_col_type_with_raw(
                    &SqlDataType::Int64,
                    Some(&raw.to_lowercase())
                ),
                *expected,
                "lowercase raw type {raw:?} must refine to {expected:?}"
            );
        }
    }

    /// A raw string that isn't a recognized integer keyword leaves the base
    /// `Int8` untouched — the refinement is best-effort, never a hard error.
    #[test]
    fn refine_unrecognized_raw_keyword_stays_int8() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_raw(&SqlDataType::Int64, Some("NOT_A_TYPE")),
            DdlColType::Int8
        );
    }

    /// `raw = None` (planner-synthesized columns, e.g. an auto-injected
    /// `id`, carry no raw type) passes through to the unrefined base type.
    #[test]
    fn refine_with_no_raw_type_stays_base() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_raw(&SqlDataType::Int64, None),
            DdlColType::Int8
        );
    }

    /// Only an `Int8` base is eligible for refinement — every other
    /// `SqlDataType` passes through `sql_data_type_to_ddl_col_type` exactly,
    /// with `raw` ignored even when it names an integer keyword (a raw type
    /// string can never disagree with the planner's own resolved
    /// `SqlDataType`, but this proves the fn doesn't misapply integer
    /// narrowing to non-integer columns regardless).
    #[test]
    fn refine_passes_through_non_int8_types_untouched() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_raw(&SqlDataType::String, Some("SMALLINT")),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_raw(&SqlDataType::Float64, None),
            DdlColType::Float8
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
