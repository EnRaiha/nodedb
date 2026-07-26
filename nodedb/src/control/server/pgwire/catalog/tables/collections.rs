// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for catalog row producers: collection loading, field-type →
//! OID mapping, and msgpack row encoding.

use std::collections::HashMap;

use nodedb_types::DatabaseId;
use nodedb_types::Value;
use nodedb_types::columnar::ColumnType;

use crate::control::security::catalog::types::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Encode one catalog row (column-name → value) as msgpack `Value::Object`
/// bytes. The map keys must be the relation's schema column names, matching
/// the order declared in `super::super::schema::catalog_columns`.
pub fn encode_row(row: HashMap<String, Value>) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_msgpack(&Value::Object(row)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: e.to_string(),
    })
}

/// Load the collections visible to `identity` (all active collections for a
/// superuser, tenant-scoped otherwise).
pub fn load_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Vec<StoredCollection> {
    let catalog = state.credentials.catalog();
    if identity.is_superuser {
        catalog
            .load_all_collections(DatabaseId::DEFAULT)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.is_active)
            .collect()
    } else {
        catalog
            .load_collections_for_tenant(DatabaseId::DEFAULT, identity.tenant_id.as_u64())
            .unwrap_or_default()
    }
}

/// True if the collection has at least one secondary index (drives
/// `pg_class.relhasindex`, consistent with what `pg_index` reports).
pub fn has_secondary_index(coll: &StoredCollection) -> bool {
    !coll.indexes.is_empty()
}

/// Map a declared catalog type string to the PostgreSQL type OID that
/// `pg_attribute` / `\d` reports for it.
///
/// Integer and float widths are resolved through
/// [`IntWidth::from_declared_type`](nodedb_types::columnar::IntWidth::from_declared_type)
/// and
/// [`FloatWidth::from_declared_type`](nodedb_types::columnar::FloatWidth::from_declared_type)
/// rather than matched here, so catalog introspection and `RowDescription`
/// cannot disagree about how wide a column is — a split that previously showed
/// up as `\d` reporting OID 23 for a column `SELECT` advertised as 20, and (in
/// the opposite direction) `\d` reporting OID 700 for a bare `FLOAT` column
/// `SELECT` correctly advertised as 701. Bare `FLOAT` is *double precision* in
/// PostgreSQL; only `REAL`/`FLOAT4` are single.
pub fn field_type_to_oid(field_type: &str) -> i64 {
    if let Some(width) = nodedb_types::columnar::IntWidth::from_declared_type(field_type) {
        return i64::from(width.pg_oid());
    }
    if let Some(width) = nodedb_types::columnar::FloatWidth::from_declared_type(field_type) {
        return i64::from(width.pg_oid());
    }

    let normalized = field_type.trim().to_ascii_lowercase();
    // Same boundary rule the numeric width resolvers use — shared so the
    // three matchers cannot drift apart on what counts as a keyword match.
    let starts_with_type =
        |name: &str| nodedb_types::columnar::declared_type_matches(&normalized, name);

    if starts_with_type("timestamp with time zone") || starts_with_type("timestamptz") {
        1184
    } else if starts_with_type("timestamp without time zone") || starts_with_type("timestamp") {
        1114
    } else if starts_with_type("time without time zone") || starts_with_type("time") {
        1083
    } else if starts_with_type("character varying") || starts_with_type("varchar") {
        1043
    } else if starts_with_type("bool") || starts_with_type("boolean") {
        16
    } else if starts_with_type("text") {
        25
    } else if starts_with_type("date") {
        1082
    } else if starts_with_type("uuid") {
        2950
    } else if starts_with_type("jsonb") {
        3802
    } else if starts_with_type("json") {
        114
    } else {
        field_type
            .parse::<ColumnType>()
            .map_or(25, |column_type| column_type.to_pg_oid() as i64)
    }
}

pub fn type_oid_is_collatable(oid: i64) -> bool {
    matches!(oid, 18 | 19 | 25 | 1042 | 1043)
}

#[cfg(test)]
mod tests {
    use super::field_type_to_oid;
    use nodedb_types::columnar::{FloatWidth, IntWidth};

    /// `\d` must report exactly the OID the SELECT path advertises for the
    /// same declared type. Both sides resolve through the same width types, so
    /// this test compares against those rather than restating the numbers.
    #[test]
    fn float_types_report_the_width_resolvers_oid() {
        for declared in ["REAL", "FLOAT4", "FLOAT32", "real not null", "FLOAT(24)"] {
            assert_eq!(
                field_type_to_oid(declared),
                i64::from(FloatWidth::F32.pg_oid()),
                "{declared} must report the single-precision OID"
            );
        }
        for declared in [
            "FLOAT",
            "FLOAT8",
            "FLOAT64",
            "DOUBLE",
            "DOUBLE PRECISION",
            "FLOAT(53)",
        ] {
            assert_eq!(
                field_type_to_oid(declared),
                i64::from(FloatWidth::F64.pg_oid()),
                "{declared} must report the double-precision OID"
            );
        }
    }

    /// Bare `FLOAT` is double precision in PostgreSQL. This arm previously
    /// reported 700, contradicting the SELECT path's 701 for the same column.
    #[test]
    fn bare_float_reports_double_precision() {
        assert_eq!(field_type_to_oid("FLOAT"), 701);
    }

    /// The integer delegation is unchanged by the float one — neither family
    /// may shadow the other.
    #[test]
    fn integer_types_still_report_their_declared_width() {
        assert_eq!(
            field_type_to_oid("SMALLINT"),
            i64::from(IntWidth::I16.pg_oid())
        );
        assert_eq!(field_type_to_oid("INT4"), i64::from(IntWidth::I32.pg_oid()));
        assert_eq!(
            field_type_to_oid("BIGINT"),
            i64::from(IntWidth::I64.pg_oid())
        );
    }

    /// Non-numeric arms are untouched by the deleted float branches.
    #[test]
    fn non_numeric_types_are_unaffected() {
        assert_eq!(field_type_to_oid("TEXT"), 25);
        assert_eq!(field_type_to_oid("BOOL"), 16);
        assert_eq!(field_type_to_oid("timestamptz"), 1184);
        assert_eq!(field_type_to_oid("VARCHAR(20)"), 1043);
    }
}
