// SPDX-License-Identifier: BUSL-1.1

//! Conversion helpers: `StoredCollection` → planner-facing catalog types.

use nodedb_sql::types::{ColumnInfo, EngineType, SqlDataType};

/// Convert a StoredCollection to engine type, columns, and primary key.
pub(super) fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let columns = schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                    raw_type: None,
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            (EngineType::DocumentStrict, columns, pk)
        }

        CollectionType::Document(DocumentMode::Schemaless) => {
            // Schemaless collections normally key documents off the
            // built-in `id` field, but `CREATE COLLECTION` may have
            // declared an explicit `PRIMARY KEY` column instead (e.g.
            // `sku STRING PRIMARY KEY`); fall back to `id` when absent.
            let pk_name = stored
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string());
            let mut columns = vec![ColumnInfo {
                name: pk_name.clone(),
                data_type: SqlDataType::String,
                nullable: false,
                is_primary_key: true,
                default: None,
                raw_type: None,
            }];
            // Add tracked fields from catalog.
            for (name, type_str) in &stored.fields {
                if name.eq_ignore_ascii_case(&pk_name) {
                    continue;
                }
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    // Mirrors the columnar branch below: the raw declared
                    // type string is the only place the wire-width refiner
                    // (`sql_data_type_to_ddl_col_type_with_raw`) can recover
                    // `SMALLINT`/`INT4` vs `BIGINT` — `parse_type_str`
                    // collapses all of them to the same `SqlDataType::Int64`.
                    raw_type: Some(type_str.clone()),
                });
            }
            (EngineType::DocumentSchemaless, columns, Some(pk_name))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                    raw_type: None,
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            (EngineType::KeyValue, columns, pk)
        }

        CollectionType::Columnar(profile) => {
            let engine = if profile.is_timeseries() {
                EngineType::Timeseries
            } else if profile.is_spatial() {
                EngineType::Spatial
            } else {
                EngineType::Columnar
            };
            let pk_name = "id";
            // If the DDL declared its own `id` field, the synthetic primary key
            // adopts that declared type and is client-supplied — an explicit
            // `id INT PRIMARY KEY` must stay INT rather than being dropped in
            // favor of a String surrogate (which would make every insert fail a
            // type check). With no declared `id`, synthesize a UUID_V7 String
            // surrogate primary key.
            let declared_pk = stored
                .fields
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(pk_name));
            let mut columns = Vec::new();
            if !profile.is_timeseries() {
                let (pk_type, pk_default, pk_raw) = match declared_pk {
                    Some((_, type_str)) => (parse_type_str(type_str), None, Some(type_str.clone())),
                    None => (SqlDataType::String, Some("UUID_V7".into()), None),
                };
                columns.push(ColumnInfo {
                    name: pk_name.into(),
                    data_type: pk_type,
                    nullable: false,
                    is_primary_key: true,
                    default: pk_default,
                    raw_type: pk_raw,
                });
            }
            for (name, type_str) in &stored.fields {
                if !profile.is_timeseries() && name.eq_ignore_ascii_case(pk_name) {
                    continue;
                }
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    raw_type: Some(type_str.clone()),
                });
            }
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some(pk_name.into())
            };
            (engine, columns, pk)
        }
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry | ColumnType::Json => SqlDataType::Bytes,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Timestamptz => SqlDataType::Timestamptz,
        ColumnType::Decimal { .. } => SqlDataType::Decimal,
        ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex | ColumnType::SparseVector => {
            SqlDataType::String
        }
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
            SqlDataType::Bytes
        }
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
        // ColumnType is #[non_exhaustive]; unknown types surface as Bytes
        // until the planner learns about them.
        _ => SqlDataType::Bytes,
    }
}

fn parse_type_str(s: &str) -> SqlDataType {
    let upper = s.to_uppercase();
    // Handle DECIMAL/NUMERIC with optional (p,s) params.
    if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
        return SqlDataType::Decimal;
    }
    match upper.as_str() {
        "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "INT2" => SqlDataType::Int64,
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => SqlDataType::Float64,
        "BOOL" | "BOOLEAN" => SqlDataType::Bool,
        "BYTES" | "BYTEA" | "BLOB" => SqlDataType::Bytes,
        "TIMESTAMP" | "TIMESTAMPTZ" => SqlDataType::Timestamp,
        _ => SqlDataType::String,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::CollectionType;

    use super::{SqlDataType, convert_collection_type, parse_type_str};
    use crate::control::security::catalog::StoredCollection;

    /// `SMALLINT`/`INT2` are valid PostgreSQL wire-width integer keywords
    /// (issue #217) that must resolve to the same `SqlDataType::Int64` arm as
    /// `INT`/`INTEGER`/`INT4`/`INT8`/`BIGINT` — previously they were unlisted
    /// and fell through to the `_ => SqlDataType::String` default, which is
    /// what produced the wire OID 25 (text) bug for `SMALLINT` columns.
    #[test]
    fn parse_type_str_smallint_and_int2_map_to_int64() {
        assert_eq!(parse_type_str("SMALLINT"), SqlDataType::Int64);
        assert_eq!(parse_type_str("INT2"), SqlDataType::Int64);
        // Case-insensitivity, matching every other arm in this function.
        assert_eq!(parse_type_str("smallint"), SqlDataType::Int64);
        assert_eq!(parse_type_str("int2"), SqlDataType::Int64);
    }

    /// A columnar (or spatial, which shares the same non-timeseries
    /// synthetic-PK path) collection whose DDL declares an explicit
    /// `id` field must not surface two `id` columns to the planner —
    /// the synthetic primary-key column and the user-declared field
    /// must collapse into a single entry.
    fn assert_single_id_column(collection_type: CollectionType) {
        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = collection_type;
        stored.fields = vec![
            ("id".to_string(), "STRING".to_string()),
            ("ID".to_string(), "STRING".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];

        let (_, columns, _) = convert_collection_type(&stored);
        let id_count = columns
            .iter()
            .filter(|c| c.name.eq_ignore_ascii_case("id"))
            .count();
        assert_eq!(
            id_count,
            1,
            "expected exactly one `id` column, got: {:?}",
            columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn columnar_declared_id_field_does_not_duplicate_synthetic_pk() {
        assert_single_id_column(CollectionType::columnar());
    }

    #[test]
    fn spatial_declared_id_field_does_not_duplicate_synthetic_pk() {
        assert_single_id_column(CollectionType::spatial("geom"));
    }

    /// A columnar collection that declares an explicitly typed `id` primary
    /// key (`id INT PRIMARY KEY`) must surface that column with the declared
    /// type — not the String surrogate default. Collapsing it to String makes
    /// every integer insert fail a type check.
    #[test]
    fn declared_typed_id_pk_keeps_its_declared_type() {
        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = CollectionType::columnar();
        stored.fields = vec![
            ("id".to_string(), "INT".to_string()),
            ("v".to_string(), "INT".to_string()),
        ];

        let (_, columns, pk) = convert_collection_type(&stored);
        let id_col = columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case("id"))
            .expect("id column present");
        assert!(id_col.is_primary_key, "declared id must remain the pk");
        assert_eq!(
            id_col.data_type,
            SqlDataType::Int64,
            "declared `id INT` pk must keep its INT type, not the String surrogate"
        );
        assert!(
            id_col.default.is_none(),
            "a client-supplied typed id pk must not carry the UUID_V7 surrogate default"
        );
        assert_eq!(pk.as_deref(), Some("id"));
    }
}
