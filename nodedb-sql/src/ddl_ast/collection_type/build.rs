// SPDX-License-Identifier: Apache-2.0

//! The `build_collection_type` entry point.
//!
//! Consumes the pre-parsed fields from the typed DDL AST and returns a
//! fully-constructed `CollectionType` plus the columnar schema columns that
//! the caller must use to populate `StoredCollection::fields`.

use crate::error::SqlError;

use super::designated::{find_geom_col, reject_reserved_columns, resolve_time_key};
use super::kv::build_kv_collection_type;
use super::strict::build_strict_schema;

/// Build a `CollectionType` from the pre-parsed DDL fields.
///
/// # Parameters
///
/// - `engine`: value of `engine=` from the WITH clause (already lowercased and
///   validated against the canonical list), or `None` for the caller's default.
/// - `columns`: `(name, type_str)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `bitemporal`: whether the `BITEMPORAL` modifier flag was present.
/// - `default_to_strict`: controls the `None` branch:
///   - `true`  → `None` engine maps to `document_strict` (CREATE TABLE semantics).
///   - `false` → `None` engine maps to `document_schemaless` (CREATE COLLECTION semantics).
///
/// # Returns
///
/// `(collection_type, columnar_schema_columns)`. The second element is only
/// non-empty for `columnar`, `timeseries`, and `spatial` engines; the caller
/// should use it to populate `StoredCollection::fields` when `fields` would
/// otherwise be empty.
pub fn build_collection_type(
    engine: Option<&str>,
    columns: &[(String, String)],
    options: &[(String, String)],
    bitemporal: bool,
    default_to_strict: bool,
) -> Result<(nodedb_types::CollectionType, Vec<(String, String)>), SqlError> {
    let opt_val = |key: &str| -> Option<String> {
        options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    };

    match engine {
        Some("kv") => {
            let ct = build_kv_collection_type(columns, options)?;
            Ok((ct, Vec::new()))
        }
        Some("document_strict") => {
            let schema = build_strict_schema(columns, bitemporal)?;
            Ok((nodedb_types::CollectionType::strict(schema), Vec::new()))
        }
        Some("columnar") => {
            reject_reserved_columns(columns)?;
            Ok((nodedb_types::CollectionType::columnar(), columns.to_vec()))
        }
        Some("timeseries") => {
            reject_reserved_columns(columns)?;
            let partition_by = opt_val("partition_by").unwrap_or_else(|| "1h".to_string());
            let time_key = resolve_time_key(columns)?;
            Ok((
                nodedb_types::CollectionType::timeseries(time_key, partition_by),
                columns.to_vec(),
            ))
        }
        Some("spatial") => {
            reject_reserved_columns(columns)?;
            let geom_col = find_geom_col(columns).ok_or_else(|| SqlError::MissingField {
                field: "geometry column (GEOMETRY type or SPATIAL_INDEX modifier)".to_string(),
                context: "spatial engine".to_string(),
            })?;
            Ok((
                nodedb_types::CollectionType::spatial(geom_col),
                columns.to_vec(),
            ))
        }
        Some("document_schemaless") | Some("vector") => {
            Ok((nodedb_types::CollectionType::document(), Vec::new()))
        }
        None => {
            if default_to_strict {
                let schema = build_strict_schema(columns, bitemporal)?;
                Ok((nodedb_types::CollectionType::strict(schema), Vec::new()))
            } else {
                Ok((nodedb_types::CollectionType::document(), Vec::new()))
            }
        }
        Some(other) => Err(SqlError::Parse {
            detail: format!("internal: unhandled canonical engine '{other}'"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // ── engine name → CollectionType variant ─────────────────────────────

    #[test]
    fn engine_document_schemaless() {
        let (ct, schema_cols) = build_collection_type(
            Some("document_schemaless"),
            &cols(&[("id", "BIGINT")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
        assert!(schema_cols.is_empty());
    }

    #[test]
    fn engine_vector_maps_to_document() {
        let (ct, _) = build_collection_type(
            Some("vector"),
            &cols(&[("emb", "VECTOR(128)")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
    }

    #[test]
    fn engine_document_strict_produces_strict() {
        let (ct, schema_cols) = build_collection_type(
            Some("document_strict"),
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(_))
        ));
        assert!(schema_cols.is_empty());
    }

    #[test]
    fn engine_strict_requires_columns() {
        let err =
            build_collection_type(Some("document_strict"), &[], &[], false, false).unwrap_err();
        assert!(err.to_string().contains("at least one column"), "{err}");
    }

    #[test]
    fn engine_columnar_returns_schema_cols() {
        let input_cols = cols(&[("ts", "TIMESTAMP"), ("val", "FLOAT64")]);
        let (ct, schema_cols) =
            build_collection_type(Some("columnar"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
    }

    #[test]
    fn engine_timeseries_auto_detects_time_key() {
        let input_cols = cols(&[("ts", "TIMESTAMP"), ("val", "FLOAT64")]);
        let (ct, schema_cols) =
            build_collection_type(Some("timeseries"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
        // Verify time_key wired into the type.
        if let nodedb_types::CollectionType::Columnar(profile) = &ct {
            if let nodedb_types::ColumnarProfile::Timeseries { time_key, .. } = profile {
                assert_eq!(time_key, "ts");
            } else {
                panic!("expected Timeseries profile, got {profile:?}");
            }
        }
    }

    #[test]
    fn engine_timeseries_rejects_missing_time_key() {
        let err = build_collection_type(
            Some("timeseries"),
            &cols(&[("val", "FLOAT64")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("time_key"), "{err}");
    }

    #[test]
    fn engine_timeseries_rejects_reserved_column_name() {
        let err = build_collection_type(
            Some("timeseries"),
            &cols(&[("ts", "TIMESTAMP TIME_KEY"), ("_ts_system", "BIGINT")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("_ts_system"), "{err}");
    }

    #[test]
    fn engine_spatial_auto_detects_geom_col() {
        let input_cols = cols(&[("id", "BIGINT"), ("geom", "GEOMETRY")]);
        let (ct, schema_cols) =
            build_collection_type(Some("spatial"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
        if let nodedb_types::CollectionType::Columnar(profile) = &ct {
            if let nodedb_types::ColumnarProfile::Spatial {
                geometry_column, ..
            } = profile
            {
                assert_eq!(geometry_column, "geom");
            } else {
                panic!("expected Spatial profile, got {profile:?}");
            }
        }
    }

    #[test]
    fn engine_spatial_rejects_missing_geom_col() {
        let err = build_collection_type(
            Some("spatial"),
            &cols(&[("id", "BIGINT"), ("val", "FLOAT64")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("geometry column"), "{err}");
    }

    // ── default_to_strict flag ────────────────────────────────────────────

    #[test]
    fn none_engine_default_to_strict_true() {
        let (ct, _) = build_collection_type(
            None,
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            false,
            true,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(_))
        ));
    }

    #[test]
    fn none_engine_default_to_strict_false() {
        let (ct, _) =
            build_collection_type(None, &cols(&[("id", "BIGINT")]), &[], false, false).unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
    }

    // ── bitemporal flag ───────────────────────────────────────────────────

    #[test]
    fn bitemporal_flag_flows_through_strict() {
        let (ct, _) = build_collection_type(
            Some("document_strict"),
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            true,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) =
            &ct
        {
            assert!(schema.bitemporal, "expected bitemporal schema");
        } else {
            panic!("expected Strict variant");
        }
    }

    // ── KV engine ─────────────────────────────────────────────────────────

    #[test]
    fn kv_requires_primary_key() {
        let err = build_collection_type(
            Some("kv"),
            &cols(&[("session_id", "TEXT"), ("data", "BYTES")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("PRIMARY KEY"), "{err}");
    }

    #[test]
    fn kv_correct_construction() {
        let (ct, schema_cols) = build_collection_type(
            Some("kv"),
            &cols(&[("session_id", "TEXT PRIMARY KEY"), ("data", "BYTES")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(schema_cols.is_empty());
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            let pk = config.primary_key_column().unwrap();
            assert_eq!(pk.name, "session_id");
            assert!(pk.primary_key);
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_rejects_invalid_pk_type() {
        let err = build_collection_type(
            Some("kv"),
            &cols(&[("geom_key", "GEOMETRY PRIMARY KEY"), ("val", "TEXT")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not supported"), "{err}");
    }

    #[test]
    fn kv_capacity_option_parsed() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("v", "BYTES")]),
            &opts(&[("capacity", "10000")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(config.capacity_hint, 10000);
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_ttl_fixed_duration() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("v", "BYTES")]),
            &opts(&[("ttl", "INTERVAL '1h'")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(
                config.ttl,
                Some(nodedb_types::KvTtlPolicy::FixedDuration {
                    duration_ms: 3_600_000
                })
            );
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_ttl_field_based() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("last_active", "TIMESTAMP")]),
            &opts(&[("ttl", "last_active + INTERVAL '30m'")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(
                config.ttl,
                Some(nodedb_types::KvTtlPolicy::FieldBased {
                    field: "last_active".to_string(),
                    offset_ms: 1_800_000,
                })
            );
        } else {
            panic!("expected KeyValue");
        }
    }
}
