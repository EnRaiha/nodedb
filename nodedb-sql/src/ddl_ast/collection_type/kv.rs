// SPDX-License-Identifier: Apache-2.0

//! `CollectionType::KeyValue` construction: schema, TTL policy, capacity hint.

use std::str::FromStr;

use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::kv_parsing;

use crate::error::SqlError;

use super::type_str::parse_column_type_str_full;

/// Build a `CollectionType::KeyValue` from pre-parsed column pairs and options.
///
/// Validates:
/// - Exactly one `PRIMARY KEY` column present (detected from the type_str token).
/// - PRIMARY KEY type is a valid hash key.
/// - TTL option is well-formed when present.
/// - Capacity hint is a positive integer when present.
pub(crate) fn build_kv_collection_type(
    columns: &[(String, String)],
    options: &[(String, String)],
) -> Result<nodedb_types::CollectionType, SqlError> {
    // Build ColumnDef list from pre-parsed (name, type_str) pairs.
    // The type_str may contain modifiers like "PRIMARY KEY", "NOT NULL", etc.
    // We strip those to get the bare type, then apply primary_key flag.
    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(columns.len());
    for (name, type_str) in columns {
        let (bare_type, is_pk, not_null, default_expr) = parse_column_type_str_full(type_str);
        let column_type = ColumnType::from_str(&bare_type).map_err(
            |e: nodedb_types::columnar::ColumnTypeParseError| SqlError::Parse {
                detail: format!("column '{}': {}", name, e),
            },
        )?;
        let nullable = !not_null && !is_pk;
        let mut col = if nullable {
            ColumnDef::nullable(name.clone(), column_type)
        } else {
            ColumnDef::required(name.clone(), column_type)
        };
        if is_pk {
            col = col.with_primary_key();
        }
        // Without this the DDL parses, the column is created, and the DEFAULT is
        // dropped on the floor: `ColumnInfo::default` reaches the planner as
        // `None` forever, so an omitted column can never be materialized. The
        // strict-document schema builder keeps it for exactly this reason.
        if let Some(expr) = default_expr {
            col = col.with_default(expr);
        }
        col_defs.push(col);
    }

    // Validate: exactly one PRIMARY KEY column.
    let pk_count = col_defs.iter().filter(|c| c.primary_key).count();
    if pk_count == 0 {
        return Err(SqlError::Parse {
            detail: "KV collections require a PRIMARY KEY column (the hash key)".to_string(),
        });
    }
    if pk_count > 1 {
        return Err(SqlError::Parse {
            detail: "KV collections support exactly one PRIMARY KEY column".to_string(),
        });
    }

    // Validate: PK type must be hashable.
    let pk = col_defs
        .iter()
        .find(|c| c.primary_key)
        .expect("invariant: pk_count validated to be exactly 1 in the checks above");
    if !nodedb_types::is_valid_kv_key_type(&pk.column_type) {
        return Err(SqlError::Parse {
            detail: format!(
                "KV PRIMARY KEY type '{}' is not supported; \
                 use TEXT, UUID, INT, BIGINT, BYTES, or TIMESTAMP",
                pk.column_type
            ),
        });
    }

    let schema = StrictSchema::new(col_defs).map_err(|e| SqlError::Parse {
        detail: e.to_string(),
    })?;

    let ttl = build_kv_ttl(options, &schema)?;
    let capacity_hint = build_kv_capacity(options);

    let config = nodedb_types::KvConfig {
        schema,
        ttl,
        capacity_hint,
        inline_threshold: nodedb_types::KV_DEFAULT_INLINE_THRESHOLD,
    };

    Ok(nodedb_types::CollectionType::KeyValue(config))
}

/// Parse TTL from the options list.
///
/// Looks for a key `ttl` whose value is either:
/// - An INTERVAL literal: `INTERVAL '15 minutes'` → FixedDuration
/// - A field-based expression: `last_active + INTERVAL '1 hour'` → FieldBased
fn build_kv_ttl(
    options: &[(String, String)],
    schema: &StrictSchema,
) -> Result<Option<nodedb_types::KvTtlPolicy>, SqlError> {
    let ttl_val = match options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("ttl"))
        .map(|(_, v)| v.as_str())
    {
        Some(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };

    let expr = ttl_val.trim();

    // Field-based: <field_name> + INTERVAL '...'
    if let Some(plus_pos) = expr.find('+') {
        let field_name = expr[..plus_pos].trim().to_lowercase();
        let interval_part = expr[plus_pos + 1..].trim();

        if !schema.columns.iter().any(|c| c.name == field_name) {
            return Err(SqlError::Parse {
                detail: format!("TTL field '{field_name}' not found in schema"),
            });
        }

        let offset_ms =
            kv_parsing::parse_interval_to_ms(interval_part).map_err(|e| SqlError::Parse {
                detail: e.to_string(),
            })?;

        return Ok(Some(nodedb_types::KvTtlPolicy::FieldBased {
            field: field_name,
            offset_ms,
        }));
    }

    // Fixed duration: INTERVAL '...' or bare short-form.
    let duration_ms = kv_parsing::parse_interval_to_ms(expr).map_err(|e| SqlError::Parse {
        detail: format!("invalid TTL expression '{expr}': {e}"),
    })?;

    Ok(Some(nodedb_types::KvTtlPolicy::FixedDuration {
        duration_ms,
    }))
}

/// Parse optional `capacity` from the options list.
fn build_kv_capacity(options: &[(String, String)]) -> u32 {
    options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("capacity"))
        .and_then(|(_, v)| v.trim().parse::<u32>().ok())
        .unwrap_or(0)
}
