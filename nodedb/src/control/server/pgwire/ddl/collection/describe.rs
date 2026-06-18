// SPDX-License-Identifier: BUSL-1.1

//! DESCRIBE COLLECTION and SHOW COLLECTIONS DDL.

use nodedb_types::DatabaseId;
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// DESCRIBE <collection> — show fields, types, and schema info.
pub fn describe_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 2 {
        return Err(sqlstate_error("42601", "syntax: DESCRIBE <collection>"));
    }

    let name_lower = parts[1].to_lowercase();
    let name = name_lower.as_str();
    let tenant_id = identity.tenant_id;

    let catalog = match state.credentials.catalog() {
        Some(c) => c,
        None => return Err(sqlstate_error("XX000", "catalog not available")),
    };

    let coll = match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), name) {
        Ok(Some(c)) if c.is_active => c,
        _ => {
            return Err(sqlstate_error(
                "42P01",
                &format!("collection '{name}' not found"),
            ));
        }
    };

    let schema = Arc::new(vec![
        text_field("field"),
        text_field("type"),
        text_field("nullable"),
    ]);

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());

    // Always has an 'id' field.
    encoder
        .encode_field(&"id")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&"TEXT")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&"false")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    rows.push(Ok(encoder.take_row()));

    if coll.fields.is_empty() {
        encoder
            .encode_field(&"document")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"JSON")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"true")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    } else {
        for (field_name, field_type) in &coll.fields {
            encoder
                .encode_field(field_name)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(field_type)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"true")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    // Show storage mode info.
    if coll.collection_type.is_strict()
        || coll.collection_type.is_columnar_family()
        || coll.collection_type.is_kv()
    {
        encoder
            .encode_field(&"__storage")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.collection_type.as_str())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"false")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    // Timeseries-specific info: show collection_type and config.
    if coll.collection_type.is_timeseries() {
        encoder
            .encode_field(&"__collection_type")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"timeseries")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&"false")
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));

        if let Some(config) = coll.get_timeseries_config() {
            for (key, value) in config.as_object().into_iter().flatten() {
                let val_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                encoder
                    .encode_field(&format!("__ts_{key}"))
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&val_str)
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                encoder
                    .encode_field(&"config")
                    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                rows.push(Ok(encoder.take_row()));
            }
        }
    }

    // KV-specific info: show TTL policy and key type.
    if let Some(kv_config) = coll.collection_type.kv_config() {
        if let Some(pk) = kv_config.primary_key_column() {
            encoder
                .encode_field(&"__kv_key")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&format!("{} ({})", pk.name, pk.column_type))
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"false")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
        if let Some(ttl) = &kv_config.ttl {
            let ttl_str = match ttl {
                nodedb_types::KvTtlPolicy::FixedDuration { duration_ms } => {
                    format!("INTERVAL '{duration_ms}ms'")
                }
                nodedb_types::KvTtlPolicy::FieldBased { field, offset_ms } => {
                    format!("{field} + INTERVAL '{offset_ms}ms'")
                }
                _ => "UNKNOWN TTL POLICY".to_string(),
            };
            encoder
                .encode_field(&"__kv_ttl")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&ttl_str)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            encoder
                .encode_field(&"false")
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW COLLECTIONS
///
/// Lists all active collections for the current tenant.
pub fn show_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("owner"),
        int8_field("created_at"),
        text_field("partition_strategy"),
    ]);

    let collections = if let Some(catalog) = state.credentials.catalog() {
        if identity.is_superuser {
            catalog
                .load_all_collections(DatabaseId::DEFAULT)
                .unwrap_or_default()
                .into_iter()
                .filter(|c| c.is_active)
                .collect::<Vec<_>>()
        } else {
            catalog
                .load_collections_for_tenant(DatabaseId::DEFAULT, tenant_id.as_u64())
                .unwrap_or_default()
        }
    } else {
        Vec::new()
    };

    let mut rows = Vec::with_capacity(collections.len());
    let mut encoder = DataRowEncoder::new(schema.clone());

    for coll in &collections {
        encoder
            .encode_field(&coll.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(coll.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&coll.partition_strategy.as_str())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
