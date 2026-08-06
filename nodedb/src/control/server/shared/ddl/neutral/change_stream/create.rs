// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CHANGE STREAM` DDL handler.
//!
//! Ported from the pgwire `ddl::change_stream::create` handler. All non-return
//! logic (WITH-clause parsing, `ChangeStreamDef` build, `propose_and_apply` +
//! `log_index == 0` local registry refresh, webhook / kafka task startup, and
//! the `audit_record` call) is preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! ```sql
//! CREATE CHANGE STREAM <name> ON <collection|*>
//!   [WITH (FORMAT = 'json'|'msgpack', INCLUDE = 'INSERT,UPDATE,DELETE')]
//! ```

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::state::SharedState;
use crate::event::cdc::stream_def::{
    ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
};
use crate::event::webhook::WebhookConfig;
use crate::types::DatabaseId;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Handle `CREATE CHANGE STREAM <name> ON <collection> [WITH (...)]`
///
/// `with_clause_raw` is the raw text inside the outer `WITH (...)` parens, or empty.
pub fn create_change_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    collection: &str,
    with_clause_raw: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create change streams")?;

    if state.event_plane_budget.should_reject_new_streams() {
        return Err(DdlError {
            sqlstate: "53000".to_string(),
            message: "Event Plane memory budget exceeded — cannot create new change streams. \
             Existing streams continue with reduced retention."
                .to_string(),
        });
    }

    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    if let Ok(Some(_)) = catalog.get_change_stream(database_id, tenant_id, name) {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!("change stream '{name}' already exists"),
        });
    }

    // Parse WITH clause options.
    let kv_pairs: Vec<(String, String)> = if with_clause_raw.is_empty() {
        Vec::new()
    } else {
        extract_key_value_pairs(with_clause_raw)
    };

    let mut op_filter = OpFilter::all();
    let mut format = StreamFormat::Json;
    let mut compaction = CompactionConfig::default();
    let mut webhook = WebhookConfig::default();
    let mut late_data = LateDataPolicy::default();

    for (key, val) in &kv_pairs {
        match key.as_str() {
            "FORMAT" => {
                if let Some(f) = StreamFormat::from_str_opt(val) {
                    format = f;
                }
            }
            "INCLUDE" => {
                op_filter = OpFilter {
                    insert: false,
                    update: false,
                    delete: false,
                };
                for op in val.split(',') {
                    match op.trim().to_uppercase().as_str() {
                        "INSERT" => op_filter.insert = true,
                        "UPDATE" => op_filter.update = true,
                        "DELETE" => op_filter.delete = true,
                        _ => {}
                    }
                }
            }
            "COMPACTION" if val.eq_ignore_ascii_case("key") => {
                compaction.enabled = true;
            }
            "KEY" if !val.is_empty() => {
                compaction.key_field = val.clone();
                compaction.enabled = true;
            }
            "URL" if !val.is_empty() => {
                webhook.url = val.clone();
            }
            "RETRY" => {
                webhook.max_retries = val.parse().unwrap_or(3);
            }
            "TIMEOUT" => {
                let secs = val
                    .strip_suffix('s')
                    .or(Some(val))
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(5);
                webhook.timeout_secs = secs;
            }
            "LATE_DATA" => {
                if let Some(policy) = LateDataPolicy::from_str_opt(val) {
                    late_data = policy;
                }
            }
            _ => {}
        }
    }

    let kafka =
        crate::event::kafka::KafkaDeliveryConfig::from_with_params(&kv_pairs).unwrap_or_default();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock before UNIX epoch".to_string(),
        })?
        .as_secs();

    // Capture the creating principal's roles onto the subscription record.
    // The webhook and Kafka delivery tasks this stream may own run on the
    // Event Plane, where no request identity exists and none may be resolved
    // across the Data→Event bus, so the scope their column redaction is keyed
    // on has to be resolved here and carried by the definition itself.
    let subscriber_roles =
        RequestAuthScope::for_database(identity, state.auth_stores(), database_id)
            .auth()
            .roles
            .clone();

    let def = ChangeStreamDef {
        database_id,
        tenant_id,
        name: name.to_string(),
        collection: collection.to_string(),
        op_filter,
        format,
        retention: RetentionConfig::default(),
        compaction,
        webhook,
        late_data,
        kafka,
        owner: identity.username.clone(),
        created_at: now,
        subscriber_roles,
    };

    let has_webhook = def.webhook.is_configured();
    let webhook_config = def.webhook.clone();
    let kafka_config = def.kafka.clone();

    let entry = crate::control::catalog_entry::CatalogEntry::PutChangeStream(Box::new(def.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        state.stream_registry.register(def.clone());
    }

    if has_webhook {
        state
            .webhook_manager
            .start_task(database_id, tenant_id, name, webhook_config);
    }
    if kafka_config.enabled {
        state
            .kafka_manager
            .start(database_id, tenant_id, name, kafka_config);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE CHANGE STREAM {name} ON {collection}"),
    );

    Ok(status("CREATE CHANGE STREAM"))
}

/// Extract all `KEY = VALUE` pairs from a WITH clause inner string.
fn extract_key_value_pairs(inner: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut pairs = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for ch in inner.chars() {
        if ch == '\'' && !in_quote {
            in_quote = true;
            current.push(ch);
        } else if ch == '\'' && in_quote {
            in_quote = false;
            current.push(ch);
        } else if ch == ',' && !in_quote {
            pairs.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        pairs.push(current);
    }
    for pair in pairs {
        let pair = pair.trim().to_string();
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim().to_uppercase();
            let value = value
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            result.push((key, value));
        }
    }
    result
}
