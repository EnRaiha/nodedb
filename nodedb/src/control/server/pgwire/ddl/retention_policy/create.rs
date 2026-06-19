// SPDX-License-Identifier: BUSL-1.1

//! `CREATE RETENTION POLICY` DDL handler.
//!
//! Syntax:
//! ```sql
//! CREATE RETENTION POLICY <name> ON <collection> (
//!     RAW RETAIN '<duration>',
//!     DOWNSAMPLE TO '<interval>'
//!         AGGREGATE (func(col) [AS alias], ...)
//!         RETAIN '<duration>',
//!     ...
//!     [ARCHIVE TO '<s3_url>']
//! ) [WITH (EVAL_INTERVAL = '<duration>')]
//! ```

use nodedb_types::DatabaseId;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_tenant_admin, sqlstate_error};
use super::RETENTION_POLICIES_CRDT_COLLECTION;
use super::parse::parse_create_retention_policy;

/// Handle `CREATE RETENTION POLICY` from typed AST fields extracted by nodedb-sql parser.
///
/// `body_raw` is the raw text between the outer parentheses.
/// `eval_interval_raw` is the optional EVAL_INTERVAL string from the WITH clause.
pub async fn create_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    collection: &str,
    body_raw: &str,
    eval_interval_raw: Option<&str>,
) -> PgWireResult<Vec<Response>> {
    require_tenant_admin(identity, "create retention policies")?;

    // Reconstruct minimal SQL for the existing complex parser.
    let reconstructed = if let Some(eval) = eval_interval_raw {
        format!(
            "CREATE RETENTION POLICY {name} ON {collection} ({body_raw}) WITH (EVAL_INTERVAL = '{eval}')"
        )
    } else {
        format!("CREATE RETENTION POLICY {name} ON {collection} ({body_raw})")
    };
    let parsed = parse_create_retention_policy(&reconstructed)?;
    let tenant_id = identity.tenant_id.as_u64();

    // Validate collection exists and is timeseries.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(database_id, tenant_id, &parsed.collection) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{}' is not a timeseries collection", parsed.collection),
                ));
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{}' does not exist", parsed.collection),
                ));
            }
        }
    }

    // Check for duplicate policy name.
    if state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, &parsed.name)
        .is_some()
    {
        return Err(sqlstate_error(
            "42710",
            &format!("retention policy '{}' already exists", parsed.name),
        ));
    }

    // Check no other policy already targets this collection.
    if state
        .retention_policy_registry
        .get_for_collection(database_id.as_u64(), tenant_id, &parsed.collection)
        .is_some()
    {
        return Err(sqlstate_error(
            "42710",
            &format!(
                "collection '{}' already has a retention policy",
                parsed.collection
            ),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock error"))?
        .as_secs();

    let def = crate::engine::timeseries::retention_policy::types::RetentionPolicyDef {
        database_id: database_id.as_u64(),
        tenant_id,
        name: parsed.name.clone(),
        collection: parsed.collection.clone(),
        tiers: parsed.tiers,
        auto_tier: false,
        enabled: true,
        eval_interval_ms: parsed.eval_interval_ms,
        owner: identity.username.clone(),
        created_at: now,
    };

    // Persist to catalog.
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_retention_policy(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    // Emit CRDT sync delta for Lite visibility.
    {
        let delta_payload = zerompk::to_msgpack_vec(&def).unwrap_or_default();
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            collection: RETENTION_POLICIES_CRDT_COLLECTION.into(),
            document_id: def.name.clone(),
            payload: delta_payload,
            op: crate::event::crdt_sync::types::DeltaOp::Upsert,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(tenant_id, delta);
    }

    // Register in memory.
    state.retention_policy_registry.register(def.clone());

    // Auto-wire continuous aggregates for each downsample tier.
    if !def.downsample_tiers().is_empty() {
        crate::engine::timeseries::retention_policy::autowire::register_tiers(state, &def)
            .await
            .map_err(|e| {
                // Roll back: remove from registry and catalog on failure.
                state.retention_policy_registry.unregister(
                    database_id.as_u64(),
                    tenant_id,
                    &def.name,
                );
                let _ = catalog.delete_retention_policy(database_id.as_u64(), tenant_id, &def.name);
                sqlstate_error("XX000", &format!("failed to auto-wire aggregates: {e}"))
            })?;
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "CREATE RETENTION POLICY {} ON {}",
            parsed.name, parsed.collection
        ),
    );

    tracing::info!(
        name = parsed.name,
        collection = parsed.collection,
        tiers = parsed.tier_count,
        "retention policy created"
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE RETENTION POLICY",
    ))])
}
