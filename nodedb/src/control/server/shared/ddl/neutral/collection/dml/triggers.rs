// SPDX-License-Identifier: BUSL-1.1

//! Trigger-firing helpers shared by the protocol-neutral INSERT/UPSERT DML
//! handlers.
//!
//! Relocated verbatim from the pgwire `ddl::collection::insert_parse` module
//! (now deleted) except for the result type, which is [`DdlError`] /
//! [`DdlResult`] instead of pgwire `Response` / `PgWireResult`.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

/// Fire SYNC AFTER INSERT triggers, returning an error response on failure.
pub(super) async fn fire_sync_after_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
        crate::control::trigger::fire::FireAfterInsertParams {
            state,
            identity,
            database_id,
            tenant_id,
            collection: coll_name,
            new_fields: fields,
            cascade_depth: 0,
            mode_filter: Some(TriggerExecutionMode::Sync),
            // SYNC AFTER triggers fire in the Control-Plane write path, which
            // has no source-write LSN/HWM identity; cross-shard origination for
            // this path is a tracked follow-up.
            cross_shard_origin: None,
        },
    )
    .await
    {
        return Some(Err(ddl_err("XX000", &format!("trigger error: {e}"))));
    }
    None
}

/// Fire SYNC AFTER UPDATE triggers, returning an error response on failure.
///
/// Used by the UPSERT DSL when the probe finds a pre-existing row —
/// without this, AFTER UPDATE subscribers would silently miss overwrite
/// events because the UPSERT handler historically fired only AFTER INSERT.
pub(super) async fn fire_sync_after_update_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    old_fields: &std::collections::HashMap<String, nodedb_types::Value>,
    new_fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire_after::fire_after_update(
        crate::control::trigger::fire_after::FireAfterUpdateParams {
            state,
            identity,
            database_id,
            tenant_id,
            collection: coll_name,
            old_fields,
            new_fields,
            cascade_depth: 0,
            mode_filter: Some(TriggerExecutionMode::Sync),
            // SYNC AFTER triggers run in the Control-Plane write path (no
            // source-write LSN/HWM identity); cross-shard origination here is a
            // tracked follow-up.
            cross_shard_origin: None,
        },
    )
    .await
    {
        return Some(Err(ddl_err("XX000", &format!("trigger error: {e}"))));
    }
    None
}

/// Fire INSTEAD OF INSERT triggers, returning the result.
pub(super) async fn fire_instead_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
    tag: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match crate::control::trigger::fire_instead::fire_instead_of_insert(
        state,
        identity,
        database_id,
        tenant_id,
        coll_name,
        fields,
        0,
    )
    .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            Some(Ok(vec![DdlResult::Status {
                command: tag.to_string(),
                rows_affected: None,
            }]))
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => None,
        Err(e) => Some(Err(ddl_err("XX000", &format!("trigger error: {e}")))),
    }
}

/// Fire BEFORE INSERT triggers, returning mutated fields or an error.
pub(super) async fn fire_before_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, Result<Vec<DdlResult>, DdlError>>
{
    match crate::control::trigger::fire_before::fire_before_insert(
        state,
        identity,
        database_id,
        tenant_id,
        coll_name,
        fields,
        0,
    )
    .await
    {
        Ok(f) => Ok(f),
        Err(e) => Err(Err(ddl_err("XX000", &format!("BEFORE trigger error: {e}")))),
    }
}

fn ddl_err(sqlstate: &str, msg: &str) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: msg.to_string(),
    }
}
