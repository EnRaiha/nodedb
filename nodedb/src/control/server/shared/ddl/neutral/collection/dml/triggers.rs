// SPDX-License-Identifier: BUSL-1.1

//! Trigger-firing helpers of the protocol-neutral INSERT DML handler. Every
//! body joins the statement's transaction, so the statement's write and its
//! bodies' writes commit together. Each helper fires for one row.

use std::collections::HashMap;

use crate::control::server::shared::ddl::result::DdlError;
use crate::control::trigger::SyncFire;

/// Fire the SYNC AFTER INSERT triggers of one row. Each body joins the
/// statement's transaction `fire.txn`.
pub(super) async fn fire_sync_after_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<(), DdlError> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    crate::control::trigger::fire::fire_after_insert(
        crate::control::trigger::fire::FireAfterInsertParams {
            state: fire.state,
            identity: fire.identity,
            database_id: fire.scope.database_id,
            tenant_id: fire.scope.tenant_id,
            collection: coll_name,
            new_fields: fields,
            cascade_depth: fire.cascade_depth,
            mode_filter: Some(TriggerExecutionMode::Sync),
            // A SYNC body stages into the statement's transaction on this
            // node, which routes each write to its vShard leader.
            cross_shard_origin: None,
            on_error: crate::control::trigger::fire_common::FireErrorPolicy::Abort,
            only_trigger: None,
            joined: Some(fire.txn),
        },
    )
    .await
    .into_result()
    .map_err(|e| DdlError::from_error_in_context("trigger error", &e))
}

/// Fire the INSTEAD OF INSERT triggers of one row. `true` when a trigger
/// replaced the row's write: the row counts as processed and is not written.
pub(super) async fn fire_instead_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<bool, DdlError> {
    use crate::control::trigger::fire_instead::InsteadOfResult;
    match crate::control::trigger::fire_instead::fire_instead_of_insert(fire, coll_name, fields)
        .await
    {
        Ok(InsteadOfResult::Handled) => Ok(true),
        Ok(InsteadOfResult::NoTrigger) => Ok(false),
        Err(e) => Err(DdlError::from_error_in_context("trigger error", &e)),
    }
}

/// Fire the BEFORE INSERT triggers of one row. A body can reject the row via
/// RAISE EXCEPTION, or return the row's mutated fields.
pub(super) async fn fire_before_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &HashMap<String, nodedb_types::Value>,
) -> Result<HashMap<String, nodedb_types::Value>, DdlError> {
    crate::control::trigger::fire_before::fire_before_insert(fire, coll_name, fields)
        .await
        .map_err(|e| DdlError::from_error_in_context("BEFORE trigger error", &e))
}
