// SPDX-License-Identifier: BUSL-1.1

//! Trigger-firing helpers of the protocol-neutral INSERT DML handler. Every
//! body joins the statement's transaction, so the statement's write and its
//! bodies' writes commit together.

use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::trigger::SyncFire;

/// Fire SYNC AFTER INSERT triggers, returning an error response on failure.
/// Each body joins the statement's transaction `fire.txn`.
pub(super) async fn fire_sync_after_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
    if let Err(e) = crate::control::trigger::fire::fire_after_insert(
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
    {
        return Some(Err(DdlError::from_error_in_context("trigger error", &e)));
    }
    None
}

/// Fire INSTEAD OF INSERT triggers, returning the result.
pub(super) async fn fire_instead_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
    tag: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match crate::control::trigger::fire_instead::fire_instead_of_insert(fire, coll_name, fields)
        .await
    {
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::Handled) => {
            // The INSTEAD OF trigger replaced a single-document statement, so
            // it always stands in for exactly one logical row — never a bare
            // tag (real `psql` cannot parse a bare `INSERT`).
            Some(Ok(vec![DdlResult::Status {
                command: tag.to_string(),
                rows_affected: Some(1),
            }]))
        }
        Ok(crate::control::trigger::fire_instead::InsteadOfResult::NoTrigger) => None,
        Err(e) => Some(Err(DdlError::from_error_in_context("trigger error", &e))),
    }
}

/// Fire BEFORE INSERT triggers, returning mutated fields or an error.
pub(super) async fn fire_before_triggers(
    fire: SyncFire<'_>,
    coll_name: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<std::collections::HashMap<String, nodedb_types::Value>, Result<Vec<DdlResult>, DdlError>>
{
    match crate::control::trigger::fire_before::fire_before_insert(fire, coll_name, fields).await {
        Ok(f) => Ok(f),
        Err(e) => Err(Err(DdlError::from_error_in_context(
            "BEFORE trigger error",
            &e,
        ))),
    }
}
