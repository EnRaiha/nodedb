// SPDX-License-Identifier: BUSL-1.1

//! BEFORE / INSTEAD-OF / AFTER trigger dispatch helpers for DML hooks.
//!
//! Every body these fire joins the triggering statement's transaction
//! (`DispatchTriggerParams::txn`): the statement's write and its BEFORE,
//! INSTEAD OF and SYNC AFTER bodies commit in one record.

use std::collections::HashMap;

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::dml_hook::DmlWriteInfo;
use super::fire_after;
use super::fire_before;
use super::fire_common::FireErrorPolicy;
use super::fire_instead::InsteadOfResult;
use super::fire_statement;
use super::registry::DmlEvent;

/// Result of firing BEFORE + INSTEAD OF triggers before dispatch.
pub enum PreDispatchResult {
    /// INSTEAD OF trigger handled the write — skip normal dispatch.
    Handled,
    /// Proceed with dispatch. If a BEFORE trigger mutated the row,
    /// `mutated_fields` contains the new fields to use instead of the original.
    Proceed {
        mutated_fields: Option<HashMap<String, nodedb_types::Value>>,
    },
}

/// Parameters shared by [`fire_pre_dispatch_triggers`] and [`fire_after_row_triggers`].
pub struct DispatchTriggerParams<'a> {
    /// Shared server state (trigger registry, block cache).
    pub state: &'a SharedState,
    /// Caller identity (used unless a trigger is SECURITY DEFINER).
    pub identity: &'a AuthenticatedIdentity,
    /// Database scope for trigger lookup and execution.
    pub database_id: DatabaseId,
    /// Tenant scope for trigger lookup and execution.
    pub tenant_id: TenantId,
    /// The DML write being dispatched.
    pub info: &'a DmlWriteInfo,
    /// The row's prior state, for UPDATE/DELETE (`None` for INSERT).
    pub old_row: &'a Option<HashMap<String, nodedb_types::Value>>,
    /// Current cascade depth, for infinite-loop protection.
    pub cascade_depth: u32,
    /// The triggering statement's transaction, which every body joins.
    pub txn: &'a DmlTxnCtx<'a>,
}

impl<'a> DispatchTriggerParams<'a> {
    /// The joined firing context of these parameters.
    pub fn sync_fire(&self) -> super::SyncFire<'a> {
        super::SyncFire {
            state: self.state,
            identity: self.identity,
            scope: super::TriggerScope {
                database_id: self.database_id,
                tenant_id: self.tenant_id,
            },
            cascade_depth: self.cascade_depth,
            txn: self.txn,
        }
    }
}

/// Fire BEFORE + INSTEAD OF triggers for a point write.
///
/// Returns `PreDispatchResult::Proceed` if the caller dispatches normally.
/// The `mutated_fields` inside can contain fields modified by a BEFORE trigger —
/// the caller MUST use these to patch the task before dispatch.
///
/// Returns `PreDispatchResult::Handled` if an INSTEAD OF trigger handled the write.
///
/// On BEFORE trigger error (RAISE EXCEPTION), the error propagates and
/// the caller aborts the write.
pub async fn fire_pre_dispatch_triggers(
    params: DispatchTriggerParams<'_>,
) -> crate::Result<PreDispatchResult> {
    let fire = params.sync_fire();
    let DispatchTriggerParams { info, old_row, .. } = params;
    let empty = HashMap::new();
    let old_fields = old_row.as_ref().unwrap_or(&empty);
    let new_fields = info.new_fields.as_ref().unwrap_or(&empty);

    // Check INSTEAD OF first — if it handles the write, skip everything else.
    let instead = match info.event {
        DmlEvent::Insert => match info.new_fields {
            Some(ref new_fields) => {
                super::fire_instead::fire_instead_of_insert(fire, &info.collection, new_fields)
                    .await?
            }
            None => InsteadOfResult::NoTrigger,
        },
        DmlEvent::Update => {
            super::fire_instead::fire_instead_of_update(
                fire,
                &info.collection,
                old_fields,
                new_fields,
            )
            .await?
        }
        DmlEvent::Delete => {
            super::fire_instead::fire_instead_of_delete(fire, &info.collection, old_fields).await?
        }
    };
    if matches!(instead, InsteadOfResult::Handled) {
        return Ok(PreDispatchResult::Handled);
    }

    // Fire BEFORE triggers — capture mutated fields from INSERT/UPDATE.
    let mutated_fields = match info.event {
        DmlEvent::Insert => match info.new_fields {
            Some(ref new_fields) => {
                let mutated =
                    fire_before::fire_before_insert(fire, &info.collection, new_fields).await?;
                (mutated != *new_fields).then_some(mutated)
            }
            None => None,
        },
        DmlEvent::Update => {
            let mutated =
                fire_before::fire_before_update(fire, &info.collection, old_fields, new_fields)
                    .await?;
            (mutated != *new_fields).then_some(mutated)
        }
        DmlEvent::Delete => {
            fire_before::fire_before_delete(fire, &info.collection, old_fields).await?;
            None
        }
    };

    Ok(PreDispatchResult::Proceed { mutated_fields })
}

/// Fire SYNC AFTER ROW triggers once a write staged.
///
/// Called once the row's write staged into the statement's transaction.
/// Only fires triggers with `execution_mode = Sync`, and each body joins that
/// transaction. ASYNC triggers are handled by the Event Plane. The statement's
/// SYNC AFTER STATEMENT triggers fire once per statement, through
/// [`fire_after_statement_triggers`].
pub async fn fire_after_row_triggers(params: DispatchTriggerParams<'_>) -> crate::Result<()> {
    let DispatchTriggerParams {
        state,
        identity,
        database_id,
        tenant_id,
        info,
        old_row,
        cascade_depth,
        txn,
    } = params;

    let empty = HashMap::new();

    match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                fire_after::fire_after_insert(fire_after::FireAfterInsertParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection: &info.collection,
                    new_fields,
                    cascade_depth,
                    mode_filter: Some(TriggerExecutionMode::Sync),
                    // A SYNC body stages into the statement's transaction on
                    // this node, which routes each write to its vShard leader.
                    cross_shard_origin: None,
                    on_error: FireErrorPolicy::Abort,
                    only_trigger: None,
                    joined: Some(txn),
                })
                .await
                .into_result()?;
            }
        }
        DmlEvent::Update => {
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            fire_after::fire_after_update(fire_after::FireAfterUpdateParams {
                state,
                identity,
                database_id,
                tenant_id,
                collection: &info.collection,
                old_fields,
                new_fields,
                cascade_depth,
                mode_filter: Some(TriggerExecutionMode::Sync),
                cross_shard_origin: None,
                on_error: FireErrorPolicy::Abort,
                only_trigger: None,
                joined: Some(txn),
            })
            .await
            .into_result()?;
        }
        DmlEvent::Delete => {
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            fire_after::fire_after_delete(fire_after::FireAfterDeleteParams {
                state,
                identity,
                database_id,
                tenant_id,
                collection: &info.collection,
                old_fields,
                cascade_depth,
                mode_filter: Some(TriggerExecutionMode::Sync),
                cross_shard_origin: None,
                on_error: FireErrorPolicy::Abort,
                only_trigger: None,
                joined: Some(txn),
            })
            .await
            .into_result()?;
        }
    }
    Ok(())
}

/// Fire the SYNC AFTER STATEMENT triggers of `event` on `collection` once
/// for a statement. Each body joins the statement's transaction `fire.txn`.
pub async fn fire_after_statement_triggers(
    fire: super::SyncFire<'_>,
    collection: &str,
    event: DmlEvent,
) -> crate::Result<()> {
    fire_statement::fire_after_statement(fire_statement::FireAfterStatementParams {
        state: fire.state,
        identity: fire.identity,
        scope: fire.scope,
        collection,
        event,
        cascade_depth: fire.cascade_depth,
        mode_filter: Some(TriggerExecutionMode::Sync),
        // The SYNC write path has no source-write position to dedup on.
        cross_shard_origin: None,
        on_error: FireErrorPolicy::Abort,
        only_trigger: None,
        joined: Some(fire.txn),
    })
    .await
    .into_result()
}
