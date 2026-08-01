// SPDX-License-Identifier: BUSL-1.1

//! BEFORE / INSTEAD-OF / AFTER trigger dispatch helpers for DML hooks.

use std::collections::HashMap;

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::dml_hook::DmlWriteInfo;
use super::fire_after;
use super::fire_before;
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

/// Parameters shared by [`fire_pre_dispatch_triggers`] and [`fire_post_dispatch_triggers`].
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
}

/// Fire BEFORE + INSTEAD OF triggers for a point write.
///
/// Returns `PreDispatchResult::Proceed` if the caller should dispatch normally.
/// The `mutated_fields` inside may contain fields modified by a BEFORE trigger —
/// the caller MUST use these to patch the task before dispatch.
///
/// Returns `PreDispatchResult::Handled` if an INSTEAD OF trigger handled the write.
///
/// On BEFORE trigger error (RAISE EXCEPTION), the error propagates and
/// the caller should abort the write.
pub async fn fire_pre_dispatch_triggers(
    params: DispatchTriggerParams<'_>,
) -> crate::Result<PreDispatchResult> {
    let DispatchTriggerParams {
        state,
        identity,
        database_id,
        tenant_id,
        info,
        old_row,
        cascade_depth,
    } = params;

    // Check INSTEAD OF first — if it handles the write, skip everything else.
    match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                match super::fire_instead::fire_instead_of_insert(
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    &info.collection,
                    new_fields,
                    cascade_depth,
                )
                .await?
                {
                    InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                    InsteadOfResult::NoTrigger => {}
                }
            }
        }
        DmlEvent::Update => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            match super::fire_instead::fire_instead_of_update(
                super::fire_instead::InsteadOfUpdateParams {
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    collection: &info.collection,
                    old_fields,
                    new_fields,
                    cascade_depth,
                },
            )
            .await?
            {
                InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                InsteadOfResult::NoTrigger => {}
            }
        }
        DmlEvent::Delete => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            match super::fire_instead::fire_instead_of_delete(
                state,
                identity,
                database_id,
                tenant_id,
                &info.collection,
                old_fields,
                cascade_depth,
            )
            .await?
            {
                InsteadOfResult::Handled => return Ok(PreDispatchResult::Handled),
                InsteadOfResult::NoTrigger => {}
            }
        }
    }

    // Fire BEFORE triggers — capture mutated fields from INSERT/UPDATE.
    let mutated_fields = match info.event {
        DmlEvent::Insert => {
            if let Some(ref new_fields) = info.new_fields {
                let mutated = fire_before::fire_before_insert(
                    state,
                    identity,
                    database_id,
                    tenant_id,
                    &info.collection,
                    new_fields,
                    cascade_depth,
                )
                .await?;
                if mutated != *new_fields {
                    Some(mutated)
                } else {
                    None
                }
            } else {
                None
            }
        }
        DmlEvent::Update => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            let new_fields = info.new_fields.as_ref().unwrap_or(&empty);
            let mutated = fire_before::fire_before_update(
                state,
                identity,
                super::TriggerScope {
                    database_id,
                    tenant_id,
                },
                &info.collection,
                old_fields,
                new_fields,
                cascade_depth,
            )
            .await?;
            if mutated != *new_fields {
                Some(mutated)
            } else {
                None
            }
        }
        DmlEvent::Delete => {
            let empty = HashMap::new();
            let old_fields = old_row.as_ref().unwrap_or(&empty);
            fire_before::fire_before_delete(
                state,
                identity,
                database_id,
                tenant_id,
                &info.collection,
                old_fields,
                cascade_depth,
            )
            .await?;
            None
        }
    };

    Ok(PreDispatchResult::Proceed { mutated_fields })
}

/// Fire SYNC AFTER ROW + SYNC AFTER STATEMENT triggers post-dispatch.
///
/// Called after the Data Plane has committed the write. Only fires triggers
/// with `execution_mode = Sync`. ASYNC triggers are handled by the Event Plane.
pub async fn fire_post_dispatch_triggers(params: DispatchTriggerParams<'_>) -> crate::Result<()> {
    let DispatchTriggerParams {
        state,
        identity,
        database_id,
        tenant_id,
        info,
        old_row,
        cascade_depth,
    } = params;

    let empty = HashMap::new();

    // Fire SYNC AFTER ROW triggers.
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
                    // SYNC post-dispatch triggers run in the Control-Plane write
                    // path (no source-write LSN/HWM identity); cross-shard
                    // origination for this path is a tracked follow-up.
                    cross_shard_origin: None,
                })
                .await?;
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
            })
            .await?;
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
            })
            .await?;
        }
    }

    // Fire SYNC AFTER STATEMENT triggers (once per DML statement, not per row).
    fire_statement::fire_after_statement(
        state,
        identity,
        super::TriggerScope {
            database_id,
            tenant_id,
        },
        &info.collection,
        info.event,
        cascade_depth,
        Some(TriggerExecutionMode::Sync),
    )
    .await?;

    Ok(())
}
