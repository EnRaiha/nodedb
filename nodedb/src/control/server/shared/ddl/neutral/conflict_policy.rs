// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral handlers for CRDT conflict-policy DDL.
//!
//! - `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`
//! - `SHOW CONFLICT POLICY ON <name>`
//!
//! Ported from the pgwire `ddl::conflict_policy` handlers; the Data Plane
//! read-modify-write cycle (`CrdtOp::GetPolicy` / `SetPolicy`), the policy
//! serialization, and the fallback-to-empty behavior are preserved verbatim.
//! Only the result construction changed from pgwire `Response` /
//! `QueryResponse` to the protocol-neutral [`DdlResult`] over [`ShapedRows`];
//! the SQLSTATE codes and messages are unchanged.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use nodedb_crdt::policy::{CollectionPolicy, ConflictPolicy};
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_sql::ddl_ast::alter_ops::{ConflictPolicyKind, ConstraintKindKeyword};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Handle `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`.
///
/// Implements a read-modify-write cycle against the Data Plane:
/// 1. Read the current policy via `CrdtOp::GetPolicy`.
/// 2. Replace the targeted constraint-kind field.
/// 3. Write the updated policy back via `CrdtOp::SetPolicy`.
pub async fn alter_set_on_conflict(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    policy_kind: &ConflictPolicyKind,
    constraint_kind: &ConstraintKindKeyword,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);

    // Step 1: read current policy.
    let get_plan = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
        collection: collection.to_string(),
    });
    let policy_bytes = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        get_plan,
        timeout,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    let mut policy: CollectionPolicy =
        sonic_rs::from_slice(&policy_bytes).map_err(|e| err("XX000", e.to_string()))?;

    // Step 2: apply the partial update.
    let new_conflict_policy = resolve_policy_kind(policy_kind);
    apply_conflict_policy(&mut policy, constraint_kind, new_conflict_policy);

    // Step 3: write back.
    let policy_json = sonic_rs::to_string(&policy).map_err(|e| err("XX000", e.to_string()))?;
    let set_plan = PhysicalPlan::Crdt(CrdtOp::SetPolicy {
        collection: collection.to_string(),
        policy_json,
    });
    crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        set_plan,
        timeout,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String("OK".to_string()));
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![row],
        notice: None,
    })])
}

/// Handle `SHOW CONFLICT POLICY ON <collection>`.
///
/// Returns one row with a single `policy` column containing the JSON-serialized
/// `CollectionPolicy`. Falls back to the ephemeral default when no policy is set.
pub async fn show_conflict_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);

    let plan = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
        collection: collection.to_string(),
    });
    let policy_bytes = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        timeout,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    let columns = vec!["policy".to_string()];
    let column_types = ShapedRows::text_types(1);

    if policy_bytes.is_empty() {
        return Ok(vec![DdlResult::Rows(ShapedRows {
            columns,
            column_types,
            rows: Vec::new(),
            notice: None,
        })]);
    }

    let text = String::from_utf8_lossy(&policy_bytes).into_owned();
    let mut row = Map::new();
    row.insert("policy".to_string(), JsonValue::String(text));
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows: vec![row],
        notice: None,
    })])
}

fn resolve_policy_kind(kind: &ConflictPolicyKind) -> ConflictPolicy {
    match kind {
        ConflictPolicyKind::LastWriterWins => ConflictPolicy::LastWriterWins,
        ConflictPolicyKind::RenameSuffix => ConflictPolicy::RenameSuffix,
        ConflictPolicyKind::CascadeDefer => ConflictPolicy::CascadeDefer {
            max_retries: 3,
            ttl_secs: 300,
        },
        ConflictPolicyKind::EscalateToDlq => ConflictPolicy::EscalateToDlq,
    }
}

fn apply_conflict_policy(
    policy: &mut CollectionPolicy,
    kind: &ConstraintKindKeyword,
    conflict_policy: ConflictPolicy,
) {
    match kind {
        ConstraintKindKeyword::Unique => policy.unique = conflict_policy,
        ConstraintKindKeyword::ForeignKey => policy.foreign_key = conflict_policy,
        ConstraintKindKeyword::NotNull => policy.not_null = conflict_policy,
        ConstraintKindKeyword::Check => policy.check = conflict_policy,
    }
}
