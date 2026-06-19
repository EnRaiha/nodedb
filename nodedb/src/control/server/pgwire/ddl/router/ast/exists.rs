// SPDX-License-Identifier: BUSL-1.1

//! Existence-check helpers used by IF EXISTS / IF NOT EXISTS guards.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use nodedb_types::DatabaseId;

pub(super) fn collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database_id: DatabaseId,
) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_collection(database_id, tid, name), Ok(Some(_)))
}

pub(super) fn trigger_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_trigger(tid, name), Ok(Some(_)))
}

pub(super) fn schedule_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.schedule_registry.get(tid, name).is_some()
}

pub(super) fn sequence_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.sequence_registry.exists(tid, name)
}

pub(super) fn alert_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state
        .alert_registry
        .get(database_id.as_u64(), tid, name)
        .is_some()
}

pub(super) fn retention_policy_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state
        .retention_policy_registry
        .get(database_id.as_u64(), tid, name)
        .is_some()
}

pub(super) fn change_stream_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.stream_registry.get(tid, name).is_some()
}

pub(super) fn materialized_view_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(tid, name).is_some()
}

pub(super) fn continuous_aggregate_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.mv_registry.get_def(tid, name).is_some()
}
