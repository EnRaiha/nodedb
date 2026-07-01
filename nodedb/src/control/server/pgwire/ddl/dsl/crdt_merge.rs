// SPDX-License-Identifier: BUSL-1.1

//! `CRDT MERGE INTO` DSL handler.

use std::time::Duration;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

/// CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'
pub async fn crdt_merge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 7 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'",
        ));
    }

    let collection = parts[3];
    let tenant_id = identity.tenant_id;

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| sqlstate_error("42601", "expected FROM keyword"))?;
    let to_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TO"))
        .ok_or_else(|| sqlstate_error("42601", "expected TO keyword"))?;

    let source_id = parts
        .get(from_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| sqlstate_error("42601", "missing source document ID"))?;
    let target_id = parts
        .get(to_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| sqlstate_error("42601", "missing target document ID"))?;

    let source_plan = PhysicalPlan::Crdt(CrdtOp::Read {
        collection: collection.to_string(),
        document_id: source_id.to_string(),
    });

    let source_bytes = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        source_plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if source_bytes.is_empty() {
        return Err(sqlstate_error(
            "02000",
            &format!("source document '{source_id}' not found"),
        ));
    }

    let target_surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, collection, target_id.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let apply_plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.to_string(),
        document_id: target_id.to_string(),
        delta: source_bytes,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate: target_surrogate,
        provenance: None,
        // Server-side merge result, not a replicated peer sync — no fence.
        descriptor_version_required: 0,
    });

    // Route the merge result through the Raft proposer gate so the applied delta
    // is quorum-durable under replication, not lost to followers on failover.
    crate::control::server::sync::raft_dispatch::dispatch_write_replicated(
        state,
        tenant_id,
        database_id,
        collection,
        apply_plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
        crate::event::EventSource::User,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("CRDT merge: {source_id} → {target_id} in '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("CRDT MERGE"))])
}
