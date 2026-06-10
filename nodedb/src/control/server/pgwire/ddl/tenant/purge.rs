// SPDX-License-Identifier: BUSL-1.1

//! `PURGE TENANT <id|name> CONFIRM` — Data Plane meta op that deletes
//! ALL tenant data across every engine. Superuser-only, requires
//! the literal `CONFIRM` keyword.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser {
        return Err(sqlstate_error(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: PURGE TENANT <id|name> CONFIRM",
        ));
    }

    // Accept either a numeric id or a tenant name (mirrors CREATE/SHOW/DROP).
    let tenant_id = super::resolve_tenant_ref(state, parts[2])?
        .ok_or_else(|| sqlstate_error("42704", &format!("tenant '{}' does not exist", parts[2])))?;
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(sqlstate_error("42501", "cannot purge system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: refuse to
    // dispatch the destructive meta op for a tenant that does not exist.
    if !super::tenant_exists(state, tenant_id)? {
        return Err(sqlstate_error(
            "42704",
            &format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(sqlstate_error(
            "42601",
            "PURGE TENANT requires CONFIRM keyword to prevent accidental data destruction",
        ));
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("PURGE TENANT {tid} CONFIRM — deleting all data across all engines"),
    );

    let plan = crate::bridge::envelope::PhysicalPlan::Meta(
        nodedb_physical::physical_plan::MetaOp::PurgeTenant { tenant_id: tid },
    );

    match super::super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        "__system",
        plan,
        std::time::Duration::from_secs(300),
    )
    .await
    {
        Ok(_) => {
            state.audit_record(
                AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!("PURGE TENANT {tid} completed successfully"),
            );
            Ok(vec![Response::Execution(Tag::new("PURGE TENANT"))])
        }
        Err(e) => Err(sqlstate_error("XX000", &format!("purge failed: {e}"))),
    }
}
