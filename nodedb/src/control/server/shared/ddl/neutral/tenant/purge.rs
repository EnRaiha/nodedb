// SPDX-License-Identifier: BUSL-1.1

//! `PURGE TENANT <id|name> CONFIRM` — Data Plane meta op that deletes
//! ALL tenant data across every engine. Superuser-only, requires
//! the literal `CONFIRM` keyword.
//!
//! The tenant reference accepts either a numeric id or a tenant name
//! (single-quoted optional), parallel to `CREATE TENANT <name>` and
//! `SHOW TENANT <name|id>`.
//!
//! Ported verbatim from the pgwire `ddl::tenant::purge` handler, including
//! the `PhysicalPlan::Meta(MetaOp::PurgeTenant)` Data Plane dispatch (300s
//! timeout via `sync_dispatch::dispatch_system`).

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::support::{ddl_err, resolve_tenant_ref, status, tenant_exists};

pub async fn purge_tenant(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can purge tenants",
        ));
    }

    if parts.len() < 4 {
        return Err(ddl_err("42601", "syntax: PURGE TENANT <id|name> CONFIRM"));
    }

    // Accept either a numeric id or a tenant name (mirrors CREATE/SHOW/DROP).
    let tenant_id = resolve_tenant_ref(state, parts[2])?
        .ok_or_else(|| ddl_err("42704", format!("tenant '{}' does not exist", parts[2])))?;
    let tid = tenant_id.as_u64();

    if tid == 0 {
        return Err(ddl_err("42501", "cannot purge system tenant (0)"));
    }

    // Existence gate, uniform across numeric ids and resolved names: refuse to
    // dispatch the destructive meta op for a tenant that does not exist.
    if !tenant_exists(state, tenant_id)? {
        return Err(ddl_err(
            "42704",
            format!("tenant '{}' does not exist", parts[2]),
        ));
    }

    if !parts[3].eq_ignore_ascii_case("CONFIRM") {
        return Err(ddl_err(
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

    match sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::TenantLifecycle,
            tenant_id,
            database_id,
            "__system",
            plan,
        ),
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
            Ok(status("PURGE TENANT"))
        }
        Err(e) => Err(ddl_err("XX000", format!("purge failed: {e}"))),
    }
}
