// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)`.
//!
//! Loads the tenant's stored `QuotaRecord` (or `QuotaRecord::DEFAULT`), merges
//! the partial spec, validates the result, and persists it to
//! `_system.tenant_quotas`.
//!
//! Ported verbatim from the pgwire `ddl::tenant::alter_quota` handler. The
//! `require_tenant_admin` gate is byte-identical to the pgwire
//! `types::privilege::require_tenant_admin` gate used here originally, so it
//! is reused directly from `neutral::database::gate` rather than duplicated.

use nodedb_sql::ddl_ast::AlterTenantOperation;
use nodedb_types::QuotaRecord;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::database::gate::require_tenant_admin;
use super::support::{ddl_err, status};

/// Handle `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)`.
pub fn handle_alter_tenant_quota(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database: &str,
    operation: &AlterTenantOperation,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter tenant quota")?;

    let catalog = state.credentials.catalog();

    // Resolve database name → id.
    let db_id = catalog
        .get_database_id_by_name(database)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{database}' does not exist")))?;

    // Resolve tenant name → id via a linear scan of stored tenants.
    let tenants = catalog
        .load_all_tenants()
        .map_err(|e| ddl_err("XX000", format!("tenant load failed: {e}")))?;
    let tenant_id = tenants
        .iter()
        .find(|t| t.name == name)
        .map(|t| TenantId::new(t.tenant_id))
        .ok_or_else(|| ddl_err("42704", format!("tenant '{name}' does not exist")))?;

    let AlterTenantOperation::SetQuota(spec) = operation;

    // Load existing record (or DEFAULT) and keep a verbatim copy so the audit
    // entry records the exact before/after; the catalog layer enforces the
    // sum-of-tenant-quotas ≤ database-quota invariant on `put_tenant_quota`.
    let before = catalog
        .get_tenant_quota(db_id, tenant_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);
    let mut record = before.clone();
    record.merge(spec);

    catalog
        .put_tenant_quota(db_id, tenant_id, &record)
        .map_err(|e| ddl_err("53400", format!("{e}")))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "ALTER TENANT {name} IN DATABASE {database} SET QUOTA — before: [{}] — after: [{}]",
            before.audit_summary(),
            record.audit_summary()
        ),
    );

    Ok(status("ALTER TENANT"))
}
