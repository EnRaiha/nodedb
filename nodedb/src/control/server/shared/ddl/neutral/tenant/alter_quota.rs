// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER TENANT <name> IN DATABASE <db> SET QUOTA (...)`.
//!
//! Loads the tenant's stored `QuotaRecord` (or `QuotaRecord::DEFAULT`), merges
//! the partial spec, validates the result, and replicates it to
//! `_system.tenant_quotas` through the metadata raft group. Post-apply pushes
//! the record into live enforcement on every node: the admission registry's
//! tenant connection cap and the memory governor's tenant byte ceiling.
//!
//! The `require_tenant_admin` gate is reused directly from
//! `neutral::database::gate` rather than duplicated.

use nodedb_sql::ddl_ast::AlterTenantOperation;
use nodedb_types::QuotaRecord;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::database::gate::require_tenant_admin;
use super::super::replicate::propose_and_apply;
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
    // sum-of-tenant-quotas ≤ database-quota invariant on `check_tenant_quota`.
    let before = catalog
        .get_tenant_quota(db_id, tenant_id)
        .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
        .unwrap_or(QuotaRecord::DEFAULT);
    let mut record = before.clone();
    record.merge(spec);

    catalog
        .check_tenant_quota(db_id, tenant_id, &record)
        .map_err(|e| ddl_err("53400", format!("{e}")))?;

    // Replicated: every node writes the row and installs the quota in its live
    // enforcement components via post-apply.
    propose_and_apply(
        state,
        &CatalogEntry::PutTenantQuota {
            db_id: db_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            record: Box::new(record.clone()),
        },
        || {
            catalog
                .write_tenant_quota(db_id, tenant_id, &record)
                .map_err(|e| ddl_err("53400", format!("{e}")))?;
            crate::control::catalog_entry::post_apply::quota::put_tenant(
                db_id, tenant_id, &record, state,
            );
            Ok(())
        },
    )?;

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
