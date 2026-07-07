// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER DATABASE <name> <operation>`.
//!
//! Ported from the pgwire `ddl::database::alter` handler. Every per-operation
//! privilege gate, catalog read/write, Raft propose / single-node fallback,
//! live-cache / enforcement-component update, and audit record is preserved
//! verbatim; only the result construction changed from pgwire `Response` to the
//! protocol-neutral [`DdlResult`]. `MATERIALIZE` and `PROMOTE` delegate to the
//! `materialize` / `mirror::promote` handlers exactly as before.

use nodedb_sql::ddl_ast::AlterDatabaseOperation;
use nodedb_types::QuotaRecord;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::{require_cluster_admin, require_database_owner};
use super::support::{ddl_err, status};

/// Handle `ALTER DATABASE <name> <operation>`.
///
/// Required role varies by operation (see per-arm gates below).
pub fn alter_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    operation: &AlterDatabaseOperation,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    let mut descriptor = catalog
        .get_database(db_id)
        .map_err(|e| ddl_err("XX000", format!("catalog read failed: {e}")))?
        .ok_or_else(|| ddl_err("XX000", format!("database '{name}' descriptor missing")))?;

    match operation {
        AlterDatabaseOperation::Rename { new_name } => {
            // Required role: DatabaseOwner(db) or Superuser.
            require_database_owner(
                state,
                identity,
                db_id,
                &format!("ALTER DATABASE {name} RENAME"),
            )?;
            // Reject rename if a different database already holds the target name.
            match catalog.get_database_id_by_name(new_name) {
                Ok(Some(existing_id)) if existing_id != db_id => {
                    return Err(ddl_err(
                        "42P04",
                        format!("database '{new_name}' already exists"),
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(ddl_err("XX000", format!("catalog lookup failed: {e}")));
                }
            }
            descriptor.name = new_name.clone();
            let proposed = propose_catalog_entry(
                state,
                &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
            )
            .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;
            if proposed == 0 {
                catalog
                    .put_database(&descriptor)
                    .map_err(|e| ddl_err("XX000", format!("catalog write failed: {e}")))?;
            }

            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::DatabaseRenamed,
                None,
                Some(db_id),
                &identity.username,
                &format!("ALTER DATABASE {name} RENAME TO {new_name}"),
            );
        }

        AlterDatabaseOperation::SetQuota(spec) => {
            // Required role: ClusterAdmin or Superuser.
            require_cluster_admin(
                state,
                identity,
                Some(db_id),
                &format!("ALTER DATABASE {name} SET QUOTA"),
            )?;
            // Load existing record (or DEFAULT) — kept verbatim for the audit
            // before/after diff so operators can reconstruct what changed.
            let before = catalog
                .get_database_quota(db_id)
                .map_err(|e| ddl_err("XX000", format!("quota read failed: {e}")))?
                .unwrap_or(QuotaRecord::DEFAULT);
            let mut record = before.clone();
            record.merge(spec);

            // Snapshot the live cluster-wide ceiling configured at startup
            // from `[server]` config; the catalog layer enforces the
            // sum-of-database-quotas invariant against it.
            let ceiling = state.quota_ceiling_snapshot();
            catalog
                .put_database_quota(db_id, &record, &ceiling)
                .map_err(|e| ddl_err("53400", format!("{e}")))?;

            // Push the new quota into live enforcement components.
            state
                .maintenance_budget
                .set_cap(db_id, record.maintenance_cpu_pct);
            if let Some(ref gov) = state.governor {
                if record.max_memory_bytes > 0 {
                    gov.set_database_budget(db_id, record.max_memory_bytes as usize);
                } else {
                    gov.clear_database_budget(db_id);
                }
            }

            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::DatabaseQuotaChanged,
                None,
                Some(db_id),
                &identity.username,
                &format!(
                    "ALTER DATABASE {name} SET QUOTA — before: [{}] — after: [{}]",
                    before.audit_summary(),
                    record.audit_summary()
                ),
            );
        }

        AlterDatabaseOperation::SetDefault => {
            // Required role: ClusterAdmin or Superuser.
            require_cluster_admin(
                state,
                identity,
                Some(db_id),
                &format!("ALTER DATABASE {name} SET DEFAULT"),
            )?;
            // The per-user default database field lives on AuthenticatedIdentity;
            // the canonical wiring is `ALTER USER <name> SET DEFAULT DATABASE <db>`,
            // which is owned by the user-management DDL path, not this one.
            return Err(ddl_err(
                "0A000",
                "ALTER DATABASE SET DEFAULT is not yet implemented; \
                 use ALTER USER <name> SET DEFAULT DATABASE <db>",
            ));
        }

        AlterDatabaseOperation::SetAuditDml(mode) => {
            // Required role: ClusterAdmin or Superuser.
            require_cluster_admin(
                state,
                identity,
                Some(db_id),
                &format!("ALTER DATABASE {name} SET AUDIT_DML"),
            )?;
            // Update the descriptor's `audit_dml` field and persist it.
            descriptor.audit_dml = *mode;
            let proposed = propose_catalog_entry(
                state,
                &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
            )
            .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;
            if proposed == 0 {
                catalog
                    .put_database(&descriptor)
                    .map_err(|e| ddl_err("XX000", format!("catalog write failed: {e}")))?;
            }

            // Update live cache so the Event Plane consumer sees the new mode
            // without a restart.
            state.audit_dml_cache.set(db_id, *mode);

            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::DatabaseAuditDmlChanged,
                None,
                Some(db_id),
                &identity.username,
                &format!("ALTER DATABASE {name} SET AUDIT_DML = {mode}",),
            );
        }

        AlterDatabaseOperation::SetIdleTimeout(secs) => {
            // Required role: ClusterAdmin or Superuser.
            require_cluster_admin(
                state,
                identity,
                Some(db_id),
                &format!("ALTER DATABASE {name} SET IDLE_TIMEOUT"),
            )?;
            let before = descriptor.idle_session_timeout_secs;
            descriptor.idle_session_timeout_secs = *secs;
            let proposed = propose_catalog_entry(
                state,
                &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
            )
            .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;
            if proposed == 0 {
                catalog
                    .put_database(&descriptor)
                    .map_err(|e| ddl_err("XX000", format!("catalog write failed: {e}")))?;
            }

            // Update the live idle-timeout cache so the sweep loop sees the
            // new value immediately without a restart.
            state.idle_timeout_cache.set(db_id, *secs);

            state.audit_record_with_db(
                crate::control::security::audit::AuditEvent::DatabaseIdleTimeoutChanged,
                None,
                Some(db_id),
                &identity.username,
                &format!("ALTER DATABASE {name} SET IDLE_TIMEOUT = {secs} (was {before})"),
            );
        }

        AlterDatabaseOperation::Materialize => {
            return super::materialize::alter_database_materialize(state, identity, name);
        }

        AlterDatabaseOperation::Promote => {
            return super::mirror::promote::promote_database(state, identity, name);
        }
    }

    Ok(status("ALTER DATABASE"))
}
