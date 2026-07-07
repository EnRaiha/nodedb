// SPDX-License-Identifier: BUSL-1.1

//! Handler for `CREATE [IF NOT EXISTS] DATABASE <name> [WITH (...)]`.
//!
//! Ported from the pgwire `ddl::database::create` handler. The catalog
//! allocation, Raft propose / single-node fallback, allocator-hwm flush,
//! per-database metric registration, and the `DatabaseCreated` audit record are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` to the protocol-neutral [`DdlResult`].

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_cluster_admin;
use super::support::{ddl_err, status};

/// Options accepted in `CREATE DATABASE ... WITH (...)`. Resolving them here
/// up front makes the unknown-key error path explicit and keeps the descriptor
/// builder a pure function of the parsed options.
#[derive(Debug, Default)]
struct CreateDatabaseOptions {
    /// Quota reference id; `0` means "inherit global default".
    quota_id: u64,
}

fn parse_create_options(options: &[(String, String)]) -> Result<CreateDatabaseOptions, DdlError> {
    let mut out = CreateDatabaseOptions::default();
    for (k, v) in options {
        match k.to_ascii_lowercase().as_str() {
            "quota_id" | "quota" => {
                out.quota_id = v.parse::<u64>().map_err(|_| {
                    ddl_err(
                        "22023",
                        format!("CREATE DATABASE: invalid {k}='{v}' (expected unsigned integer)"),
                    )
                })?;
            }
            other => {
                return Err(ddl_err(
                    "0A000",
                    format!("CREATE DATABASE: unsupported WITH option '{other}'"),
                ));
            }
        }
    }
    Ok(out)
}

/// Handle `CREATE [IF NOT EXISTS] DATABASE <name> [WITH (...)]`.
///
/// Required role: `ClusterAdmin` or `Superuser`.
pub fn create_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_not_exists: bool,
    options: &[(String, String)],
) -> Result<Vec<DdlResult>, DdlError> {
    require_cluster_admin(state, identity, None, &format!("CREATE DATABASE {name}"))?;

    let opts = parse_create_options(options)?;

    let catalog = state.credentials.catalog();

    // Check for duplicate name.
    match catalog.get_database_id_by_name(name) {
        Ok(Some(_)) => {
            if if_not_exists {
                return Ok(status("CREATE DATABASE"));
            }
            return Err(ddl_err(
                "42P04",
                format!("database '{name}' already exists"),
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(ddl_err("XX000", format!("catalog lookup failed: {e}")));
        }
    }

    // Allocate a new DatabaseId from the registry (local atomic counter;
    // authoritative proposal via Raft metadata group 0 is wired separately).
    let db_id = state.database_registry.alloc_one();

    // Stamp the descriptor with the next WAL LSN. This is the LSN the very
    // next WAL append on this server would receive; it is monotonically
    // greater than any record observed before this DDL ran and gives the
    // descriptor a well-ordered creation point relative to the WAL.
    let created_at_lsn = state.wal.next_lsn().as_u64();

    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: name.to_string(),
        status: DatabaseStatus::Active,
        created_at_lsn,
        quota_ref: opts.quota_id,
        parent_clone: None,
        mirror_origin: None,
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };

    // Propose through metadata Raft group 0 so all replicas apply the
    // descriptor atomically. In single-node mode `propose_catalog_entry`
    // returns Ok(0) immediately and falls through to the direct write below.
    let proposed = propose_catalog_entry(
        state,
        &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
    )
    .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;

    // Direct write for single-node mode (proposed == 0) or as a fallback
    // when the cluster is in mixed-version compat mode.
    if proposed == 0 {
        catalog
            .put_database(&descriptor)
            .map_err(|e| ddl_err("XX000", format!("catalog write failed: {e}")))?;
    }

    // Flush the allocator hwm on the periodic threshold so restarts
    // pick up the correct next-id boundary.
    if state.database_registry.should_flush() {
        let hwm = state.database_registry.current_hwm();
        if let Err(e) = catalog.put_database_hwm(hwm) {
            tracing::warn!("database hwm flush failed: {e}");
        }
    }

    // Register per-database metric series so the names appear in Prometheus
    // output immediately after creation. Tenants, memory, and storage start
    // at zero and are updated by their respective subsystems.
    if let Some(m) = &state.system_metrics {
        m.set_database_collections(name, 0);
        m.set_database_tenants(name, 0);
        m.set_database_memory_bytes(name, 0);
        m.set_database_storage_bytes(name, 0);
    }

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseCreated,
        None,
        Some(db_id),
        &identity.username,
        &format!("CREATE DATABASE {name}"),
    );

    Ok(status("CREATE DATABASE"))
}
