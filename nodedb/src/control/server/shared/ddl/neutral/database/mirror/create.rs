// SPDX-License-Identifier: BUSL-1.1

//! Handler for `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = sync | async]`.
//!
//! Ported from the pgwire `ddl::database::mirror::create` handler. The superuser
//! gate, duplicate-name rejection, self-mirror pre-flight, descriptor build with
//! `MirrorStatus::Bootstrapping`, Raft propose / single-node fallback,
//! allocator-hwm flush, and `DatabaseMirrored` audit are preserved verbatim;
//! only the result construction changed from pgwire `Response` to the
//! protocol-neutral [`DdlResult`].

use nodedb_types::{DatabaseId, Lsn, MirrorMode, MirrorOrigin, MirrorStatus};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_superuser;
use super::super::support::{ddl_err, status};

/// Handle `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = ...]`.
///
/// Required role: `Superuser`.
pub fn mirror_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    local_name: &str,
    source_cluster: &str,
    source_database: &str,
    mode: MirrorMode,
) -> Result<Vec<DdlResult>, DdlError> {
    // db_id=None — local mirror does not exist yet at gate time.
    require_superuser(state, identity, None, "MIRROR DATABASE")?;

    let catalog = state.credentials.catalog();

    // Reject if the local name already exists.
    match catalog.get_database_id_by_name(local_name) {
        Ok(Some(_)) => {
            return Err(ddl_err(
                "42P04",
                format!("database '{local_name}' already exists"),
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(ddl_err("XX000", format!("catalog lookup failed: {e}")));
        }
    }

    // Reject self-mirror: a non-empty source_cluster string that is
    // demonstrably "self" can be caught here. The definitive guard is at the
    // QUIC transport layer — the source cluster's handshake handler rejects
    // connections from the same cluster-id. The check here is a best-effort
    // pre-flight that avoids creating a descriptor for an obviously invalid
    // mirror. Empty source_cluster is already rejected by the parser.
    //
    // When the cluster transport is configured and exposes its own cluster-id
    // we compare; otherwise we skip the check (single-node / test mode).
    let own_node_id = state.node_id;
    if source_cluster.parse::<u64>().ok() == Some(own_node_id) {
        return Err(ddl_err(
            "0A000",
            format!(
                "MIRROR DATABASE: source cluster '{source_cluster}' matches this node's id; \
                 self-mirroring is not supported"
            ),
        ));
    }

    // Allocate a DatabaseId for the new mirror.
    let db_id = state.database_registry.alloc_one();
    let created_at_lsn = state.wal.next_lsn().as_u64();

    // The source database numeric id on the source cluster is not known until
    // the bootstrap handshake completes. We store DatabaseId(0) here as the
    // pre-handshake sentinel; the bootstrap process writes the actual id into
    // MirrorOrigin.source_database after receiving the MirrorHelloAck from
    // the source cluster's handshake response.
    let source_db_id = DatabaseId::new(0);

    let mirror_origin = MirrorOrigin {
        source_cluster: source_cluster.to_string(),
        source_database: source_db_id,
        mode,
        last_applied: Lsn::new(0),
        status: MirrorStatus::Bootstrapping {
            bytes_done: 0,
            bytes_total: 0,
        },
    };

    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: local_name.to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(mirror_origin),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };

    // Propose through Raft; fall back to direct write in single-node mode.
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

    // Flush allocator hwm on threshold.
    if state.database_registry.should_flush() {
        let hwm = state.database_registry.current_hwm();
        if let Err(e) = catalog.put_database_hwm(hwm) {
            tracing::warn!("database hwm flush failed after MIRROR DATABASE: {e}");
        }
    }

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseMirrored,
        None,
        Some(db_id),
        &identity.username,
        &format!(
            "MIRROR DATABASE {local_name} FROM {source_cluster}.{source_database} MODE={mode:?}"
        ),
    );

    Ok(status("MIRROR DATABASE"))
}
