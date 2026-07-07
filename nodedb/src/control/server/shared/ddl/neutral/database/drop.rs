// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DROP [IF EXISTS] DATABASE <name> [CASCADE | FORCE]`.
//!
//! Ported from the pgwire `ddl::database::drop` handler. The default-database
//! guard, superuser gate, mirror-unsubscribe teardown, clone orphan-protection
//! (reject / force-materialize), cascade collection drop, `DatabaseDropped`
//! audit (emitted before the catalog mutation), Raft propose / single-node
//! fallback, and per-database metrics cleanup are preserved verbatim; only the
//! result construction changed from pgwire `Response` to the protocol-neutral
//! [`DdlResult`].

use nodedb_types::DatabaseId;
use nodedb_types::MirrorStatus;
use nodedb_types::error::sqlstate;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::maintenance::clone_materializer::{
    CloneMaterializerHandle, force_materialize_blocking,
};
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_superuser;
use super::support::{ddl_err, status};

/// Handle `DROP [IF EXISTS] DATABASE <name> [CASCADE | FORCE]`.
///
/// Required role: `Superuser`.
pub fn drop_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
    cascade: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    // `default` is immutable — cannot be dropped.
    if name.eq_ignore_ascii_case("default") {
        return Err(ddl_err(
            sqlstate::CANNOT_DROP_DEFAULT_DATABASE,
            "cannot drop the built-in 'default' database",
        ));
    }

    let catalog = state.credentials.catalog();

    let db_id = match catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
    {
        Some(id) => id,
        None => {
            // If the database does not exist and if_exists=true, no actor to record.
            if if_exists {
                return Ok(status("DROP DATABASE"));
            }
            return Err(ddl_err(
                "3D000",
                format!("database '{name}' does not exist"),
            ));
        }
    };

    // Gate: Superuser required. Resolving db_id first so we can include it in the
    // audit record on denial.
    require_superuser(
        state,
        identity,
        Some(db_id),
        &format!("DROP DATABASE {name}"),
    )?;

    // Guard: `default` identity check by id (rename resilience).
    if db_id == DatabaseId::DEFAULT {
        return Err(ddl_err(
            sqlstate::CANNOT_DROP_DEFAULT_DATABASE,
            "cannot drop the built-in 'default' database",
        ));
    }

    // ── Mirror unsubscribe ────────────────────────────────────────────────────
    //
    // If this database is an active mirror, tear down the cross-cluster observer
    // link before removing local state. On the source side the observer simply
    // stops receiving entries; the source cluster does not need to be notified
    // (this is consistent with the design where promotion is the mirror's local
    // decision, e.g. in a DR scenario where the source is unreachable).
    //
    // The link teardown is best-effort: if the link is already disconnected
    // (e.g. source was unreachable), the drop proceeds anyway. The mirror's
    // catalog state is the authoritative record of whether a subscription exists.
    {
        let descriptor_for_mirror = catalog
            .get_database(db_id)
            .map_err(|e| ddl_err("XX000", format!("catalog read failed: {e}")))?;
        if let Some(descriptor) = descriptor_for_mirror
            && let Some(origin) = descriptor.mirror_origin.as_ref()
            // Promoted mirrors are now standalone writable databases — the
            // observer link was torn down and the mirror_collection_map /
            // mirror_lag rows were cleared at promotion time. Skip the
            // teardown branch so we don't re-delete already-removed rows
            // and don't emit a misleading "subscription teardown" log line.
            && !matches!(origin.status, MirrorStatus::Promoted)
        {
            // Remove the mirror collection map and lag records.
            // These are best-effort; we proceed even if they fail because
            // the descriptor delete below is the authoritative removal.
            if let Err(e) = catalog.delete_mirror_collection_map(db_id) {
                tracing::warn!(
                    db = ?db_id, "DROP DATABASE mirror: failed to remove collection map: {e}"
                );
            }
            if let Err(e) = catalog.delete_mirror_lag(db_id) {
                tracing::warn!(
                    db = ?db_id, "DROP DATABASE mirror: failed to remove lag record: {e}"
                );
            }
            tracing::info!(
                db = ?db_id,
                source_cluster = %origin.source_cluster,
                "DROP DATABASE mirror: observer subscription teardown complete"
            );
        }
    }

    // ── Orphan protection ─────────────────────────────────────────────────────
    //
    // Check whether any live clones depend on this database as their source.
    // If dependents exist and `cascade` is false, reject immediately.
    // If dependents exist and `cascade` is true, block-materialize each one
    // before proceeding.
    let dependent_ids = catalog
        .get_clone_children(db_id)
        .map_err(|e| ddl_err("XX000", format!("lineage check failed: {e}")))?;

    if !dependent_ids.is_empty() {
        if !cascade {
            let id_list: Vec<String> = dependent_ids
                .iter()
                .map(|id| id.as_u64().to_string())
                .collect();
            return Err(ddl_err(
                sqlstate::CLONE_DEPENDENCY,
                format!(
                    "database '{}' cannot be dropped: {} clone(s) depend on it \
                     (database ids: {}); use FORCE or CASCADE to materialize them first",
                    name,
                    dependent_ids.len(),
                    id_list.join(", ")
                ),
            ));
        }

        // FORCE path: block-materialize each dependent clone so it is no longer
        // backed by this source, then proceed with the drop.
        //
        // Crash safety: if the server dies mid-force-drop, the dependents
        // retain their `Materializing { .. }` status and finish on restart.
        // The original DROP command is retried by the caller, which will
        // succeed once the dependents are fully materialized.
        for dep_id in &dependent_ids {
            let handle = CloneMaterializerHandle::new(*dep_id);
            // Blocking materialization on this thread (pgwire DDL handlers
            // execute on a blocking thread pool).
            force_materialize_blocking(*dep_id, state, catalog, Some(&handle)).map_err(
                |e| match e {
                    // Gated until per-engine row copy lands — surface `0A000`
                    // (`feature_not_supported`) so clients know not to retry.
                    crate::Error::BadRequest { detail } => ddl_err("0A000", detail),
                    other => ddl_err(
                        "XX000",
                        format!(
                            "force materialization of dependent clone {} failed: {other}",
                            dep_id.as_u64()
                        ),
                    ),
                },
            )?;
        }
    }

    // ── Cascade: drop all collections ────────────────────────────────────────
    let collections = catalog
        .load_all_collections(db_id)
        .map_err(|e| ddl_err("XX000", format!("catalog scan failed: {e}")))?;

    if !cascade && !collections.is_empty() {
        return Err(ddl_err(
            "2BP01",
            format!(
                "database '{name}' has {} collection(s); \
                 use CASCADE to drop all collections automatically",
                collections.len()
            ),
        ));
    }

    if cascade {
        drop_all_collections_in_database(catalog, db_id, &collections)?;
    }

    // Emit audit BEFORE the catalog mutation so the record is durable even
    // if the catalog delete fails (the database still exists in that case,
    // but the attempt is documented).
    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseDropped,
        None,
        Some(db_id),
        &identity.username,
        &format!("DROP DATABASE {name}"),
    );

    // Propose the delete through Raft; fall back to direct write in single-node mode.
    let proposed = propose_catalog_entry(
        state,
        &CatalogEntry::DeleteDatabase {
            db_id: db_id.as_u64(),
        },
    )
    .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;

    if proposed == 0 {
        catalog
            .delete_database(db_id)
            .map_err(|e| ddl_err("XX000", format!("catalog delete failed: {e}")))?;
    }

    // Remove per-database metrics entries on drop.
    if let Some(m) = &state.system_metrics {
        if let Ok(mut map) = m.database_collections_by_name.write() {
            map.remove(name);
        }
        if let Ok(mut map) = m.database_queries_by_name.write() {
            map.remove(name);
        }
        if let Ok(mut map) = m.database_errors_by_name.write() {
            map.remove(name);
        }
    }

    Ok(status("DROP DATABASE"))
}

/// Drop every collection in `collections` from the catalog under `db_id`.
///
/// On the first failure the cascade aborts and the error is returned to the
/// caller. The descriptor is left intact so retrying the DROP picks up the
/// remaining collections; this is the only way to avoid a half-dropped
/// database where the descriptor is gone but the collection rows persist.
fn drop_all_collections_in_database(
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    collections: &[StoredCollection],
) -> Result<(), DdlError> {
    for coll in collections {
        catalog
            .delete_collection(db_id, coll.tenant_id, &coll.name)
            .map_err(|e| {
                ddl_err(
                    "XX000",
                    format!(
                        "CASCADE DROP DATABASE {}: failed to delete collection '{}': {e}",
                        db_id.as_u64(),
                        coll.name
                    ),
                )
            })?;
    }
    Ok(())
}
