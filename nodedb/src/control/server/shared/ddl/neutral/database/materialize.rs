// SPDX-License-Identifier: BUSL-1.1

//! Handler for `ALTER DATABASE <name> MATERIALIZE`.
//!
//! Ported from the pgwire `ddl::database::materialize` handler. The catalog
//! lookup, `DatabaseOwner`-or-higher gate, blocking force-materialization (with
//! `BadRequest` → `0A000` mapping), and `DatabaseMaterialized` audit record are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` to the protocol-neutral [`DdlResult`].

use crate::control::maintenance::clone_materializer::{
    CloneMaterializerHandle, force_materialize_blocking,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_database_owner_or_higher;
use super::support::{ddl_err, status};

/// Handle `ALTER DATABASE <name> MATERIALIZE`.
///
/// Required role: `DatabaseOwner(db)`, `ClusterAdmin`, or `Superuser`.
///
/// Forces synchronous full materialization of all clone collections in the
/// named database.  Returns once all collections are in `Materialized` state.
pub fn alter_database_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| ddl_err("3D000", format!("database '{name}' does not exist")))?;

    require_database_owner_or_higher(
        state,
        identity,
        db_id,
        &format!("ALTER DATABASE {name} MATERIALIZE"),
    )?;

    // Build a completion handle so callers can observe progress if needed.
    let handle = CloneMaterializerHandle::new(db_id);

    // Run blocking materialization. The pgwire handler executes on a
    // dedicated blocking thread pool, so this will not starve the Tokio runtime.
    //
    // `BadRequest` from the gating walker is surfaced as SQLSTATE `0A000`
    // (`feature_not_supported`) so clients can distinguish it from generic
    // failures and retry strategy is unambiguous (don't retry — wait for the
    // per-engine bulk-copy implementation to land).
    force_materialize_blocking(db_id, state, catalog, Some(&handle)).map_err(|e| match e {
        crate::Error::BadRequest { detail } => ddl_err("0A000", detail),
        other => ddl_err(
            "XX000",
            format!("clone materialization of '{name}' failed: {other}"),
        ),
    })?;

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseMaterialized,
        None,
        Some(db_id),
        &identity.username,
        &format!("ALTER DATABASE {name} MATERIALIZE"),
    );

    Ok(status("ALTER DATABASE"))
}
