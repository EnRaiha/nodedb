// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `BACKUP DATABASE <name>` and `RESTORE DATABASE <name>`.
//!
//! Ported verbatim from the pgwire typed-AST database router (`database_ops`),
//! where both were inline placeholder arms. Both perform their privilege gate
//! (with the exact db-id resolution + audit-on-denial behaviour) BEFORE the
//! `0A000` (`feature_not_supported`) placeholder return, so the gate side
//! effects are preserved. Only the result type changed from pgwire
//! `PgWireResult` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::{require_database_owner_or_higher, require_superuser};
use super::support::ddl_err;

/// Handle `BACKUP DATABASE <name>`.
///
/// Gate: `DatabaseOwner(db)` or higher before the placeholder return. Resolve
/// db_id first; unknown name returns 3D000, not 42501.
pub fn backup_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();
    let db_id = match catalog.get_database_id_by_name(name) {
        Ok(Some(id)) => id,
        Ok(None) => {
            return Err(ddl_err(
                "3D000",
                format!("database '{name}' does not exist"),
            ));
        }
        Err(e) => {
            return Err(ddl_err("XX000", format!("catalog lookup failed: {e}")));
        }
    };
    require_database_owner_or_higher(state, identity, db_id, &format!("BACKUP DATABASE {name}"))?;
    Err(ddl_err("0A000", "BACKUP DATABASE is not yet implemented"))
}

/// Handle `RESTORE DATABASE <name>`.
///
/// Gate: `Superuser` required before the placeholder return. The target
/// database may not exist yet; if it doesn't, pass db_id=None.
pub fn restore_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let db_id_opt = state
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .ok()
        .flatten();
    require_superuser(
        state,
        identity,
        db_id_opt,
        &format!("RESTORE DATABASE {name}"),
    )?;
    Err(ddl_err("0A000", "RESTORE DATABASE is not yet implemented"))
}
