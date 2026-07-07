// SPDX-License-Identifier: BUSL-1.1

//! Handler for `USE DATABASE <name>` — mid-session database switch.
//!
//! Issues a session reset per the design:
//!   1. Aborts any open transaction.
//!   2. Invalidates all prepared statements for this connection.
//!   3. Rebinds `current_database` to the new database.
//!
//! If the named database does not exist, returns `DATABASE_NOT_FOUND` (3D000).
//! `\c <name>` in the CLI expands to `USE DATABASE <name>`.

use std::net::SocketAddr;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Handle `USE DATABASE <name>`.
///
/// `sessions` must be the per-handler `SessionStore` so the transaction and
/// prepared-statement state for this connection can be reset atomically.
pub fn handle_use_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sessions: &SessionStore,
    addr: &SocketAddr,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let catalog = state.credentials.catalog();

    // Verify the named database exists.
    let db_id = catalog
        .get_database_id_by_name(name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| sqlstate_error("3D000", &format!("database '{name}' does not exist")))?;

    // Enforce `accessible_databases`: reject the switch if the identity does
    // not have access to the target database. Superusers bypass this check.
    if !identity.can_access_database(db_id) {
        use crate::control::security::audit::{ArcAuditEmitter, AuditEmitContext, AuditEmitter};
        let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        emitter.emit(
            crate::control::security::audit::AuditEvent::PermissionDenied,
            &identity.username,
            &format!("USE DATABASE access denied: {name}"),
            AuditEmitContext::new(None, &identity.user_id.to_string(), &identity.username),
        );
        return Err(sqlstate_error(
            "42501",
            &format!("permission denied for database '{name}'"),
        ));
    }

    // Session reset: abort open transaction, invalidate prepared statements.
    sessions.reset_for_database_switch(addr, db_id);

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DdlChange,
        None,
        Some(db_id),
        &identity.username,
        &format!("USE DATABASE {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("USE DATABASE"))])
}
