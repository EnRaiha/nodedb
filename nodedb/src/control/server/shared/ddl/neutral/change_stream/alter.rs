// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER CHANGE STREAM` DDL handler.
//!
//! Ported from the pgwire `ddl::change_stream::alter` handler. The tenant-admin
//! gate and the action matching are preserved verbatim; only the error
//! construction changed from pgwire `PgWireError` to the protocol-neutral
//! [`DdlError`].
//!
//! Syntax:
//! ```sql
//! ALTER CHANGE STREAM <name> ENABLE
//! ALTER CHANGE STREAM <name> DISABLE
//! ALTER CHANGE STREAM <name> SUSPEND
//! ALTER CHANGE STREAM <name> RESUME
//! ```
//!
//! ENABLE/DISABLE/SUSPEND/RESUME require a `paused` field on
//! `ChangeStreamDef` which is not yet present; those actions return
//! SQLSTATE 0A000 until that field is added.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;

/// Handle `ALTER CHANGE STREAM <name> <action>`.
///
/// Typed entry point called from the AST router.
pub fn alter_change_stream(
    _state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter change streams")?;

    match action {
        "ENABLE" | "DISABLE" | "SUSPEND" | "RESUME" | "PAUSE" => Err(DdlError {
            sqlstate: "0A000".to_string(),
            message: format!(
                "ALTER CHANGE STREAM {name} {action} is not yet supported; \
                     stream pause/resume requires a schema migration to add the \
                     'paused' field to ChangeStreamDef"
            ),
        }),
        _ => Err(DdlError {
            sqlstate: "42601".to_string(),
            message: format!(
                "unknown ALTER CHANGE STREAM action '{action}'; \
                 expected ENABLE, DISABLE, SUSPEND, or RESUME"
            ),
        }),
    }
}
