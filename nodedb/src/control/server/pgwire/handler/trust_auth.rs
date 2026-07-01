// SPDX-License-Identifier: BUSL-1.1

//! Trust-mode username resolution for the pgwire startup path.
//!
//! Split from the handler core so the connection struct + trait impls stay
//! within the file-size budget. The logic runs on the trust startup path
//! (see the pgwire factory) before AuthenticationOk is announced.

use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::config::auth::AuthMode;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthMethod, Role};
use crate::types::TenantId;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Trust-mode username resolution: skips password verification but still
    /// resolves the connecting username against the credential store, matching
    /// PostgreSQL's `trust` method semantics. Unknown users are rejected before
    /// the server sends ReadyForQuery.
    ///
    /// Runs on the trust startup path (see the pgwire factory) after the
    /// startup parameters are saved to the client metadata and before
    /// AuthenticationOk is announced, so an unknown user never reaches
    /// ReadyForQuery. Only reads `client.metadata()` / `client.socket_addr()`,
    /// so `C: ClientInfo` is sufficient.
    pub(crate) async fn resolve_trust_user<C>(&self, client: &C) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        if !matches!(self.auth_mode, AuthMode::Trust) {
            return Ok(());
        }

        let username = client
            .metadata()
            .get("user")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        if self
            .state
            .credentials
            .to_identity(&username, AuthMethod::Trust)
            .is_some()
        {
            return Ok(());
        }

        // Bootstrap: an empty credential store admits the first connecting
        // user as a tenant-1 superuser and persists them so subsequent
        // queries on the same connection (and any reconnect) resolve
        // through the normal strict path.
        if self.state.credentials.is_empty() {
            let _ = self.state.credentials.create_user(
                &username,
                "",
                TenantId::new(1),
                vec![Role::Superuser],
            );
            return Ok(());
        }

        let source = client.socket_addr().to_string();
        self.state.audit_record(
            AuditEvent::AuthFailure,
            None,
            &source,
            &format!("trust auth: user '{username}' does not exist"),
        );
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "28000".to_owned(),
            format!("trust auth: user '{username}' does not exist"),
        ))))
    }
}
