// SPDX-License-Identifier: BUSL-1.1

//! Auth handshake handling: the `"auth"` op, trust-mode auto-authentication,
//! database resolution, and JWT expiry extraction.

use crate::types::{DatabaseId, TenantId};

use super::Session;

impl Session {
    /// Resolve the database for this session at the first authenticated request.
    ///
    /// Resolution order:
    /// 1. Explicit database from connection-string or handshake.
    /// 2. Per-user default database from `AuthenticatedIdentity.default_database`
    ///    (set via `ALTER USER <name> SET DEFAULT DATABASE <db>`).
    /// 3. Tenant default database (not yet stored; reserved for future use).
    /// 4. `DatabaseId::DEFAULT` — the built-in `default` database.
    pub(super) fn resolve_database(
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        explicit: Option<DatabaseId>,
    ) -> DatabaseId {
        if let Some(db) = explicit {
            return db;
        }
        if let Some(db) = identity.default_database {
            return db;
        }
        // Tenant default database: not yet stored; falls through to built-in default.
        DatabaseId::DEFAULT
    }

    /// Handle the `"auth"` op: authenticate, resolve/validate the bound
    /// database, register the session, and build the handshake response.
    ///
    /// Must be the first frame on the connection.
    pub(super) async fn handle_auth_frame(
        &mut self,
        body: &serde_json::Value,
    ) -> crate::Result<Vec<u8>> {
        let (identity, warning) = super::super::session_auth::authenticate(
            &self.state,
            &self.auth_mode,
            body,
            &self.peer_addr.to_string(),
        )
        .await?;

        // Optional `"database"` field in the auth payload — bind the session
        // database at handshake time. If absent, falls back to the resolution
        // chain (user-default → tenant-default → DatabaseId::DEFAULT) below.
        let explicit_db = if let Some(db_name) = body["database"].as_str() {
            if db_name.is_empty() {
                None
            } else {
                // Validate the database name against the catalog.
                let resolved = self
                    .state
                    .credentials
                    .catalog()
                    .get_database_id_by_name(db_name)
                    .ok()
                    .flatten();
                match resolved {
                    Some(db_id) => Some(db_id),
                    None => {
                        let msg = format!(
                            r#"{{"status":"error","code":"DATABASE_NOT_FOUND","error":"database '{db_name}' does not exist"}}"#
                        );
                        return Ok(msg.into_bytes());
                    }
                }
            }
        } else {
            None
        };

        let resolved_db = Self::resolve_database(&identity, explicit_db);

        // Enforce accessible_databases at session bind. Superusers bypass
        // this check (can_access_database returns true for all databases).
        if !identity.can_access_database(resolved_db) {
            let msg =
                r#"{"status":"error","code":"ACCESS_DENIED","error":"access denied to database"}"#;
            return Ok(msg.as_bytes().to_vec());
        }

        self.current_database = Some(resolved_db);

        let warning_field = match &warning {
            Some(w) => format!(r#","warning":"{}""#, w.replace('"', "'")),
            None => String::new(),
        };
        let resp = format!(
            r#"{{"status":"ok","username":"{}","tenant_id":{}{}}}"#,
            identity.username,
            identity.tenant_id.as_u64(),
            warning_field
        );

        // For OIDC bearer sessions, decode the JWT exp claim and store it
        // so the idle-sweep loop can enforce token lifetime.
        let token_expiry_ms =
            if identity.auth_method == crate::control::security::identity::AuthMethod::OidcBearer {
                body["token"].as_str().and_then(extract_jwt_exp_ms)
            } else {
                None
            };

        self.register_session(&identity, token_expiry_ms);
        self.identity = Some(identity);
        Ok(resp.into_bytes())
    }

    /// Ensure the session is authenticated before dispatching a non-auth op.
    ///
    /// In trust mode, auto-authenticates as the configured durable principal on
    /// the first frame. Otherwise rejects if no identity is bound yet.
    pub(super) fn ensure_authenticated(&mut self) -> crate::Result<()> {
        if self.identity.is_none() {
            if self.auth_mode == crate::config::auth::AuthMode::Trust {
                let trust_id = super::super::session_auth::configured_trust_identity(&self.state)
                    .ok_or_else(|| crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "configured trust identity is unavailable".into(),
                })?;
                self.register_session(&trust_id, None);
                self.identity = Some(trust_id);
            } else {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: r#"not authenticated. Send {"op":"auth",...} first."#.into(),
                });
            }
        }
        Ok(())
    }
}

/// Decode the `exp` claim from a JWT payload (no signature verification).
///
/// Returns `Some(exp_ms)` where `exp_ms = exp * 1000` (milliseconds since epoch),
/// or `None` if the token is malformed or the exp claim is absent/zero.
fn extract_jwt_exp_ms(token: &str) -> Option<u64> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    let payload_b64 = parts.get(1)?;
    let bytes = crate::control::security::util::base64_url_decode(payload_b64)?;
    let claims: serde_json::Value = sonic_rs::from_slice(&bytes).ok()?;
    let exp = claims["exp"].as_u64()?;
    if exp == 0 {
        None
    } else {
        Some(exp.saturating_mul(1000))
    }
}
