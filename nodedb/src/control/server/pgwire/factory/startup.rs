// SPDX-License-Identifier: BUSL-1.1

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use pgwire::api::ClientInfo;
use pgwire::api::auth::StartupHandler;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::control::security::audit::{ArcAuditEmitter, AuditEvent};
use crate::control::security::credential::store::{AuthRejection, ScramLookup};
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::state::SharedState;

use super::super::handler::NodeDbPgHandler;
use super::super::types::sqlstate_error;
use super::provider::NodeDbParameterProvider;

/// Enum dispatch for startup handler — avoids dyn trait object issues.
pub(crate) enum AuthStartup {
    Trust(Arc<NodeDbPgHandler>),
    Scram {
        sasl: Box<pgwire::api::auth::sasl::SASLAuthStartupHandler<NodeDbParameterProvider>>,
        state: Arc<SharedState>,
        /// Handler reference so we can bind the startup `database` param to
        /// the session store after SCRAM succeeds (mirrors the trust path).
        handler: Arc<NodeDbPgHandler>,
    },
}

/// Resolve the pgwire `database` StartupMessage parameter to a `DatabaseId`
/// and bind it to the session store for this connection.
///
/// The key `"database"` is set by clients via `dbname=` or `psql -d <name>`.
/// An absent or empty value leaves `current_database` unset, and the session
/// falls back to the caller's default — the one legitimate meaning of `None`.
///
/// A name that does not resolve is refused here with `3D000`, matching
/// `USE DATABASE`, the native protocol, the HTTP path, and PostgreSQL
/// (`FATAL: database "x" does not exist`). It must not be encoded as `None`:
/// consumers read `None` as "no database selected" and fall back to
/// `DatabaseId::DEFAULT`, so an unresolved name would silently read and write
/// `default` while the client believes it named something else.
///
/// A catalog read failure is refused for the same reason — it says nothing
/// about whether the database exists, so it cannot become a fallback.
fn bind_startup_database<C: pgwire::api::ClientInfo>(
    client: &C,
    session_id: crate::control::server::shared::session::SessionId,
    handler: &NodeDbPgHandler,
) -> PgWireResult<()> {
    let db_name = match client.metadata().get("database") {
        Some(n) if !n.is_empty() => n.clone(),
        _ => return Ok(()),
    };

    let db_id = handler
        .state
        .credentials
        .catalog()
        .get_database_id_by_name(&db_name)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| sqlstate_error("3D000", &format!("database '{db_name}' does not exist")))?;

    handler.sessions.set_current_database(session_id, db_id);
    Ok(())
}

#[async_trait]
impl StartupHandler for AuthStartup {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::sink::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            AuthStartup::Trust(handler) => {
                // Run the handshake with NodeDB's custom parameter provider so
                // trust clients receive `server_version` / `server_version_num`
                // in the startup ParameterStatus burst — pgwire's default noop
                // path would emit its own hardcoded server_version instead. The
                // trust user-gating (unknown user → reject) is preserved and
                // must run before AuthenticationOk is announced.
                if let PgWireFrontendMessage::Startup(ref startup) = message {
                    pgwire::api::auth::protocol_negotiation(client, startup).await?;
                    pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                    // Reject unknown trust users before we announce AuthenticationOk,
                    // then bind the resolved identity for this socket. Trust
                    // identities must remain connection-local, including the
                    // empty-store bootstrap identity.
                    let identity = handler.resolve_trust_user(client)?;
                    handler.sessions.set_identity(handler.session_id, identity);
                    // Before AuthenticationOk, for the same reason the trust
                    // user is: once `finish_authentication` announces
                    // ReadyForQuery the client treats the connection as
                    // established, and a refusal after that point arrives too
                    // late to stop it being used.
                    bind_startup_database(client, handler.session_id, handler)?;
                    pgwire::api::auth::finish_authentication(
                        client,
                        &super::provider::nodedb_parameter_provider(),
                    )
                    .await?;
                }

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();
                handler.state.audit_record(
                    AuditEvent::AuthSuccess,
                    None,
                    &source,
                    &format!("trust auth: {username}"),
                );

                Ok(())
            }
            AuthStartup::Scram {
                sasl,
                state,
                handler,
            } => {
                let was_in_auth = matches!(
                    client.state(),
                    pgwire::api::PgWireConnectionState::AuthenticationInProgress
                );

                let result = sasl.on_startup(client, message).await;

                let username = client
                    .metadata()
                    .get("user")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let source = client.socket_addr().to_string();

                match &result {
                    Ok(())
                        if was_in_auth
                            && matches!(
                                client.state(),
                                pgwire::api::PgWireConnectionState::ReadyForQuery
                            ) =>
                    {
                        // SCRAM succeeded — reset lockout counter and bind database.
                        state.credentials.record_login_success(&username);
                        state.audit_record(
                            AuditEvent::AuthSuccess,
                            None,
                            &source,
                            &format!("SCRAM-SHA-256 auth: {username}"),
                        );
                        // Bind the `database` startup parameter to the session.
                        //
                        // This lands after AuthenticationOk, unlike the trust
                        // path, and deliberately so: the SASL handler owns the
                        // whole exchange through ReadyForQuery, so the only
                        // earlier hook is the Startup message — before the
                        // client has authenticated. Refusing there would tell
                        // an unauthenticated caller which database names
                        // exist. The session is instead torn down by this
                        // error before it can execute anything, so an
                        // unresolved name still never reaches `default`.
                        bind_startup_database(client, handler.session_id, handler)?;
                    }
                    Err(_) if was_in_auth => {
                        // SCRAM failed. This is the single place the lockout
                        // counter is driven for the SCRAM path. A SASL
                        // failure counts as a credential failure only when
                        // the account's credentials were actually usable
                        // (so the failure is a wrong client proof) or the
                        // user is unknown. A policy rejection from the
                        // credential lookup (expired / must-change password,
                        // inactive or service account) or an internal error
                        // must not count — the password may well be correct.
                        let scram_ip_str = source
                            .parse::<std::net::SocketAddr>()
                            .map(|s| s.ip().to_string())
                            .unwrap_or_else(|_| source.clone());
                        // A SASL failure that was actually caused by the
                        // pre-verify admission gate (rate-limit / DoS ceiling)
                        // must NOT move the brute-force or lockout counters —
                        // the client proof was never even checked. Only a
                        // genuine wrong-proof / unknown-user failure counts.
                        let rate_limited = state
                            .rate_limiter
                            .is_login_rate_limited(&scram_ip_str, &username);
                        let counts = !rate_limited
                            && matches!(
                                state.credentials.get_scram_credentials(&username),
                                ScramLookup::Found(_)
                                    | ScramLookup::Rejected(AuthRejection::BadCredential)
                            );
                        if counts {
                            let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
                            let scram_ip =
                                source.parse::<std::net::SocketAddr>().ok().map(|s| s.ip());
                            state
                                .credentials
                                .record_login_failure(&username, scram_ip, &emitter);
                            // Drive the per-IP / per-user brute-force window from
                            // the same genuine-failure site as the lockout
                            // counter.
                            state
                                .rate_limiter
                                .record_login_failure(&scram_ip_str, &username);
                        }
                        // Escalation follows the same `counts` gate as the
                        // lockout and brute-force counters: a rate-limited or
                        // policy-rejected SASL failure says nothing about the
                        // client's proof, so it is audited without being
                        // counted against the account.
                        let subject = if counts {
                            ViolationSubject::Username(username.as_str())
                        } else {
                            ViolationSubject::AuditOnly
                        };
                        record_auth_violation(
                            state,
                            AuthViolation {
                                subject,
                                tenant_id: None,
                                source: &source,
                                detail: &format!("SCRAM-SHA-256 auth failed: {username}"),
                            },
                        );
                    }
                    _ => {}
                }

                result
            }
        }
    }
}
