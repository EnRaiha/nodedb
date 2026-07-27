// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol auth handshake: authenticates the client, assembles the
//! three-level (global/database/tenant) admission permit, and builds the
//! auth response.

use nodedb_types::protocol::{NativeResponse, RequestFields};

use crate::control::server::admission::ConnectionPermit;

use super::NativeSession;
use super::dispatch;

impl NativeSession {
    /// Handle authentication request.
    pub(super) async fn handle_auth(&mut self, seq: u64, fields: &RequestFields) -> NativeResponse {
        // Re-authentication is not supported on the native protocol. Once a
        // session has assembled its three-level admission permit, the identity
        // is fixed for the connection's lifetime — allowing re-auth would let
        // a client silently swap to a different (database, tenant) scope while
        // still holding the original scope's connection slots.
        if self.identity.is_some() || self.connection_permit.is_some() {
            return NativeResponse::error(
                seq,
                "0A000",
                "already authenticated; reconnect to switch identity",
            );
        }

        let auth = match fields {
            RequestFields::Text(f) => match &f.auth {
                Some(a) => a,
                None => {
                    return NativeResponse::error(seq, "28000", "missing 'auth' field");
                }
            },
            _ => {
                return NativeResponse::error(seq, "0A000", "unsupported request fields variant");
            }
        };

        match dispatch::handle_auth(
            &self.state,
            &self.auth_mode,
            auth,
            &self.peer_addr.to_string(),
        )
        .await
        {
            Ok((identity, warning)) => {
                // Phase 2 admission: acquire per-database and per-tenant permits
                // now that we know the identity. The database scope is the
                // identity's default database (or DEFAULT if none is set).
                let db_id = identity
                    .default_database
                    .unwrap_or(nodedb_types::DatabaseId::DEFAULT);
                let tenant_id = identity.tenant_id;

                let db_permit = match self.admission_registry.try_acquire_database(db_id) {
                    Ok(p) => p,
                    Err(e) => {
                        return NativeResponse::error(
                            seq,
                            nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
                            format!("{e}"),
                        );
                    }
                };
                let tenant_permit =
                    match self.admission_registry.try_acquire_tenant(db_id, tenant_id) {
                        Ok(p) => p,
                        Err(e) => {
                            // db_permit is dropped here, releasing the DB slot.
                            drop(db_permit);
                            return NativeResponse::error(
                                seq,
                                nodedb_types::error::sqlstate::QUOTA_EXCEEDED,
                                format!("{e}"),
                            );
                        }
                    };

                // Assemble the three-level permit. The global slot moves from
                // `global_permit` into the `ConnectionPermit`. The re-auth
                // guard at the top of this function ensures `global_permit`
                // is still `Some` here — it is initialized at construction
                // and only consumed on the auth path.
                let Some(global) = self.global_permit.take() else {
                    // Release the freshly acquired Phase 2 permits so we
                    // don't leak slots into the per-DB / per-tenant pools.
                    drop(tenant_permit);
                    drop(db_permit);
                    return NativeResponse::error(
                        seq,
                        "XX000",
                        "internal error: global admission permit missing during auth assembly",
                    );
                };
                self.connection_permit = Some(ConnectionPermit {
                    global,
                    database: db_permit,
                    tenant: tenant_permit,
                    db_id,
                    tenant_id,
                });

                let mut resp = NativeResponse::auth_ok(
                    seq,
                    identity.username.clone(),
                    identity.tenant_id.as_u64(),
                );
                if let Some(w) = warning {
                    resp.warnings.push(w);
                }
                self.auth_context = Some(super::super::super::session_auth::build_auth_context(
                    &identity,
                ));
                self.cleanup.publish_identity(identity.clone());
                self.identity = Some(identity);
                resp
            }
            // A transient login rate-limit is distinct from a credential
            // failure: it maps to TOO_MANY_CONNECTIONS (53300), which clients
            // recognise as retryable, and carries a distinct message. Every
            // other auth error (wrong password, lockout, unknown user) stays
            // collapsed into the generic invalid-password 28P01 so none can be
            // distinguished from the others.
            Err(e @ crate::Error::RateExceeded { .. }) => NativeResponse::error(
                seq,
                nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
                format!("{e}"),
            ),
            Err(e) => NativeResponse::error(seq, "28P01", format!("{e}")),
        }
    }
}
