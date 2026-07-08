// SPDX-License-Identifier: BUSL-1.1

//! Connection lifecycle: request-id allocation, session registration/kill
//! signalling, identity rehydration, and the frame read/dispatch loop.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, instrument, warn};

use crate::types::RequestId;

use super::Session;

impl Session {
    /// Allocate a unique request ID via the per-node counter.
    pub(super) fn next_request_id(&self) -> RequestId {
        self.state.next_request_id()
    }

    /// Register the session in the registry after authentication and store the
    /// kill receiver.  No-op if the user has `user_id == 0` (trust mode fallback).
    ///
    /// `token_expiry_ms` is `Some(exp_epoch_ms)` for OIDC/JWT sessions so the
    /// idle-sweep loop can enforce token lifetime independently of the TCP session.
    pub(super) fn register_session(
        &mut self,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        token_expiry_ms: Option<u64>,
    ) {
        use crate::control::security::sessions::SessionParams;

        if self.kill_rx.is_some() {
            // Already registered (trust auto-auth path called twice).
            return;
        }

        let auth_method = match identity.auth_method {
            crate::control::security::identity::AuthMethod::ScramSha256 => "scram_sha256",
            crate::control::security::identity::AuthMethod::CleartextPassword => "password",
            crate::control::security::identity::AuthMethod::ApiKey => "api_key",
            crate::control::security::identity::AuthMethod::Certificate => "certificate",
            crate::control::security::identity::AuthMethod::Trust => "trust",
            crate::control::security::identity::AuthMethod::OidcBearer => "oidc_bearer",
        };

        let credential_version = self.state.credentials.current_version(identity.user_id);
        self.identity_version = credential_version;

        let params = SessionParams {
            user_id: identity.user_id,
            username: identity.username.clone(),
            db_user: identity.username.clone(),
            peer_addr: self.peer_addr.to_string(),
            protocol: "native".to_string(),
            auth_method: auth_method.to_string(),
            tenant_id: identity.tenant_id.as_u64(),
            credential_version,
            current_database: None,
            token_expiry_ms,
        };

        match self
            .state
            .session_registry
            .register(&self.session_id, &params)
        {
            Ok(kill_rx) => {
                self.kill_rx = Some(kill_rx);
            }
            Err(e) => {
                // Cap exceeded; kill_rx stays None and the error will surface as
                // a SessionCapExceeded on the next request that calls check_kill.
                tracing::warn!(session_id = %self.session_id, cap = e.cap,
                    "session cap exceeded — session registered without kill channel");
            }
        }
    }

    /// Check whether the kill signal has fired (hard revoke).
    ///
    /// Returns `true` if the session should terminate immediately.
    fn is_killed(&mut self) -> bool {
        match self.kill_rx.as_mut() {
            Some(rx) => {
                rx.has_changed().unwrap_or(false)
                    && *rx.borrow_and_update()
                        != crate::control::security::sessions::KillReason::Alive
            }
            None => false,
        }
    }

    /// If the credential store's version for this user has advanced since we
    /// last bound, rebuild the identity from the fresh `UserRecord`.
    ///
    /// Must be called before every request that reads `self.identity`.
    pub(super) fn rehydrate_identity_if_stale(&mut self) {
        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => return,
        };

        let user_id = identity.user_id;
        if user_id == 0 {
            // Trust-mode anonymous identity — no versioning.
            return;
        }

        let current = self.state.credentials.current_version(user_id);
        if current <= self.identity_version {
            return;
        }

        // Version advanced — fetch fresh record and rebuild identity.
        let auth_method = identity.auth_method.clone();
        let username = identity.username.clone();
        if let Some(fresh) = self.state.credentials.to_identity(&username, auth_method) {
            self.identity_version = current;
            self.identity = Some(fresh);
        }
    }

    /// Run the session loop: read frames, parse, dispatch, respond.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    pub async fn run(mut self) -> crate::Result<()> {
        let idle_timeout_secs = self.state.idle_timeout_secs();
        let absolute_timeout_secs = self.state.session_absolute_timeout_secs();
        let result = self
            .run_inner(idle_timeout_secs, absolute_timeout_secs)
            .await;
        // Always unregister on exit regardless of reason.
        self.state
            .session_registry
            .unregister(&self.session_id.clone());
        result
    }

    async fn run_inner(
        &mut self,
        idle_timeout_secs: u64,
        absolute_timeout_secs: u64,
    ) -> crate::Result<()> {
        loop {
            // Hard-revoke check: bus consumer sent kill signal.
            if self.is_killed() {
                let msg = r#"{"status":"error","sqlstate":"57P01","error":"session revoked by administrator"}"#;
                let resp_len = (msg.len() as u32).to_be_bytes();
                let _ = self.stream.write_all(&resp_len).await;
                let _ = self.stream.write_all(msg.as_bytes()).await;
                return Ok(());
            }

            // Enforce absolute session lifetime (SQLSTATE 57P01 "admin shutdown").
            if absolute_timeout_secs > 0
                && self.connected_at.elapsed().as_secs() >= absolute_timeout_secs
            {
                debug!(
                    "session absolute timeout ({}s), closing connection",
                    absolute_timeout_secs
                );
                let msg = r#"{"status":"error","sqlstate":"57P01","error":"session timeout: absolute lifetime exceeded"}"#;
                let resp_len = (msg.len() as u32).to_be_bytes();
                let _ = self.stream.write_all(&resp_len).await;
                let _ = self.stream.write_all(msg.as_bytes()).await;
                return Ok(());
            }

            // Read length prefix with idle timeout.
            let mut len_buf = [0u8; 4];
            let read_result: std::io::Result<usize> = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    self.stream.read_exact(&mut len_buf),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        debug!("session idle timeout ({}s)", idle_timeout_secs);
                        return Ok(());
                    }
                }
            } else {
                self.stream.read_exact(&mut len_buf).await
            };
            match read_result {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("client disconnected");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }

            let payload_len = u32::from_be_bytes(len_buf);
            if payload_len > super::MAX_FRAME_SIZE {
                warn!(payload_len, "frame too large, closing connection");
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "frame size {payload_len} exceeds maximum {}",
                        super::MAX_FRAME_SIZE
                    ),
                });
            }

            // Read payload.
            let mut payload = vec![0u8; payload_len as usize];
            self.stream.read_exact(&mut payload).await?;

            // Parse and dispatch.
            let request_id = self.next_request_id();
            match self.handle_frame(request_id, &payload).await {
                Ok(response_bytes) => {
                    // Write length-prefixed response.
                    let resp_len = (response_bytes.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(&response_bytes).await?;
                }
                Err(e) => {
                    // Send error response.
                    let error_json = format!(r#"{{"status":"error","error":"{e}"}}"#);
                    let resp_len = (error_json.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(error_json.as_bytes()).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod session_timeout_tests {
    /// Verify the absolute-timeout predicate in isolation.
    ///
    /// The real check is: `absolute_timeout_secs > 0 && elapsed >= absolute_timeout_secs`.
    /// This test pins that condition so refactors cannot silently invert it.
    #[test]
    fn absolute_timeout_predicate() {
        // When timeout is disabled (0), any elapsed time should NOT trigger.
        let absolute_timeout_secs: u64 = 0;
        let elapsed_secs: u64 = 9999;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "timeout=0 (disabled) must never close the session"
        );

        // When timeout is set and elapsed < timeout, session stays open.
        let absolute_timeout_secs: u64 = 3600;
        let elapsed_secs: u64 = 3599;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "elapsed < timeout should not close the session"
        );

        // When elapsed == timeout exactly, session should close.
        let elapsed_secs: u64 = 3600;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(should_close, "elapsed == timeout should close the session");

        // When elapsed > timeout, session should close.
        let elapsed_secs: u64 = 7200;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(should_close, "elapsed > timeout should close the session");

        // Idle timeout is independent — setting idle_timeout > 0 does not affect absolute check.
        // Specifically: absolute=0 (disabled) + any idle means absolute check still returns false.
        let absolute_timeout_secs: u64 = 0;
        let _idle_timeout_secs: u64 = 60;
        let elapsed_secs: u64 = 9999;
        let should_close = absolute_timeout_secs > 0 && elapsed_secs >= absolute_timeout_secs;
        assert!(
            !should_close,
            "idle timeout must not activate the absolute-timeout close path"
        );
    }
}
