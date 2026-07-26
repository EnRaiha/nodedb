// SPDX-License-Identifier: BUSL-1.1

//! Token refresh: rotate JWT without reconnecting.

use std::time::Instant;

use tracing::{info, warn};

use crate::control::security::jwt::JwtValidator;

use super::super::wire::*;
use super::state::SyncSession;

impl SyncSession {
    /// Handle a token refresh request. Validate the new JWT, and if
    /// it belongs to the same tenant, upgrade the session with the
    /// new credentials. Invalid tokens keep the existing session
    /// credentials and respond with an error.
    pub fn handle_token_refresh(
        &mut self,
        msg: &TokenRefreshMsg,
        jwt_validator: &JwtValidator,
    ) -> Option<SyncFrame> {
        self.last_activity = Instant::now();

        if msg.new_token.is_empty() {
            let ack = TokenRefreshAckMsg {
                success: false,
                error: Some("empty token".into()),
                expires_in_secs: 0,
            };
            return SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack);
        }

        match jwt_validator.validate(&msg.new_token) {
            Ok(new_identity) => {
                if let Some(current_tenant) = self.tenant_id
                    && new_identity.tenant_id != current_tenant
                {
                    warn!(
                        session = %self.session_id,
                        current_tenant = current_tenant.as_u64(),
                        new_tenant = new_identity.tenant_id.as_u64(),
                        "token refresh rejected: tenant mismatch"
                    );
                    let ack = TokenRefreshAckMsg {
                        success: false,
                        error: Some("tenant mismatch".into()),
                        expires_in_secs: 0,
                    };
                    return SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack);
                }
                if let Some(current) = self.identity.as_ref()
                    && new_identity.user_id != current.user_id
                {
                    warn!(
                        session = %self.session_id,
                        current_user_id = current.user_id,
                        new_user_id = new_identity.user_id,
                        "token refresh rejected: user mismatch"
                    );
                    let ack = TokenRefreshAckMsg {
                        success: false,
                        error: Some("user mismatch".into()),
                        expires_in_secs: 0,
                    };
                    return SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack);
                }
                self.username = Some(new_identity.username.clone());
                self.identity = Some(new_identity);
                info!(
                    session = %self.session_id,
                    "JWT token refreshed successfully"
                );
                let ack = TokenRefreshAckMsg {
                    success: true,
                    error: None,
                    expires_in_secs: 3600,
                };
                SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack)
            }
            Err(e) => {
                warn!(
                    session = %self.session_id,
                    error = %e,
                    "token refresh FAILED — keeping existing credentials"
                );
                let ack = TokenRefreshAckMsg {
                    success: false,
                    error: Some(e.to_string()),
                    expires_in_secs: 0,
                };
                SyncFrame::try_encode(SyncMessageType::TokenRefreshAck, &ack)
            }
        }
    }
}
