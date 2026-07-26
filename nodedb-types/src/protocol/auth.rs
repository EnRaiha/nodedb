// SPDX-License-Identifier: Apache-2.0

//! Authentication method types.

use serde::{Deserialize, Serialize};

/// Authentication method in an `Auth` request.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[serde(tag = "method", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthMethod {
    #[serde(rename = "trust")]
    Trust { username: String },
    #[serde(rename = "password")]
    Password { username: String, password: String },
    #[serde(rename = "api_key")]
    ApiKey { token: String },
    /// OIDC bearer token (native / HTTP clients only; NOT pgwire).
    #[serde(rename = "oidc_bearer")]
    OidcBearer {
        token: String,
        /// Optional provider name hint. When absent the provider is resolved by
        /// the `iss` claim in the token.
        #[serde(default)]
        provider: Option<String>,
    },
}

/// Successful auth response payload.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AuthResponse {
    pub username: String,
    pub tenant_id: u64,
}

#[cfg(test)]
mod tests {
    use super::AuthMethod;

    /// A `trust` auth frame that omits `username` entirely must fail to
    /// decode via serde/JSON — never silently resolve to a default identity.
    #[test]
    fn trust_missing_username_rejected_by_serde() {
        let value = serde_json::json!({ "method": "trust" });
        let decoded: Result<AuthMethod, _> = serde_json::from_value(value);
        assert!(
            decoded.is_err(),
            "trust auth frame without username must not decode"
        );
    }

    /// A well-formed trust frame with an explicit username still decodes.
    #[test]
    fn trust_explicit_username_accepted_by_serde() {
        let value = serde_json::json!({ "method": "trust", "username": "alice" });
        let decoded: AuthMethod = serde_json::from_value(value).expect("must decode");
        match decoded {
            AuthMethod::Trust { username } => assert_eq!(username, "alice"),
            other => panic!("expected Trust variant, got {other:?}"),
        }
    }

    /// Same guarantee on the zerompk wire path: a `trust` variant payload
    /// with no `username` key must fail to decode, not default to "admin".
    #[test]
    fn trust_missing_username_rejected_by_zerompk() {
        // Hand-encoded MessagePack for zerompk's externally-tagged enum repr:
        // map-len(1) { "trust" => <payload> }, with the payload map
        // deliberately empty (no `username` key). Markers: 0x81 = fixmap(1),
        // 0xA5 = fixstr(5) "trust", 0x80 = fixmap(0).
        let bytes: &[u8] = &[0x81, 0xA5, b't', b'r', b'u', b's', b't', 0x80];

        let decoded: Result<AuthMethod, _> = zerompk::from_msgpack(bytes);
        assert!(
            decoded.is_err(),
            "trust auth frame without username must not decode via zerompk"
        );
    }

    /// A well-formed trust frame with an explicit username still decodes via zerompk.
    #[test]
    fn trust_explicit_username_accepted_by_zerompk() {
        let trust = AuthMethod::Trust {
            username: "alice".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&trust).unwrap();
        let decoded: AuthMethod = zerompk::from_msgpack(&bytes).unwrap();
        match decoded {
            AuthMethod::Trust { username } => assert_eq!(username, "alice"),
            other => panic!("expected Trust variant, got {other:?}"),
        }
    }
}
