// SPDX-License-Identifier: BUSL-1.1

//! Wire protocol for the bootstrap cred-delivery RPC.
//!
//! One bidi QUIC stream per request. Both directions carry a single
//! length-prefixed MessagePack frame:
//!
//! ```text
//!   [len:u32 BE | msgpack bytes]
//! ```
//!
//! The envelope is deliberately **not** the authenticated
//! `auth_envelope` used for the Raft transport — the joiner doesn't
//! yet have the `cluster_secret`, so an HMAC envelope is impossible.
//! Token authentication lives inside the request payload.

use serde::{Deserialize, Serialize};

use crate::error::ClusterError;

/// Maximum accepted request/response frame size (1 MiB). A real
/// bundle is under 8 KiB; the cap guards against a malformed
/// `len` prefix exhausting memory before decode.
pub const MAX_FRAME_BYTES: usize = 1 << 20;
pub(super) const DELIVERY_ACK: &[u8] = b"nodedb-bootstrap-delivered-v1";

/// Request: "I am `node_id`, this is my join token, issue me creds."
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct BootstrapCredsRequest {
    /// Hex-encoded join token from `nodedb join-token --create`.
    /// Verified server-side via `nodedb::ctl::join_token::verify_token`
    /// against the cluster secret. The token binds the request to a
    /// `for_node` id which must equal `node_id` below, and to an
    /// expiry timestamp.
    pub token_hex: String,
    /// The node id the joiner claims. Must match the `for_node` in
    /// the decoded token — rebinding rejected with `error = "node id
    /// mismatch"`.
    pub node_id: u64,
}

/// Response: creds bundle or an error string. `ok = true` guarantees
/// the DER fields are populated; on error the DER fields are empty.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct BootstrapCredsResponse {
    pub ok: bool,
    pub error: String,
    pub ca_cert_der: Vec<u8>,
    pub node_cert_der: Vec<u8>,
    /// PKCS#8 DER of the node's private key.
    pub node_key_der: Vec<u8>,
    /// Raw 32-byte cluster secret. Handle with the same care as a
    /// private key — log redaction, 0600 at rest.
    pub cluster_secret: Vec<u8>,
}

impl BootstrapCredsResponse {
    pub fn error(detail: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: detail.into(),
            ca_cert_der: Vec::new(),
            node_cert_der: Vec::new(),
            node_key_der: Vec::new(),
            cluster_secret: Vec::new(),
        }
    }
}

pub(super) fn encode_request(v: &BootstrapCredsRequest) -> Result<Vec<u8>, ClusterError> {
    zerompk::to_msgpack_vec(v).map_err(|e| ClusterError::Transport {
        detail: format!("msgpack encode: {e}"),
    })
}

pub(super) fn decode_request(bytes: &[u8]) -> Result<BootstrapCredsRequest, ClusterError> {
    zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Transport {
        detail: format!("msgpack decode: {e}"),
    })
}

pub(super) fn encode_response(v: &BootstrapCredsResponse) -> Result<Vec<u8>, ClusterError> {
    zerompk::to_msgpack_vec(v).map_err(|e| ClusterError::Transport {
        detail: format!("msgpack encode: {e}"),
    })
}

pub(super) fn decode_response(bytes: &[u8]) -> Result<BootstrapCredsResponse, ClusterError> {
    zerompk::from_msgpack(bytes).map_err(|e| ClusterError::Transport {
        detail: format!("msgpack decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips() {
        let req = BootstrapCredsRequest {
            token_hex: "deadbeef".into(),
            node_id: 7,
        };
        let bytes = encode_request(&req).unwrap();
        let back = decode_request(&bytes).unwrap();
        assert_eq!(back.token_hex, "deadbeef");
        assert_eq!(back.node_id, 7);
    }

    #[test]
    fn response_error_omits_der() {
        let resp = BootstrapCredsResponse::error("bad token");
        assert!(!resp.ok);
        assert_eq!(resp.error, "bad token");
        assert!(resp.ca_cert_der.is_empty());
    }

    #[test]
    fn response_success_roundtrips() {
        let resp = BootstrapCredsResponse {
            ok: true,
            error: String::new(),
            ca_cert_der: vec![1, 2, 3],
            node_cert_der: vec![4, 5, 6],
            node_key_der: vec![7, 8, 9],
            cluster_secret: vec![0xAAu8; 32],
        };
        let bytes = encode_response(&resp).unwrap();
        let back = decode_response(&bytes).unwrap();
        assert!(back.ok);
        assert_eq!(back.ca_cert_der, vec![1, 2, 3]);
        assert_eq!(back.cluster_secret.len(), 32);
    }
}
