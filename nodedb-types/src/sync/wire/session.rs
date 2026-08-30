// SPDX-License-Identifier: Apache-2.0

//! Session lifecycle messages: handshake, token refresh, keepalive.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Handshake message (client → server, 0x01).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HandshakeMsg {
    /// JWT bearer token for authentication.
    pub jwt_token: String,
    /// Client's vector clock: `{ collection: { doc_id: lamport_ts } }`.
    pub vector_clock: HashMap<String, HashMap<String, u64>>,
    /// Shape IDs the client is subscribed to.
    pub subscribed_shapes: Vec<String>,
    /// Client version string.
    pub client_version: String,
    /// Lite instance identity (UUID v7). Default empty for non-Lite peers.
    #[serde(default)]
    pub lite_id: String,
    /// Monotonic epoch counter (incremented on every open). Default 0 for non-Lite peers.
    #[serde(default)]
    pub epoch: u64,
    /// Wire format version. Server rejects connections with incompatible versions.
    /// Missing field deserializes to 0 and is rejected by the server explicitly.
    #[serde(default)]
    pub wire_version: u16,
}

/// Handshake acknowledgment (server → client, 0x02).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct HandshakeAckMsg {
    /// Whether the handshake succeeded.
    pub success: bool,
    /// Session ID assigned by the server.
    pub session_id: String,
    /// Server's vector clock (for initial sync).
    pub server_clock: HashMap<String, u64>,
    /// Error message (if !success).
    pub error: Option<String>,
    /// Fork detection: if true, client must regenerate LiteId and reconnect.
    #[serde(default)]
    pub fork_detected: bool,
    /// Server's wire format version (for client-side compatibility check).
    #[serde(default)]
    pub server_wire_version: u16,
    /// Server-assigned producer ID for this session. 0 if not yet assigned.
    #[serde(default)]
    pub producer_id: u64,
    /// Server's current accepted epoch for this producer. 0 if not yet tracked.
    #[serde(default)]
    pub accepted_epoch: u64,
    /// Stable per-user key for signing queued CRDT deltas. Issued only after
    /// successful authentication over the protected sync transport. All zeros
    /// means signing is unavailable for this session.
    #[serde(default)]
    pub delta_signing_key: [u8; 32],
}

/// Token refresh request (client → server, 0x60).
///
/// Sent by Lite before the current JWT expires. The client provides
/// a fresh token obtained from the application's auth layer.
/// Origin validates the new token and either upgrades the session
/// or disconnects if the token is invalid.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TokenRefreshMsg {
    /// New JWT bearer token.
    pub new_token: String,
}

/// Token refresh acknowledgment (server → client, 0x61).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct TokenRefreshAckMsg {
    /// Whether the token refresh succeeded.
    pub success: bool,
    /// Error message (if !success).
    pub error: Option<String>,
    /// Seconds until this new token expires (so Lite can schedule next refresh).
    #[serde(default)]
    pub expires_in_secs: u64,
}

/// Ping/Pong keepalive (0xFF).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PingPongMsg {
    /// Timestamp (epoch milliseconds) for RTT measurement.
    pub timestamp_ms: u64,
    /// Whether this is a pong (response to ping).
    pub is_pong: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn handshake_serialization() {
        let msg = HandshakeMsg {
            jwt_token: "test.jwt.token".into(),
            vector_clock: HashMap::new(),
            subscribed_shapes: vec!["shape1".into()],
            client_version: "0.1.0".into(),
            lite_id: String::new(),
            epoch: 0,
            wire_version: 1,
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::Handshake, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert!(bytes.len() > SyncFrame::HEADER_SIZE);
        assert_eq!(bytes[0], SyncFrame::FORMAT_VERSION);
        assert_eq!(bytes[1], 0x01);
    }
}
