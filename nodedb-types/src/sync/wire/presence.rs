// SPDX-License-Identifier: Apache-2.0

//! Presence / awareness messages.

use serde::{Deserialize, Serialize};

/// Presence update message (client → server, 0x80).
///
/// Sends ephemeral user state to a channel. The server broadcasts the state
/// to all other subscribers of the same channel. Presence is NOT persisted,
/// NOT CRDT-merged — it is fire-and-forget with latest-state-wins semantics.
///
/// Sending a `PresenceUpdate` implicitly subscribes the sender to the channel
/// (if not already subscribed).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceUpdateMsg {
    /// Channel scoping key (e.g., `"doc:doc-123"`, `"workspace:ws-acme"`).
    pub channel: String,
    /// Opaque user state (MessagePack-encoded application-defined payload).
    /// Common fields: user_id, user_name, cursor_position, selection_range,
    /// active_document_id, color, avatar_url.
    pub state: Vec<u8>,
}

/// A single peer's presence state within a channel.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PeerPresence {
    /// User identifier.
    pub user_id: String,
    /// Opaque user state (same format as `PresenceUpdateMsg::state`).
    pub state: Vec<u8>,
    /// Milliseconds since this peer's last update.
    pub last_seen_ms: u64,
}

/// Presence broadcast message (server → all subscribers except sender, 0x81).
///
/// Contains the full set of currently-present peers in the channel.
/// Sent whenever any peer updates their state or leaves.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceBroadcastMsg {
    /// Channel this broadcast belongs to.
    pub channel: String,
    /// All currently-present peers and their latest state.
    pub peers: Vec<PeerPresence>,
}

/// Presence leave message (server → all subscribers, 0x82).
///
/// Emitted when a peer disconnects (WebSocket close) or when their
/// presence TTL expires (no heartbeat within `presence_ttl_ms`).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PresenceLeaveMsg {
    /// Channel the user left.
    pub channel: String,
    /// User who left.
    pub user_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn presence_update_roundtrip() {
        let msg = PresenceUpdateMsg {
            channel: "doc:doc-123".into(),
            state: b"user_id:user-42,cursor:blk-7:42".to_vec(),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceUpdate, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert_eq!(bytes[0], SyncFrame::FORMAT_VERSION);
        assert_eq!(bytes[1], 0x80);
        let decoded: PresenceUpdateMsg = SyncFrame::from_bytes(&bytes)
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.channel, "doc:doc-123");
        assert!(!decoded.state.is_empty());
    }

    #[test]
    fn presence_broadcast_roundtrip() {
        let msg = PresenceBroadcastMsg {
            channel: "doc:doc-123".into(),
            peers: vec![
                PeerPresence {
                    user_id: "user-42".into(),
                    state: vec![0xDE, 0xAD],
                    last_seen_ms: 150,
                },
                PeerPresence {
                    user_id: "user-99".into(),
                    state: vec![0xBE, 0xEF],
                    last_seen_ms: 2300,
                },
            ],
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceBroadcast, &msg).unwrap();
        let decoded: PresenceBroadcastMsg = SyncFrame::from_bytes(&frame.to_bytes())
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.channel, "doc:doc-123");
        assert_eq!(decoded.peers.len(), 2);
        assert_eq!(decoded.peers[0].user_id, "user-42");
        assert_eq!(decoded.peers[1].last_seen_ms, 2300);
    }

    #[test]
    fn presence_leave_roundtrip() {
        let msg = PresenceLeaveMsg {
            channel: "doc:doc-123".into(),
            user_id: "user-42".into(),
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::PresenceLeave, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert_eq!(bytes[0], SyncFrame::FORMAT_VERSION);
        assert_eq!(bytes[1], 0x82);
        let decoded: PresenceLeaveMsg = SyncFrame::from_bytes(&bytes)
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.channel, "doc:doc-123");
        assert_eq!(decoded.user_id, "user-42");
    }
}
