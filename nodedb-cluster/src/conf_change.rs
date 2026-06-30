// SPDX-License-Identifier: BUSL-1.1

//! Raft configuration change types.
//!
//! Configuration changes (add/remove peer) are proposed as regular Raft log
//! entries with a special prefix byte. When committed, the state machine
//! detects the prefix and applies the membership change to the Raft group.
//!
//! Uses single-server changes (one peer at a time) for simplicity and safety.

/// Discriminator byte at offset 0 of a Raft log entry that marks it as a
/// configuration change. Layout: `[kind:1][msgpack(ConfChange)]`.
///
/// `0xC1` is the only byte the MessagePack spec lists as "never used" — no
/// valid msgpack payload can start with it. All current app-data proposals
/// are msgpack-encoded (MetadataEntry, distributed-applier batches, auth
/// transitions), so the discriminator is unambiguous. (`0xFF` was incorrect
/// here: it is msgpack negative fixint -1 and collides with valid scalars.)
pub const CONF_CHANGE_PREFIX: u8 = 0xC1;

/// Type of configuration change.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ConfChangeType {
    /// Add a voting member to the Raft group.
    AddNode = 0,
    /// Remove a voting member from the Raft group.
    RemoveNode = 1,
    /// Add a non-voting learner (catches up before becoming voter).
    AddLearner = 2,
    /// Promote a learner to a full voting member.
    PromoteLearner = 3,
    /// Remove a non-voting learner from the Raft group.
    ///
    /// Safe at any time: learners are not in quorum, commit, or election paths.
    RemoveLearner = 4,
}

/// A configuration change for a Raft group.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ConfChange {
    pub change_type: ConfChangeType,
    /// The node being added or removed.
    pub node_id: u64,
}

impl ConfChange {
    /// Serialize to bytes for a Raft log entry (prefixed with CONF_CHANGE_PREFIX).
    pub fn to_entry_data(&self) -> crate::error::Result<Vec<u8>> {
        let mut data = vec![CONF_CHANGE_PREFIX];
        let payload =
            zerompk::to_msgpack_vec(self).map_err(|e| crate::error::ClusterError::Codec {
                detail: format!("conf_change encode: {e}"),
            })?;
        data.extend_from_slice(&payload);
        Ok(data)
    }

    /// Try to deserialize from a Raft log entry's data bytes.
    ///
    /// Returns `None` if the entry is not a configuration change (wrong prefix).
    pub fn from_entry_data(data: &[u8]) -> Option<Self> {
        if data.first() != Some(&CONF_CHANGE_PREFIX) {
            return None;
        }
        zerompk::from_msgpack(&data[1..]).ok()
    }

    /// Check if a log entry's data is a configuration change (without full deserialization).
    pub fn is_conf_change(data: &[u8]) -> bool {
        data.first() == Some(&CONF_CHANGE_PREFIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_add_node() {
        let cc = ConfChange {
            change_type: ConfChangeType::AddNode,
            node_id: 42,
        };
        let data = cc.to_entry_data().unwrap();
        assert_eq!(data[0], CONF_CHANGE_PREFIX);

        let decoded = ConfChange::from_entry_data(&data).unwrap();
        assert_eq!(decoded.change_type, ConfChangeType::AddNode);
        assert_eq!(decoded.node_id, 42);
    }

    #[test]
    fn roundtrip_remove_node() {
        let cc = ConfChange {
            change_type: ConfChangeType::RemoveNode,
            node_id: 7,
        };
        let data = cc.to_entry_data().unwrap();
        let decoded = ConfChange::from_entry_data(&data).unwrap();
        assert_eq!(decoded.change_type, ConfChangeType::RemoveNode);
        assert_eq!(decoded.node_id, 7);
    }

    #[test]
    fn regular_data_not_conf_change() {
        assert!(!ConfChange::is_conf_change(b"hello"));
        assert!(!ConfChange::is_conf_change(&[]));
        assert!(ConfChange::from_entry_data(b"hello").is_none());
    }

    #[test]
    fn prefix_is_msgpack_never_used_byte() {
        // 0xC1 is the only byte the MessagePack spec marks "never used".
        // If anyone changes this constant, they must re-prove non-collision
        // with every app-data proposal path.
        assert_eq!(CONF_CHANGE_PREFIX, 0xC1);
    }

    #[test]
    fn prefix_does_not_collide_with_msgpack_metadata_entry() {
        // App data on the metadata group is msgpack(MetadataEntry), which
        // is a struct — encodes as a fixmap/fixarray (0x80..=0x9f). It
        // must never start with the conf-change prefix.
        let cc = ConfChange {
            change_type: ConfChangeType::AddNode,
            node_id: 1,
        };
        let msgpack_struct = zerompk::to_msgpack_vec(&cc).unwrap();
        assert_ne!(msgpack_struct.first(), Some(&CONF_CHANGE_PREFIX));
    }

    #[test]
    fn all_change_types() {
        for ct in [
            ConfChangeType::AddNode,
            ConfChangeType::RemoveNode,
            ConfChangeType::AddLearner,
            ConfChangeType::PromoteLearner,
            ConfChangeType::RemoveLearner,
        ] {
            let cc = ConfChange {
                change_type: ct,
                node_id: 1,
            };
            let data = cc.to_entry_data().unwrap();
            let decoded = ConfChange::from_entry_data(&data).unwrap();
            assert_eq!(decoded.change_type, ct);
        }
    }
}
