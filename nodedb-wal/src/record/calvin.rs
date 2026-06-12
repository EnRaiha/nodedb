// SPDX-License-Identifier: Apache-2.0

//! Calvin scheduler WAL payload types.
//!
//! Uses fixed little-endian encoding (no msgpack framing) for zero-allocation
//! replay, consistent with other WAL payload types in this crate.

use crate::error::{Result, WalError};

/// Fixed size of a `CalvinApplied` payload on disk.
///
/// Layout: `epoch (u64 LE) | position (u32 LE) | vshard_id (u32 LE)` = 16 bytes.
pub const CALVIN_APPLIED_PAYLOAD_SIZE: usize = 16;

/// Payload for [`super::types::RecordType::CalvinApplied`].
///
/// Written by the Calvin executor after a `MetaOp::CalvinExecute` batch
/// commits successfully. Encodes `{ epoch, position, vshard_id }` so the
/// scheduler's restart path can scan the WAL and find `last_applied_epoch`
/// for a given vshard without reading the full Raft log.
///
/// Fixed little-endian encoding — no msgpack framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalvinAppliedPayload {
    /// Sequencer epoch of the applied transaction.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
    /// The vshard that applied this transaction.
    pub vshard_id: u32,
}

impl CalvinAppliedPayload {
    /// Construct a new payload.
    pub fn new(epoch: u64, position: u32, vshard_id: u32) -> Self {
        Self {
            epoch,
            position,
            vshard_id,
        }
    }

    /// Serialize to fixed-size bytes for WAL append.
    ///
    /// Layout: `epoch (8 LE) | position (4 LE) | vshard_id (4 LE)`.
    pub fn to_bytes(&self) -> [u8; CALVIN_APPLIED_PAYLOAD_SIZE] {
        let mut buf = [0u8; CALVIN_APPLIED_PAYLOAD_SIZE];
        buf[..8].copy_from_slice(&self.epoch.to_le_bytes());
        buf[8..12].copy_from_slice(&self.position.to_le_bytes());
        buf[12..16].copy_from_slice(&self.vshard_id.to_le_bytes());
        buf
    }

    /// Deserialize from bytes read from the WAL.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < CALVIN_APPLIED_PAYLOAD_SIZE {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "CalvinApplied payload must be {CALVIN_APPLIED_PAYLOAD_SIZE} bytes, got {}",
                    bytes.len()
                ),
            });
        }
        let epoch = u64::from_le_bytes(bytes[..8].try_into().expect(
            "invariant: bounds-checked above (bytes.len() >= CALVIN_APPLIED_PAYLOAD_SIZE >= 8)",
        ));
        let position = u32::from_le_bytes(bytes[8..12].try_into().expect(
            "invariant: bounds-checked above (bytes.len() >= CALVIN_APPLIED_PAYLOAD_SIZE >= 12)",
        ));
        let vshard_id = u32::from_le_bytes(bytes[12..16].try_into().expect(
            "invariant: bounds-checked above (bytes.len() >= CALVIN_APPLIED_PAYLOAD_SIZE >= 16)",
        ));
        Ok(Self {
            epoch,
            position,
            vshard_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let p = CalvinAppliedPayload::new(42, 7, 3);
        let bytes = p.to_bytes();
        let decoded = CalvinAppliedPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.epoch, 42);
        assert_eq!(decoded.position, 7);
        assert_eq!(decoded.vshard_id, 3);
    }

    #[test]
    fn too_short_payload_returns_error() {
        let result = CalvinAppliedPayload::from_bytes(&[0u8; 5]);
        assert!(result.is_err());
    }

    #[test]
    fn payload_size_is_correct() {
        let p = CalvinAppliedPayload::new(u64::MAX, u32::MAX, u32::MAX);
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), CALVIN_APPLIED_PAYLOAD_SIZE);
    }
}
