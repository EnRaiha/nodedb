// SPDX-License-Identifier: Apache-2.0

//! LSN ↔ wall-clock anchor payload.
//!
//! Anchors are written periodically to the WAL so that, during replay, the
//! bitemporal subsystem can reconstruct a stable `system_from_ms` for every
//! LSN via interpolation between the nearest surrounding anchors.
//!
//! Payload layout (fixed 16 bytes, little-endian):
//!
//! ```text
//! ┌─────────┬────────────┐
//! │ lsn u64 │ wall_ms i64│
//! └─────────┴────────────┘
//! ```

use crate::error::{Result, WalError};

/// Size of an anchor payload on disk.
pub const ANCHOR_PAYLOAD_SIZE: usize = 16;

/// LSN ↔ wall-clock milliseconds anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LsnMsAnchorPayload {
    /// WAL LSN at which this anchor was written.
    pub lsn: u64,
    /// Wall-clock milliseconds since Unix epoch at write time.
    pub wall_ms: i64,
}

impl LsnMsAnchorPayload {
    pub const fn new(lsn: u64, wall_ms: i64) -> Self {
        Self { lsn, wall_ms }
    }

    pub fn to_bytes(&self) -> [u8; ANCHOR_PAYLOAD_SIZE] {
        let mut buf = [0u8; ANCHOR_PAYLOAD_SIZE];
        buf[0..8].copy_from_slice(&self.lsn.to_le_bytes());
        buf[8..16].copy_from_slice(&self.wall_ms.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != ANCHOR_PAYLOAD_SIZE {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "LsnMsAnchor payload must be {ANCHOR_PAYLOAD_SIZE} bytes, got {}",
                    buf.len()
                ),
            });
        }
        let lsn = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let wall_ms = i64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Ok(Self { lsn, wall_ms })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_roundtrip() {
        let anchor = LsnMsAnchorPayload::new(12_345, 1_700_000_000_000);
        let bytes = anchor.to_bytes();
        assert_eq!(LsnMsAnchorPayload::from_bytes(&bytes).unwrap(), anchor);
    }

    #[test]
    fn anchor_negative_wall_ms() {
        // Wall-clock predates epoch — rare but permitted by i64 encoding.
        let anchor = LsnMsAnchorPayload::new(0, -1);
        let bytes = anchor.to_bytes();
        assert_eq!(LsnMsAnchorPayload::from_bytes(&bytes).unwrap(), anchor);
    }

    #[test]
    fn anchor_wrong_size_rejected() {
        assert!(LsnMsAnchorPayload::from_bytes(&[0u8; 15]).is_err());
        assert!(LsnMsAnchorPayload::from_bytes(&[0u8; 17]).is_err());
    }
}
