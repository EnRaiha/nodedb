// SPDX-License-Identifier: Apache-2.0

//! Time-anchor payload.
//!
//! The writer appends one `TimeAnchor` record to every group-commit batch. The
//! record's header LSN is the batch's last LSN. The payload is the HLC wall
//! time the batch committed at: 8 bytes, little-endian nanoseconds since the
//! Unix epoch.

use crate::error::{Result, WalError};

/// Size of a time-anchor payload on disk.
pub const TIME_ANCHOR_PAYLOAD_SIZE: usize = 8;

/// Commit time of the batch a `TimeAnchor` record closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeAnchorPayload {
    /// HLC wall component, in nanoseconds since the Unix epoch.
    pub hlc_wall_ns: u64,
}

impl TimeAnchorPayload {
    pub const fn new(hlc_wall_ns: u64) -> Self {
        Self { hlc_wall_ns }
    }

    pub fn to_bytes(&self) -> [u8; TIME_ANCHOR_PAYLOAD_SIZE] {
        self.hlc_wall_ns.to_le_bytes()
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let bytes: [u8; TIME_ANCHOR_PAYLOAD_SIZE] =
            buf.try_into().map_err(|_| WalError::InvalidPayload {
                detail: format!(
                    "TimeAnchor payload must be {TIME_ANCHOR_PAYLOAD_SIZE} bytes, got {}",
                    buf.len()
                ),
            })?;
        Ok(Self::new(u64::from_le_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_roundtrip() {
        let anchor = TimeAnchorPayload::new(1_700_000_000_000_000_123);
        let bytes = anchor.to_bytes();
        assert_eq!(TimeAnchorPayload::from_bytes(&bytes).unwrap(), anchor);
    }

    #[test]
    fn anchor_wrong_size_rejected() {
        assert!(TimeAnchorPayload::from_bytes(&[0u8; 7]).is_err());
        assert!(TimeAnchorPayload::from_bytes(&[0u8; 9]).is_err());
    }
}
